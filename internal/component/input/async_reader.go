package input

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/v1/internal/component"
	"github.com/warpstreamlabs/bento/v1/internal/message"
	"github.com/warpstreamlabs/bento/v1/internal/tracing"
)

// AsyncReader is an input implementation that reads messages from an
// input.Async component.
type AsyncReader struct {
	connected   int32
	connBackoff backoff.BackOff
	readBackoff backoff.BackOff

	typeStr string
	reader  Async

	mgr component.Observability

	transactions chan message.Transaction
	shutSig      *shutdown.Signaller
}

// NewAsyncReader creates a new AsyncReader input type.
func NewAsyncReader(
	typeStr string,
	r Async,
	mgr component.Observability,
	opts ...func(a *AsyncReader),
) (Streamed, error) {
	connBoff := backoff.NewExponentialBackOff()
	connBoff.InitialInterval = time.Millisecond * 100
	connBoff.MaxInterval = time.Second
	connBoff.MaxElapsedTime = 0

	readBoff := backoff.NewExponentialBackOff()
	readBoff.InitialInterval = time.Millisecond * 100
	readBoff.MaxInterval = time.Second
	readBoff.MaxElapsedTime = 0

	rdr := &AsyncReader{
		connBackoff:  connBoff,
		readBackoff:  readBoff,
		typeStr:      typeStr,
		reader:       r,
		mgr:          mgr,
		transactions: make(chan message.Transaction),
		shutSig:      shutdown.NewSignaller(),
	}
	for _, opt := range opts {
		opt(rdr)
	}

	go rdr.loop()
	return rdr, nil
}

// AsyncReaderWithConnBackOff set the backoff used for limiting connection
// attempts. If the maximum number of retry attempts is reached then the input
// will gracefully stop.
func AsyncReaderWithConnBackOff(boff backoff.BackOff) func(a *AsyncReader) {
	return func(a *AsyncReader) {
		a.connBackoff = boff
	}
}

//------------------------------------------------------------------------------

func (r *AsyncReader) loop() {
	// Metrics paths
	var (
		mRcvd       = r.mgr.Metrics().GetCounter("input_received")
		mConn       = r.mgr.Metrics().GetCounter("input_connection_up")
		mFailedConn = r.mgr.Metrics().GetCounter("input_connection_failed")
		mLostConn   = r.mgr.Metrics().GetCounter("input_connection_lost")
		mLatency    = r.mgr.Metrics().GetTimer("input_latency_ns")

		traceName = "input_" + r.typeStr
	)

	closeAtLeisureCtx, calDone := r.shutSig.SoftStopCtx(context.Background())
	defer calDone()

	closeNowCtx, cnDone := r.shutSig.HardStopCtx(context.Background())
	defer cnDone()

	defer func() {
		_ = r.reader.Close(context.Background())

		atomic.StoreInt32(&r.connected, 0)

		close(r.transactions)
		r.shutSig.TriggerHasStopped()
	}()

	pendingAcks := sync.WaitGroup{}
	defer func() {
		r.mgr.Logger().Debug("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		r.mgr.Logger().Debug("Pending acks resolved.")
	}()

	initConnection := func() bool {
		for {
			if r.shutSig.IsSoftStopSignalled() {
				return false
			}
			if err := r.reader.Connect(closeAtLeisureCtx); err != nil {
				if r.shutSig.IsSoftStopSignalled() || errors.Is(err, component.ErrTypeClosed) {
					return false
				}
				r.mgr.Logger().Error("Failed to connect to %v: %v\n", r.typeStr, err)
				mFailedConn.Incr(1)

				var nextBoff time.Duration

				var e *component.ErrBackOff
				if errors.As(err, &e) {
					nextBoff = e.Wait
				} else {
					nextBoff = r.connBackoff.NextBackOff()
				}

				if nextBoff == backoff.Stop {
					r.mgr.Logger().Error("Maximum number of connection attempt retries has been met, gracefully terminating input %v", r.typeStr)
					return false
				}
				if sleepWithCancellation(closeAtLeisureCtx, nextBoff) != nil {
					return false
				}
			} else {
				r.connBackoff.Reset()
				return true
			}
		}
	}
	if !initConnection() {
		return
	}

	r.mgr.Logger().Info("Input type %v is now active", r.typeStr)
	mConn.Incr(1)
	atomic.StoreInt32(&r.connected, 1)

	for {
		msg, ackFn, err := r.reader.ReadBatch(closeAtLeisureCtx)

		// If our reader says it is not connected.
		if errors.Is(err, component.ErrNotConnected) {
			mLostConn.Incr(1)
			atomic.StoreInt32(&r.connected, 0)

			// Continue to try to reconnect while still active.
			if !initConnection() {
				return
			}
			mConn.Incr(1)
			atomic.StoreInt32(&r.connected, 1)
			continue
		}

		// Close immediately if our reader is closed.
		if r.shutSig.IsSoftStopSignalled() || errors.Is(err, component.ErrTypeClosed) {
			return
		}

		if err != nil || len(msg) == 0 {
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, component.ErrTimeout) && !errors.Is(err, component.ErrNotConnected) {
				r.mgr.Logger().Error("Failed to read message: %v\n", err)
			}

			nextBoff := r.readBackoff.NextBackOff()
			if nextBoff == backoff.Stop {
				r.mgr.Logger().Error("Maximum number of read attempt retries has been met, gracefully terminating input %v", r.typeStr)
				return
			}
			select {
			case <-time.After(nextBoff):
			case <-r.shutSig.SoftStopChan():
				return
			}
			continue
		}

		r.readBackoff.Reset()
		mRcvd.Incr(int64(msg.Len()))
		r.mgr.Logger().Trace("Consumed %v messages from '%v'.\n", msg.Len(), r.typeStr)

		startedAt := time.Now()

		resChan := make(chan error, 1)
		tracing.InitSpans(r.mgr.Tracer(), traceName, msg)
		select {
		case r.transactions <- message.NewTransaction(msg, resChan):
		case <-r.shutSig.SoftStopChan():
			return
		}

		pendingAcks.Add(1)
		go func(
			m message.Batch,
			aFn AsyncAckFn,
			rChan chan error,
		) {
			defer pendingAcks.Done()

			var res error
			select {
			case res = <-rChan:
			case <-r.shutSig.HardStopChan():
				// Even if the pipeline is terminating we still want to attempt
				// to propagate an acknowledgement from in-transit messages.
				return
			}

			mLatency.Timing(time.Since(startedAt).Nanoseconds())
			tracing.FinishSpans(m)

			if err = aFn(closeNowCtx, res); err != nil {
				r.mgr.Logger().Error("Failed to acknowledge message: %v\n", err)
			}
		}(msg, ackFn, resChan)
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *AsyncReader) TransactionChan() <-chan message.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *AsyncReader) Connected() bool {
	return atomic.LoadInt32(&r.connected) == 1
}

// TriggerStopConsuming instructs the input to start shutting down resources
// once all pending messages are delivered and acknowledged. This call does
// not block.
func (r *AsyncReader) TriggerStopConsuming() {
	r.shutSig.TriggerSoftStop()
}

// TriggerCloseNow triggers the shut down of this component but should not block
// the calling goroutine.
func (r *AsyncReader) TriggerCloseNow() {
	r.shutSig.TriggerHardStop()
}

// WaitForClose is a blocking call to wait until the component has finished
// shutting down and cleaning up resources.
func (r *AsyncReader) WaitForClose(ctx context.Context) error {
	select {
	case <-r.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func sleepWithCancellation(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
