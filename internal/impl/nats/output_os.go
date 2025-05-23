package nats

import (
	"context"
	"sync"

	"github.com/Jeffail/shutdown"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	osoBucketField = "bucket"
	osoNameField   = "object_name"
)

func natsOSOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("1.8.0").
		Summary("Put messages in a NATS object-store bucket.").
		Description(`
The field ` + "`object_name`" + ` supports
[interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing
you to create a unique object name for each message.

` + connectionNameDescription() + authDescription()).
		Fields(Docs("object store", []*service.ConfigField{
			service.NewInterpolatedStringField(osoNameField).
				Description("The object name for each message."),
			service.NewOutputMaxInFlightField().Default(64),
		}...)...)
}

func init() {
	err := service.RegisterOutput(
		"nats_object_store", natsOSOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			o, err := newOSOutput(conf, mgr)
			return o, maxInFlight, err
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type osOutput struct {
	connDetails connectionDetails
	bucket      string
	objName     *service.InterpolatedString

	log *service.Logger

	connMut     sync.Mutex
	natsConn    *nats.Conn
	objectStore jetstream.ObjectStore

	shutSig *shutdown.Signaller
}

func newOSOutput(conf *service.ParsedConfig, mgr *service.Resources) (*osOutput, error) {
	oso := osOutput{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if oso.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if oso.bucket, err = conf.FieldString(osoBucketField); err != nil {
		return nil, err
	}

	if oso.objName, err = conf.FieldInterpolatedString(osoNameField); err != nil {
		return nil, err
	}

	return &oso, nil
}

//------------------------------------------------------------------------------

func (oso *osOutput) Connect(ctx context.Context) (err error) {
	oso.connMut.Lock()
	defer oso.connMut.Unlock()

	if oso.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn

	defer func() {
		if err != nil && natsConn != nil {
			natsConn.Close()
		}
	}()

	if natsConn, err = oso.connDetails.get(ctx); err != nil {
		return err
	}

	jsc, err := jetstream.New(natsConn)
	if err != nil {
		return err
	}

	oso.objectStore, err = jsc.ObjectStore(ctx, oso.bucket)
	oso.natsConn = natsConn

	return nil
}

func (oso *osOutput) Write(ctx context.Context, msg *service.Message) error {
	oso.connMut.Lock()
	objectStore := oso.objectStore
	oso.connMut.Unlock()
	if objectStore == nil {
		return service.ErrNotConnected
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	objn, err := oso.objName.TryString(msg)
	if err != nil {
		return err
	}

	_, err = objectStore.PutBytes(ctx, objn, msgBytes)
	if err != nil {
		return err
	}

	return nil
}

func (oso *osOutput) Close(ctx context.Context) error {
	go func() {
		oso.disconnect()
		oso.shutSig.TriggerHasStopped()
	}()
	select {
	case <-oso.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

//------------------------------------------------------------------------------

func (oso *osOutput) disconnect() {
	oso.connMut.Lock()
	defer oso.connMut.Unlock()

	if oso.natsConn != nil {
		oso.natsConn.Close()
		oso.natsConn = nil
	}
	oso.objectStore = nil
}
