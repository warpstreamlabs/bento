package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/api"
	"github.com/warpstreamlabs/bento/internal/batch"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/httpserver"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	hsoFieldAddress            = "address"
	hsoFieldPath               = "path"
	hsoFieldStreamPath         = "stream_path"
	hsoFieldWSPath             = "ws_path"
	hsoFieldWSMessageType      = "ws_message_type"
	hsoFieldAllowedVerbs       = "allowed_verbs"
	hsoFieldTimeout            = "timeout"
	hsoFieldCertFile           = "cert_file"
	hsoFieldKeyFile            = "key_file"
	hsoFieldCORS               = "cors"
	hsoFieldCORSEnabled        = "enabled"
	hsoFieldCORSAllowedOrigins = "allowed_origins"
	hsoFieldWriteWait          = "write_wait"
	hsoFieldPongWait           = "pong_wait"
	hsoFieldPingPeriod         = "ping_period"
)

type hsoConfig struct {
	Address       string
	Path          string
	StreamPath    string
	WSPath        string
	WSMessageType string
	AllowedVerbs  map[string]struct{}
	Timeout       time.Duration
	CertFile      string
	KeyFile       string
	CORS          httpserver.CORSConfig
	WriteWait     time.Duration
	PongWait      time.Duration
	PingPeriod    time.Duration
}

func hsoConfigFromParsed(pConf *service.ParsedConfig) (conf hsoConfig, err error) {
	if conf.Address, err = pConf.FieldString(hsoFieldAddress); err != nil {
		return
	}
	if conf.Path, err = pConf.FieldString(hsoFieldPath); err != nil {
		return
	}
	if conf.StreamPath, err = pConf.FieldString(hsoFieldStreamPath); err != nil {
		return
	}
	if conf.WSPath, err = pConf.FieldString(hsoFieldWSPath); err != nil {
		return
	}
	if pConf.Contains(hsoFieldWSMessageType) {
		if conf.WSMessageType, err = pConf.FieldString(hsoFieldWSMessageType); err != nil {
			return
		}
	}
	{
		var verbsList []string
		if verbsList, err = pConf.FieldStringList(hsoFieldAllowedVerbs); err != nil {
			return
		}
		if len(verbsList) == 0 {
			err = errors.New("must specify at least one allowed verb")
			return
		}
		conf.AllowedVerbs = map[string]struct{}{}
		for _, v := range verbsList {
			conf.AllowedVerbs[v] = struct{}{}
		}
	}
	if conf.Timeout, err = pConf.FieldDuration(hsoFieldTimeout); err != nil {
		return
	}
	if conf.CertFile, err = pConf.FieldString(hsoFieldCertFile); err != nil {
		return
	}
	if conf.KeyFile, err = pConf.FieldString(hsoFieldKeyFile); err != nil {
		return
	}
	if conf.CORS, err = corsConfigFromParsed(pConf.Namespace(hsoFieldCORS)); err != nil {
		return
	}
	if conf.WriteWait, err = pConf.FieldDuration(hsoFieldWriteWait); err != nil {
		return
	}
	if conf.PongWait, err = pConf.FieldDuration(hsoFieldPongWait); err != nil {
		return
	}
	if conf.PingPeriod, err = pConf.FieldDuration(hsoFieldPingPeriod); err != nil {
		return
	}
	return
}

func hsoSpec() *service.ConfigSpec {
	corsSpec := httpserver.ServerCORSFieldSpec()
	corsSpec.Description += " Only valid with a custom `address`."

	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary(`Sets up an HTTP server that will send messages over HTTP(S) GET requests. HTTP 2.0 is supported when using TLS, which is enabled when key and cert files are specified.`).
		Description(`Sets up an HTTP server that will send messages over HTTP(S) GET requests. If the `+"`address`"+` config field is left blank the [service-wide HTTP server](/docs/components/http/about) will be used.

Three endpoints will be registered at the paths specified by the fields `+"`path`, `stream_path` and `ws_path`"+`. Which allow you to consume a single message batch, a continuous stream of line delimited messages, or a websocket of messages for each request respectively.

When messages are batched the `+"`path`"+` endpoint encodes the batch according to [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). This behaviour can be overridden by [archiving your batches](/docs/configuration/batching#post-batch-processing).

Please note, messages are considered delivered as soon as the data is written to the client. There is no concept of at least once delivery on this output.

`+api.EndpointCaveats()+`
`).
		Fields(
			service.NewStringField(hsoFieldAddress).
				Description("An alternative address to host from. If left empty the service wide address is used.").
				Default(""),
			service.NewStringField(hsoFieldPath).
				Description("The path from which discrete messages can be consumed.").
				Default("/get"),
			service.NewStringField(hsoFieldStreamPath).
				Description("The path from which a continuous stream of messages can be consumed.").
				Default("/get/stream"),
			service.NewStringField(hsoFieldWSPath).
				Description("The path from which websocket connections can be established.").
				Default("/get/ws"),
			service.NewStringField(hsoFieldWSMessageType).
				Description("Type of websocket message").
				Default("binary"),
			service.NewStringListField(hsoFieldAllowedVerbs).
				Description("An array of verbs that are allowed for the `path` and `stream_path` HTTP endpoint.").
				Default([]any{"GET"}),
			service.NewDurationField(hsoFieldTimeout).
				Description("The maximum time to wait before a blocking, inactive connection is dropped (only applies to the `path` endpoint).").
				Default("5s").
				Advanced(),
			service.NewStringField(hsoFieldCertFile).
				Description("Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").
				Advanced().
				Default(""),
			service.NewStringField(hsoFieldKeyFile).
				Description("Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").
				Advanced().
				Default(""),
			service.NewInternalField(corsSpec),
			service.NewDurationField(hsoFieldWriteWait).
				Description("The time allowed to write a message to the websocket.").
				Default("10s").
				Advanced(),
			service.NewDurationField(hsoFieldPongWait).
				Description("The time allowed to read the next pong message from the client.").
				Default("60s").
				Advanced(),
			service.NewDurationField(hsoFieldPingPeriod).
				Description("Send pings to client with this period. Must be less than pong wait.").
				Default("54s").
				Advanced(),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"http_server", hsoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, pol service.BatchPolicy, mif int, err error) {
			var hsoConf hsoConfig
			if hsoConf, err = hsoConfigFromParsed(conf); err != nil {
				return
			}

			// TODO: If we refactor this input to implement WriteBatch then we
			// can return a proper service.BatchOutput implementation.

			oldMgr := interop.UnwrapManagement(mgr)

			var outStrm output.Streamed
			if outStrm, err = newHTTPServerOutput(hsoConf, oldMgr); err != nil {
				return
			}

			out = interop.NewUnwrapInternalOutput(outStrm)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpServerOutput struct {
	conf hsoConfig
	log  log.Modular
	mgr  bundle.NewManagement

	mux    *mux.Router
	server *http.Server

	transactions <-chan message.Transaction

	mGetSent      metrics.StatCounter
	mGetBatchSent metrics.StatCounter

	mWSSent      metrics.StatCounter
	mWSBatchSent metrics.StatCounter
	mWSError     metrics.StatCounter

	mStreamSent      metrics.StatCounter
	mStreamBatchSent metrics.StatCounter
	mStreamError     metrics.StatCounter

	closeServerOnce sync.Once
	shutSig         *shutdown.Signaller
}

func newHTTPServerOutput(conf hsoConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	var gMux *mux.Router
	var server *http.Server

	var err error
	if conf.Address != "" {
		gMux = mux.NewRouter()
		server = &http.Server{Addr: conf.Address}
		if server.Handler, err = conf.CORS.WrapHandler(gMux); err != nil {
			return nil, fmt.Errorf("bad CORS configuration: %w", err)
		}
	}

	stats := mgr.Metrics()
	mSent := stats.GetCounter("output_sent")
	mBatchSent := stats.GetCounter("output_batch_sent")
	mError := stats.GetCounter("output_error")

	h := httpServerOutput{
		shutSig: shutdown.NewSignaller(),
		conf:    conf,
		log:     mgr.Logger(),
		mgr:     mgr,
		mux:     gMux,
		server:  server,

		mGetSent:      mSent,
		mGetBatchSent: mBatchSent,

		mWSSent:      mSent,
		mWSBatchSent: mBatchSent,
		mWSError:     mError,

		mStreamSent:      mSent,
		mStreamBatchSent: mBatchSent,
		mStreamError:     mError,
	}

	if gMux != nil {
		if h.conf.Path != "" {
			api.GetMuxRoute(gMux, h.conf.Path).HandlerFunc(h.getHandler)
		}
		if h.conf.StreamPath != "" {
			api.GetMuxRoute(gMux, h.conf.StreamPath).HandlerFunc(h.streamHandler)
		}
		if h.conf.WSPath != "" {
			api.GetMuxRoute(gMux, h.conf.WSPath).HandlerFunc(h.wsHandler)
		}
	} else {
		if h.conf.Path != "" {
			mgr.RegisterEndpoint(
				h.conf.Path, "Read a single message from Bento.",
				h.getHandler,
			)
		}
		if h.conf.StreamPath != "" {
			mgr.RegisterEndpoint(
				h.conf.StreamPath,
				"Read a continuous stream of messages from Bento.",
				h.streamHandler,
			)
		}
		if h.conf.WSPath != "" {
			mgr.RegisterEndpoint(
				h.conf.WSPath,
				"Read messages from Bento via websockets.",
				h.wsHandler,
			)
		}
	}

	return &h, nil
}

//------------------------------------------------------------------------------

func (h *httpServerOutput) getHandler(w http.ResponseWriter, r *http.Request) {
	if h.shutSig.IsSoftStopSignalled() {
		http.Error(w, "Server closed", http.StatusServiceUnavailable)
		return
	}

	ctx, done := h.shutSig.SoftStopCtx(r.Context())
	defer done()

	if _, exists := h.conf.AllowedVerbs[r.Method]; !exists {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	tStart := time.Now()

	var ts message.Transaction
	var open bool
	var err error

	select {
	case ts, open = <-h.transactions:
		if !open {
			http.Error(w, "Server closed", http.StatusServiceUnavailable)
			go h.TriggerCloseNow()
			return
		}
	case <-time.After(h.conf.Timeout - time.Since(tStart)):
		http.Error(w, "Timed out waiting for message", http.StatusRequestTimeout)
		return
	}

	if ts.Payload.Len() > 1 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < ts.Payload.Len() && err == nil; i++ {
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(ts.Payload.Get(i).AsBytes()))
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		_, _ = w.Write(body.Bytes())
	} else {
		w.Header().Add("Content-Type", "application/octet-stream")
		_, _ = w.Write(ts.Payload.Get(0).AsBytes())
	}

	h.mGetBatchSent.Incr(1)
	h.mGetSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))

	_ = ts.Ack(ctx, nil)
}

func (h *httpServerOutput) streamHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Server error", http.StatusInternalServerError)
		h.log.Error("Failed to cast response writer to flusher")
		return
	}

	if _, exists := h.conf.AllowedVerbs[r.Method]; !exists {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	ctx, done := h.shutSig.SoftStopCtx(r.Context())
	defer done()

	for !h.shutSig.IsSoftStopSignalled() {
		var ts message.Transaction
		var open bool

		select {
		case ts, open = <-h.transactions:
			if !open {
				go h.TriggerCloseNow()
				return
			}
		case <-r.Context().Done():
			return
		}

		var data []byte
		if ts.Payload.Len() == 1 {
			data = ts.Payload.Get(0).AsBytes()
		} else {
			data = append(bytes.Join(message.GetAllBytes(ts.Payload), []byte("\n")), byte('\n'))
		}

		_, err := w.Write(data)
		_ = ts.Ack(ctx, err)
		if err != nil {
			h.mStreamError.Incr(1)
			return
		}

		_, _ = w.Write([]byte("\n"))
		flusher.Flush()
		h.mStreamSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))
		h.mStreamBatchSent.Incr(1)
	}
}

func (h *httpServerOutput) wsHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warn("WebSocket request failed: %v", err)
			return
		}
	}()

	upgrader := websocket.Upgrader{}

	// Upgrade the HTTP connection to a WebSocket connection
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warn("WebSocket upgrade failed: %v", err)
		return
	}
	defer ws.Close()

	ws.SetReadLimit(512)
	if err := ws.SetReadDeadline(time.Now().Add(h.conf.PongWait)); err != nil {
		h.log.Warn("Failed to set read deadline: %v", err)
		return
	}

	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(h.conf.PongWait))
	})

	// Start a goroutine to read messages (to process control frames)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, _, err := ws.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					h.log.Warn("WebSocket read error: %v", err)
				}
				break
			}
		}
	}()

	// Start ticker to send ping messages to the client periodically
	ticker := time.NewTicker(h.conf.PingPeriod)
	defer ticker.Stop()

	ctx, doneCtx := h.shutSig.SoftStopCtx(r.Context())
	defer doneCtx()

	for !h.shutSig.IsSoftStopSignalled() {
		select {
		case ts, open := <-h.transactions:
			if !open {
				// If the transactions channel is closed, trigger server shutdown
				go h.TriggerCloseNow()
				return
			}
			var msgType int
			switch h.conf.WSMessageType {
			case "text":
				msgType = websocket.TextMessage
			default:
				msgType = websocket.BinaryMessage
			}
			// Write messages to the client
			var writeErr error
			for _, msg := range message.GetAllBytes(ts.Payload) {
				_ = ws.SetWriteDeadline(time.Now().Add(h.conf.WriteWait))
				if writeErr = ws.WriteMessage(msgType, msg); writeErr != nil {
					break
				}
				h.mWSBatchSent.Incr(1)
				h.mWSSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))
			}
			if writeErr != nil {
				h.mWSError.Incr(1)
				_ = ts.Ack(ctx, writeErr)
				return // Exit the loop on write error
			}
			_ = ts.Ack(ctx, nil)
		case <-ticker.C:
			// Send a ping message to the client
			//nolint:errcheck // this function does not actually return an error
			ws.SetWriteDeadline(time.Now().Add(h.conf.WriteWait))
			if err := ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.log.Warn("WebSocket ping error: %v", err)
				return
			}
		case <-done:
			// The read goroutine has exited, indicating the client has disconnected
			h.log.Debug("WebSocket client disconnected")
			return
		case <-ctx.Done():
			// The context has been canceled (e.g., server is shutting down)
			return
		}
	}
}

func (h *httpServerOutput) Consume(ts <-chan message.Transaction) error {
	if h.transactions != nil {
		return component.ErrAlreadyStarted
	}
	h.transactions = ts

	if h.server != nil {
		go func() {
			if h.conf.KeyFile != "" || h.conf.CertFile != "" {
				h.log.Info(
					"Serving messages through HTTPS GET request at: https://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServeTLS(
					h.conf.CertFile, h.conf.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Error("Server error: %v\n", err)
				}
			} else {
				h.log.Info(
					"Serving messages through HTTP GET request at: http://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Error("Server error: %v\n", err)
				}
			}

			h.shutSig.TriggerSoftStop()
			h.shutSig.TriggerHasStopped()
		}()
	}
	return nil
}

func (h *httpServerOutput) ConnectionStatus() component.ConnectionStatuses {
	return component.ConnectionStatuses{
		component.ConnectionActive(h.mgr),
	}
}

func (h *httpServerOutput) TriggerCloseNow() {
	h.shutSig.TriggerHardStop()
	h.closeServerOnce.Do(func() {
		if h.server != nil {
			_ = h.server.Shutdown(context.Background())
		}
		h.shutSig.TriggerHasStopped()
	})
}

func (h *httpServerOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-h.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
