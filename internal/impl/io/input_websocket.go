package io

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/input/config"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

type wsOpenMsgType string

const (
	// wsOpenMsgTypeBinary sets the type of open_message to binary.
	wsOpenMsgTypeBinary wsOpenMsgType = "binary"
	// wsOpenMsgTypeText sets the type of open_message to text (UTF-8 encoded text data).
	wsOpenMsgTypeText wsOpenMsgType = "text"
)

func websocketInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Connects to a websocket server and continuously receives messages.").
		Description(`It is possible to configure a list of `+"`open_messages`"+`, which when set will be sent to the websocket server each time a connection is first established.`).
		Fields(
			service.NewURLField("url").
				Description("The URL to connect to.").
				Example("ws://localhost:4195/get/ws"),
			service.NewURLField("proxy_url").
				Description("An optional HTTP proxy URL.").
				Advanced().Optional(),
			service.NewInterpolatedStringMapField("headers").
				Description("A map of custom headers to add to the websocket handshake.").
				Example(map[string]any{
					"Sec-WebSocket-Protocol": "graphql-ws",
					"User-Agent":             `${! uuid_v4() }`,
					"X-Client-ID":            `${CLIENT_ID}`,
				}).
				Advanced().Optional().
				Default(map[string]any{}),
			service.NewStringField("open_message").
				Description("An optional message to send to the server upon connection.").
				Advanced().Optional().Deprecated(),
			service.NewStringListField("open_messages").
				Description("An optional list of messages to send to the server upon connection. This field replaces `open_message`, which will be removed in a future version.").
				Advanced().Optional(),
			service.NewStringAnnotatedEnumField("open_message_type", map[string]string{
				string(wsOpenMsgTypeBinary): "Binary data open_message.",
				string(wsOpenMsgTypeText):   "Text data open_message. The text message payload is interpreted as UTF-8 encoded text data.",
			}).Description("An optional flag to indicate the data type of open_message.").
				Advanced().Default(string(wsOpenMsgTypeBinary)),
			service.NewAutoRetryNacksToggleField(),
			service.NewTLSToggledField("tls"),
		).
		LintRule(`root = match {this.exists("open_message") && this.open_messages != [] => "both open_message and open_messages cannot be set"}`).
		Fields(config.AsyncOptsFields()...).
		Fields(service.NewHTTPRequestAuthSignerFields()...)
}

func init() {
	err := service.RegisterBatchInput(
		"websocket", websocketInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (in service.BatchInput, err error) {
			oldMgr := interop.UnwrapManagement(mgr)
			var r input.Async
			if r, err = newWebsocketReaderFromParsed(conf, oldMgr); err != nil {
				return
			}

			var opts []func(*input.AsyncReader)
			if opts, err = config.AsyncOptsFromParsed(conf); err != nil {
				return
			}

			if autoRetry, _ := conf.FieldBool(service.AutoRetryNacksToggleFieldName); autoRetry {
				r = input.NewAsyncPreserver(r)
			}

			var i input.Streamed
			if i, err = input.NewAsyncReader("websocket", r, oldMgr, opts...); err != nil {
				return
			}
			in = interop.NewUnwrapInternalInput(i)
			return
		})
	if err != nil {
		panic(err)
	}
}

type websocketReader struct {
	log log.Modular
	mgr bundle.NewManagement

	lock *sync.Mutex

	client         *websocket.Conn
	urlParsed      *url.URL
	urlStr         string
	proxyURLParsed *url.URL
	tlsEnabled     bool
	tlsConf        *tls.Config
	reqSigner      func(f fs.FS, req *http.Request) error

	openMsgType wsOpenMsgType
	openMsg     [][]byte
	headers     map[string]*service.InterpolatedString
}

func newWebsocketReaderFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (*websocketReader, error) {
	ws := &websocketReader{
		log:  mgr.Logger(),
		mgr:  mgr,
		lock: &sync.Mutex{},
	}
	var err error
	if ws.urlParsed, err = conf.FieldURL("url"); err != nil {
		return nil, err
	}
	if ws.urlStr, err = conf.FieldString("url"); err != nil {
		return nil, err
	}
	if conf.Contains("proxy_url") {
		if ws.proxyURLParsed, err = conf.FieldURL("proxy_url"); err != nil {
			return nil, err
		}
	}
	if ws.tlsConf, ws.tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return nil, err
	}
	if ws.reqSigner, err = conf.HTTPRequestAuthSignerFromParsed(); err != nil {
		return nil, err
	}
	if ws.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}
	var openMsgStr, openMsgTypeStr string
	var openMsgsStr []string
	if openMsgTypeStr, err = conf.FieldString("open_message_type"); err != nil {
		return nil, err
	}
	ws.openMsgType = wsOpenMsgType(openMsgTypeStr)
	if openMsgsStr, _ = conf.FieldStringList("open_messages"); len(openMsgsStr) > 0 {
		ws.openMsg = make([][]byte, len(openMsgsStr))
		for i, msg := range openMsgsStr {
			ws.openMsg[i] = []byte(msg)
		}
	} else if openMsgStr, _ = conf.FieldString("open_message"); openMsgStr != "" {
		ws.openMsg = make([][]byte, 1)
		ws.openMsg[0] = []byte(openMsgStr)
	}
	return ws, nil
}

func (w *websocketReader) getWS() *websocket.Conn {
	w.lock.Lock()
	ws := w.client
	w.lock.Unlock()
	return ws
}

func (w *websocketReader) Connect(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.client != nil {
		return nil
	}

	headers := http.Header{}
	for k, v := range w.headers {
		value, err := v.TryString(service.NewMessage(nil))
		if err != nil {
			return fmt.Errorf(`failed string interpolation on header %q: %w`, k, err)
		}
		headers.Add(k, value)
	}

	err := w.reqSigner(w.mgr.FS(), &http.Request{
		URL:    w.urlParsed,
		Header: headers,
	})
	if err != nil {
		return err
	}

	var (
		client *websocket.Conn
		res    *http.Response
	)

	defer func() {
		if res != nil {
			res.Body.Close()
		}
	}()

	dialer := *websocket.DefaultDialer
	if w.proxyURLParsed != nil {
		dialer.Proxy = http.ProxyURL(w.proxyURLParsed)
	}

	if w.tlsEnabled {
		dialer.TLSClientConfig = w.tlsConf
		if client, res, err = dialer.Dial(w.urlStr, headers); err != nil {
			return err
		}
	} else if client, res, err = dialer.Dial(w.urlStr, headers); err != nil {
		return err
	}

	var openMsgType int
	switch w.openMsgType {
	case wsOpenMsgTypeBinary:
		openMsgType = websocket.BinaryMessage
	case wsOpenMsgTypeText:
		openMsgType = websocket.TextMessage
	default:
		return fmt.Errorf("unrecognised open_message_type: %s", w.openMsgType)
	}

	for _, msg := range w.openMsg {
		if err := client.WriteMessage(openMsgType, msg); err != nil {
			return err
		}
	}

	w.client = client
	return nil
}

func (w *websocketReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	client := w.getWS()
	if client == nil {
		return nil, nil, component.ErrNotConnected
	}

	_, data, err := client.ReadMessage()
	if err != nil {
		w.lock.Lock()
		w.client = nil
		w.lock.Unlock()
		err = component.ErrNotConnected
		return nil, nil, err
	}

	return message.QuickBatch([][]byte{data}), func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (w *websocketReader) Close(ctx context.Context) (err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.client != nil {
		err = w.client.Close()
		w.client = nil
	}
	return
}
