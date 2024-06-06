package io

import (
	"context"
	"crypto/tls"
	"errors"
	"io/fs"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

func websocketOutputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Sends messages to an HTTP server via a websocket connection.").
		Field(service.NewURLField("url").Description("The URL to connect to.")).
		Field(service.NewTLSToggledField("tls"))

	for _, f := range service.NewHTTPRequestAuthSignerFields() {
		spec = spec.Field(f)
	}

	return spec
}

func init() {
	err := service.RegisterBatchOutput(
		"websocket", websocketOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1
			oldMgr := interop.UnwrapManagement(mgr)
			var w *websocketWriter
			if w, err = newWebsocketWriterFromParsed(conf, oldMgr); err != nil {
				return
			}
			var o output.Streamed
			if o, err = output.NewAsyncWriter("websocket", 1, w, oldMgr); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(o)
			return
		})
	if err != nil {
		panic(err)
	}
}

type websocketWriter struct {
	log log.Modular
	mgr bundle.NewManagement

	lock *sync.Mutex

	client     *websocket.Conn
	urlParsed  *url.URL
	urlStr     string
	tlsEnabled bool
	tlsConf    *tls.Config
	reqSigner  func(f fs.FS, req *http.Request) error
}

func newWebsocketWriterFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (*websocketWriter, error) {
	ws := &websocketWriter{
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
	if ws.tlsConf, ws.tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return nil, err
	}
	if ws.reqSigner, err = conf.HTTPRequestAuthSignerFromParsed(); err != nil {
		return nil, err
	}
	return ws, nil
}

func (w *websocketWriter) getWS() *websocket.Conn {
	w.lock.Lock()
	ws := w.client
	w.lock.Unlock()
	return ws
}

func (w *websocketWriter) Connect(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.client != nil {
		return nil
	}

	headers := http.Header{}

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

	if w.tlsEnabled {
		dialer := websocket.Dialer{
			TLSClientConfig: w.tlsConf,
		}
		if client, res, err = dialer.Dial(w.urlStr, headers); err != nil {
			return err
		}
	} else if client, res, err = websocket.DefaultDialer.Dial(w.urlStr, headers); err != nil {
		return err
	}

	go func(c *websocket.Conn) {
		for {
			if _, _, cerr := c.NextReader(); cerr != nil {
				c.Close()
				break
			}
		}
	}(client)

	w.client = client
	return nil
}

func (w *websocketWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	client := w.getWS()
	if client == nil {
		return component.ErrNotConnected
	}

	err := msg.Iter(func(i int, p *message.Part) error {
		return client.WriteMessage(websocket.BinaryMessage, p.AsBytes())
	})
	if err != nil {
		w.lock.Lock()
		w.client = nil
		w.lock.Unlock()
		if errors.Is(err, websocket.ErrCloseSent) {
			return component.ErrNotConnected
		}
		return err
	}
	return nil
}

func (w *websocketWriter) Close(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	var err error
	if w.client != nil {
		err = w.client.Close()
		w.client = nil
	}
	return err
}
