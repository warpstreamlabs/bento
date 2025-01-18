package zmq4n

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"

	"github.com/warpstreamlabs/bento/public/service"
)

func zmqOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Writes messages to a ZeroMQ socket.").
		Description(`

This is a native Go implementation of ZeroMQ using the go-zeromq/zmq4 library.
There is a specific docker tag postfix ` + "`-cgo`" + ` for C builds containing the original zmq4 component.`).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"tcp://localhost:5556"})).
		Field(service.NewBoolField("bind").
			Description("Whether to bind to the specified URLs (otherwise they are connected to).").
			Default(true)).
		Field(service.NewStringEnumField("socket_type", "PUSH", "PUB").
			Description("The socket type to connect as.")).
		Field(service.NewIntField("high_water_mark").
			Description("The message high water mark to use. (experimental i go-zeromq)").
			Default(0).
			Advanced()).
		Field(service.NewDurationField("poll_timeout").
			Description("The poll timeout to use.").
			Default("5s").
			Advanced())
}

func init() {
	_ = service.RegisterBatchOutput("zmq4n", zmqOutputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
		w, err := zmqOutputFromConfig(conf, mgr)
		if err != nil {
			return nil, service.BatchPolicy{}, 1, err
		}
		return w, service.BatchPolicy{}, 1, nil
	})
}

//------------------------------------------------------------------------------

// zmqOutput is an output type that writes zmqOutput messages.
type zmqOutput struct {
	log *service.Logger

	urls        []string
	socketType  string
	hwm         int
	bind        bool
	pollTimeout time.Duration

	socket zmq4.Socket
}

func zmqOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*zmqOutput, error) {
	z := zmqOutput{
		log: mgr.Logger(),
	}

	urlStrs, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}

	for _, u := range urlStrs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
	}

	if z.bind, err = conf.FieldBool("bind"); err != nil {
		return nil, err
	}
	if z.socketType, err = conf.FieldString("socket_type"); err != nil {
		return nil, err
	}
	if _, err = getZMQOutputType(z.socketType); err != nil {
		return nil, err
	}

	if z.hwm, err = conf.FieldInt("high_water_mark"); err != nil {
		return nil, err
	}

	if z.pollTimeout, err = conf.FieldDuration("poll_timeout"); err != nil {
		return nil, err
	}

	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQOutputType(t string) (zmq4.SocketType, error) {
	switch t {
	case "PUB":
		return zmq4.Pub, nil
	case "PUSH":
		return zmq4.Push, nil
	}
	return zmq4.Push, errors.New("invalid ZMQ socket type")
}

//------------------------------------------------------------------------------

func (z *zmqOutput) Connect(_ context.Context) (err error) {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQOutputType(z.socketType)
	if err != nil {
		return err
	}

	ctx := context.Background()

	var socket zmq4.Socket
	switch t {
	case zmq4.Pub:
		socket = zmq4.NewPub(ctx, zmq4.WithTimeout(z.pollTimeout))
	case zmq4.Push:
		socket = zmq4.NewPush(ctx, zmq4.WithTimeout(z.pollTimeout))
	}

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	if err = socket.SetOption(zmq4.OptionHWM, z.hwm); err != nil {
		return err
	}

	for _, address := range z.urls {
		if z.bind {
			err = socket.Listen(address)
		} else {
			err = socket.Dial(address)
		}

		if err != nil {
			return err
		}
	}

	z.socket = socket
	return nil
}

func (z *zmqOutput) WriteBatch(_ context.Context, batch service.MessageBatch) error {
	if z.socket == nil {
		return service.ErrNotConnected
	}

	var parts [][]byte
	for _, m := range batch {
		b, err := m.AsBytes()

		if err != nil {
			return err
		}
		parts = append(parts, b)
	}
	msg := zmq4.NewMsgFrom(parts...)
	err := z.socket.Send(msg)

	return err
}

func (z *zmqOutput) Close(ctx context.Context) error {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
	return nil
}
