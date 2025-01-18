package zmq4n

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"

	"github.com/warpstreamlabs/bento/public/service"
)

func zmqInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Consumes messages from a ZeroMQ socket.").
		Description(`

This is a native Go implementation of ZeroMQ using the go-zeromq/zmq4 library.
There is a specific docker tag postfix ` + "`-cgo`" + ` for C builds containing the original zmq4 component.`).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"tcp://localhost:5555"})).
		Field(service.NewBoolField("bind").
			Description("Whether to bind to the specified URLs (otherwise they are connected to).").
			Default(false)).
		Field(service.NewStringEnumField("socket_type", "PULL", "SUB").
			Description("The socket type to connect as.")).
		Field(service.NewStringListField("sub_filters").
			Description("A list of subscription topic filters to use when consuming from a SUB socket. Specifying a single sub_filter of `''` will subscribe to everything.").
			Default([]any{})).
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
	_ = service.RegisterBatchInput("zmq4n", zmqInputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := zmqInputFromConfig(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatched(r), nil
	})
}

//------------------------------------------------------------------------------

type zmqInput struct {
	log *service.Logger

	urls        []string
	socketType  string
	hwm         int
	bind        bool
	subFilters  []string
	pollTimeout time.Duration

	socket zmq4.Socket
}

func zmqInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*zmqInput, error) {
	z := zmqInput{
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
	if _, err := getZMQInputType(z.socketType); err != nil {
		return nil, err
	}

	if z.subFilters, err = conf.FieldStringList("sub_filters"); err != nil {
		return nil, err
	}

	if z.socketType == "SUB" && len(z.subFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
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

func getZMQInputType(t string) (zmq4.SocketType, error) {

	switch t {
	case "SUB":
		return zmq4.Sub, nil
	case "PULL":
		return zmq4.Pull, nil
	}
	return zmq4.Pull, errors.New("invalid ZMQ socket type")
}

func (z *zmqInput) Connect(ignored context.Context) (err error) {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQInputType(z.socketType)
	if err != nil {
		return err
	}

	ctx := context.Background()

	var socket zmq4.Socket
	switch t {
	case zmq4.Sub:
		socket = zmq4.NewSub(ctx, zmq4.WithTimeout(z.pollTimeout))
	case zmq4.Pull:
		socket = zmq4.NewPull(ctx, zmq4.WithTimeout(z.pollTimeout))

	}

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()
	if z.hwm > 0 {
		if err = socket.SetOption(zmq4.OptionHWM, z.hwm); err != nil {
			fmt.Printf("Input set hwm to %v error %v\n", z.hwm, err)
			return err

		}
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

	for _, filter := range z.subFilters {
		if err := socket.SetOption(zmq4.OptionSubscribe, filter); err != nil {
			return err
		}
	}

	z.socket = socket
	return nil
}

func (z *zmqInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if z.socket == nil {
		return nil, nil, service.ErrNotConnected
	}

	msg, err := z.socket.Recv()
	if err != nil {
		return nil, nil, err
	}

	var batch service.MessageBatch

	switch msg.Type {
	case zmq4.UsrMsg:
		for _, d := range msg.Frames {
			batch = append(batch, service.NewMessage(d))
		}
	default:
		return nil, nil, errors.New("unsupported message type")
	}

	return batch, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

// CloseAsync shuts down the zmqInput input and stops processing requests.
func (z *zmqInput) Close(ctx context.Context) error {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
	return nil
}
