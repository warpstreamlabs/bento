package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/warpstreamlabs/bento/public/service"
)

func etcdWatchFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(etcdKeyField).
			Description("The key or prefix being watched. For prefix watching, options.with_prefix should be `true`"),
		service.NewObjectField(etcdOperationOptions,
			service.NewBoolField(etcdWithPrefixField).
				Description("Whether to watch for events on a prefix.").
				Default(false),
			service.NewBoolField(etcdWatchWithProgressNotifyField).
				Description("Whether to send periodic progress updates every 10 minutes when there is no incoming events.").
				Default(false),
			service.NewBoolField(etcdWatchWithFilterPut).
				Description("Whether to discard PUT events from the watcher.").
				Default(false),
			service.NewBoolField(etcdWatchWithFilterDelete).
				Description("Whether to discard DELETE events from the watcher.").
				Default(false),
			service.NewBoolField(etcdWatchWithCreatedNotifyField).
				Description("Whether to send CREATED notify events to the watcher.").
				Default(false),
			service.NewStringField(etcdWatchWithRangeField).
				Description("Will cause the watcher to return a range of lexicographically sorted keys to return in the form `[key, end)` where `end` is the passed parameter.").
				Default(""),
		).Description("Collection of options to configure an etcd watcher."),
	}
}

var description = `
Watches an etcd key or prefix for new events.

From the [etcd docs](https://etcd.io/docs/v3.5/learning/api/#watch-api):
> The Watch API provides an event-based interface for asynchronously monitoring changes to keys.
> An etcd watch waits for changes to keys by continuously watching from a given revision,
> either current or historical, and streams key updates back to the client.
>
> Watches are long-running requests and use gRPC streams to stream event data.
> A watch stream is bi-directional; the client writes to the stream to establish watches and reads to receive watch events.
> A single watch stream can multiplex many distinct watches by tagging events with per-watch identifiers. 
> This multiplexing helps reducing the memory footprint and connection overhead on the core etcd cluster.

### Events

Each event object is flattened and returned as an array, with each individual event in the form:

` + "```json" + `
{
  "key": "<string or []byte>",
  "value": "<string or []byte>",
  "type": "<'PUT' or 'DELETE'>",
  "version": "<int64>",
  "mod_revision": "<int64>",
  "create_revision": "<int64>",
  "lease": "<int64>"
}
` + "```" + `

Where a ` + "`key`" + ` or ` + "`value`" + ` is only a string if it is valid UTF-8.
`

func etcdConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("1.2.0").
		Summary("Configures an etcd event watcher on a key or prefix.").
		Description(description).
		Fields(etcdClientFields()...).
		Fields(etcdWatchFields()...).
		Field(service.NewAutoRetryNacksToggleField())

	return spec
}

func newEtcdWatchInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	reader, err := newEtcdWatchInputFromConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	return service.AutoRetryNacksToggled(conf, reader)
}

func init() {
	err := service.RegisterInput("etcd", etcdConfigSpec(), newEtcdWatchInput)
	if err != nil {
		panic(err)
	}
}

type etcdWatchInput struct {
	watchKey string // TODO: Potentially allow multiple keys/prefixes to be watched (multiplexing)

	client     *clientv3.Client
	clientConf *clientv3.Config

	watchCh      clientv3.WatchChan
	watchOptions []clientv3.OpOption
}

func getWatchOptionsFromConfig(parsedConf *service.ParsedConfig) ([]clientv3.OpOption, error) {
	var opts []clientv3.OpOption

	shouldAddToWatchOptions := func(should bool, option clientv3.OpOption) {
		if should {
			opts = append(opts, option)
		}
	}

	withPrefix, err := parsedConf.FieldBool(etcdOperationOptions, etcdWithPrefixField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withPrefix, clientv3.WithPrefix())

	withProgressNotify, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithProgressNotifyField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withProgressNotify, clientv3.WithProgressNotify())

	withCreatedNotify, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithCreatedNotifyField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withCreatedNotify, clientv3.WithCreatedNotify())

	withFilterPut, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithFilterPut)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withFilterPut, clientv3.WithFilterPut())

	withFilterDelete, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithFilterDelete)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withFilterDelete, clientv3.WithFilterDelete())

	withRange, err := parsedConf.FieldString(etcdOperationOptions, etcdWatchWithRangeField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withRange != "", clientv3.WithRange(withRange))

	return opts, nil
}

func newEtcdWatchInputFromConfig(parsedConf *service.ParsedConfig, mgr *service.Resources) (*etcdWatchInput, error) {
	config, err := newEtcdConfigFromParsed(parsedConf)
	if err != nil {
		return nil, err
	}

	watchKey, err := parsedConf.FieldString(etcdKeyField)
	if err != nil {
		return nil, err
	}

	opts, err := getWatchOptionsFromConfig(parsedConf)
	if err != nil {
		return nil, err
	}

	return &etcdWatchInput{
		clientConf:   config,
		watchKey:     watchKey,
		watchOptions: opts,
	}, nil
}

func (e *etcdWatchInput) Connect(ctx context.Context) error {
	client, err := newEtcdClientFromConfig(ctx, e.clientConf)
	if err != nil {
		return err
	}

	e.client = client
	e.watchCh = clientv3.NewWatcher(e.client).Watch(ctx, e.watchKey, e.watchOptions...)

	return nil
}

func (e *etcdWatchInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case resp, open := <-e.watchCh:
		if err := resp.Err(); err != nil {
			return nil, nil, err
		}

		if resp.Canceled {
			return nil, nil, service.ErrEndOfInput
		}

		if !open {
			return nil, nil, service.ErrNotConnected
		}

		msg := service.NewMessage(nil)
		msg.SetStructured(etcdEventsToMap(resp.Events))

		return msg, func(ctx context.Context, err error) error {
			return err
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (e *etcdWatchInput) Close(ctx context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}

	return nil
}
