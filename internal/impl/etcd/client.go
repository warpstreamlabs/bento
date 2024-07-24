package etcd

import (
	"context"
	"errors"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/warpstreamlabs/bento/public/service"
)

func etcdClientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLListField(etcdEndpointsField).
			Description("A set of URLs (schemes, hosts and ports only) that can be used to communicate with a logical etcd cluster. If multiple endpoints are provided, the Client will attempt to use them all in the event that one or more of them are unusable.").
			Examples(
				[]string{"etcd://:2379"},
				[]string{"etcd://localhost:2379"},
				[]string{"etcd://localhost:2379", "etcd://localhost:22379", "etcd://localhost:32379"},
			),
		service.NewObjectField(etcdAuthField,
			service.NewBoolField(etcdAuthEnabledField).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(etcdAuthUsernameField).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(etcdAuthPasswordField).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of etcd authentication headers.").
			Optional().
			Advanced(),
		service.NewDurationField(etcdDialTimeoutField).
			Description("Timeout for failing to establish a connection.").
			Optional().
			Default("5s").
			Advanced(),
		service.NewDurationField(etcdKeepAliveTimeField).
			Description("Time after which client pings the server to see if transport is alive.").
			Optional().
			Default("5s").
			Advanced(),
		service.NewDurationField(etcdKeepAliveTimeoutField).
			Description("Time that the client waits for a response for the keep-alive probe. If the response is not received in this time, the connection is closed.").
			Optional().
			Default("1s").
			Advanced(),
		service.NewDurationField(etcdRequestTimeoutField).
			Description("Timeout for a single request. This includes connection time, any redirects, and header wait time.").
			Optional().
			Default("1s").
			Advanced(),
		service.NewTLSToggledField(etcdTLSField).
			Description("Custom TLS settings can be used to override system defaults.").
			Advanced(),
		service.NewDurationField(etcdAutoSyncIntervalField).
			Description("The interval to update endpoints with its latest members. 0 disables auto-sync. By default auto-sync is disabled.").
			Optional(),
		service.NewIntField(etcdMaxCallSendMsgSizeField).
			Description("The client-side request send limit in bytes. If 0, it defaults to 2.0 MiB (2 * 1024 * 1024).").
			Optional().
			Advanced(),
		service.NewIntField(etcdMaxCallRecvMsgSizeField).
			Description("The client-side response receive limit. If 0, it defaults to math.MaxInt32.").
			Optional().
			Advanced(),
		service.NewBoolField(etcdRejectOldClusterField).
			Description("When set, will refuse to create a client against an outdated cluster.").
			Default(false).
			Advanced(),
		service.NewBoolField(etcdPermitWithoutStreamField).
			Description("When set, will allow client to send keepalive pings to server without any active streams (RPCs).").
			Default(false).
			Advanced(),
		service.NewIntField(etcdMaxUnaryRetriesField).
			Description("The maximum number of retries for unary RPCs.").
			Optional().
			Advanced(),
		service.NewDurationField(etcdBackoffWaitBetweenField).
			Description("The wait time before retrying an RPC.").
			Optional().
			Advanced(),
		service.NewFloatField(etcdBackoffJitterFractionField).
			Description("The jitter fraction to randomize backoff wait time.").
			Optional().
			Advanced(),
	}
}

func newEtcdClientFromConfig(ctx context.Context, cfg *clientv3.Config) (*clientv3.Client, error) {
	if cfg == nil {
		return nil, errors.New("etcd config cannot be nil")
	}

	cfg.Context = ctx

	client, err := clientv3.New(*cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func newEtcdConfigFromParsed(parsedConf *service.ParsedConfig) (*clientv3.Config, error) {
	var cfg clientv3.Config

	endpointStrs, err := parsedConf.FieldStringList(etcdEndpointsField)
	if err != nil {
		return nil, err
	}
	if len(endpointStrs) == 0 {
		return nil, errors.New("must specify at least one URL")
	}
	for _, u := range endpointStrs {
		for _, splitURL := range strings.Split(u, ",") {
			if trimmed := strings.TrimSpace(splitURL); trimmed != "" {
				cfg.Endpoints = append(cfg.Endpoints, trimmed)
			}
		}
	}

	if cfg.DialTimeout, err = parsedConf.FieldDuration(etcdDialTimeoutField); err != nil {
		return nil, err
	}

	if cfg.DialKeepAliveTime, err = parsedConf.FieldDuration(etcdKeepAliveTimeField); err != nil {
		return nil, err
	}

	if cfg.DialKeepAliveTimeout, err = parsedConf.FieldDuration(etcdKeepAliveTimeoutField); err != nil {
		return nil, err
	}

	if parsedConf.Contains(etcdAutoSyncIntervalField) {
		if cfg.AutoSyncInterval, err = parsedConf.FieldDuration(etcdAutoSyncIntervalField); err != nil {
			return nil, err
		}
	}

	if parsedConf.Contains(etcdMaxCallSendMsgSizeField) {
		if cfg.MaxCallSendMsgSize, err = parsedConf.FieldInt(etcdMaxCallSendMsgSizeField); err != nil {
			return nil, err
		}
	}

	if parsedConf.Contains(etcdMaxCallRecvMsgSizeField) {
		if cfg.MaxCallRecvMsgSize, err = parsedConf.FieldInt(etcdMaxCallRecvMsgSizeField); err != nil {
			return nil, err
		}
	}

	if cfg.RejectOldCluster, err = parsedConf.FieldBool(etcdRejectOldClusterField); err != nil {
		return nil, err
	}

	if cfg.PermitWithoutStream, err = parsedConf.FieldBool(etcdPermitWithoutStreamField); err != nil {
		return nil, err
	}

	if parsedConf.Contains(etcdMaxUnaryRetriesField) {
		if maxUnaryRetries, err := parsedConf.FieldInt(etcdMaxUnaryRetriesField); err != nil {
			return nil, err
		} else {
			cfg.MaxUnaryRetries = uint(maxUnaryRetries)
		}
	}

	if parsedConf.Contains(etcdBackoffWaitBetweenField) {
		if cfg.BackoffWaitBetween, err = parsedConf.FieldDuration(etcdBackoffWaitBetweenField); err != nil {
			return nil, err
		}
	}

	if parsedConf.Contains(etcdBackoffJitterFractionField) {
		if backoffJitterFraction, err := parsedConf.FieldFloat(etcdBackoffJitterFractionField); err != nil {
			return nil, err
		} else {
			cfg.BackoffJitterFraction = float64(backoffJitterFraction)
		}
	}

	tlsConf, tlsEnabled, err := parsedConf.FieldTLSToggled(etcdTLSField)
	if err != nil {
		return nil, err
	}

	if tlsEnabled {
		cfg.TLS = tlsConf
	}

	if parsedConf.Contains(etcdAuthField) {
		var authEnabled bool
		authConf := parsedConf.Namespace(etcdAuthField)
		if authEnabled, err = authConf.FieldBool(etcdAuthEnabledField); err != nil {
			return nil, err
		}

		if authEnabled {
			if cfg.Username, err = authConf.FieldString(etcdAuthUsernameField); err != nil {
				return nil, err
			}
			if cfg.Password, err = authConf.FieldString(etcdAuthPasswordField); err != nil {
				return nil, err
			}
		}

	}

	return &cfg, nil

}
