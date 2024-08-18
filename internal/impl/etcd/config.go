package etcd

// etcd client configuration
const (
	etcdEndpointsField   = "endpoints"
	etcdTLSField         = "tls"
	etcdOperationOptions = "options"

	// Auth
	etcdAuthField         = "auth"
	etcdAuthEnabledField  = "enabled"
	etcdAuthUsernameField = "username"
	etcdAuthPasswordField = "password"

	// Timeouts
	etcdDialTimeoutField      = "dial_timeout"
	etcdKeepAliveTimeField    = "keep_alive_time"
	etcdKeepAliveTimeoutField = "keep_alive_timeout"
	etcdRequestTimeoutField   = "request_timeout"

	etcdAutoSyncIntervalField = "auto_sync_interval"

	etcdMaxCallSendMsgSizeField = "max_call_send_msg_size"
	etcdMaxCallRecvMsgSizeField = "max_call_recv_msg_size"
	etcdMaxUnaryRetriesField    = "max_unary_retries"

	etcdRejectOldClusterField    = "reject_old_cluster"
	etcdPermitWithoutStreamField = "permit_without_stream"

	etcdBackoffWaitBetweenField    = "backoff_wait_between"
	etcdBackoffJitterFractionField = "backoff_jitter_fraction"
)

// etcd common options
const (
	etcdKeyField = "key"
)

// etcd input configuration
const (
	etcdWithPrefixField              = "with_prefix"
	etcdWatchWithProgressNotifyField = "with_progress_notify"
	etcdWatchWithCreatedNotifyField  = "with_created_notify"
	etcdWatchWithFilterPut           = "with_put_filter"
	etcdWatchWithFilterDelete        = "with_delete_filter"
	etcdWatchWithRangeField          = "with_range"
)
