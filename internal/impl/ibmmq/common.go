//go:build x_bento_extra

package ibmmq

import (
	"errors"

	mq "github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	queueManagerField        = "queue_manager"
	channelField             = "channel"
	connectionNameField      = "connection_name"
	queueField               = "queue"
	authField                = "auth"
	authUserField            = "user"
	authPasswordField        = "password"
	tlsField                 = "tls"
	tlsEnabledField          = "enabled"
	tlsCipherSpecField       = "cipher_spec"
	tlsKeyRepositoryField    = "key_repository"
	tlsCertificateLabelField = "certificate_label"
)

const ibmMQConnectionLintRule = `root = match {
  this.auth.password.or("") != "" && this.auth.user.or("") == "" => [ "auth.password cannot be set without auth.user" ]
  this.tls.enabled.or(false) && this.tls.cipher_spec.or("") == "" => [ "tls.cipher_spec must be set when tls.enabled is true" ]
  this.tls.enabled.or(false) && this.tls.key_repository.or("") == "" => [ "tls.key_repository must be set when tls.enabled is true" ]
}`

type ibmMQConnectionConfig struct {
	queueManager        string
	channel             string
	connectionName      string
	authUser            string
	authPassword        string
	tlsEnabled          bool
	tlsCipherSpec       string
	tlsKeyRepository    string
	tlsCertificateLabel string
}

func ibmMQConnectionHeadFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(queueManagerField).
			Description("The queue manager name to connect to.").
			Example("QM1"),
		service.NewStringField(channelField).
			Description("The server-connection (SVRCONN) channel to connect through.").
			Example("DEV.APP.SVRCONN"),
		service.NewStringField(connectionNameField).
			Description("The host and listener port in MQ connection name format.").
			Example("localhost(1414)"),
	}
}

func ibmMQConnectionTailFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewObjectField(authField,
			service.NewStringField(authUserField).
				Description("Optional user for authentication.").
				Example("app").
				Default(""),
			service.NewStringField(authPasswordField).
				Description("Optional password for authentication.").
				Example("${MQ_PASSWORD}").
				Default("").
				Secret(),
		).Description("Optional authentication credentials.").Optional().Advanced(),
		service.NewObjectField(tlsField,
			service.NewBoolField(tlsEnabledField).
				Description("Enables TLS for the client connection.").
				Default(false),
			service.NewStringField(tlsCipherSpecField).
				Description("The IBM MQ CipherSpec configured on the target channel.").
				Example("ANY_TLS12_OR_HIGHER").
				Default("").
				Advanced(),
			service.NewStringField(tlsKeyRepositoryField).
				Description("Path to the key repository without the `.kdb` extension. The repository is a CMS key database (`.kdb`) with its password stash file (`.sth`) next to it. The IBM MQ C client initializes TLS once per process, so all `ibm_mq` inputs and outputs in a Bento process must use the same key repository, with `certificate_label` selecting each connection's certificate.").
				Example("/opt/mqm/ssl/key").
				Default("").
				Advanced(),
			service.NewStringField(tlsCertificateLabelField).
				Description("Optional label of the client certificate in the key repository. If empty, IBM MQ uses `ibmwebspheremq` followed by the user ID of the client in lowercase.").
				Default("").
				Advanced(),
		).Description("Optional IBM MQ TLS settings. TLS is handled by the IBM MQ C client rather than Go, so these fields differ from the `tls` fields of other components.").Optional().Advanced(),
	}
}

func ibmMQConnectionConfigFromParsed(config *service.ParsedConfig) (ibmMQConnectionConfig, error) {
	connectionConfig := ibmMQConnectionConfig{}
	var err error

	if connectionConfig.queueManager, err = config.FieldString(queueManagerField); err != nil {
		return connectionConfig, err
	}
	if connectionConfig.channel, err = config.FieldString(channelField); err != nil {
		return connectionConfig, err
	}
	if connectionConfig.connectionName, err = config.FieldString(connectionNameField); err != nil {
		return connectionConfig, err
	}

	if config.Contains(authField) {
		authConfig := config.Namespace(authField)

		if connectionConfig.authUser, err = authConfig.FieldString(authUserField); err != nil {
			return connectionConfig, err
		}
		if connectionConfig.authPassword, err = authConfig.FieldString(authPasswordField); err != nil {
			return connectionConfig, err
		}
	}

	if config.Contains(tlsField) {
		tlsConfig := config.Namespace(tlsField)

		if connectionConfig.tlsEnabled, err = tlsConfig.FieldBool(tlsEnabledField); err != nil {
			return connectionConfig, err
		}
		if connectionConfig.tlsCipherSpec, err = tlsConfig.FieldString(tlsCipherSpecField); err != nil {
			return connectionConfig, err
		}
		if connectionConfig.tlsKeyRepository, err = tlsConfig.FieldString(tlsKeyRepositoryField); err != nil {
			return connectionConfig, err
		}
		if connectionConfig.tlsCertificateLabel, err = tlsConfig.FieldString(tlsCertificateLabelField); err != nil {
			return connectionConfig, err
		}

		// The lint rule catches this too, but linting can be skipped, for
		// example with --chilled, and without a CipherSpec the client
		// connects without TLS.
		if connectionConfig.tlsEnabled && connectionConfig.tlsCipherSpec == "" {
			return connectionConfig, errors.New("tls.cipher_spec must be set when tls.enabled is true")
		}
	}

	return connectionConfig, nil
}

func ibmMQConnect(connectionConfig ibmMQConnectionConfig, log *service.Logger) (mq.MQQueueManager, error) {
	channelDefinition := mq.NewMQCD()
	channelDefinition.ChannelName = connectionConfig.channel
	channelDefinition.ConnectionName = connectionConfig.connectionName
	channelDefinition.MaxMsgLength = 100 * 1024 * 1024

	connectionOptions := mq.NewMQCNO()
	connectionOptions.Options = mq.MQCNO_CLIENT_BINDING
	connectionOptions.ClientConn = channelDefinition
	connectionOptions.ApplName = "Bento ibm_mq"

	if connectionConfig.authUser != "" {
		securityParameters := mq.NewMQCSP()
		securityParameters.AuthenticationType = mq.MQCSP_AUTH_USER_ID_AND_PWD
		securityParameters.UserId = connectionConfig.authUser
		securityParameters.Password = connectionConfig.authPassword
		connectionOptions.SecurityParms = securityParameters
	}

	if connectionConfig.tlsEnabled {
		sslConfigurationOptions := mq.NewMQSCO()
		sslConfigurationOptions.KeyRepository = connectionConfig.tlsKeyRepository
		sslConfigurationOptions.CertificateLabel = connectionConfig.tlsCertificateLabel
		channelDefinition.SSLCipherSpec = connectionConfig.tlsCipherSpec
		connectionOptions.SSLConfig = sslConfigurationOptions
	}

	queueManager, err := mq.Connx(connectionConfig.queueManager, connectionOptions)

	switch ibmMQReasonCode(err) {
	case mq.MQRC_SSL_ALREADY_INITIALIZED:
		// Warning returned for every TLS connection after the first in a
		// process whose TLS options differ, such as certificate_label. The
		// connection is good.
		return queueManager, nil
	case mq.MQRC_CLUSTER_EXIT_LOAD_ERROR:
		// Despite its name a warning: the queue manager couldn't load its
		// cluster workload exit and carries on without it. Only its
		// administrator can fix that.
		log.Warnf("IBM MQ queue manager could not load its cluster workload exit: %v", err)
		return queueManager, nil
	}

	return queueManager, err
}

func ibmMQReasonCode(err error) int32 {
	if mqReturn, ok := errors.AsType[*mq.MQReturn](err); ok {
		return mqReturn.MQRC
	}
	return mq.MQRC_NONE
}
