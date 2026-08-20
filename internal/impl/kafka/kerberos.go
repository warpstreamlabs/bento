package kafka

import (
	"fmt"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"

	"github.com/warpstreamlabs/bento/public/service"
)

func kerberosSaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	krbConfPath, err := c.FieldString("kerberos_config_path")
	if err != nil {
		return nil, err
	}

	keytabPath, err := c.FieldString("keytab_path")
	if err != nil {
		return nil, err
	}

	principal, err := c.FieldString("principal")
	if err != nil {
		return nil, err
	}

	realm, err := c.FieldString("realm")
	if err != nil {
		return nil, err
	}

	serviceName, err := c.FieldString("service_name")
	if err != nil {
		return nil, err
	}

	disablePAFXFAST, err := c.FieldBool("disable_pafx_fast")
	if err != nil {
		return nil, err
	}

	if krbConfPath == "" {
		return nil, fmt.Errorf("field 'kerberos_config_path' is required for GSSAPI SASL mechanism")
	}

	krb5Conf, err := config.Load(krbConfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load kerberos config from '%s': %w", krbConfPath, err)
	}

	var krbClient *client.Client
	if keytabPath != "" {
		kt, err := keytab.Load(keytabPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load keytab from '%s': %w", keytabPath, err)
		}
		var settings []func(*client.Settings)
		if disablePAFXFAST {
			settings = append(settings, client.DisablePAFXFAST(true))
		}
		krbClient = client.NewWithKeytab(principal, realm, kt, krb5Conf, settings...)
	} else {
		// Password-based auth not supported for now; keytab is the primary
		// use case for Kafka service accounts.
		return nil, fmt.Errorf("field 'keytab_path' is required for GSSAPI SASL mechanism")
	}

	if err := krbClient.Login(); err != nil {
		return nil, fmt.Errorf("failed to login to kerberos: %w", err)
	}

	auth := kerberos.Auth{
		Client:  krbClient,
		Service: serviceName,
	}
	return auth.AsMechanismWithClose(), nil
}
