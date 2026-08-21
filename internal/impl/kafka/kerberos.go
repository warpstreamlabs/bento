package kafka

import (
	"context"
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

	if keytabPath == "" {
		return nil, fmt.Errorf("field 'keytab_path' is required for GSSAPI SASL mechanism")
	}

	// Load once at config parse time. The actual gokrb5 client is created
	// fresh per SASL session so reconnects get a clean login.
	krb5Conf, err := config.Load(krbConfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load kerberos config from '%s': %w", krbConfPath, err)
	}

	kt, err := keytab.Load(keytabPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keytab from '%s': %w", keytabPath, err)
	}

	return kerberos.Kerberos(func(ctx context.Context) (kerberos.Auth, error) {
		var settings []func(*client.Settings)
		if disablePAFXFAST {
			settings = append(settings, client.DisablePAFXFAST(true))
		}
		krbClient := client.NewWithKeytab(principal, realm, kt, krb5Conf, settings...)

		if err := krbClient.Login(); err != nil {
			return kerberos.Auth{}, fmt.Errorf("failed to login to kerberos: %w", err)
		}

		return kerberos.Auth{
			Client:  krbClient,
			Service: serviceName,
		}, nil
	}), nil
}
