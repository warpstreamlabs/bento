package cypher

import (
	"context"
	"crypto/tls"

	"github.com/Jeffail/shutdown"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/public/service"
)

var cypherInputDescription string = `Once the records from the query are exhausted, this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a sequence to execute).`

func cypherInputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Version("1.6.0").
		Categories("Services").
		Summary("Executes a cypher query and creates a message for each record received.").
		Description(cypherInputDescription).
		Fields(
			service.NewStringField(cypherQuery).
				Description("The cypher query to execute.").
				Example("MATCH (n) RETURN n"),
		).
		Fields(connFields()...)

	spec = spec.Field(service.NewAutoRetryNacksToggleField())

	return spec
}

func init() {
	err := service.RegisterInput(
		"cypher", cypherInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (in service.Input, err error) {
			in, err = NewCypherInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, in)
		})
	if err != nil {
		panic(err)
	}
}

//----------------------------------------------------------------------------

type CypherInput struct {
	database  string
	uri       string
	noAuth    bool
	basicAuth CypherBasicAuth
	tlsConfig *tls.Config
	query     string
	driver    neo4j.DriverWithContext

	recordsChan chan *neo4j.Record

	shutSig *shutdown.Signaller
	logger  *service.Logger
}

func NewCypherInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*CypherInput, error) {
	database, err := conf.FieldString(cypherDatabase)
	if err != nil {
		return nil, err
	}
	uri, err := conf.FieldString(cypherURI)
	if err != nil {
		return nil, err
	}
	noAuth, err := conf.FieldBool(cypherNoAuth)
	if err != nil {
		return nil, err
	}
	query, err := conf.FieldString(cypherQuery)
	if err != nil {
		return nil, err
	}

	recordsChan := make(chan *neo4j.Record, 10)

	var tlsEnabled bool
	var tlsConfig *tls.Config
	if tlsConfig, tlsEnabled, err = conf.FieldTLSToggled(cypherTLS); err != nil {
		return nil, err
	}

	input := &CypherInput{
		database:    database,
		uri:         uri,
		noAuth:      noAuth,
		query:       query,
		recordsChan: recordsChan,
		shutSig:     shutdown.NewSignaller(),
		logger:      mgr.Logger(),
	}

	if !noAuth {

		basicAuthMap, err := conf.FieldStringMap(cypherBasicAuth)
		if err != nil {
			return nil, err
		}

		basicAuth := CypherBasicAuth{
			user:     basicAuthMap[cypherUser],
			password: basicAuthMap[cypherPassword],
			realm:    basicAuthMap[cypherRealm],
		}

		input.basicAuth = basicAuth

		if tlsEnabled {
			input.tlsConfig = tlsConfig
		}
	}

	return input, nil
}

func (cyp *CypherInput) Connect(ctx context.Context) error {

	if cyp.driver != nil {
		return component.ErrAlreadyStarted
	}

	var driver neo4j.DriverWithContext
	var err error

	var connConfigurers []func(*config.Config)
	if cyp.tlsConfig != nil {
		connConfigurers = append(connConfigurers, func(config *config.Config) {
			config.TlsConfig = cyp.tlsConfig
		})
	}

	if cyp.noAuth {
		driver, err = neo4j.NewDriverWithContext(cyp.uri, neo4j.NoAuth())
	} else {
		driver, err = neo4j.NewDriverWithContext(cyp.uri, neo4j.BasicAuth(cyp.basicAuth.user, cyp.basicAuth.password, cyp.basicAuth.realm), connConfigurers...)
	}

	if err != nil {
		return err
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		return err
	}
	if err := driver.VerifyAuthentication(ctx, nil); err != nil {
		return err
	}

	cyp.driver = driver

	go func() {
		session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
		cypCtx, done := cyp.shutSig.HardStopCtx(context.Background())

		defer func() {
			done()
			close(cyp.recordsChan)
			session.Close(ctx)
		}()

		if _, err := session.ExecuteRead(cypCtx,
			func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(cypCtx, cyp.query, nil)
				if err != nil {
					cyp.logger.With("err", err).Error("unexpected error while executing cypher query")
					return nil, err
				}

				for result.Next(cypCtx) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-cypCtx.Done():
						return nil, cypCtx.Err()
					case <-cyp.shutSig.HardStopChan():
						return nil, service.ErrEndOfInput
					case cyp.recordsChan <- result.Record():
					}
				}
				return nil, err
			}); err != nil {
			cyp.logger.With("err", err).Error("unexpected error while executing cypher query")
		}
	}()

	return nil
}

func (cyp *CypherInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if cyp.driver == nil {
		return nil, nil, service.ErrNotConnected
	}

	msg := service.NewMessage(nil)
	select {
	case record, open := <-cyp.recordsChan:
		if !open {
			return nil, nil, service.ErrEndOfInput
		}
		msg.SetStructuredMut(record.AsMap())
	case <-ctx.Done():
		return nil, nil, service.ErrEndOfInput
	}

	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (cyp *CypherInput) Close(ctx context.Context) (err error) {
	cyp.shutSig.TriggerHardStop()
	return nil
}
