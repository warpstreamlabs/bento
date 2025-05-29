package cypher

import "github.com/warpstreamlabs/bento/public/service"

const (
	cypherDatabase  = "database"
	cypherURI       = "uri"
	cypherNoAuth    = "no_auth"
	cypherBasicAuth = "basic_auth"
	cypherQuery     = "query"
	cypherValues    = "values"
	cypherBatching  = "batching"

	// Basic Auth
	cypherUser     = "user"
	cypherPassword = "password"
	cypherRealm    = "realm"

	cypherTLS = "tls"
)

type CypherBasicAuth struct {
	user     string
	password string
	realm    string
}

func basicAuthSpec() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(cypherUser).
			Default("").
			Description("The username for basic auth."),
		service.NewStringField(cypherPassword).
			Default("").
			Secret().
			Description("The password for basic auth."),
		service.NewStringField(cypherRealm).
			Default("").
			Description("The realm for basic auth."),
	}
}

func connFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(cypherDatabase).
			Description("The name of the database to connect to.").
			Example("neo4j"),
		service.NewStringField(cypherURI).
			Description("The URL of the database engine.").
			Example("bolt://localhost:7687"),
		service.NewBoolField(cypherNoAuth).
			Description("Set to true to connect without authentication.").
			Default(false),
		service.NewObjectField(cypherBasicAuth, basicAuthSpec()...).
			Description("Basic Authentication fields"),
		service.NewTLSToggledField(cypherTLS),
	}
}
