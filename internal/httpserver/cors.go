package httpserver

import (
	"errors"
	"net/http"

	"github.com/gorilla/handlers"

	"github.com/warpstreamlabs/bento/internal/docs"
)

const (
	fieldCORS               = "cors"
	fieldCORSEnabled        = "enabled"
	fieldCORSAllowedOrigins = "allowed_origins"
	fieldCORSAllowedHeaders = "allowed_headers"
	fieldCORSAllowedMethods = "allowed_methods"
)

var defaultHeaders = []string{"Accept", "Accept-Language", "Content-Language", "Origin"}
var defaultMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}

// CORSConfig contains struct configuration for allowing CORS headers.
type CORSConfig struct {
	Enabled        bool     `json:"enabled" yaml:"enabled"`
	AllowedOrigins []string `json:"allowed_origins" yaml:"allowed_origins"`
	AllowedHeaders []string `json:"allowed_headers" yaml:"allowed_headers"`
	AllowedMethods []string `json:"allowed_methods" yaml:"allowed_methods"`
}

// NewServerCORSConfig returns a new server CORS config with default fields.
func NewServerCORSConfig() CORSConfig {
	return CORSConfig{
		Enabled:        false,
		AllowedOrigins: []string{},
		AllowedHeaders: defaultHeaders,
		AllowedMethods: defaultMethods,
	}
}

// WrapHandler wraps a provided HTTP handler with middleware that enables CORS
// requests (when configured).
func (conf CORSConfig) WrapHandler(handler http.Handler) (http.Handler, error) {
	if !conf.Enabled {
		return handler, nil
	}
	if len(conf.AllowedOrigins) == 0 {
		return nil, errors.New("must specify at least one allowed origin")
	}

	return handlers.CORS(
		handlers.AllowedOrigins(conf.AllowedOrigins),
		handlers.AllowedMethods(conf.AllowedMethods),
		handlers.AllowedHeaders(conf.AllowedHeaders),
	)(handler), nil
}

// ServerCORSFieldSpec returns a field spec for an http server CORS component.
func ServerCORSFieldSpec() docs.FieldSpec {
	return docs.FieldObject(fieldCORS, "Adds Cross-Origin Resource Sharing headers.").WithChildren(
		docs.FieldBool(fieldCORSEnabled, "Whether to allow CORS requests.").HasDefault(false),
		docs.FieldString(fieldCORSAllowedOrigins, "An explicit list of origins that are allowed for CORS requests.").Array().HasDefault([]any{}),
		docs.FieldString(fieldCORSAllowedHeaders, "Appends additional headers to the list of default allowed headers: Accept, Accept-Language, Content-Language & Origin. These default headers are therefore always allowed.").Array().HasDefault([]any{}).AtVersion("1.13.0"),
		docs.FieldString(fieldCORSAllowedMethods, "Used to explicitly set allowed methods in the Access-Control-Allow-Methods header.").Array().HasDefault(defaultMethods).AtVersion("1.13.0"),
	).AtVersion("1.0.0").Advanced()
}

func CORSConfigFromParsed(pConf *docs.ParsedConfig) (conf CORSConfig, err error) {
	pConf = pConf.Namespace(fieldCORS)

	if conf.Enabled, err = pConf.FieldBool(fieldCORSEnabled); err != nil {
		return
	}

	if conf.AllowedOrigins, err = pConf.FieldStringList(fieldCORSAllowedOrigins); err != nil {
		return
	}

	conf.AllowedHeaders, err = pConf.FieldStringList(fieldCORSAllowedHeaders)
	if err != nil {
		return
	}

	if conf.AllowedMethods, err = pConf.FieldStringList(fieldCORSAllowedMethods); err != nil {
		return
	}

	return
}
