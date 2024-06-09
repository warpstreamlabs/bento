package common

import (
	"bytes"
	"os"
	"text/template"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/log"
)

type CLIStreamBootstrapFunc func()

type CLIOpts struct {
	Version   string
	DateBuilt string

	BinaryName       string
	ProductName      string
	DocumentationURL string

	MainConfigSpecCtor   func() docs.FieldSpecs // TODO: This becomes a service.Environment
	OnManagerInitialised func(mgr bundle.NewManagement, pConf *docs.ParsedConfig) error
	OnLoggerInit         func(l log.Modular) (log.Modular, error)
}

func NewCLIOpts(version, dateBuilt string) *CLIOpts {
	binaryName := ""
	if len(os.Args) > 0 {
		binaryName = os.Args[0]
	}
	return &CLIOpts{
		Version:            version,
		DateBuilt:          dateBuilt,
		BinaryName:         binaryName,
		ProductName:        "Bento",
		DocumentationURL:   "https://warpstreamlabs.github.io/bento/docs",
		MainConfigSpecCtor: config.Spec,
		OnManagerInitialised: func(mgr bundle.NewManagement, pConf *docs.ParsedConfig) error {
			return nil
		},
		OnLoggerInit: func(l log.Modular) (log.Modular, error) {
			return l, nil
		},
	}
}

func (c *CLIOpts) ExecTemplate(str string) string {
	t, err := template.New("cli").Parse(str)
	if err != nil {
		return str
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, struct {
		BinaryName       string
		ProductName      string
		DocumentationURL string
	}{
		BinaryName:       c.BinaryName,
		ProductName:      c.ProductName,
		DocumentationURL: c.DocumentationURL,
	}); err != nil {
		return str
	}

	return buf.String()
}
