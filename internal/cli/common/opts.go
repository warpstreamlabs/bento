package common

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
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

	ShowRunCommand    bool
	ConfigSearchPaths []string

	MainConfigSpecCtor   func() docs.FieldSpecs // TODO: This becomes a service.Environment
	OnManagerInitialised func(mgr bundle.NewManagement, pConf *docs.ParsedConfig) error
	OnLoggerInit         func(l log.Modular) (log.Modular, error)

	// ErrWriter receives error messages that occur before the configured
	// logger exists (or that otherwise bypass it) along the run path, such
	// as a config file failing to load. It defaults to os.Stderr.
	ErrWriter io.Writer
}

func NewCLIOpts(version, dateBuilt string) *CLIOpts {
	binaryName := ""
	if len(os.Args) > 0 {
		binaryName = path.Base(os.Args[0])
	}
	return &CLIOpts{
		Version:          version,
		DateBuilt:        dateBuilt,
		BinaryName:       binaryName,
		ProductName:      "Bento",
		DocumentationURL: "https://warpstreamlabs.github.io/bento/docs",
		ShowRunCommand:   false,
		ConfigSearchPaths: []string{
			"/bento.yaml",
			"/etc/bento/config.yaml",
			"/etc/bento.yaml",
		},
		MainConfigSpecCtor: config.Spec,
		ErrWriter:          os.Stderr,
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

// Errorf writes a formatted failure message to ErrWriter (os.Stderr by
// default). It is the single choke point every failure that occurs before
// the configured logger exists (or that otherwise needs to bypass it) along
// the run path should go through, so that a program embedding the CLI via
// public/service.RunCLI and CLIOptSetErrWriter can observe and capture it.
func (c *CLIOpts) Errorf(format string, args ...any) {
	fmt.Fprintf(c.ErrWriter, format, args...)
}
