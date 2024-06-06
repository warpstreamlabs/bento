package lambda

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/config"
	"github.com/warpstreamlabs/bento/v1/internal/docs"
	"github.com/warpstreamlabs/bento/v1/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/v1/internal/serverless"
)

var handler *serverless.Handler

// Run executes Bento as an AWS Lambda function. Configuration can be stored
// within the environment variable BENTO_CONFIG.
func Run() {
	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"./bento.yaml",
		"./config.yaml",
		"/bento.yaml",
		"/etc/bento/config.yaml",
		"/etc/bento.yaml",
	}
	if path := os.Getenv("BENTO_CONFIG_PATH"); path != "" {
		defaultPaths = append([]string{path}, defaultPaths...)
	}

	conf, confSpec, err := DefaultConfigAndSpec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file create error: %v\n", err)
		os.Exit(1)
	}

	if confStr := os.Getenv("BENTO_CONFIG"); confStr != "" {
		confBytes, err := config.ReplaceEnvVariables([]byte(confStr), os.LookupEnv)
		if err != nil {
			// TODO: Make this configurable somehow maybe, along with linting
			// errors.
			var errEnvMissing *config.ErrMissingEnvVars
			if errors.As(err, &errEnvMissing) {
				confBytes = errEnvMissing.BestAttempt
			} else {
				fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
				os.Exit(1)
			}
		}

		confNode, err := docs.UnmarshalYAML(confBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file parse error: %v\n", err)
			os.Exit(1)
		}

		pConf, err := confSpec.ParsedConfigFromAny(confNode)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file parse error: %v\n", err)
			os.Exit(1)
		}

		conf, err = config.FromParsed(bundle.GlobalEnvironment, pConf, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if _, err := ifs.OS().Stat(path); err == nil {
				conf, _, err = config.ReadYAMLFileLinted(ifs.OS(), confSpec, path, false, docs.NewLintConfig(bundle.GlobalEnvironment))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
					os.Exit(1)
				}
				break
			}
		}
	}

	if handler, err = serverless.NewHandler(conf); err != nil {
		fmt.Fprintf(os.Stderr, "Initialisation error: %v\n", err)
		os.Exit(1)
	}

	lambda.Start(handler.Handle)
	if err = handler.Close(time.Second * 30); err != nil {
		fmt.Fprintf(os.Stderr, "Shut down error: %v\n", err)
		os.Exit(1)
	}
}
