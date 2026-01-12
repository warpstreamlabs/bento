package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/warpstreamlabs/bento/public/service"
)

// AuthFields returns the config fields for Kubernetes authentication.
// These fields are shared across all Kubernetes components.
func AuthFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBoolField("auto_auth").
			Description("Automatically detect authentication method. Tries in-cluster config first, then falls back to kubeconfig.").
			Default(true),
		service.NewStringField("kubeconfig").
			Description("Path to kubeconfig file. If empty, uses $KUBECONFIG (if set) or the default kubeconfig location (~/.kube/config).").
			Default("").
			Optional(),
		service.NewStringField("kubeconfig_yaml").
			Description("Kubeconfig content as a YAML string. Use this to embed kubeconfig directly in your config (e.g., from a secret or environment variable) instead of referencing a file path.").
			Default("").
			Secret().
			Optional(),
		service.NewStringField("context").
			Description("Kubernetes context to use from kubeconfig. If empty, uses the current context.").
			Default("").
			Optional(),
		service.NewStringField("api_server").
			Description("Kubernetes API server URL. Only used when providing explicit credentials.").
			Default("").
			Optional().
			Advanced(),
		service.NewStringField("token").
			Description("Bearer token for authentication. Can be a service account token.").
			Default("").
			Secret().
			Optional().
			Advanced(),
		service.NewStringField("token_file").
			Description("Path to file containing bearer token.").
			Default("").
			Optional().
			Advanced(),
		service.NewStringField("ca_file").
			Description("Path to CA certificate file for verifying API server.").
			Default("").
			Optional().
			Advanced(),
		service.NewBoolField("insecure_skip_verify").
			Description("Skip TLS certificate verification. Not recommended for production.").
			Default(false).
			Advanced(),
		service.NewFloatField("client_qps").
			Description("QPS limit for Kubernetes API client. 0 uses the client-go default.").
			Default(0).
			Advanced(),
		service.NewIntField("client_burst").
			Description("Burst limit for Kubernetes API client. 0 uses the client-go default.").
			Default(0).
			Advanced(),
	}
}

// ClientSet contains the typed, dynamic, and discovery Kubernetes clients.
type ClientSet struct {
	Typed     kubernetes.Interface
	Dynamic   dynamic.Interface
	Discovery discovery.DiscoveryInterface
	Mapper    meta.RESTMapper
	Config    *rest.Config
}

// GetClientSet creates Kubernetes clients from parsed configuration.
func GetClientSet(ctx context.Context, conf *service.ParsedConfig, fs *service.FS) (*ClientSet, error) {
	autoAuth, err := conf.FieldBool("auto_auth")
	if err != nil {
		return nil, fmt.Errorf("failed to parse auto_auth: %w", err)
	}

	var config *rest.Config

	if autoAuth {
		// Try in-cluster first
		config, err = rest.InClusterConfig()
		if err != nil {
			// Fall back to kubeconfig
			config, err = buildKubeconfigClient(conf)
			if err != nil {
				return nil, fmt.Errorf("auto auth failed: not running in cluster and kubeconfig not available: %w", err)
			}
		}
	} else {
		// Check for explicit credentials first
		apiServer, _ := conf.FieldString("api_server")
		if apiServer != "" {
			config, err = buildExplicitClient(conf, fs)
		} else {
			config, err = buildKubeconfigClient(conf)
		}
		if err != nil {
			return nil, err
		}
	}

	// Apply TLS settings
	insecure, _ := conf.FieldBool("insecure_skip_verify")
	if insecure {
		config.Insecure = true
		config.CAFile = ""
		config.CAData = nil
	}

	qps, err := conf.FieldFloat("client_qps")
	if err != nil {
		return nil, fmt.Errorf("failed to parse client_qps: %w", err)
	}
	if qps > 0 {
		config.QPS = float32(qps)
	}

	burst, err := conf.FieldInt("client_burst")
	if err != nil {
		return nil, fmt.Errorf("failed to parse client_burst: %w", err)
	}
	if burst > 0 {
		config.Burst = burst
	}

	// Create typed client
	typedClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create dynamic client for CRD support
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Create cached discovery client and RESTMapper for GVR resolution
	discoveryClient := typedClient.Discovery()
	cachedDiscovery := memory.NewMemCacheClient(discoveryClient)
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedDiscovery)

	return &ClientSet{
		Typed:     typedClient,
		Dynamic:   dynamicClient,
		Discovery: discoveryClient,
		Mapper:    mapper,
		Config:    config,
	}, nil
}

func buildKubeconfigClient(conf *service.ParsedConfig) (*rest.Config, error) {
	kubeconfigYAML, _ := conf.FieldString("kubeconfig_yaml")
	kubeconfigPath, _ := conf.FieldString("kubeconfig")
	kubeContext, _ := conf.FieldString("context")

	// If raw kubeconfig YAML is provided, use it directly
	if kubeconfigYAML != "" {
		clientConfig, err := clientcmd.NewClientConfigFromBytes([]byte(kubeconfigYAML))
		if err != nil {
			return nil, fmt.Errorf("failed to parse kubeconfig_yaml: %w", err)
		}
		// Apply context override if specified
		if kubeContext != "" {
			rawConfig, err := clientConfig.RawConfig()
			if err != nil {
				return nil, fmt.Errorf("failed to get raw config: %w", err)
			}
			rawConfig.CurrentContext = kubeContext
			clientConfig = clientcmd.NewDefaultClientConfig(rawConfig, &clientcmd.ConfigOverrides{})
		}
		return clientConfig.ClientConfig()
	}

	// Fall back to file-based kubeconfig
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = expandHomePath(kubeconfigPath)
	} else if envKubeconfig := os.Getenv("KUBECONFIG"); envKubeconfig != "" {
		paths := filepath.SplitList(envKubeconfig)
		expanded := make([]string, 0, len(paths))
		for _, p := range paths {
			if p == "" {
				continue
			}
			expanded = append(expanded, expandHomePath(p))
		}
		if len(expanded) > 0 {
			loadingRules.Precedence = expanded
		}
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	if kubeContext != "" {
		configOverrides.CurrentContext = kubeContext
	}

	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		configOverrides,
	)

	return clientConfig.ClientConfig()
}

func expandHomePath(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return home
		}
	}

	prefix := "~" + string(os.PathSeparator)
	if strings.HasPrefix(path, prefix) {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return filepath.Join(home, path[len(prefix):])
		}
	}

	return path
}

func buildExplicitClient(conf *service.ParsedConfig, fs *service.FS) (*rest.Config, error) {
	apiServer, _ := conf.FieldString("api_server")
	if apiServer == "" {
		return nil, errors.New("api_server is required for explicit authentication")
	}

	config := &rest.Config{
		Host: apiServer,
	}

	// Token authentication
	token, _ := conf.FieldString("token")
	tokenFile, _ := conf.FieldString("token_file")

	if token != "" {
		config.BearerToken = token
	} else if tokenFile != "" {
		tokenBytes, err := ifs.ReadFile(fs, tokenFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read token file: %w", err)
		}
		config.BearerToken = strings.TrimSpace(string(tokenBytes))
	}

	// CA certificate
	caFile, _ := conf.FieldString("ca_file")
	if caFile != "" {
		config.CAFile = caFile
	}

	return config, nil
}
