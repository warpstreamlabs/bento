package sftp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	sftplib "github.com/pkg/sftp"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	CodecAllBytes           = "all-bytes"
	CodecLines              = "lines"
	CodecCsvLinesWithHeader = "csv-lines-with-header"
)

type config struct {
	address               *service.InterpolatedString
	user                  *service.InterpolatedString
	pass                  *service.InterpolatedString
	key                   *service.InterpolatedString
	keypass               *service.InterpolatedString
	paths                 []*service.InterpolatedString
	algs                  []*service.InterpolatedString
	insecureIgnoreHostKey bool
	knownHostsPath        *service.InterpolatedString
	codec                 string
}

type baseSftpIO struct {
	sshClient   *ssh.Client
	sftpClient  *sftplib.Client
	config      config
	parsedPaths []string
	parsedAlgs  []string
}

func (b *baseSftpIO) baseConnect(ctx context.Context) error {
	addr, err := b.config.address.TryString(rootMsg)
	if err != nil {
		return fmt.Errorf("address could not be resolved: %w", err)
	}

	user, err := b.config.user.TryString(rootMsg)
	if err != nil {
		return fmt.Errorf("user could not be resolved: %w", err)
	}

	authMethods := []ssh.AuthMethod{}

	if b.config.pass != nil {
		pass, _ := b.config.pass.TryString(rootMsg)
		if pass != "" {
			authMethods = append(authMethods, ssh.Password(pass))
		}
	}

	resolveSigner := func(s ssh.Signer) (ssh.Signer, error) {
		b.parsedAlgs = make([]string, len(b.config.algs))
		for idx := range b.config.algs {
			b.parsedAlgs[idx], err = b.config.algs[idx].TryString(rootMsg)
			if err != nil {
				return nil, err
			}
		}

		if len(b.parsedAlgs) > 0 {
			ns, err := ssh.NewSignerWithAlgorithms(s.(ssh.AlgorithmSigner), b.parsedAlgs)
			if err != nil {
				return nil, fmt.Errorf("error multisign: %w", err)
			}
			return ns, nil
		}

		return s, nil
	}

	if b.config.key != nil {
		key, _ := b.config.key.TryString(rootMsg)
		if key != "" {
			var s ssh.Signer
			bs, err := os.ReadFile(key)
			if err != nil {
				return fmt.Errorf("could not read key file: %w", err)
			}
			if b.config.keypass != nil {
				keypass, _ := b.config.keypass.TryString(rootMsg)
				if keypass != "" {
					s, err = ssh.ParsePrivateKeyWithPassphrase(bs, []byte(keypass))
					if err != nil {
						return fmt.Errorf("could not parse key file with pass: %w", err)
					}
				}
			} else {
				s, err = ssh.ParsePrivateKey(bs)
				if err != nil {
					return fmt.Errorf("could not parse key file: %w", err)
				}

			}

			s, err = resolveSigner(s)
			if err != nil {
				return fmt.Errorf("could not resolve signer: %w", err)
			}

			authMethods = append(authMethods, ssh.PublicKeys(s))
		}

	}

	b.parsedPaths = make([]string, len(b.config.paths))
	for idx := range b.config.paths {
		b.parsedPaths[idx], err = b.config.paths[idx].TryString(rootMsg)
		if err != nil {
			return fmt.Errorf("could not parse paths: %w", err)
		}
	}

	//nolint:gosec
	hostKeyCallback := ssh.InsecureIgnoreHostKey()

	if !b.config.insecureIgnoreHostKey {
		khPath := filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts")

		if b.config.knownHostsPath != nil {
			khPath, err = b.config.knownHostsPath.TryString(rootMsg)
			if err != nil {
				return fmt.Errorf("error resolving known-hosts file: %w", err)
			}
		}

		innerHostKeyCallback, err := knownhosts.New(khPath)
		if err != nil {
			return errors.New("error creating known-hosts")
		}

		hostKeyCallback = func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			err := innerHostKeyCallback(hostname, remote, key)
			if err != nil {
				return fmt.Errorf("error validating ssh key (got type: %s): %w", key.Type(), err)
			}

			return nil
		}
	}

	sshClient, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		Config:          ssh.Config{},
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	})
	if err != nil {
		return fmt.Errorf("could not setup ssh client: %w", err)
	}

	go func() {
		<-ctx.Done()

		_ = sshClient.Close()
	}()

	sftpcli, err := sftplib.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("error creating sftp client on top of ssh: %w", err)
	}

	go func() {
		<-ctx.Done()

		_ = sftpcli.Close()
	}()

	b.sshClient = sshClient
	b.sftpClient = sftpcli

	return nil
}

func (b *baseSftpIO) baseClose(_ context.Context) error {
	if b.sftpClient != nil {
		_ = b.sftpClient.Close()
	}

	if b.sshClient != nil {
		_ = b.sshClient.Close()
	}

	return nil
}

var rootMsg = service.NewMessage([]byte{})

var configSpec = service.NewConfigSpec().
	Summary("Allows reading/writing from files placed in an sftp server.").
	Fields(
		service.NewInterpolatedStringField("address"),
		service.NewInterpolatedStringField("user"),
		service.NewInterpolatedStringField("pass").Optional(),
		service.NewInterpolatedStringField("key").Optional(),
		service.NewInterpolatedStringField("keypass").Optional(),
		service.NewBoolField("insecureIgnoreHostKey").Optional(),
		service.NewInterpolatedStringField("knownHosts").Optional(),
		service.NewInterpolatedStringListField("paths"),
		service.NewInterpolatedStringListField("algs").Optional(),
		service.NewStringField("codec"),
		service.NewBoolField("debug").Optional(),
	)

func buildConfig(conf *service.ParsedConfig) (config, error) {
	cfg := config{}

	var err error

	cfg.address, err = conf.FieldInterpolatedString("address")
	if err != nil {
		return config{}, fmt.Errorf("error retrieving address field from config: %w", err)
	}

	cfg.user, err = conf.FieldInterpolatedString("user")
	if err != nil {
		return config{}, fmt.Errorf("error retrieving user field from config: %w", err)
	}

	cfg.pass, err = conf.FieldInterpolatedString("pass")
	if err != nil {
		slog.Debug("field pass not loaded", "err", err)
	}

	cfg.key, err = conf.FieldInterpolatedString("key")
	if err != nil {
		slog.Debug("field key not loaded", "err", err)
	}

	cfg.keypass, err = conf.FieldInterpolatedString("keypass")
	if err != nil {
		slog.Debug("field keypass not loaded", "err", err)
	}

	cfg.paths, err = conf.FieldInterpolatedStringList("paths")
	if err != nil {
		return config{}, fmt.Errorf("error retrieving paths field from config: %w", err)
	}

	cfg.algs, err = conf.FieldInterpolatedStringList("algs")
	if err != nil {
		return config{}, fmt.Errorf("error retrieving paths field from config: %w", err)
	}

	if len(cfg.paths) < 1 {
		return config{}, errors.New("error retrieving paths field - although exists, has no entries (zero len)")
	}

	cfg.knownHostsPath, err = conf.FieldInterpolatedString("knownHosts")
	if err != nil {
		slog.Debug("field knownHosts not loaded", "err", err)
	}

	cfg.codec, err = conf.FieldString("codec")
	if err != nil {
		slog.Debug("field codec not loaded", "err", err)
	}

	cfg.insecureIgnoreHostKey, err = conf.FieldBool("insecureIgnoreHostKey")
	if err != nil {
		slog.Debug("field insecureIgnoreHostKey not loaded", "err", err)
	}

	return cfg, nil
}

func Setup() error {
	if err := service.RegisterInput(
		"sftp_v2", configSpec,
		func(serviceParsedConfig *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			cfg, err := buildConfig(serviceParsedConfig)
			if err != nil {
				return nil, err
			}
			return &Input{
				chBytes: make(chan []byte),
				chErr:   make(chan error),
				baseSftpIO: baseSftpIO{
					config: cfg,
				},
			}, nil
		}); err != nil {
		return fmt.Errorf("could not register sftp_v2 input: %w", err)
	}

	if err := service.RegisterOutput(
		"sftp_v2", configSpec,
		func(serviceParsedConfig *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			cfg, err := buildConfig(serviceParsedConfig)
			if err != nil {
				return nil, 1, err
			}
			return &Output{
				baseSftpIO: baseSftpIO{
					config: cfg,
				},
			}, 1, nil
		}); err != nil {
		return fmt.Errorf("could not register sftp_v2 output: %w", err)
	}

	return nil
}
