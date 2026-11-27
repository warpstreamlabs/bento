package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v2"
	"golang.org/x/term"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/warpstreamlabs/bento/internal/cli/common"
	"github.com/warpstreamlabs/bento/internal/plugin/oci"
)

func pluginCliCommand() *cli.Command {
	return &cli.Command{
		Name:  "plugin",
		Usage: "EXPERIMENTAL: Build, share and manage WASM plugins",
		Description: `
Build WASM plugins into OCI artifacts, push them to and pull them from
container registries, and manage the local plugin store. The commands work
like their docker equivalents.

  bento plugin build -t ghcr.io/me/uppercase:1.0.0 ./uppercase
  bento plugin push ghcr.io/me/uppercase:1.0.0
  bento -c config.yaml # with a plugins source of oci://ghcr.io/me/uppercase:1.0.0

Plugins are kept in a local store, an OCI image layout directory, set with
--plugin-store or BENTO_PLUGIN_STORE.`[1:],
		Subcommands: []*cli.Command{
			pluginBuildCommand(),
			pluginPushCommand(),
			pluginPullCommand(),
			pluginListCommand(),
			pluginInspectCommand(),
			pluginTagCommand(),
			pluginRemoveCommand(),
			pluginPruneCommand(),
			pluginSaveCommand(),
			pluginLoadCommand(),
			pluginLoginCommand(),
			pluginLogoutCommand(),
		},
	}
}

// pluginAction runs fn with the local store, printing any error.
func pluginAction(fn func(c *cli.Context, store *oci.Store) error) cli.ActionFunc {
	return func(c *cli.Context) error {
		store, err := common.OpenPluginStore(c)
		if err == nil {
			err = fn(c, store)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return nil
	}
}

func requireArgs(c *cli.Context, minArgs, maxArgs int, usage string) error {
	n := c.Args().Len()
	if n < minArgs || (maxArgs >= 0 && n > maxArgs) {
		return fmt.Errorf("usage: bento plugin %v", usage)
	}
	return nil
}

func pluginBuildCommand() *cli.Command {
	return &cli.Command{
		Name:      "build",
		Usage:     "Build a plugin directory into the local store",
		ArgsUsage: "DIRECTORY",
		Description: `
Checks the plugin.yaml and plugin.wasm in DIRECTORY and stores them as a plugin
artifact, tagged with each --tag. It doesn't compile plugin.wasm, build that
with your usual tools first.

  bento plugin build -t ghcr.io/me/uppercase:1.0.0 -t ghcr.io/me/uppercase:latest ./uppercase`[1:],
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "tag",
				Aliases: []string{"t"},
				Usage:   "a reference to tag the plugin with, such as ghcr.io/me/plugin:1.0.0",
			},
			&cli.StringSliceFlag{
				Name:  "annotation",
				Usage: "an annotation to add to the plugin's manifest, as key=value, such as org.opencontainers.image.version=1.0.0",
			},
		},
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 1, 1, "build [--tag REF]... DIRECTORY"); err != nil {
				return err
			}

			annotations := map[string]string{}
			for _, a := range c.StringSlice("annotation") {
				k, v, ok := strings.Cut(a, "=")
				if !ok || k == "" {
					return fmt.Errorf("invalid annotation %q, expected key=value", a)
				}
				annotations[k] = v
			}

			res, err := store.Build(c.Context, c.Args().First(), c.StringSlice("tag"), annotations)
			if err != nil {
				return err
			}
			for _, w := range res.Warnings {
				fmt.Fprintf(os.Stderr, "WARN: %v\n", w)
			}
			fmt.Printf("Built plugin %v %v\n", res.Name, res.Descriptor.Digest)
			for _, t := range c.StringSlice("tag") {
				ref, _ := oci.ParseReference(t)
				fmt.Printf("Tagged %v\n", ref)
			}
			return nil
		}),
	}
}

func pluginPushCommand() *cli.Command {
	return &cli.Command{
		Name:      "push",
		Usage:     "Push a plugin from the local store to its registry",
		ArgsUsage: "REF",
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 1, 1, "push REF"); err != nil {
				return err
			}
			desc, err := store.Push(c.Context, c.Args().First())
			if err != nil {
				return err
			}
			fmt.Printf("Pushed %v\nDigest: %v\n", c.Args().First(), desc.Digest)
			return nil
		}),
	}
}

func pluginPullCommand() *cli.Command {
	return &cli.Command{
		Name:      "pull",
		Usage:     "Pull a plugin from its registry into the local store",
		ArgsUsage: "REF",
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 1, 1, "pull REF"); err != nil {
				return err
			}
			desc, err := store.Pull(c.Context, c.Args().First())
			if err != nil {
				return err
			}
			fmt.Printf("Pulled %v\nDigest: %v\n", c.Args().First(), desc.Digest)
			return nil
		}),
	}
}

func pluginListCommand() *cli.Command {
	return &cli.Command{
		Name:    "ls",
		Aliases: []string{"list"},
		Usage:   "List the plugins in the local store",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "quiet",
				Aliases: []string{"q"},
				Usage:   "only show references",
			},
		},
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			entries, err := store.List(c.Context)
			if err != nil {
				return err
			}
			if c.Bool("quiet") {
				for _, e := range entries {
					fmt.Println(e.Tag)
				}
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 4, 3, ' ', 0)
			fmt.Fprintln(w, "REFERENCE\tNAME\tTYPE\tABI\tDIGEST\tSIZE")
			for _, e := range entries {
				a := e.Manifest.Annotations
				fmt.Fprintf(w, "%v\t%v\t%v\tv%v\t%v\t%v\n",
					e.Tag, a[oci.AnnotationName], a[oci.AnnotationType], a[oci.AnnotationABI],
					shortDigest(e.Descriptor.Digest.String()), humanSize(e.Size()))
			}
			return w.Flush()
		}),
	}
}

func shortDigest(d string) string {
	if _, hex, ok := strings.Cut(d, ":"); ok && len(hex) > 12 {
		return hex[:12]
	}
	return d
}

func humanSize(n int64) string {
	const unit = 1000
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "kMGT"[exp])
}

func pluginInspectCommand() *cli.Command {
	return &cli.Command{
		Name:      "inspect",
		Usage:     "Show the details of plugins in the local store, as JSON",
		ArgsUsage: "REF...",
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 1, -1, "inspect REF..."); err != nil {
				return err
			}

			type details struct {
				Reference   string            `json:"reference"`
				Digest      string            `json:"digest"`
				Size        int64             `json:"size"`
				Annotations map[string]string `json:"annotations,omitempty"`
				Config      json.RawMessage   `json:"config"`
				Lints       []string          `json:"lints,omitempty"`
			}

			var out []details
			for _, ref := range c.Args().Slice() {
				p, err := store.Get(c.Context, ref, false)
				if err != nil {
					return err
				}
				_, lints, err := oci.ReadConfig(p.Config)
				if err != nil {
					return fmt.Errorf("%v: %w", ref, err)
				}
				out = append(out, details{
					Reference:   ref,
					Digest:      p.Descriptor.Digest.String(),
					Size:        p.Descriptor.Size + p.Manifest.Config.Size + p.Manifest.Layers[0].Size,
					Annotations: p.Manifest.Annotations,
					Config:      p.Config,
					Lints:       lints,
				})
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		}),
	}
}

func pluginTagCommand() *cli.Command {
	return &cli.Command{
		Name:      "tag",
		Usage:     "Add a tag to a plugin in the local store",
		ArgsUsage: "SOURCE_REF TARGET_REF",
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 2, 2, "tag SOURCE_REF TARGET_REF"); err != nil {
				return err
			}
			return store.Tag(c.Context, c.Args().Get(0), c.Args().Get(1))
		}),
	}
}

func pluginRemoveCommand() *cli.Command {
	return &cli.Command{
		Name:      "rm",
		Aliases:   []string{"remove"},
		Usage:     "Remove plugins from the local store",
		ArgsUsage: "REF...",
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 1, -1, "rm REF..."); err != nil {
				return err
			}
			for _, ref := range c.Args().Slice() {
				if err := store.Remove(c.Context, ref); err != nil {
					return err
				}
				fmt.Printf("Removed %v\n", ref)
			}
			return nil
		}),
	}
}

func pluginPruneCommand() *cli.Command {
	return &cli.Command{
		Name:  "prune",
		Usage: "Remove untagged plugins and unused content from the local store",
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			removed, err := store.Prune(c.Context)
			for _, d := range removed {
				fmt.Printf("Removed %v\n", d)
			}
			return err
		}),
	}
}

func pluginSaveCommand() *cli.Command {
	return &cli.Command{
		Name:      "save",
		Usage:     "Save plugins to a tar archive",
		ArgsUsage: "REF...",
		Description: `
Saves plugins from the local store to a tar archive of an OCI image layout,
to share them without a registry. Load them with bento plugin load.

  bento plugin save -o uppercase.tar ghcr.io/me/uppercase:1.0.0`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the file to write to, instead of stdout",
			},
		},
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			if err := requireArgs(c, 1, -1, "save [-o FILE] REF..."); err != nil {
				return err
			}

			var w io.Writer = os.Stdout
			if out := c.String("output"); out != "" {
				f, err := os.Create(out)
				if err != nil {
					return err
				}
				defer f.Close()
				w = f
			} else if term.IsTerminal(int(os.Stdout.Fd())) {
				return errors.New("refusing to write an archive to a terminal, use -o or redirect stdout")
			}
			return store.Save(c.Context, c.Args().Slice(), w)
		}),
	}
}

func pluginLoadCommand() *cli.Command {
	return &cli.Command{
		Name:  "load",
		Usage: "Load plugins from a tar archive",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "input",
				Aliases:  []string{"i"},
				Usage:    "the archive to read, made by bento plugin save",
				Required: true,
			},
		},
		Action: pluginAction(func(c *cli.Context, store *oci.Store) error {
			tags, err := store.Load(c.Context, c.String("input"))
			for _, t := range tags {
				fmt.Printf("Loaded %v\n", t)
			}
			return err
		}),
	}
}

func pluginLoginCommand() *cli.Command {
	return &cli.Command{
		Name:      "login",
		Usage:     "Log in to a registry",
		ArgsUsage: "[SERVER]",
		Description: `
Logs in to a registry, Docker Hub if no server is given, and saves the
credentials in the same place as docker login, so either can be used. Like
docker login, credentials are kept in a credential helper if one is configured,
and in Docker's config file otherwise.

  echo $GITHUB_TOKEN | bento plugin login ghcr.io -u me --password-stdin`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "username", Aliases: []string{"u"}, Usage: "Username"},
			&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Usage: "Password or Personal Access Token (PAT)"},
			&cli.BoolFlag{Name: "password-stdin", Usage: "Take the Password or Personal Access Token (PAT) from stdin"},
		},
		Action: func(c *cli.Context) error {
			if err := pluginLogin(c); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return nil
		},
	}
}

func pluginLogin(c *cli.Context) error {
	if err := requireArgs(c, 0, 1, "login [OPTIONS] [SERVER]"); err != nil {
		return err
	}
	registry := oci.DefaultRegistry
	if c.Args().Len() > 0 {
		registry = c.Args().First()
	}

	username, password := strings.TrimSpace(c.String("username")), c.String("password")
	if password != "" {
		fmt.Fprintln(os.Stderr, "WARNING! Using --password via the CLI is insecure. Use --password-stdin.")
		if c.Bool("password-stdin") {
			return errors.New("--password and --password-stdin are mutually exclusive")
		}
	}
	if c.Bool("password-stdin") {
		if username == "" {
			return errors.New("Must provide --username with --password-stdin") //nolint:staticcheck // Matches docker.
		}
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		password = strings.TrimRight(string(b), "\r\n")
	}

	creds, err := oci.CredentialStore()
	if err != nil {
		return err
	}
	registries, err := common.PluginRegistries(c)
	if err != nil {
		return err
	}

	// With no credentials given, try the saved ones first, as docker does.
	var stored auth.Credential
	if username == "" && password == "" {
		stored, _ = creds.Get(c.Context, oci.ServerAddress(registry))
		if stored.Username != "" && stored.Password != "" {
			fmt.Printf("Authenticating with existing credentials... [Username: %v]\n", stored.Username)
			err := registries.Login(c.Context, creds, registry, stored.Username, stored.Password)
			if err == nil {
				return loginSucceeded(creds, registry)
			}
			fmt.Fprintf(os.Stderr, "Login did not succeed, error: %v\n", err)
		}
	}

	if username == "" || password == "" {
		if !term.IsTerminal(int(os.Stdin.Fd())) {
			return errors.New("Cannot perform an interactive login from a non TTY device") //nolint:staticcheck // Matches docker.
		}
		in := bufio.NewReader(os.Stdin)
		if username == "" {
			if stored.Username != "" {
				fmt.Printf("Username (%v): ", stored.Username)
			} else {
				fmt.Print("Username: ")
			}
			line, err := in.ReadString('\n')
			if err != nil {
				return err
			}
			if username = strings.TrimSpace(line); username == "" {
				username = stored.Username
			}
			if username == "" {
				return errors.New("Username is required") //nolint:staticcheck // Matches docker.
			}
		}
		if password == "" {
			fmt.Print("Password: ")
			b, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				return err
			}
			if password = string(b); password == "" {
				return errors.New("Password is required") //nolint:staticcheck // Matches docker.
			}
		}
	}

	if err := registries.Login(c.Context, creds, registry, username, password); err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	return loginSucceeded(creds, registry)
}

func loginSucceeded(creds *credentials.DynamicStore, registry string) error {
	if oci.StoredInPlaintext(creds, registry) {
		fmt.Fprintf(os.Stderr, `
WARNING! Your credentials are stored unencrypted in '%v'.
Configure a credential helper to remove this warning. See
https://docs.docker.com/go/credential-store/

`, creds.ConfigPath())
	}
	fmt.Println("Login Succeeded")
	return nil
}

func pluginLogoutCommand() *cli.Command {
	return &cli.Command{
		Name:        "logout",
		Usage:       "Log out from a registry",
		ArgsUsage:   "[SERVER]",
		Description: "Removes the saved credentials of a registry, Docker Hub if no server is given.",
		Action: func(c *cli.Context) error {
			if err := pluginLogout(c); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return nil
		},
	}
}

func pluginLogout(c *cli.Context) error {
	if err := requireArgs(c, 0, 1, "logout [SERVER]"); err != nil {
		return err
	}
	registry := oci.DefaultRegistry
	if c.Args().Len() > 0 {
		registry = c.Args().First()
	}

	creds, err := oci.CredentialStore()
	if err != nil {
		return err
	}
	if stored, err := creds.Get(c.Context, oci.ServerAddress(registry)); err != nil || stored == auth.EmptyCredential {
		fmt.Printf("Not logged in to %v\n", registry)
		return nil
	}

	fmt.Printf("Removing login credentials for %v\n", registry)
	return oci.Logout(c.Context, creds, registry)
}
