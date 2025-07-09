package blobl

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/cli/common"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

var red = color.New(color.FgRed).SprintFunc()

// CliCommand is a cli.Command definition for running a blobl mapping.
func CliCommand(opts *common.CLIOpts) *cli.Command {
	return &cli.Command{
		Name:  "blobl",
		Usage: opts.ExecTemplate("Execute a {{.ProductName}} mapping on documents consumed via stdin"),
		Description: opts.ExecTemplate(`
Provides a convenient tool for mapping JSON documents over the command line:

  cat documents.jsonl | {{.BinaryName}} blobl 'foo.bar.map_each(this.uppercase())'

  echo '{"foo":"bar"}' | {{.BinaryName}} blobl -f ./mapping.blobl

Find out more about Bloblang at: {{.DocumentationURL}}/guides/bloblang/about`)[1:],
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "threads",
				Aliases: []string{"t"},
				Value:   1,
				Usage:   "the number of processing threads to use, when >1 ordering is no longer guaranteed.",
			},
			&cli.BoolFlag{
				Name:    "raw",
				Aliases: []string{"r"},
				Usage:   "consume raw strings.",
			},
			&cli.BoolFlag{
				Name:    "pretty",
				Aliases: []string{"p"},
				Usage:   "pretty-print output.",
			},
			&cli.StringFlag{
				Name:    "file",
				Aliases: []string{"f"},
				Usage:   "execute a mapping from a file.",
			},
			&cli.IntFlag{
				Name:  "max-token-length",
				Usage: "Set the buffer size for document lines.",
				Value: bufio.MaxScanTokenSize,
			},
		},
		Action: run,
		Subcommands: []*cli.Command{
			{
				Name:    "server",
				Aliases: []string{"playground"},
				Usage:   "Run an interactive Bloblang playground with live editing and testing.",
				Description: `Run a web server that provides an interactive Bloblang playground for writing and testing Bloblang mappings in real time.

Example: bento blobl playground -m mapping.blobl -i input.json`,
				Action: runPlayground,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "host",
						Value: "localhost",
						Usage: "host address to bind the playground server to.",
					},
					&cli.StringFlag{
						Name:    "port",
						Value:   "4195",
						Aliases: []string{"p"},
						Usage:   "port number for the playground server (default: 4195).",
					},
					&cli.BoolFlag{
						Name:    "no-open",
						Value:   false,
						Aliases: []string{"n"},
						Usage:   "prevent automatic browser opening when starting the playground.",
					},
					&cli.StringFlag{
						Name:    "input-file",
						Value:   "",
						Aliases: []string{"i"},
						Usage:   "preload sample input data into the playground.",
					},
					&cli.StringFlag{
						Name:    "mapping-file",
						Value:   "",
						Aliases: []string{"m"},
						Usage:   "preload a Bloblang mapping file into the playground editor.",
					},
					&cli.BoolFlag{
						Name:    "write",
						Value:   false,
						Aliases: []string{"w"},
						Usage:   "auto-save playground changes back to the source files.",
					},
				},
			},
		},
	}
}

func run(c *cli.Context) error {
	t := max(c.Int("threads"), 1)
	raw := c.Bool("raw")
	pretty := c.Bool("pretty")
	file := c.String("file")
	m := c.Args().First()

	if file != "" {
		if m != "" {
			fmt.Fprintln(os.Stderr, red("invalid flags, unable to execute both a file mapping and an inline mapping"))
			os.Exit(1)
		}
		mappingBytes, err := ifs.ReadFile(ifs.OS(), file)
		if err != nil {
			fmt.Fprintf(os.Stderr, red("failed to read mapping file: %v\n"), err)
			os.Exit(1)
		}
		m = string(mappingBytes)
	}

	bEnv := bloblang.NewEnvironment().WithImporterRelativeToFile(file)
	exec, err := bEnv.NewMapping(m)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			fmt.Fprintf(os.Stderr, "%v %v\n", red("failed to parse mapping:"), perr.ErrorAtPositionStructured("", []rune(m)))
		} else {
			fmt.Fprintln(os.Stderr, red(err.Error()))
		}
		os.Exit(1)
	}

	inputsChan := make(chan []byte)
	go func() {
		defer close(inputsChan)

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(nil, c.Int("max-token-length"))
		for scanner.Scan() {
			input := make([]byte, len(scanner.Bytes()))
			copy(input, scanner.Bytes())
			inputsChan <- input
		}
		if scanner.Err() != nil {
			fmt.Fprintln(os.Stderr, red(scanner.Err()))
			os.Exit(1)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(t)
	resultsChan := make(chan string)
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for range t {
		go func() {
			defer wg.Done()

			execCache := newExecCache()
			for {
				input, open := <-inputsChan
				if !open {
					return
				}

				resultStr, err := execCache.executeBloblangMapping(exec, raw, pretty, input)
				if err != nil {
					fmt.Fprintln(os.Stderr, red(fmt.Sprintf("failed to execute map: %v", err)))
					continue
				}
				resultsChan <- resultStr
			}
		}()
	}

	for res := range resultsChan {
		fmt.Println(res)
	}
	os.Exit(0)
	return nil
}
