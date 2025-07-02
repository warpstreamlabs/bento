package blobl

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

type fileSync struct {
	mut sync.Mutex

	dirty         bool
	mappingString string
	inputString   string

	writeBack   bool
	mappingFile string
	inputFile   string
}

//go:embed resources/playground
var playgroundFS embed.FS

var bloblangPlaygroundPage string

func init() {
	page, err := playgroundFS.ReadFile("resources/playground/index.html")
	if err != nil {
		log.Fatalf("Failed to read embedded playground: %v", err)
	}
	bloblangPlaygroundPage = string(page)
}

func openBrowserAt(url string) {
	switch runtime.GOOS {
	case "linux":
		_ = exec.Command("xdg-open", url).Start()
	case "windows":
		_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		_ = exec.Command("open", url).Start()
	}
}

// Generates and marshals the Bloblang syntax spec as template.JS for HTML templates
func generateBloblangSyntaxTemplate() (template.JS, error) {
	syntax, err := GenerateBloblangSyntax(bloblang.GlobalEnvironment())
	if err != nil {
		return "", fmt.Errorf("failed to generate bloblang syntax: %w", err)
	}

	jsonBytes, err := json.Marshal(syntax)
	if err != nil {
		return "", fmt.Errorf("failed to marshal bloblang syntax: %w", err)
	}

	return template.JS(jsonBytes), nil
}

func newFileSync(inputFile, mappingFile string, writeBack bool) *fileSync {
	f := &fileSync{
		inputString:   `{"message":"hello world"}`,
		mappingString: "root = this",
		writeBack:     writeBack,
		inputFile:     inputFile,
		mappingFile:   mappingFile,
	}

	if inputFile != "" {
		inputBytes, err := ifs.ReadFile(ifs.OS(), inputFile)
		if err != nil {
			if !writeBack || !errors.Is(err, fs.ErrNotExist) {
				log.Fatal(err)
			}
		} else {
			f.inputString = string(inputBytes)
		}
	}

	if mappingFile != "" {
		mappingBytes, err := ifs.ReadFile(ifs.OS(), mappingFile)
		if err != nil {
			if !writeBack || !errors.Is(err, fs.ErrNotExist) {
				log.Fatal(err)
			}
		} else {
			f.mappingString = string(mappingBytes)
		}
	}

	if writeBack {
		go func() {
			t := time.NewTicker(time.Second * 5)
			for {
				<-t.C
				f.write()
			}
		}()
	}

	return f
}

func (f *fileSync) update(input, mapping string) {
	f.mut.Lock()
	if mapping != f.mappingString || input != f.inputString {
		f.dirty = true
	}
	f.mappingString = mapping
	f.inputString = input
	f.mut.Unlock()
}

func (f *fileSync) write() {
	f.mut.Lock()
	defer f.mut.Unlock()

	if !f.writeBack || !f.dirty {
		return
	}

	if f.inputFile != "" {
		if err := ifs.WriteFile(ifs.OS(), f.inputFile, []byte(f.inputString), 0o644); err != nil {
			log.Printf("Failed to write input file: %v\n", err)
		}
	}
	if f.mappingFile != "" {
		if err := ifs.WriteFile(ifs.OS(), f.mappingFile, []byte(f.mappingString), 0o644); err != nil {
			log.Printf("Failed to write mapping file: %v\n", err)
		}
	}
	f.dirty = false
}

func (f *fileSync) input() string {
	f.mut.Lock()
	defer f.mut.Unlock()
	return f.inputString
}

func (f *fileSync) mapping() string {
	f.mut.Lock()
	defer f.mut.Unlock()
	return f.mappingString
}

func runServer(c *cli.Context) error {
	fSync := newFileSync(c.String("input-file"), c.String("mapping-file"), c.Bool("write"))
	defer fSync.write()

	mux := http.NewServeMux()

	mux.HandleFunc("/execute", func(w http.ResponseWriter, r *http.Request) {
		req := struct {
			Mapping string `json:"mapping"`
			Input   string `json:"input"`
		}{}
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fSync.update(req.Input, req.Mapping)

		res := struct {
			ParseError   string `json:"parse_error"`
			MappingError string `json:"mapping_error"`
			Result       string `json:"result"`
		}{}
		defer func() {
			resBytes, err := json.Marshal(res)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			_, _ = w.Write(resBytes)
		}()

		exec, err := bloblang.GlobalEnvironment().NewMapping(req.Mapping)
		if err != nil {
			if perr, ok := err.(*parser.Error); ok {
				res.ParseError = fmt.Sprintf("failed to parse mapping: %v\n", perr.ErrorAtPositionStructured("", []rune(req.Mapping)))
			} else {
				res.ParseError = err.Error()
			}
			return
		}

		execCache := newExecCache()
		output, err := execCache.executeMapping(exec, false, true, []byte(req.Input))
		if err != nil {
			res.MappingError = err.Error()
		} else {
			res.Result = output
		}
	})

	assetsFS, err := fs.Sub(playgroundFS, "resources/playground/assets")
	if err != nil {
		return fmt.Errorf("failed to get assets subFS: %w", err)
	}
	jsFS, err := fs.Sub(playgroundFS, "resources/playground/js")
	if err != nil {
		return fmt.Errorf("failed to get js subFS: %w", err)
	}

	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assetsFS))))
	mux.Handle("/js/", http.StripPrefix("/js/", http.FileServer(http.FS(jsFS))))

	indexTemplate := template.Must(template.New("index").Parse(bloblangPlaygroundPage))
	bloblangSyntaxTemplate, err := generateBloblangSyntaxTemplate()
	if err != nil {
		return err
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		err := indexTemplate.Execute(w, struct {
			InitialInput   string
			InitialMapping string
			BloblangSyntax template.JS
		}{
			fSync.input(),
			fSync.mapping(),
			bloblangSyntaxTemplate,
		})

		if err != nil {
			http.Error(w, "Template error", http.StatusBadGateway)
			return
		}
	})

	host, port := c.String("host"), c.String("port")
	bindAddress := host + ":" + port

	if !c.Bool("no-open") {
		u, err := url.Parse("http://localhost:" + port)
		if err != nil {
			return fmt.Errorf("failed to parse URL: %w", err)
		}
		openBrowserAt(u.String())
	}

	log.Printf("Serving at: http://%s?mode=server", bindAddress)

	server := http.Server{
		Addr:    bindAddress,
		Handler: mux,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Wait for termination signal
		<-sigChan
		_ = server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}
	return nil
}
