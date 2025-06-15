package blobl

import (
	"context"
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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"

	_ "embed"
)

//go:embed resources/bloblang_editor_page.html
var bloblangEditorPage string

// Assets for the new playground
//
//go:embed resources/playground/playground.html
var playgroundPage string

//go:embed resources/playground/assets/css/main.css
var mainCSS string

//go:embed resources/playground/assets/css/components.css
var componentsCSS string

//go:embed resources/playground/assets/css/ace-theme.css
var aceThemeCSS string

//go:embed resources/playground/js/utils.js
var utilsJS string

//go:embed resources/playground/js/ui.js
var uiJS string

//go:embed resources/playground/js/editor.js
var editorJS string

//go:embed resources/playground/js/playground.js
var playgroundJS string

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

type fileSync struct {
	mut sync.Mutex

	dirty         bool
	mappingString string
	inputString   string

	writeBack   bool
	mappingFile string
	inputFile   string
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

// Serve embedded static assets
func serveEmbeddedAsset(w http.ResponseWriter, r *http.Request, content string, contentType string) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "public, max-age=3600") // Cache for 1 hour
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(content))
}

// Serve static files from filesystem if they exist, otherwise serve embedded
func serveStaticFile(w http.ResponseWriter, r *http.Request, staticDir string) {
	// Clean the path to prevent directory traversal
	cleanPath := filepath.Clean(r.URL.Path)
	if strings.Contains(cleanPath, "..") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Try to serve from filesystem first (for development)
	if staticDir != "" {
		fsPath := filepath.Join(staticDir, cleanPath)
		if _, err := os.Stat(fsPath); err == nil {
			http.ServeFile(w, r, fsPath)
			return
		}
	}

	// Serve embedded assets as fallback
	var content string
	var contentType string

	switch cleanPath {
	case "/assets/css/main.css":
		content = mainCSS
		contentType = "text/css"
	case "/assets/css/components.css":
		content = componentsCSS
		contentType = "text/css"
	case "/assets/css/ace-theme.css":
		content = aceThemeCSS
		contentType = "text/css"
	case "/js/utils.js":
		content = utilsJS
		contentType = "application/javascript"
	case "/js/ui.js":
		content = uiJS
		contentType = "application/javascript"
	case "/js/editor.js":
		content = editorJS
		contentType = "application/javascript"
	case "/js/playground.js":
		content = playgroundJS
		contentType = "application/javascript"
	default:
		http.NotFound(w, r)
		return
	}

	serveEmbeddedAsset(w, r, content, contentType)
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

	// Determine which editor to use
	var pageTemplate string
	if c.Bool("playground") {
		pageTemplate = playgroundPage

		// Only serve static assets for playground
		mux.HandleFunc("/assets/", func(w http.ResponseWriter, r *http.Request) {
			serveStaticFile(w, r, "resources")
		})
		mux.HandleFunc("/js/", func(w http.ResponseWriter, r *http.Request) {
			serveStaticFile(w, r, "resources")
		})
	} else {
		pageTemplate = bloblangEditorPage
	}

	indexTemplate := template.Must(template.New("index").Parse(pageTemplate))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		err := indexTemplate.Execute(w, struct {
			InitialInput   string
			InitialMapping string
		}{
			fSync.input(),
			fSync.mapping(),
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

	log.Printf("Serving at: http://%v\n", bindAddress)

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
