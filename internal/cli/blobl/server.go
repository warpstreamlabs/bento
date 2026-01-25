package blobl

import (
	"context"
	"embed"
	"encoding/json"
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
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

const (
	defaultPlaygroundInput   = `{"name": "bento", "type": "stream_processor", "features": ["fast", "fancy"], "stars": 1500}`
	defaultPlaygroundMapping = `root.about = "%s 🍱 is a %s %s".format(this.name.capitalize(), this.features.join(" & "), this.type.split("_").join(" "))
root.stars = "★".repeat((this.stars / 300))`
)

//go:embed playground
var playgroundFS embed.FS
var bloblangPlaygroundPage string

func init() {
	page, err := playgroundFS.ReadFile("playground/index.html")
	if err != nil {
		log.Fatalf("Failed to read embedded playground: %v", err)
	}
	bloblangPlaygroundPage = string(page)
}

type server struct {
	env   *bloblang.Environment
	fSync *fileSync
	mux   *http.ServeMux
}

func newServer(inputFile, mappingFile string, writeBack bool) *server {
	return &server{
		env:   bloblang.GlobalEnvironment(),
		fSync: newFileSync(inputFile, mappingFile, writeBack),
		mux:   http.NewServeMux(),
	}
}

func (s *server) registerRoutes() error {
	// API endpoints
	s.mux.HandleFunc("/execute", s.handleExecute)
	s.mux.HandleFunc("/validate", s.handleValidate)
	s.mux.HandleFunc("/syntax", s.handleSyntax)
	s.mux.HandleFunc("/format", s.handleFormat)
	s.mux.HandleFunc("/autocomplete", s.handleAutocomplete)

	// Static assets
	assetsFS, err := fs.Sub(playgroundFS, "playground/assets")
	if err != nil {
		return fmt.Errorf("failed to get assets subFS: %w", err)
	}
	jsFS, err := fs.Sub(playgroundFS, "playground/js")
	if err != nil {
		return fmt.Errorf("failed to get js subFS: %w", err)
	}

	s.mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assetsFS))))
	s.mux.Handle("/js/", http.StripPrefix("/js/", http.FileServer(http.FS(jsFS))))

	// Index page
	s.mux.HandleFunc("/", s.handleIndex)

	return nil
}

func (s *server) listenAndServe(host, port string, noOpen bool) error {
	bindAddress := host + ":" + port

	if !noOpen {
		u, err := url.Parse("http://localhost:" + port)
		if err != nil {
			return fmt.Errorf("failed to parse URL: %w", err)
		}
		openBrowserAt(u.String())
	}

	fmt.Printf("✓ Playground serving at: http://%s\n", bindAddress)

	server := &http.Server{
		Addr:    bindAddress,
		Handler: s.mux,
	}

	go s.handleShutdown(server)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}
	return nil
}

func (s *server) handleShutdown(server *http.Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	s.fSync.write() // Ensure final write before shutdown
	_ = server.Shutdown(context.Background())
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	indexTemplate := template.Must(template.New("index").Parse(bloblangPlaygroundPage))

	bloblangSyntaxTemplate, err := s.generateSyntaxTemplate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	initialInputJSON, _ := json.Marshal(s.fSync.input())
	initialMappingJSON, _ := json.Marshal(s.fSync.mapping())

	err = indexTemplate.Execute(w, struct {
		WasmMode       bool
		InitialInput   template.JS
		InitialMapping template.JS
		BloblangSyntax template.JS
	}{
		WasmMode:       false, // WASM not available in server mode
		InitialInput:   template.JS(initialInputJSON),
		InitialMapping: template.JS(initialMappingJSON),
		BloblangSyntax: bloblangSyntaxTemplate,
	})

	if err != nil {
		http.Error(w, "Template error", http.StatusInternalServerError)
	}
}

func (s *server) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mapping string `json:"mapping"`
		Input   string `json:"input"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.fSync.update(req.Input, req.Mapping)

	response := executeBloblangMapping(s.env, req.Input, req.Mapping)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Result       any `json:"result"`
		ParseError   any `json:"parse_error"`
		MappingError any `json:"mapping_error"`
	}{
		Result:       response.Result,
		ParseError:   response.ParseError,
		MappingError: response.MappingError,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleFormat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Mapping string `json:"mapping"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.Mapping == "" {
		http.Error(w, "Mapping cannot be empty", http.StatusBadRequest)
		return
	}

	response, err := formatBloblangMapping(s.env, request.Mapping)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleAutocomplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request AutocompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	response, err := generateAutocompletion(s.env, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleSyntax(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response, err := generateBloblangSyntax(s.env)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Mapping string `json:"mapping"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	response, err := validateBloblangMapping(s.env, request.Mapping)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) generateSyntaxTemplate() (template.JS, error) {
	syntax, err := generateBloblangSyntax(s.env)
	if err != nil {
		return "", fmt.Errorf("failed to generate bloblang syntax: %w", err)
	}

	jsonBytes, err := json.Marshal(syntax)
	if err != nil {
		return "", fmt.Errorf("failed to marshal bloblang syntax: %w", err)
	}

	return template.JS(jsonBytes), nil
}

func openBrowserAt(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		return
	}
	_ = cmd.Start()
}

func runPlayground(c *cli.Context) error {
	server := newServer(
		c.String("input-file"),
		c.String("mapping-file"),
		c.Bool("write"),
	)

	if err := server.registerRoutes(); err != nil {
		return err
	}

	return server.listenAndServe(
		c.String("host"),
		c.String("port"),
		c.Bool("no-open"),
	)
}
