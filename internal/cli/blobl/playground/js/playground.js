class BloblangPlayground {
  static CONNECTION_ERROR_DELAY = 3000;

  constructor() {
    // If BLOBLANG_SYNTAX is defined in index.html, we're running through the Go server
    const isServerMode =
      typeof window.BLOBLANG_SYNTAX !== "undefined" ||
      window.location.search.includes("server=true");

    this.state = {
      executionMode: isServerMode ? "server" : "wasm",
      isExecuting: false,
      executionTimeout: null,
      inputFormatMode: "format", // "format" or "minify"
      outputFormatMode: "minify", // "format" or "minify"
      firstExecutionStartTime: null,
    };

    this.elements = {
      loadingOverlay: document.getElementById("loadingOverlay"),
      outputArea: document.getElementById("output"),
      inputPanel: document.getElementById("inputPanel"),
      mappingPanel: document.getElementById("mappingPanel"),
      inputFileInput: document.getElementById("inputFileInput"),
      mappingFileInput: document.getElementById("mappingFileInput"),
      toggleFormatInputBtn: document.getElementById("toggleFormatInputBtn"),
      toggleFormatOutputBtn: document.getElementById("toggleFormatOutputBtn"),
    };

    this.editor = new EditorManager(
      window.INITIAL_INPUT,
      window.INITIAL_MAPPING
    );
    this.ui = new UIManager();
    this.wasm = typeof WasmManager !== "undefined" ? new WasmManager() : null;
    this.api = null; // Will be set after WASM loads or in server mode
    this.bindEvents();

    // Check if playground is in an iframe to sync light/dark mode with parent window (Docusaurus)
    if (window.parent !== window) {
      this.setupDocusaurusThemeSync();
      document.body.classList.add("embedded");
    }

    this.init();
  }

  async init() {
    try {
      this.editor.init({
        onInputChange: () => {
          this.debouncedExecute("input");
        },
        onMappingChange: () => {
          this.debouncedExecute("mapping");
        },
      });

      this.ui.init();
      this.editor.setupDocumentationClickHandlers();

      if (this.state.executionMode === "wasm") {
        // WASM mode: load WASM then setup API
        await this.wasm.load();

        if (!this.wasm.isLoaded || !window.bloblangApi) {
          throw new Error("Failed to load WebAssembly module");
        }

        this.api = new BloblangAPI("wasm", window.bloblangApi);

        if (!this.editor.syntaxLoaded) {
          await this.editor.loadSyntax();
          this.editor.setupTheme();
          this.editor.configureAutocompletion();
          this.editor.refreshSyntaxHighlighting();
        }
      } else {
        this.api = new BloblangAPI("server");
      }

      this.hideLoading();
      this.execute();
    } catch (error) {
      console.error("Application error:", error);
      this.elements.loadingOverlay.innerHTML = `
        <div style="color: var(--bento-error); text-align: center;">
          <h3>Failed to Load Playground</h3>
          <p>${error.message}</p>
        </div>
      `;
    }
  }

  setupDocusaurusThemeSync() {
    const themeToggle = document.getElementById("themeToggle");
    if (themeToggle) {
      themeToggle.style.display = "none";
    }

    window.addEventListener("message", (event) => {
      if (event.data.type === "docusaurus-theme-change") {
        const isDark = event.data.theme === "dark";

        document.documentElement.setAttribute(
          "data-theme",
          isDark ? "dark" : "light"
        );
      }
    });

    window.parent.postMessage({ type: "request-theme" }, "*");
  }

  bindEvents() {
    document.addEventListener("click", (e) => this.handleAction(e));

    this.elements.inputFileInput.addEventListener("change", (e) =>
      this.handleFileLoad(e, "input")
    );
    this.elements.mappingFileInput.addEventListener("change", (e) =>
      this.handleFileLoad(e, "mapping")
    );
  }

  handleAction(e) {
    const action = e.target.dataset.action;
    const actions = {
      "copy-input": () => copyToClipboard(this.editor.getInput()),
      "copy-mapping": () => copyToClipboard(this.editor.getMapping()),
      "copy-output": () =>
        copyToClipboard(this.elements.outputArea.textContent),
      "load-input": () => this.elements.inputFileInput.click(),
      "load-mapping": () => this.elements.mappingFileInput.click(),
      "save-output": () => this.saveOutput(),
      "format-mapping": () => this.formatMapping(),
      "toggle-format-input": () => this.toggleFormat("input"),
      "toggle-format-output": () => this.toggleFormat("output"),
    };

    actions[action]?.();
  }

  async execute() {
    if (this.state.isExecuting) return;
    this.state.isExecuting = true;

    if (!this.state.firstExecutionStartTime) {
      this.state.firstExecutionStartTime = Date.now();
    }

    try {
      const input = this.editor.getInput();
      const mapping = this.editor.getMapping();

      const result = await this.api.execute(input, mapping);
      this.handleExecution(result);
    } catch (error) {
      this.handleConnectionError(error);
    } finally {
      this.state.isExecuting = false;
    }
  }

  debouncedExecute(type) {
    if (this.state.executionTimeout) {
      clearTimeout(this.state.executionTimeout);
    }

    this.ui.updateStatus(`${type}Status`, "executing", "Processing...");

    this.state.executionTimeout = setTimeout(() => {
      this.execute();
    }, 300);
  }

  handleExecution(response) {
    this.resetErrorStates();

    const { result, mapping_error, parse_error } = response;

    if (result && result.length > 0) {
      this.elements.outputArea.classList.add("success");
      this.ui.updateStatus("outputStatus", "success", "Success");
      this.elements.toggleFormatOutputBtn.disabled = false;

      if (isValidJSON(result)) {
        const formatted = formatJSON(result);
        const highlighted = syntaxHighlightJSON(formatted);
        this.elements.outputArea.innerHTML = highlighted;
        this.elements.outputArea.classList.add("json-formatted");
      } else {
        this.elements.outputArea.textContent = result.trim();
      }
    } else if (mapping_error && mapping_error.length > 0) {
      this.elements.toggleFormatOutputBtn.disabled = true;
      this.handleError(
        "Input Error",
        "There was an error parsing your input JSON",
        mapping_error,
        "inputPanel",
        "Invalid Input"
      );
    } else if (parse_error && parse_error.length > 0) {
      this.elements.toggleFormatOutputBtn.disabled = true;
      this.handleError(
        "Mapping Error",
        "There is an error in your Bloblang mapping",
        parse_error,
        "mappingPanel",
        "Invalid Mapping"
      );
    }
  }

  toggleFormat(type) {
    const isInput = type === "input";
    const stateKey = isInput ? "inputFormatMode" : "outputFormatMode";
    const btn = isInput ? this.elements.toggleFormatInputBtn : this.elements.toggleFormatOutputBtn;

    if (this.state[stateKey] === "format") {
      this.state[stateKey] = "minify";
      btn.textContent = "Minify";
      isInput ? this.formatInput() : this.formatOutput();
    } else {
      this.state[stateKey] = "format";
      btn.textContent = "Format";
      isInput ? this.minifyInput() : this.minifyOutput();
    }
  }

  handleFileLoad(event, type) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      const content = e.target.result;
      if (type === "input") {
        this.editor.setInput(content);
      } else {
        this.editor.setMapping(content);
      }
      this.ui.showNotification(`Loaded ${file.name}`, "success");
      this.execute();
    };

    reader.readAsText(file);
  }

  hideLoading() {
    this.elements.loadingOverlay.classList.add("hidden");
  }

  resetErrorStates() {
    const { inputPanel, mappingPanel, outputArea } = this.elements;
    inputPanel.classList.remove("error");
    mappingPanel.classList.remove("error");
    outputArea.classList.remove("error", "success", "json-formatted");
  }

  handleConnectionError(error) {
    const timeSinceFirstExecution = this.state.firstExecutionStartTime
      ? Date.now() - this.state.firstExecutionStartTime
      : 0;

    // Only show connection error if we've been trying for a while or this isn't the first execution
    if (timeSinceFirstExecution > BloblangPlayground.CONNECTION_ERROR_DELAY) {
      this.elements.toggleFormatOutputBtn.disabled = true;
      this.handleError(
        "Connection Error",
        "Ensure Bloblang server is running and try again",
        error.message
      );
    } else {
      this.elements.outputArea.textContent =
        "Initializing Bloblang execution...";
      this.ui.updateStatus("outputStatus", "executing", "Initializing...");
    }
  }

  handleError(
    title,
    message,
    error,
    errorPanelClass = "outputArea",
    statusLabel = "Error"
  ) {
    const panel = this.elements[errorPanelClass];
    if (panel) panel.classList.add("error");
    this.elements.outputArea.classList.add("error");
    this.elements.outputArea.innerHTML = `
      <div class="error-message">
        <div class="error-title">${title}</div>
        <div>${message}</div>
        ${error ? `<div class="error-details">${error}</div>` : ""}
      </div>
    `;
    this.ui.updateStatus("outputStatus", "error", statusLabel);
  }

  formatInput() {
    const formatted = formatJSON(this.editor.getInput());
    if (formatted !== this.editor.getInput()) {
      this.editor.setInput(formatted);
    }
  }

  minifyInput() {
    const minified = minifyJSON(this.editor.getInput());
    if (minified !== this.editor.getInput()) {
      this.editor.setInput(minified);
    }
  }

  formatOutput() {
    const formatted = formatJSON(this.elements.outputArea.textContent);
    if (formatted !== this.elements.outputArea.textContent) {
      this.elements.outputArea.innerHTML = syntaxHighlightJSON(formatted);
      this.elements.outputArea.classList.add("json-formatted");
    }
  }

  minifyOutput() {
    const minified = minifyJSON(this.elements.outputArea.textContent);
    if (minified !== this.elements.outputArea.textContent) {
      this.elements.outputArea.innerHTML = syntaxHighlightJSON(minified);
      this.elements.outputArea.classList.remove("json-formatted");
    }
  }

  async formatMapping() {
    const result = await this.api.format(this.editor.getMapping());
    if (result.success) this.editor.setMapping(result.formatted);
  }

  async saveOutput() {
    const output = this.elements.outputArea.textContent;
    const timestamp = new Date().toISOString().slice(0, 19).replace(/[:.]/g, "-");
    downloadFile(output, `bloblang-output-${timestamp}.txt`, "text/plain");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  window.playground = new BloblangPlayground();
});
