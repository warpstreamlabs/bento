class BloblangPlayground {
  constructor() {
    // If BLOBLANG_SYNTAX is defined in index.html, we're running through the Go server
    const isServerMode =
      typeof window.BLOBLANG_SYNTAX !== "undefined" ||
      window.location.search.includes("server=true");

    this.state = {
      wasmAvailable: false,
      executionMode: isServerMode ? "server" : "wasm",
      isExecuting: false,
      executionTimeout: null,
      inputFormatMode: "format", // "format" or "minify"
      outputFormatMode: "minify", // "format" or "minify"
      firstExecutionStartTime: null,
      CONNECTION_ERROR_DELAY: 3000, // 3 seconds before showing connection errors
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
    this.bindEvents();

    // Check if playground is in an iframe to sync light/dark mode with parent window (Docusaurus)
    if (window.parent !== window) {
      this.setupDocusaurusThemeSync();
      // Add embedded class for enhanced sizing
      document.body.classList.add("embedded");
    }

    this.init();
  }

  async init() {
    try {
      // Initialize basic editor
      this.editor.init({
        onInputChange: () => {
          this.updateLinters();
          this.debouncedExecute("input");
        },
        onMappingChange: () => {
          this.updateLinters();
          this.debouncedExecute("mapping");
        },
      });

      // Show basic editor
      this.ui.init();
      this.editor.setupDocumentationClickHandlers();
      this.updateLinters();
      this.execute();
      this.hideLoading();

      // Initialize WASM and enhanced features asynchronously
      await this.initializeWasm();

      // Re-initialize editor if WASM syntax is now available
      if (this.state.executionMode === "wasm" && !this.editor.syntaxLoaded) {
        await this.editor.loadBloblangSyntax();
        // Re-setup theme with new syntax rules
        this.editor.setupTheme();
        this.editor.configureAutocompletion();
        // Refresh the mapping editor to apply new highlighting
        this.editor.refreshSyntaxHighlighting();
      }

      // Re-execute with enhanced features
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

  async initializeWasm() {
    if (this.state.executionMode === "server" || !this.wasm) {
      return;
    }

    try {
      await this.wasm.load();

      if (this.wasm.available) {
        this.state.wasmAvailable = true;
      } else {
        this.state.executionMode = "server";
      }
    } catch (error) {
      console.warn("WASM initialization failed:", error);
      this.state.executionMode = "server";
    }
  }

  setupDocusaurusThemeSync() {
    // Hide the theme toggle since parent handles theming
    const themeToggle = document.getElementById("themeToggle");
    if (themeToggle) {
      themeToggle.style.display = "none";
    }

    // Listen for theme changes from parent window
    window.addEventListener("message", (event) => {
      if (event.data.type === "docusaurus-theme-change") {
        const isDark = event.data.theme === "dark";

        // Set data-theme attribute on document element
        document.documentElement.setAttribute(
          "data-theme",
          isDark ? "dark" : "light"
        );
      }
    });

    // Request theme from parent
    window.parent.postMessage({ type: "request-theme" }, "*");
  }

  bindEvents() {
    // Button clicks
    document.addEventListener("click", (e) => this.handleAction(e));

    // File inputs
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
      "save-output": () => saveOutput(),
      "format-mapping": () => formatBloblang(),
      "toggle-format-input": () => this.toggleFormat("input"),
      "toggle-format-output": () => this.toggleFormat("output"),
    };

    actions[action]?.();
  }

  async execute() {
    if (this.state.isExecuting) return;
    this.state.isExecuting = true;

    // Track first execution time for delayed error handling
    if (!this.state.firstExecutionStartTime) {
      this.state.firstExecutionStartTime = Date.now();
    }

    try {
      const input = this.editor.getInput();
      const mapping = this.editor.getMapping();

      let result;
      switch (this.state.executionMode) {
        case "wasm":
          if (this.wasm) {
            result = this.wasm.execute(input, mapping);
            this.handleExecution(result);
          } else {
            throw new Error("WASM not available");
          }
          break;
        case "server":
          const response = await fetch("/execute", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ input, mapping }),
          });

          if (response.ok) {
            result = await response.json();
            this.handleExecution(result);
          } else {
            throw new Error(`Server error: ${response.status}`);
          }
          break;
        default:
          throw new Error("Unknown execution mode");
      }
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
    let mappingErrorMessage = null;

    if (result && result.length > 0) {
      this.elements.outputArea.classList.add("success");
      this.ui.updateStatus("outputStatus", "success", "Success");

      if (isValidJSON(result)) {
        const formatted = formatJSON(result);
        const highlighted = syntaxHighlightJSON(formatted);
        this.elements.outputArea.innerHTML = highlighted;
        this.elements.outputArea.classList.add("json-formatted");
      } else {
        this.elements.outputArea.textContent = result.trim();
      }
    } else if (mapping_error && mapping_error.length > 0) {
      this.handleError(
        "Input Error",
        "There was an error parsing your input JSON",
        mapping_error,
        "inputPanel",
        "Invalid Input",
        "Input Error"
      );
    } else if (parse_error && parse_error.length > 0) {
      this.handleError(
        "Mapping Error",
        "There is an error in your Bloblang mapping",
        parse_error,
        "mappingPanel",
        "Invalid Mapping",
        "Mapping Error"
      );
      mappingErrorMessage = parse_error;
    }

    this.updateLinters(mappingErrorMessage);
  }

  toggleFormat(type) {
    const formatMode =
      type === "input" ? "inputFormatMode" : "outputFormatMode";
    const btn =
      type === "input"
        ? this.elements.toggleFormatInputBtn
        : this.elements.toggleFormatOutputBtn;

    if (this.state[formatMode] === "format") {
      this.state[formatMode] = "minify";
      btn.textContent = "Minify";
      type === "input" ? formatInput() : formatOutput();
    } else {
      this.state[formatMode] = "format";
      btn.textContent = "Format";
      type === "input" ? minifyInput() : minifyOutput();
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

  updateLinters(mappingErrorMessage = null) {
    updateInputLinter(this.editor.getInput());
    updateMappingLinter(this.editor.getMapping(), mappingErrorMessage);
    updateOutputLinter(this.elements.outputArea.textContent);
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
    if (timeSinceFirstExecution > this.state.CONNECTION_ERROR_DELAY) {
      this.handleError(
        "Connection Error",
        "Ensure Bloblang server is running and try again",
        error.message
      );
    } else {
      // Show a brief loading message instead
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
}

// Initialize when DOM is ready
document.addEventListener("DOMContentLoaded", () => {
  window.playground = new BloblangPlayground();
});
