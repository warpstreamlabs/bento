class BloblangPlayground {
  constructor() {
    this.state = {
      isExecuting: false,
      executionTimeout: null,
      inputFormatMode: "format",
      outputFormatMode: "minify",
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

    this.editor = new EditorManager();
    this.ui = new UIManager();
    this.bindEvents();
    this.init();
  }

  async init() {
    try {
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
      this.ui.init();
      this.updateLinters();
      this.execute();
      this.hideLoading();
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

    try {
      const input = this.editor.getInput();
      const mapping = this.editor.getMapping();
      const response = await fetch("/execute", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ input, mapping }),
      });

      if (response.ok) {
        const result = await response.json();
        this.handleExecution(result);
      } else {
        throw new Error(`Server error: ${response.status}`);
      }
    } catch (error) {
      this.handleError(
        "Connection Error",
        "Ensure Bloblang server is running and try again",
        error.message
      );
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
