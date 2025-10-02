const THEME_BENTO = "ace/theme/bento";
const MODE_JSON = "ace/mode/json";
const MODE_BLOBLANG = "ace/mode/bloblang";
const STORAGE_KEY = "bloblang-playground-state";

const DOM_IDS = {
  aceInput: "aceInput",
  fallbackInput: "fallbackInput",
  aceMapping: "aceMapping",
  fallbackMapping: "fallbackMapping",
};

class EditorManager {
  constructor(
    defaultInput = '{"name": "bento", "stars": 1500}',
    defaultMapping = 'root.project = "%s 🍱".format(this.name.capitalize())\nroot.rating = "★".repeat((this.stars / 300))'
  ) {
    this.callbacks = {};
    this.aceInputEditor = null;
    this.aceMappingEditor = null;

    // Uses the injected syntax from the HTML template (server mode) or WASM function
    this.bloblangSyntax = null;
    this.syntaxLoaded = false;

    this.defaultInput = defaultInput || window.DEFAULT_INPUT;
    this.defaultMapping = defaultMapping || window.DEFAULT_MAPPING;

    // Future-proof event listeners
    this.inputChangeListeners = [];
    this.mappingChangeListeners = [];

    // State persistence
    this.stateStored = true;
  }

  // ─────────────────────────────────────────────────────
  // Initialization & Setup
  // ─────────────────────────────────────────────────────

  async init(callbacks = {}) {
    if (!ace) {
      console.error("Unable to load ACE");
      return;
    }

    this.callbacks = callbacks;

    this.loadState();

    // Initialize basic ACE theme
    ace.define(THEME_BENTO, function (require, exports, module) {
      exports.cssClass = "ace-bento";
    });

    try {
      // Initialize plain editors
      this.configureInputEditor();
      this.configureMappingEditorBasic();
      this.registerEditorChangeHandlers();

      // Load Bloblang syntax asynchronously
      this.loadBloblangSyntaxAsync();
    } catch (error) {
      console.warn("ACE editor failed to load, falling back to textarea");
      this.configureFallbackEditors();
    }
  }

  async loadBloblangSyntax() {
    try {
      // First, try direct injection (server mode fallback)
      if (typeof window.BLOBLANG_SYNTAX !== "undefined") {
        this.bloblangSyntax = window.BLOBLANG_SYNTAX;
        this.syntaxLoaded = true;
        return;
      }

      // Then check playground-based modes
      if (window.playground && window.playground.state) {
        switch (window.playground.state.executionMode) {
          case "wasm":
            if (window.playground.wasm) {
              const syntaxData = window.playground.wasm.getSyntax();
              if (syntaxData && !syntaxData.error) {
                this.bloblangSyntax = syntaxData;
                this.syntaxLoaded = true;
              } else {
                console.warn(
                  "WASM syntax function returned error:",
                  syntaxData?.error
                );
                this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
              }
            } else {
              console.warn("WASM mode but wasm manager not available");
              this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
            }
            break;

          default:
            console.warn("Unknown execution mode, using fallback syntax");
            this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
        }
      } else {
        this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
      }
    } catch (error) {
      console.warn("Failed to load syntax:", error);
      this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
    }
  }

  async loadBloblangSyntaxAsync() {
    // Try loading syntax immediately
    await this.loadBloblangSyntax();

    // If WASM mode and not loaded, retry with a delay (WASM might still be initializing)
    if (!this.syntaxLoaded && typeof window.BLOBLANG_SYNTAX === "undefined") {
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second
      await this.loadBloblangSyntax();
    }

    // Setup editor features once syntax is available
    this.setupTheme();
    this.enhanceMappingEditor();
    this.configureAutocompletion();
    this.refreshSyntaxHighlighting();
  }

  setupTheme() {
    const rules = this.bloblangSyntax?.rules || [];

    // Force reload the mode by using a unique ID each time
    const modeId = MODE_BLOBLANG + "_" + Date.now();

    ace.define(modeId, function (require, exports, module) {
      const oop = require("../lib/oop");
      const CoffeeMode = require("./coffee").Mode;
      const CoffeeHighlightRules =
        require("./coffee_highlight_rules").CoffeeHighlightRules;

      // Use CoffeeScript as base and prepend Bloblang-specific rules for priority matching
      const BloblangHighlightRules = function () {
        CoffeeHighlightRules.call(this);
        if (rules && rules.length > 0) {
          this.$rules.start = rules.concat(this.$rules.start);
        }
      };
      oop.inherits(BloblangHighlightRules, CoffeeHighlightRules);

      const BloblangMode = function () {
        CoffeeMode.call(this);
        this.HighlightRules = BloblangHighlightRules;
        this.$id = modeId;
      };
      oop.inherits(BloblangMode, CoffeeMode);

      exports.Mode = BloblangMode;
    });

    // Update the mode constant to the new ID
    this.currentBloblangMode = modeId;
  }

  // ─────────────────────────────────────────────────────
  // Editor Initialization
  // ─────────────────────────────────────────────────────

  configureInputEditor() {
    const inputEl = this.getElement(DOM_IDS.aceInput);
    if (!inputEl) return;

    this.aceInputEditor = ace.edit(inputEl);
    this.aceInputEditor.setValue(this.defaultInput, 1);
    this.aceInputEditor.session.setMode(MODE_JSON);
    this.aceInputEditor.setTheme(THEME_BENTO);
    this.configureEditorOptions(this.aceInputEditor);
  }

  configureMappingEditor() {
    this.configureMappingEditorBasic();
    this.aceMappingEditor.session.setMode(
      this.currentBloblangMode || MODE_BLOBLANG
    );
    this.configureAutocompletion();
  }

  configureMappingEditorBasic() {
    const mappingEl = this.getElement(DOM_IDS.aceMapping);
    if (!mappingEl) return;

    this.aceMappingEditor = ace.edit(mappingEl);
    const contentWithNewline = this.defaultMapping.trimEnd() + "\n"; // Trailing newline
    this.aceMappingEditor.setValue(contentWithNewline, 1);

    // Use coffee mode initially for basic syntax highlighting
    this.aceMappingEditor.session.setMode("ace/mode/coffee");
    this.aceMappingEditor.setTheme(THEME_BENTO);
    this.addMappingEditorCommands(this.aceMappingEditor);
    this.configureEditorOptions(this.aceMappingEditor);
  }

  addMappingEditorCommands(editor) {
    editor.commands.addCommand({
      name: "formatBloblang",
      bindKey: { mac: "Cmd-Shift-F", win: "Ctrl-Shift-F" },
      exec: () => formatBloblang(),
    });
    this.overrideCommentShortcut(editor);
  }

  enhanceMappingEditor() {
    if (!this.aceMappingEditor) return;

    // Apply enhanced Bloblang mode
    this.aceMappingEditor.session.setMode(
      this.currentBloblangMode || MODE_BLOBLANG
    );
  }

  // Method to refresh syntax highlighting with new rules
  refreshSyntaxHighlighting() {
    if (this.aceMappingEditor && this.currentBloblangMode) {
      this.aceMappingEditor.session.setMode(this.currentBloblangMode);
    }
  }

  configureEditorOptions(editor) {
    editor.session.setTabSize(2);
    editor.session.setUseSoftTabs(true);
    editor.session.setUseWorker(false);
    editor.setShowPrintMargin(false);
    editor.setOption("wrap", true);
  }

  configureFallbackEditors() {
    // Show fallback textareas
    this.toggleEditorDisplay(DOM_IDS.aceInput, DOM_IDS.fallbackInput);
    this.toggleEditorDisplay(DOM_IDS.aceMapping, DOM_IDS.fallbackMapping);

    // Set up fallback listeners
    this.getElement(DOM_IDS.fallbackInput)?.addEventListener("input", () =>
      this.handleInputChange()
    );
    this.getElement(DOM_IDS.fallbackMapping)?.addEventListener("input", () =>
      this.handleMappingChange()
    );
  }

  toggleEditorDisplay(hideId, showId) {
    const hideEl = this.getElement(hideId);
    const showEl = this.getElement(showId);
    if (hideEl) hideEl.style.display = "none";
    if (showEl) showEl.style.display = "block";
  }

  registerEditorChangeHandlers() {
    this.aceInputEditor.on("change", () => this.handleInputChange());
    this.aceMappingEditor.on("change", () => this.handleMappingChange());
  }

  handleInputChange() {
    this.inputChangeListeners.forEach((fn) => fn());
    this.callbacks.onInputChange?.();
    this.debouncedSaveState();
  }

  handleMappingChange() {
    this.mappingChangeListeners.forEach((fn) => fn());
    this.callbacks.onMappingChange?.();
    this.debouncedSaveState();
  }

  debouncedSaveState() {
    clearTimeout(this.saveStateTimeout);
    this.saveStateTimeout = setTimeout(() => this.saveState(), 1000);
  }
  /**
   * Modifies ACE Editor's "toggleComment" command to insert a trailing newline
   * when commenting the last line. This prevents Bloblang parse errors that occur
   * if the final line is a comment without a newline.
   */
  overrideCommentShortcut(editor) {
    const originalCommand = editor.commands.commands["togglecomment"];
    if (!originalCommand) return;

    editor.commands.addCommand({
      name: "togglecomment",
      bindKey: originalCommand.bindKey,
      exec: (editorInstance) => {
        originalCommand.exec(editorInstance);

        const session = editorInstance.getSession();
        const lastLineIndex = session.getLength() - 1;
        const lastLine = session.getLine(lastLineIndex);

        // Adds a trailing newline to prevent parse errors on final comment lines
        if (
          lastLineIndex === editorInstance.getCursorPosition().row &&
          lastLine.trim().startsWith("#")
        ) {
          session.insert({ row: lastLineIndex + 1, column: 0 }, "\n");
        }
      },
    });
  }

  // ─────────────────────────────────────────────────────
  // Autocompletion Logic
  // ─────────────────────────────────────────────────────

  static BLOBLANG_KEYWORDS = [
    { name: "root", description: "The root of the output document" },
    { name: "this", description: "The current context value" },
    { name: "if", description: "Conditional expression" },
    { name: "else", description: "Alternative branch" },
    { name: "match", description: "Pattern matching expression" },
    { name: "let", description: "Variable assignment" },
  ];

  configureAutocompletion() {
    if (!this.aceMappingEditor) return;

    if (!this.syntaxLoaded || !this.bloblangSyntax) {
      console.warn("Autocompletion disabled - syntax data not available");
      return;
    }

    try {
      const completer = {
        getCompletions: async (editor, session, pos, prefix, callback) => {
          const line = session.getLine(pos.row);
          const beforeCursor = line.substring(0, pos.column).trim();

          const request = {
            line,
            column: pos.column,
            prefix,
            beforeCursor,
          };

          try {
            if (!window.playground || !window.playground.editor) {
              return callback(null, []);
            }

            let result;
            switch (window.playground.state.executionMode) {
              case "server":
                const response = await fetch("/autocomplete", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify(request),
                });

                if (!response.ok) {
                  throw new Error(`Server error: ${response.status}`);
                }

                result = await response.json();
                break;

              case "wasm":
                if (
                  window.playground.wasm &&
                  typeof window.playground.wasm.getAutocompletion === "function"
                ) {
                  result = window.playground.wasm.getAutocompletion(
                    JSON.stringify(request)
                  );
                  if (!result.success) {
                    return callback(null, []);
                  }
                } else {
                  console.warn("WASM autocompletion not available");
                  return callback(null, []);
                }
                break;

              default:
                console.warn(
                  "Unknown execution mode, autocompletion unavailable"
                );
                return callback(null, []);
            }

            if (result && result.success && result.completions) {
              // Convert Go completion format to ACE editor format
              const aceCompletions = result.completions.map((item) => ({
                caption: item.caption,
                value: item.value || item.caption,
                snippet: item.snippet,
                meta: item.meta,
                type: item.type,
                score: item.score,
                docHTML: item.docHTML,
              }));

              callback(null, aceCompletions);
            } else {
              console.warn("Autocompletion failed:", result?.error);
              callback(null, []);
            }
          } catch (error) {
            console.error("Autocompletion error:", error);
            callback(null, []);
          }
        },
      };

      this.aceMappingEditor.setOptions({
        enableBasicAutocompletion: [completer],
        enableLiveAutocompletion: true,
        enableSnippets: true,
      });
    } catch (error) {
      console.warn("Autocompletion disabled:", error);
    }
  }

  stripAdmonitions(text) {
    return typeof text === "string"
      ? text.replace(/:::([a-zA-Z]+)[\s\S]*?:::/g, "").trim()
      : text;
  }

  processMarkdown(text) {
    if (typeof text !== "string") return text;

    let processed = text;

    // Process inline code (backticks)
    processed = processed.replace(/`([^`]+)`/g, "<code>$1</code>");

    // Process markdown links - handle docs links specially
    processed = processed.replace(
      /\[([^\]]+)\]\(([^)]+)\)/g,
      (match, linkText, url) => {
        // If it's a docs link, convert to full URL
        if (url.startsWith("/docs/")) {
          const fullUrl = `https://warpstreamlabs.github.io/bento${url}`;
          return `<a href="${fullUrl}" target="_blank" rel="noopener">${linkText}</a>`;
        }
        // For other links, use as-is
        return `<a href="${url}" target="_blank" rel="noopener">${linkText}</a>`;
      }
    );

    return processed.trim();
  }

  formatParameterSignature(params) {
    if (!params?.named?.length) return "";

    const paramStrs = params.named.map((param) => {
      const type = param.type ? `: ${param.type}` : "";

      // Handle default values properly
      let def = "";
      if (param.default !== undefined) {
        const defaultValue = String(param.default).trim();
        if (defaultValue && defaultValue !== "") {
          def = ` = ${defaultValue}`;
        }
      }

      const token = `${param.name}${type}${def}`;
      return param.optional ? `[${token}]` : token;
    });

    return paramStrs.join(", ") + (params.variadic ? ", ..." : "");
  }

  createDocumentationHTML(spec, { isMethod = false } = {}) {
    const signature = `${isMethod ? "." : ""}${
      spec.name
    }(${this.formatParameterSignature(spec.params)})`;
    const statusBadge = spec.status
      ? `<span class="ace-status-${spec.status}">${spec.status}</span>`
      : "";
    const impureBadge =
      isMethod && spec.impure
        ? '<span class="ace-impure-badge">impure</span>'
        : "";

    // Create specific docs URL with anchor
    const docsSection = isMethod ? "methods" : "functions";
    const docsUrl = `https://warpstreamlabs.github.io/bento/docs/guides/bloblang/${docsSection}#${spec.name}`;

    const strippedDesc = this.stripAdmonitions(spec.description);
    const processedDesc = this.processMarkdown(strippedDesc);

    let html = `
    <div class="ace-doc" data-docs-url="${docsUrl}" data-function-name="${spec.name}" data-is-method="${isMethod}">
      <div class="ace-doc-header clickable-header" title="Click to view documentation">
        <div class="ace-doc-signature">
          <strong>${signature}</strong>
          ${statusBadge}
          ${impureBadge}
        </div>
      </div>`;

    // Add category information
    if (spec.category) {
      html += `<div class="ace-doc-category">Category: ${spec.category}</div>`;
    }

    // Add version information
    if (spec.version) {
      html += `<div class="ace-doc-version">Since: v${spec.version}</div>`;
    }

    if (processedDesc) {
      html += `<div class="ace-doc-description">${processedDesc}</div>`;
    }

    if (spec.params?.named?.length) {
      html += `<div class="ace-doc-parameters"><strong>Parameters:</strong>`;
      for (const param of spec.params.named) {
        const optionalText = param.optional ? " (optional)" : "";
        const defaultText =
          param.default !== undefined ? ` = ${param.default}` : "";
        const typeText = param.type ? ` [${param.type}]` : "";

        const processedParamDesc = this.processMarkdown(
          param.description || "No description"
        );

        html += `
        <div class="ace-doc-param">
          <code>${param.name}${typeText}${defaultText}${optionalText}</code><br/>
          <span class="ace-doc-param-desc">${processedParamDesc}</span>
        </div>`;
      }
      html += `</div>`;
    }

    if (spec.examples?.length) {
      html += `<div class="ace-doc-examples"><strong>Examples:</strong><br/>`;
      for (const example of spec.examples.slice(0, 2)) {
        if (example.summary) {
          const processedSummary = this.processMarkdown(example.summary);
          html += `<div class="ace-doc-example-summary">${processedSummary}</div>`;
        }
        html += `<code>${example.mapping}</code><br/>`;
      }
      html += `</div>`;
    }

    html += `</div>`;
    return html;
  }

  setupDocumentationClickHandlers() {
    // Make header and signature section clickable
    document.addEventListener("click", (event) => {
      const clickedHeader = event.target.closest(".ace-doc-header");
      const clickedSignature = event.target.closest(".ace-doc-signature");

      if (clickedHeader || clickedSignature) {
        event.preventDefault();
        event.stopPropagation();

        const aceDoc = event.target.closest(".ace-doc");
        const docsUrl = aceDoc?.dataset.docsUrl;

        if (docsUrl) {
          window.open(docsUrl, "_blank", "noopener,noreferrer");
        }
      }
    });
  }

  // ─────────────────────────────────────────────────────
  // Data Accessors
  // ─────────────────────────────────────────────────────

  /**
   * Gets the current input value from the editor or fallback.
   * @returns {string}
   */
  getInput() {
    return this.getEditorValue(this.aceInputEditor, DOM_IDS.fallbackInput);
  }

  /**
   * Gets the current mapping value from the editor or fallback.
   * @returns {string}
   */
  getMapping() {
    return this.getEditorValue(this.aceMappingEditor, DOM_IDS.fallbackMapping);
  }

  /**
   * Sets the content of the input editor or fallback.
   * @param {string} content
   */
  setInput(content) {
    this.setEditorValue(this.aceInputEditor, DOM_IDS.fallbackInput, content);
  }

  /**
   * Sets the content of the mapping editor or fallback.
   * @param {string} content
   */
  setMapping(content) {
    this.setEditorValue(
      this.aceMappingEditor,
      DOM_IDS.fallbackMapping,
      content
    );
  }

  getEditorValue(aceEditor, fallbackId) {
    return aceEditor
      ? aceEditor.getValue()
      : this.getElement(fallbackId)?.value || "";
  }

  setEditorValue(aceEditor, fallbackId, content) {
    if (aceEditor) {
      aceEditor.setValue(content, 1);
    } else {
      const fallback = this.getElement(fallbackId);
      if (fallback) fallback.value = content;
    }
  }

  /**
   * Returns a DOM element by ID.
   * @param {string} id
   * @returns {HTMLElement|null}
   */
  getElement(id) {
    return document.getElementById(id);
  }

  // ─────────────────────────────────────────────────────
  // State Persistence
  // ─────────────────────────────────────────────────────

  loadState() {
    if (!this.stateStored) return;

    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const state = JSON.parse(stored);
        if (state.input) this.defaultInput = state.input;
        if (state.mapping) this.defaultMapping = state.mapping;
      }
    } catch (error) {
      console.warn("Failed to load persisted state:", error);
    }
  }

  saveState() {
    if (!this.stateStored) return;

    try {
      const state = {
        input: this.getInput(),
        mapping: this.getMapping(),
        timestamp: Date.now(),
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (error) {
      console.warn("Failed to save state:", error);
    }
  }

  clearState() {
    if (!this.stateStored) return;

    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch (error) {
      console.warn("Failed to clear persisted state:", error);
    }
  }
}
