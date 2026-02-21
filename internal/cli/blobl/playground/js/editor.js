const THEME_BENTO = "ace/theme/bento";
const MODE_JSON = "ace/mode/json";
const MODE_BLOBLANG = "ace/mode/bloblang";
const STORAGE_KEY = "bloblang-playground-state";
const SYNTAX_TIMEOUT = 5000; // 5 seconds before showing warning

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

    // Syntax loading state
    this.syntaxLoadingStartTime = null;
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
    // Try injected syntax first (server mode)
    if (typeof window.BLOBLANG_SYNTAX !== "undefined") {
      this.bloblangSyntax = window.BLOBLANG_SYNTAX;
      this.syntaxLoaded = true;
      return;
    }

    // Try WASM function (WASM mode) - with retry for ready state
    const tryWasmSyntax = () => {
      if (typeof window.generateBloblangSyntax === "function") {
        try {
          const syntaxData = window.generateBloblangSyntax();
          if (syntaxData && !syntaxData.error) {
            this.bloblangSyntax = syntaxData;
            this.syntaxLoaded = true;
            return true;
          } else {
            console.warn(
              "WASM syntax function returned error:",
              syntaxData?.error
            );
          }
        } catch (error) {
          console.warn("Failed to get syntax from WASM:", error);
        }
      }
      return false;
    };

    // Try WASM function immediately
    if (tryWasmSyntax()) return;

    // Try again if WASM is ready
    if (window.wasmReady && tryWasmSyntax()) return;

    const timeoutExceeded =
      Date.now() - this.syntaxLoadingStartTime > SYNTAX_TIMEOUT;
    if (!this.syntaxLoadingStartTime || timeoutExceeded) {
      console.warn(
        "Bloblang syntax not available - autocomplete and highlighting will be limited"
      );
    }
    this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
  }

  async loadBloblangSyntaxAsync() {
    // Start timer for delayed warning
    this.syntaxLoadingStartTime = Date.now();

    // Schedule delayed warning
    setTimeout(() => {
      if (!this.syntaxLoaded && this.syntaxLoadingStartTime) {
        console.warn(
          "Bloblang syntax taking longer than expected to load - autocomplete and highlighting will be limited"
        );
      }
    }, SYNTAX_TIMEOUT);

    // Load syntax data asynchronously
    await this.loadBloblangSyntax();

    if (this.syntaxLoaded) {
      this.setupTheme();
      this.enhanceMappingEditor();
      this.configureAutocompletion();
      this.refreshSyntaxHighlighting();
      this.syntaxLoadingStartTime = null; // Clear timer
    }
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
      const timeoutExceeded =
        Date.now() - this.syntaxLoadingStartTime > SYNTAX_TIMEOUT;
      if (!this.syntaxLoadingStartTime || timeoutExceeded) {
        console.warn("Autocompletion disabled - syntax data not available");
      }
      return;
    }

    try {
      const completer = this.createBloblangCompleter();
      this.aceMappingEditor.setOptions({
        enableBasicAutocompletion: [completer],
        enableLiveAutocompletion: true,
        enableSnippets: true,
      });
    } catch (error) {
      console.warn("Autocompletion disabled:", error);
    }
  }

  createBloblangCompleter() {
    return {
      getCompletions: (editor, session, pos, prefix, callback) => {
        const completions = [];
        const line = session.getLine(pos.row);
        const beforeCursor = line.substring(0, pos.column).trim();

        // Don't suggest completions on comments
        if (beforeCursor.includes("#")) {
          return callback(null, []);
        }

        const pushSpecCompletion = (spec, isMethod) => {
          const entry = {
            caption: spec.name,
            meta: this.getMetaCategory(spec),
            type: isMethod ? "method" : "function",
            score: this.getCompletionScore(spec),
            docHTML: this.createDocumentationHTML(spec, { isMethod }),
          };

          // If spec has parameters, place cursor inside parentheses
          if (spec.params?.named?.length > 0 || spec.params?.variadic) {
            entry.snippet = `${spec.name}($1)`;
          } else {
            entry.value = `${spec.name}()`;
          }

          completions.push(entry);
        };

        const isMethodContext = /\.\w*$/.test(beforeCursor);

        if (isMethodContext) {
          Object.values(this.bloblangSyntax.methods || {}).forEach((spec) =>
            pushSpecCompletion(spec, true)
          );
        } else {
          Object.values(this.bloblangSyntax.functions || {}).forEach((spec) =>
            pushSpecCompletion(spec, false)
          );

          for (const keyword of EditorManager.BLOBLANG_KEYWORDS) {
            if (keyword.name.startsWith(prefix.toLowerCase())) {
              completions.push({
                caption: keyword.name,
                value: keyword.name,
                meta: "keyword",
                type: "keyword",
                score: 900,
                docHTML: `
                <div class="ace-doc">
                  <strong>${keyword.name}</strong><br/>
                  ${keyword.description}
                </div>`,
              });
            }
          }
        }

        callback(null, completions);
      },
    };
  }

  // Prioritize by status
  getCompletionScore({ status }) {
    return (
      {
        stable: 1000,
        beta: 800,
        experimental: 600,
        deprecated: 200,
      }[status] || 700
    );
  }

  // Categorize by name patterns
  getMetaCategory({ name, status }) {
    if (status === "deprecated") return "Deprecated";
    if (status === "experimental") return "Experimental";

    const patterns = {
      JSON: /json/,
      String: /string|case$/,
      Time: /time|date/,
      Math: /math|calc/,
      Crypto: /crypto|hash/,
      Environment: /env|hostname/,
      "ID Generation": /uuid|nanoid/,
      Random: /random/,
    };

    return (
      Object.entries(patterns).find(([_, regex]) => regex.test(name))?.[0] ||
      "General"
    );
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
