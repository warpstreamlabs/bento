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

    this.stateStored = true;
  }

  async init(callbacks = {}) {
    if (!ace) {
      console.error("Unable to load ACE");
      return;
    }

    this.callbacks = callbacks;
    this.loadState();

    // Prevent ACE from loading external snippet files
    ace.config.setModuleUrl("ace/snippets/coffee", "data:text/javascript,");
    ace.define(THEME_BENTO, function (require, exports, module) {
      exports.cssClass = "ace-bento";
    });

    try {
      this.configureInputEditor();
      this.configureMappingEditor();
      this.registerEditorChangeHandlers();
      this.loadSyntaxAsync();
    } catch (error) {
      console.warn("ACE editor failed to load, falling back to textarea");
      this.configureFallbackEditors();
    }
  }

  async loadSyntax() {
    try {
      // Try direct injection (server mode)
      if (typeof window.BLOBLANG_SYNTAX !== "undefined") {
        this.bloblangSyntax = window.BLOBLANG_SYNTAX;
        this.syntaxLoaded = true;
        return;
      }

      // Try WASM API
      if (window.playground && window.playground.api) {
        this.bloblangSyntax = window.playground.api.getSyntax();
        this.syntaxLoaded = true;
      } else {
        this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
      }
    } catch (error) {
      console.warn("Failed to load syntax:", error);
      this.bloblangSyntax = { functions: {}, methods: {}, rules: [] };
    }
  }

  async loadSyntaxAsync() {
    await this.loadSyntax();

    // If WASM mode and not loaded, retry (WASM may still be initializing)
    if (!this.syntaxLoaded && typeof window.BLOBLANG_SYNTAX === "undefined") {
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second
      await this.loadSyntax();
    }

    if (!this.syntaxLoaded) {
      return;
    }

    this.setupTheme();
    this.applyBloblangMode();
    this.configureAutocompletion();
    this.refreshSyntaxHighlighting();
  }

  setupTheme() {
    // Force reload the mode by using a unique ID each time
    const modeId = MODE_BLOBLANG + "_" + Date.now();
    const rules = this.bloblangSyntax?.rules || [];

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

    this.currentBloblangMode = modeId;
  }

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
    const mappingEl = this.getElement(DOM_IDS.aceMapping);
    if (!mappingEl) return;

    this.aceMappingEditor = ace.edit(mappingEl);
    const contentWithNewline = this.defaultMapping.trimEnd() + "\n"; // Trailing newline
    this.aceMappingEditor.setValue(contentWithNewline, 1);

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

  applyBloblangMode() {
    if (!this.aceMappingEditor) return;

    this.aceMappingEditor.session.setMode(
      this.currentBloblangMode || MODE_BLOBLANG
    );
  }

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
    this.toggleEditorDisplay(DOM_IDS.aceInput, DOM_IDS.fallbackInput);
    this.toggleEditorDisplay(DOM_IDS.aceMapping, DOM_IDS.fallbackMapping);

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
    this.callbacks.onInputChange?.();
    this.debouncedSaveState();
  }

  handleMappingChange() {
    this.callbacks.onMappingChange?.();
    this.debouncedSaveState();
  }

  debouncedSaveState() {
    clearTimeout(this.saveStateTimeout);
    this.saveStateTimeout = setTimeout(() => this.saveState(), 1000);
  }

  /**
   * Insert trailing newline when commenting the last line.
   * Prevents Bloblang parse errors when last line is a comment without a newline.
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

        if (
          lastLineIndex === editorInstance.getCursorPosition().row &&
          lastLine.trim().startsWith("#")
        ) {
          session.insert({ row: lastLineIndex + 1, column: 0 }, "\n");
        }
      },
    });
  }

  static BLOBLANG_KEYWORDS = [
    { name: "root", description: "The root of the output document" },
    { name: "this", description: "The current context value" },
    { name: "if", description: "Conditional expression" },
    { name: "else", description: "Alternative branch" },
    { name: "match", description: "Pattern matching expression" },
    { name: "let", description: "Variable assignment" },
  ];

  configureAutocompletion() {
    if (!this.aceMappingEditor || !this.syntaxLoaded || !this.bloblangSyntax) return;

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
            if (!window.playground || !window.playground.api) {
              console.log("Autocomplete: API not available");
              return callback(null, []);
            }

            let result = await window.playground.api.autocomplete(request);

            if (
              result &&
              result.success &&
              result.completions &&
              result.completions.length > 0
            ) {
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

  setupDocumentationClickHandlers() {
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
