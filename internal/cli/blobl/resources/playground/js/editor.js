const THEME_BENTO = "ace/theme/bento";
const MODE_JSON = "ace/mode/json";
const MODE_BLOBLANG = "ace/mode/bloblang";

const DOM_IDS = {
  aceInput: "aceInput",
  fallbackInput: "fallbackInput",
  aceMapping: "aceMapping",
  fallbackMapping: "fallbackMapping",
};

class EditorManager {
  constructor(defaultInput = null, defaultMapping = null) {
    this.callbacks = {};
    this.aceInputEditor = null;
    this.aceMappingEditor = null;
    this.bloblangSyntax = null;

    this.defaultMapping =
      defaultMapping ||
      "root.greeting = this.message.uppercase()\nroot.doubled = this.number * 2";

    this.defaultInput =
      defaultInput || '{"message": "hello world", "number": 42}';

    // Future-proof event listeners
    this.inputChangeListeners = [];
    this.mappingChangeListeners = [];
  }

  // ─────────────────────────────────────────────────────
  // Initialization & Setup
  // ─────────────────────────────────────────────────────

  async init(callbacks = {}) {
    this.callbacks = callbacks;

    ace.define(THEME_BENTO, function (require, exports, module) {
      exports.cssClass = "ace-bento";
    });

    await this.loadBloblangSyntax();
    this.setupTheme();

    try {
      this.configureInputEditor();
      this.configureMappingEditor();
      this.registerEditorChangeHandlers();
    } catch (error) {
      console.warn("ACE editor failed to load, falling back to textarea");
      this.configureFallbackEditors();
    }
  }

  async loadBloblangSyntax() {
    try {
      const response = await fetch("/bloblang-syntax");
      this.bloblangSyntax = await response.json();
    } catch (error) {
      console.warn("Failed to load Bloblang syntax:", error);
      this.bloblangSyntax = {
        functions: [],
        methods: [],
        rules: [],
      };
    }
  }

  setupTheme() {
    const rules = this.bloblangSyntax?.rules || [];

    ace.define(MODE_BLOBLANG, function (require, exports, module) {
      const oop = require("../lib/oop");
      const CoffeeMode = require("./coffee").Mode;
      const CoffeeHighlightRules =
        require("./coffee_highlight_rules").CoffeeHighlightRules;

      // Use CoffeeScript as base and prepend Bloblang-specific rules for priority matching
      const BloblangHighlightRules = function () {
        CoffeeHighlightRules.call(this);
        this.$rules.start = rules.concat(this.$rules.start);
        this.normalizeRules();
      };
      oop.inherits(BloblangHighlightRules, CoffeeHighlightRules);

      const BloblangMode = function () {
        CoffeeMode.call(this);
        this.HighlightRules = BloblangHighlightRules;
        this.$id = MODE_BLOBLANG;
      };
      oop.inherits(BloblangMode, CoffeeMode);

      exports.Mode = BloblangMode;
    });
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
    const mappingEl = this.getElement(DOM_IDS.aceMapping);
    if (!mappingEl) return;

    this.aceMappingEditor = ace.edit(mappingEl);
    this.aceMappingEditor.setValue(this.defaultMapping, 1);
    this.aceMappingEditor.session.setMode(MODE_BLOBLANG);
    this.aceMappingEditor.setTheme(THEME_BENTO);
    this.configureEditorOptions(this.aceMappingEditor);
    this.configureAutocompletion();
  }

  configureEditorOptions(editor) {
    editor.session.setTabSize(2);
    editor.session.setUseSoftTabs(true);
    editor.session.setUseWorker(false);
    editor.setShowPrintMargin(false);
    editor.setOption("wrap", true);
  }

  configureFallbackEditors() {
    const aceInput = this.getElement(DOM_IDS.aceInput);
    const aceMapping = this.getElement(DOM_IDS.aceMapping);
    const fallbackInput = this.getElement(DOM_IDS.fallbackInput);
    const fallbackMapping = this.getElement(DOM_IDS.fallbackMapping);

    // Show fallback textareas
    if (aceInput) aceInput.style.display = "none";
    if (fallbackInput) fallbackInput.style.display = "block";
    if (aceMapping) aceMapping.style.display = "none";
    if (fallbackMapping) fallbackMapping.style.display = "block";

    // Set up fallback listeners
    fallbackInput?.addEventListener("input", () => {
      this.inputChangeListeners.forEach((fn) => fn());
      this.callbacks.onInputChange?.();
    });

    fallbackMapping?.addEventListener("input", () => {
      this.mappingChangeListeners.forEach((fn) => fn());
      this.callbacks.onMappingChange?.();
    });
  }

  registerEditorChangeHandlers() {
    this.aceInputEditor.on("change", () => {
      if (this.callbacks.onInputChange) {
        this.callbacks.onInputChange();
      }
    });

    this.aceMappingEditor.on("change", () => {
      if (this.callbacks.onMappingChange) {
        this.callbacks.onMappingChange();
      }
    });
  }

  // ─────────────────────────────────────────────────────
  // Autocompletion Logic
  // ─────────────────────────────────────────────────────

  configureAutocompletion() {
    if (!this.aceMappingEditor || !this.bloblangSyntax) return;

    try {
      const completer = this.createBloblangCompleter();
      this.aceMappingEditor.setOptions({
        enableBasicAutocompletion: [completer],
        enableLiveAutocompletion: true,
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
        const beforeCursor = line.substring(0, pos.column);

        if (beforeCursor.match(/\.\w*$/)) {
          this.bloblangSyntax.methods.forEach((method) =>
            completions.push({
              caption: method,
              value: `${method}()`,
              meta: "bloblang method",
              type: "method",
              score: 1000,
            })
          );
        } else {
          this.bloblangSyntax.functions.forEach((func) =>
            completions.push({
              caption: func,
              value: `${func}()`,
              meta: "bloblang function",
              type: "function",
              score: 1000,
            })
          );

          ["root", "this", "if", "else", "match", "let"].forEach((keyword) => {
            if (keyword.startsWith(prefix.toLowerCase())) {
              completions.push({
                caption: keyword,
                value: keyword,
                meta: "bloblang keyword",
                type: "keyword",
                score: 900,
              });
            }
          });
        }

        callback(null, completions);
      },
    };
  }

  // ─────────────────────────────────────────────────────
  // Data Accessors
  // ─────────────────────────────────────────────────────

  /**
   * Gets the current input value from the editor or fallback.
   * @returns {string}
   */
  getInput() {
    return this.aceInputEditor
      ? this.aceInputEditor.getValue()
      : this.getElement(DOM_IDS.fallbackInput)?.value || "";
  }

  /**
   * Gets the current mapping value from the editor or fallback.
   * @returns {string}
   */
  getMapping() {
    return this.aceMappingEditor
      ? this.aceMappingEditor.getValue()
      : this.getElement(DOM_IDS.fallbackMapping)?.value || "";
  }

  /**
   * Sets the content of the input editor or fallback.
   * @param {string} content
   */
  setInput(content) {
    if (this.aceInputEditor) {
      this.aceInputEditor.setValue(content, 1);
    } else {
      const fallback = this.getElement(DOM_IDS.fallbackInput);
      if (fallback) fallback.value = content;
    }
  }

  /**
   * Sets the content of the mapping editor or fallback.
   * @param {string} content
   */
  setMapping(content) {
    if (this.aceMappingEditor) {
      this.aceMappingEditor.setValue(content, 1);
    } else {
      const fallback = this.getElement(DOM_IDS.fallbackMapping);
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
}
