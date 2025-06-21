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
    if (!ace) {
      console.error("Unable to load ACE");
      return;
    }

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
        functions: {},
        methods: {},
        rules: [],
        function_names: [],
        method_names: [],
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
        if (rules && rules.length > 0) {
          this.$rules.start = rules.concat(this.$rules.start);
        }
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

        // Method suggestions (after a dot)
        if (beforeCursor.match(/\.\w*$/)) {
          Object.values(this.bloblangSyntax.methods).forEach((spec) =>
            completions.push({
              caption: spec.name,
              value: `${spec.name}()`,
              meta: this.getMethodCategory(spec),
              type: "method",
              score: this.getCompletionScore(spec),
              docHTML: this.createMethodDocumentationHTML(spec),
            })
          );
        }
        // Function suggestions (standalone)
        else {
          Object.values(this.bloblangSyntax.functions).forEach((spec) =>
            completions.push({
              caption: spec.name,
              value: `${spec.name}()`,
              meta: this.getFunctionCategory(spec),
              type: "function",
              score: this.getCompletionScore(spec),
              docHTML: this.createFunctionDocumentationHTML(spec),
            })
          );

          // Add Bloblang keywords
          const keywords = [
            { name: "root", description: "The root of the output document" },
            { name: "this", description: "The current context value" },
            { name: "if", description: "Conditional expression" },
            { name: "else", description: "Alternative branch" },
            { name: "match", description: "Pattern matching expression" },
            { name: "let", description: "Variable assignment" },
          ];

          keywords.forEach((keyword) => {
            if (keyword.name.startsWith(prefix.toLowerCase())) {
              completions.push({
                caption: keyword.name,
                value: keyword.name,
                meta: "keyword",
                type: "keyword",
                score: 900,
                docHTML: `<div class="ace-doc"><strong>${keyword.name}</strong><br/>${keyword.description}</div>`,
              });
            }
          });
        }

        callback(null, completions);
      },
    };
  }

  getCompletionScore(spec) {
    // Prioritize by status
    switch (spec.status) {
      case "stable":
        return 1000;
      case "beta":
        return 800;
      case "experimental":
        return 600;
      case "deprecated":
        return 200;
      default:
        return 700;
    }
  }

  getFunctionCategory(functionSpec) {
    if (functionSpec.status === "deprecated") return "Deprecated";
    if (functionSpec.status === "experimental") return "Experimental";

    // Categorize by name patterns
    const name = functionSpec.name;
    if (name.includes("json")) return "JSON";
    if (name.includes("string") || name.endsWith("case")) return "String";
    if (name.includes("time") || name.includes("date")) return "Time";
    if (name.includes("math") || name.includes("calc")) return "Math";
    if (name.includes("crypto") || name.includes("hash")) return "Crypto";
    if (name.includes("env") || name.includes("hostname")) return "Environment";
    if (name.includes("uuid") || name.includes("nanoid"))
      return "ID Generation";
    if (name.includes("random")) return "Random";
    return "General";
  }

  getMethodCategory(methodSpec) {
    if (methodSpec.categories && methodSpec.categories.length > 0) {
      return methodSpec.categories[0].category;
    }

    if (methodSpec.status === "deprecated") return "Deprecated";
    if (methodSpec.status === "experimental") return "Experimental";

    // Fallback categorization
    const name = methodSpec.name;
    if (name.includes("json")) return "JSON";
    if (name.includes("string")) return "String";
    if (name.includes("array") || name.includes("slice")) return "Array";
    if (name.includes("map") || name.includes("object")) return "Object";
    return "General";
  }

  formatParameterSignature(params) {
    if (!params || !params.named || params.named.length === 0) {
      return "";
    }

    const paramStrings = params.named.map((param) => {
      let paramStr = param.name;
      if (param.type) {
        paramStr += `: ${param.type}`;
      }
      if (param.default !== undefined) {
        paramStr += ` = ${param.default}`;
      }
      return param.optional ? `[${paramStr}]` : paramStr;
    });

    const variadicIndicator = params.variadic ? ", ..." : "";
    return paramStrings.join(", ") + variadicIndicator;
  }

  createFunctionDocumentationHTML(functionSpec) {
    const signature = `${functionSpec.name}(${this.formatParameterSignature(
      functionSpec.params
    )})`;
    const statusBadge = functionSpec.status
      ? `<span class="ace-status-${functionSpec.status}">${functionSpec.status}</span>`
      : "";

    let html = `
      <div class="ace-doc">
        <div class="ace-doc-signature">
          <strong>${signature}</strong>
          ${statusBadge}
        </div>
        <div class="ace-doc-description">${
          functionSpec.description || "No description available"
        }</div>
    `;

    // Add parameter details
    if (
      functionSpec.params &&
      functionSpec.params.named &&
      functionSpec.params.named.length > 0
    ) {
      html += `<div class="ace-doc-parameters"><strong>Parameters:</strong>`;
      functionSpec.params.named.forEach((param) => {
        const optionalText = param.optional ? " (optional)" : "";
        const defaultText =
          param.default !== undefined ? ` = ${param.default}` : "";
        const typeText = param.type ? ` [${param.type}]` : "";

        html += `<div class="ace-doc-param">`;
        html += `<code>${param.name}${typeText}${defaultText}${optionalText}</code><br/>`;
        html += `<span class="ace-doc-param-desc">${
          param.description || "No description"
        }</span>`;
        html += `</div>`;
      });
      html += `</div>`;
    }

    // Add examples if available
    if (functionSpec.examples && functionSpec.examples.length > 0) {
      html += `<div class="ace-doc-examples"><strong>Examples:</strong><br/>`;
      functionSpec.examples.slice(0, 2).forEach((example) => {
        // Limit to 2 examples
        if (example.summary) {
          html += `<div class="ace-doc-example-summary">${example.summary}</div>`;
        }
        html += `<code>${example.mapping}</code><br/>`;
      });
      html += `</div>`;
    }

    html += `</div>`;
    return html;
  }

  createMethodDocumentationHTML(methodSpec) {
    const signature = `.${methodSpec.name}(${this.formatParameterSignature(
      methodSpec.params
    )})`;
    const statusBadge = methodSpec.status
      ? `<span class="ace-status-${methodSpec.status}">${methodSpec.status}</span>`
      : "";
    const impureBadge = methodSpec.impure
      ? '<span class="ace-impure-badge">impure</span>'
      : "";

    let html = `
      <div class="ace-doc">
        <div class="ace-doc-signature">
          <strong>${signature}</strong>
          ${statusBadge}
          ${impureBadge}
        </div>
        <div class="ace-doc-description">${
          methodSpec.description || "No description available"
        }</div>
    `;

    // Add parameter details
    if (
      methodSpec.params &&
      methodSpec.params.named &&
      methodSpec.params.named.length > 0
    ) {
      html += `<div class="ace-doc-parameters"><strong>Parameters:</strong>`;
      methodSpec.params.named.forEach((param) => {
        const optionalText = param.optional ? " (optional)" : "";
        const defaultText =
          param.default !== undefined ? ` = ${param.default}` : "";
        const typeText = param.type ? ` [${param.type}]` : "";

        html += `<div class="ace-doc-param">`;
        html += `<code>${param.name}${typeText}${defaultText}${optionalText}</code><br/>`;
        html += `<span class="ace-doc-param-desc">${
          param.description || "No description"
        }</span>`;
        html += `</div>`;
      });
      html += `</div>`;
    }

    // Add examples if available
    if (methodSpec.examples && methodSpec.examples.length > 0) {
      html += `<div class="ace-doc-examples"><strong>Examples:</strong><br/>`;
      methodSpec.examples.slice(0, 2).forEach((example) => {
        if (example.summary) {
          html += `<div class="ace-doc-example-summary">${example.summary}</div>`;
        }
        html += `<code>${example.mapping}</code><br/>`;
      });
      html += `</div>`;
    }

    html += `</div>`;
    return html;
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
