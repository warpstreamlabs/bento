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

    // Uses the injected syntax from the HTML template (set in `server.go`)
    this.bloblangSyntax = window.BLOBLANG_SYNTAX;

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
    const contentWithNewline = this.defaultMapping.trimEnd() + "\n"; // Trailing newline
    this.aceMappingEditor.setValue(contentWithNewline, 1);
    this.aceMappingEditor.session.setMode(MODE_BLOBLANG);
    this.aceMappingEditor.setTheme(THEME_BENTO);
    this.aceMappingEditor.commands.addCommand({
      name: "formatBloblang",
      bindKey: { mac: "Cmd-Shift-F", win: "Ctrl-Shift-F" },
      exec: () => formatBloblang(),
    });
    this.overrideCommentShortcut(this.aceMappingEditor);
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
    if (!this.aceMappingEditor || !this.bloblangSyntax) return;

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

  formatParameterSignature(params) {
    if (!params?.named?.length) return "";

    const paramStrs = params.named.map((param) => {
      const type = param.type ? `: ${param.type}` : "";
      const def = param.default !== undefined ? ` = ${param.default}` : "";
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

    const strippedDesc = this.stripAdmonitions(spec.description);

    let html = `
    <div class="ace-doc">
      <div class="ace-doc-signature">
        <strong>${signature}</strong>
        ${statusBadge}
        ${impureBadge}
      </div>`;

    if (strippedDesc) {
      html += `<div class="ace-doc-description">${strippedDesc}</div>`;
    }

    if (spec.params?.named?.length) {
      html += `<div class="ace-doc-parameters"><strong>Parameters:</strong>`;
      for (const param of spec.params.named) {
        const optionalText = param.optional ? " (optional)" : "";
        const defaultText =
          param.default !== undefined ? ` = ${param.default}` : "";
        const typeText = param.type ? ` [${param.type}]` : "";

        html += `
        <div class="ace-doc-param">
          <code>${
            param.name
          }${typeText}${defaultText}${optionalText}</code><br/>
          <span class="ace-doc-param-desc">${
            param.description || "No description"
          }</span>
        </div>`;
      }
      html += `</div>`;
    }

    if (spec.examples?.length) {
      html += `<div class="ace-doc-examples"><strong>Examples:</strong><br/>`;
      for (const example of spec.examples.slice(0, 2)) {
        if (example.summary) {
          html += `<div class="ace-doc-example-summary">${example.summary}</div>`;
        }
        html += `<code>${example.mapping}</code><br/>`;
      }
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
