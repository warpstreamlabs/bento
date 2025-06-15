class EditorManager {
  constructor() {
    this.aceInputEditor = null;
    this.aceMappingEditor = null;
    this.callbacks = {};
  }

  init(callbacks = {}) {
    this.callbacks = callbacks;
    this.setupAceTheme();
    this.setupBloblangMode();
    this.initializeEditors();
  }

  setupAceTheme() {
    // Define custom Bento theme for ACE
    ace.define("ace/theme/bento", function (require, exports, module) {
      exports.cssClass = "ace-bento";
    });
  }

  setupBloblangMode() {
    // Define custom Bloblang mode for ACE
    ace.define("ace/mode/bloblang", function (require, exports, module) {
      const oop = require("../lib/oop");
      const TextMode = require("./text").Mode;
      const TextHighlightRules =
        require("./text_highlight_rules").TextHighlightRules;

      const BloblangHighlightRules = function () {
        this.$rules = {
          start: [
            // Comments (Python-style)
            { token: "comment", regex: "#.*$" },

            // Keywords and control flow
            { token: "keyword", regex: "\\b(if|else|match|let|root)\\b" },

            // Special Bloblang identifiers
            { token: "ace_bloblang_root", regex: "\\broot\\b" },
            { token: "ace_bloblang_this", regex: "\\bthis\\b" },

            // Variables (starting with $)
            { token: "variable", regex: "\\$[a-zA-Z_][a-zA-Z0-9_]*" },

            // Arrow function operator
            { token: "keyword.operator", regex: "->" },

            // Built-in methods (after a dot)
            {
              token: "support.function",
              regex:
                "\\.(without|exists|map_each|filter|concat|append|sort_by|group_by|" +
                "uppercase|lowercase|trim|split|join|replace|contains|length|index|slice|" +
                "prepend|reverse|sort|unique|flatten|select|merge|delete|type|string|" +
                "number|bool|array|object|keys|values|has|get|set|not_null|catch|try|" +
                "sum|min|max|floor|ceil|round|abs|sqrt|from_base64|to_base64|" +
                "parse_json|format_json|parse_yaml|format_yaml|parse_xml|" +
                "timestamp_unix|format_timestamp|parse_timestamp)\\b",
            },

            // Function calls and built-in functions
            {
              token: "support.function",
              regex:
                "\\b(deleted|error|range|json|yaml|xml|csv|file|env|hostname|" +
                "uuid_v4|timestamp_unix|timestamp_unix_nano|timestamp|now|" +
                "content|meta|batch_index|batch_size|count|random|random_int)\\b",
            },

            // Constants
            { token: "constant.language", regex: "\\b(true|false|null)\\b" },

            // Numbers (integers, floats, scientific notation)
            {
              token: "constant.numeric",
              regex: "\\b\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?\\b",
            },

            // Strings (double quotes, single quotes, backticks)
            { token: "string.quoted.double", regex: '"(?:[^"\\\\]|\\\\.)*"' },
            { token: "string.quoted.single", regex: "'(?:[^'\\\\]|\\\\.)*'" },
            { token: "string.quoted.backtick", regex: "`(?:[^`\\\\]|\\\\.)*`" },

            // Operators
            { token: "keyword.operator", regex: "\\b(and|or|not)\\b" },
            {
              token: "keyword.operator",
              regex: "[=!<>]=?|[+\\-*/%]|&&|\\|\\||!",
            },
            { token: "keyword.operator", regex: "\\|" }, // Pipe operator

            // Assignment operators
            { token: "keyword.operator", regex: "=" },

            // Punctuation
            { token: "punctuation", regex: "[{}\\[\\](),.:]" },

            // Object property access
            {
              token: "support.type",
              regex: "\\.[a-zA-Z_][a-zA-Z0-9_]*(?=\\s*[^(])",
            },

            // Identifiers (general)
            { token: "identifier", regex: "[a-zA-Z_][a-zA-Z0-9_]*" },
          ],
        };
      };

      oop.inherits(BloblangHighlightRules, TextHighlightRules);

      const BloblangMode = function () {
        this.HighlightRules = BloblangHighlightRules;
      };
      oop.inherits(BloblangMode, TextMode);

      (function () {
        this.$id = "ace/mode/bloblang";
      }).call(BloblangMode.prototype);

      exports.Mode = BloblangMode;
    });
  }

  initializeEditors() {
    try {
      // Initialize input editor
      this.aceInputEditor = ace.edit("aceInput");
      this.aceInputEditor.setValue(
        '{"message": "hello world", "number": 42}',
        1
      );
      this.aceInputEditor.session.setMode("ace/mode/json");
      this.aceInputEditor.setTheme("ace/theme/bento");
      this.setupEditorOptions(this.aceInputEditor);

      // Initialize mapping editor
      this.aceMappingEditor = ace.edit("aceMapping");
      this.aceMappingEditor.setValue(
        "root.greeting = this.message.uppercase()\nroot.doubled = this.number * 2",
        1
      );
      this.aceMappingEditor.session.setMode("ace/mode/bloblang");
      this.aceMappingEditor.setTheme("ace/theme/bento");
      this.setupEditorOptions(this.aceMappingEditor);

      // Set up change listeners
      this.aceInputEditor.on("change", () => {
        if (this.callbacks.onInputChange) this.callbacks.onInputChange();
      });

      this.aceMappingEditor.on("change", () => {
        if (this.callbacks.onMappingChange) this.callbacks.onMappingChange();
      });
    } catch (error) {
      console.warn("ACE editor failed to load, falling back to textarea");
      this.setupFallbackEditors();
    }
  }

  setupEditorOptions(editor) {
    editor.session.setTabSize(2);
    editor.session.setUseSoftTabs(true);
    editor.session.setUseWorker(false);
    editor.setShowPrintMargin(false);
    editor.setOption("wrap", true);
  }

  setupFallbackEditors() {
    // Show fallback textareas
    document.getElementById("aceInput").style.display = "none";
    document.getElementById("fallbackInput").style.display = "block";
    document.getElementById("aceMapping").style.display = "none";
    document.getElementById("fallbackMapping").style.display = "block";

    // Set up fallback listeners
    const fallbackInput = document.getElementById("fallbackInput");
    const fallbackMapping = document.getElementById("fallbackMapping");

    fallbackInput.addEventListener("input", () => {
      if (this.callbacks.onInputChange) this.callbacks.onInputChange();
    });

    fallbackMapping.addEventListener("input", () => {
      if (this.callbacks.onMappingChange) this.callbacks.onMappingChange();
    });
  }

  getInput() {
    if (this.aceInputEditor) {
      return this.aceInputEditor.getValue();
    }
    return document.getElementById("fallbackInput").value;
  }

  getMapping() {
    if (this.aceMappingEditor) {
      return this.aceMappingEditor.getValue();
    }
    return document.getElementById("fallbackMapping").value;
  }

  setInput(content) {
    if (this.aceInputEditor) {
      this.aceInputEditor.setValue(content, 1);
    } else {
      document.getElementById("fallbackInput").value = content;
    }
  }

  setMapping(content) {
    if (this.aceMappingEditor) {
      this.aceMappingEditor.setValue(content, 1);
    } else {
      document.getElementById("fallbackMapping").value = content;
    }
  }
}
