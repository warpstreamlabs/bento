// Syntax Highlighting
function syntaxHighlightJSON(json) {
  if (typeof json !== "string") {
    json = JSON.stringify(json, null, 2);
  }

  json = json
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
    function (match) {
      let cls = "json-number";
      if (/^"/.test(match)) {
        if (/:$/.test(match)) {
          cls = "json-key";
        } else {
          cls = "json-string";
        }
      } else if (/true|false/.test(match)) {
        cls = "json-boolean";
      } else if (/null/.test(match)) {
        cls = "json-null";
      }
      return `<span class="${cls}">${match}</span>`;
    }
  );
}

// Linting Functions
function lintJSON(json) {
  try {
    return JSON.parse(json);
  } catch (e) {
    return null;
  }
}

function updateInputLinter(input) {
  const lint = lintJSON(input)
    ? { valid: true, message: "Valid Syntax" }
    : { valid: false, message: "Invalid Syntax" };

  const indicator = document.getElementById("inputLint");
  if (indicator) {
    indicator.textContent = lint.message;
    indicator.className = `lint-indicator ${lint.valid ? "valid" : "invalid"}`;
  }

  return lint;
}

function updateMappingLinter(mapping, errorMessage = null) {
  const lint = !errorMessage
    ? { valid: true, message: "Valid Syntax" }
    : { valid: false, message: "Invalid Syntax" };

  const indicator = document.getElementById("mappingLint");
  if (indicator) {
    indicator.textContent = lint.message;
    indicator.className = `lint-indicator ${lint.valid ? "valid" : "invalid"}`;
  }

  return lint;
}

function updateOutputLinter(output) {
  const indicator = document.getElementById("outputLint");
  const formatBtn = document.getElementById("formatOutputBtn");
  const minifyBtn = document.getElementById("minifyOutputBtn");

  if (!indicator) return;

  if (output === "Ready to execute your first mapping...") {
    indicator.textContent = "Ready";
    indicator.className = "lint-indicator";
    if (formatBtn) formatBtn.disabled = true;
    if (minifyBtn) minifyBtn.disabled = true;
    return;
  }

  if (isValidJSON(output)) {
    indicator.textContent = "Valid JSON";
    indicator.className = "lint-indicator valid";
    if (formatBtn) formatBtn.disabled = false;
    if (minifyBtn) minifyBtn.disabled = false;
  } else {
    indicator.textContent = "Text Output";
    indicator.className = "lint-indicator warning";
    if (formatBtn) formatBtn.disabled = true;
    if (minifyBtn) minifyBtn.disabled = true;
  }
}

// File Operations
async function copyToClipboard(text, successMessage = "Copied to clipboard!") {
  try {
    await navigator.clipboard.writeText(text);
    if (window.playground && window.playground.ui) {
      window.playground.ui.showNotification(successMessage, "success");
    }
  } catch (err) {
    console.error("Failed to copy:", err);
    if (window.playground && window.playground.ui) {
      window.playground.ui.showNotification(
        "Failed to copy to clipboard",
        "error"
      );
    }
  }
}

function downloadFile(content, filename, contentType = "text/plain") {
  const blob = new Blob([content], { type: contentType });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.URL.revokeObjectURL(url);
}

// Action handlers
function copyInput() {
  if (window.playground) {
    copyToClipboard(window.playground.editor.getInput(), "Input copied!");
  }
}

function copyMapping() {
  if (window.playground) {
    copyToClipboard(window.playground.editor.getMapping(), "Mapping copied!");
  }
}

function copyOutput() {
  if (window.playground) {
    copyToClipboard(
      window.playground.elements.outputArea.textContent,
      "Output copied!"
    );
  }
}

function formatInput() {
  if (window.playground) {
    const formatted = formatJSON(window.playground.editor.getInput());
    window.playground.editor.setInput(formatted);
    updateInputLinter(formatted);
  }
}

function minifyInput() {
  if (window.playground) {
    const minified = minifyJSON(window.playground.editor.getInput());
    window.playground.editor.setInput(minified);
    updateInputLinter(minified);
  }
}

function formatBloblang() {
  if (!window.playground || !window.playground.editor) return;

  try {
    const raw = window.playground.editor.getMapping();
    if (!raw || typeof raw !== "string") return;

    const lines = raw.split("\n");
    let indentLevel = 0;
    const indentSize = 2; // spaces per indent level

    const formatted = lines
      .map((line) => {
        const trimmed = line.trim();

        // Pass through empty lines and comments
        if (trimmed === "" || trimmed.startsWith("#")) {
          return trimmed;
        }

        // Determine if this line should decrease indent (closing braces)
        if (trimmed.startsWith("}")) {
          indentLevel = Math.max(0, indentLevel - 1);
        }

        // Format the line content
        let formattedLine = trimmed;
        
        // First, protect string literals from formatting
        const stringLiterals = [];
        let stringIndex = 0;
        formattedLine = formattedLine.replace(/"([^"\\]|\\.)*"/g, (match) => {
          const placeholder = `__STRING_${stringIndex++}__`;
          stringLiterals.push(match);
          return placeholder;
        });
        
        // Now format operators safely (strings are protected)
        formattedLine = formattedLine
          // Normalize spacing around function args
          .replace(/\s*\(\s*/g, "(")
          .replace(/\s*\)\s*/g, ")")
          .replace(/\s*,\s*/g, ", ")
          // Normalize spacing around dot access
          .replace(/\s*\.\s*/g, ".")
          // Normalize spacing around object keys
          .replace(/\s*:\s*/g, ": ")
          // Use placeholders for compound operators to protect them
          .replace(/\s*(<=)\s*/g, " __LE__ ")
          .replace(/\s*(>=)\s*/g, " __GE__ ")
          .replace(/\s*(==)\s*/g, " __EQ__ ")
          .replace(/\s*(!=)\s*/g, " __NE__ ")
          .replace(/\s*(&&)\s*/g, " __AND__ ")
          .replace(/\s*(\|\|)\s*/g, " __OR__ ")
          // Handle single-character operators safely
          .replace(/\s*([+\-*/%<>=!])\s*/g, " $1 ")
          // Restore compound operators
          .replace(/ __LE__ /g, " <= ")
          .replace(/ __GE__ /g, " >= ")
          .replace(/ __EQ__ /g, " == ")
          .replace(/ __NE__ /g, " != ")
          .replace(/ __AND__ /g, " && ")
          .replace(/ __OR__ /g, " || ")
          // Collapse multiple spaces
          .replace(/\s{2,}/g, " ")
          .trim();
        
        // Restore string literals
        stringLiterals.forEach((literal, index) => {
          formattedLine = formattedLine.replace(`__STRING_${index}__`, literal);
        });

        // Apply indentation
        const indent = " ".repeat(indentLevel * indentSize);
        const result = trimmed === "" ? "" : indent + formattedLine;

        // Determine if this line should increase indent (opening braces)
        if (trimmed.includes("{") && !trimmed.includes("}")) {
          indentLevel++;
        }

        return result;
      })
      .join("\n");

    window.playground.editor.setMapping(formatted);
    updateMappingLinter(formatted);
  } catch (err) {
    console.warn("Formatting error:", err);
  }
}

function formatOutput() {
  if (window.playground) {
    const output = window.playground.elements.outputArea.textContent;
    if (isValidJSON(output)) {
      const formatted = formatJSON(output);
      const highlighted = syntaxHighlightJSON(formatted);
      window.playground.elements.outputArea.innerHTML = highlighted;
      window.playground.elements.outputArea.classList.add("json-formatted");
      updateOutputLinter(output);
    } else {
      window.playground.ui.showNotification(
        "Output is not valid JSON",
        "error"
      );
    }
  }
}

function minifyOutput() {
  if (window.playground) {
    const output = window.playground.elements.outputArea.textContent;
    if (isValidJSON(output)) {
      const minified = minifyJSON(output);
      const highlighted = syntaxHighlightJSON(minified);
      window.playground.elements.outputArea.innerHTML = highlighted;
      window.playground.elements.outputArea.classList.remove("json-formatted");
      updateOutputLinter(minified);
    } else {
      window.playground.ui.showNotification(
        "Output is not valid JSON",
        "error"
      );
    }
  }
}

function loadInputFile() {
  document.getElementById("inputFileInput").click();
}

function loadMappingFile() {
  document.getElementById("mappingFileInput").click();
}

function saveOutput() {
  if (window.playground) {
    const output = window.playground.elements.outputArea.textContent;
    const timestamp = new Date()
      .toISOString()
      .slice(0, 19)
      .replace(/[:.]/g, "-");

    let formattedOutput = output;
    let extension = "txt";

    if (isValidJSON(output)) {
      formattedOutput = formatJSON(output);
      extension = "json";
    }

    downloadFile(
      formattedOutput,
      `bloblang-output-${timestamp}.${extension}`,
      extension === "json" ? "application/json" : "text/plain"
    );
  }
}

// JSON Utilities
function isValidJSON(str) {
  try {
    JSON.parse(str);
    return true;
  } catch (e) {
    return false;
  }
}

function formatJSON(jsonString) {
  try {
    const parsed = JSON.parse(jsonString);
    return JSON.stringify(parsed, null, 2);
  } catch (e) {
    return jsonString;
  }
}

function minifyJSON(jsonString) {
  try {
    const parsed = JSON.parse(jsonString);
    return JSON.stringify(parsed);
  } catch (e) {
    return jsonString;
  }
}
