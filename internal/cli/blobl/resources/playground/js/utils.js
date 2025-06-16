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

// Bloblang Utilities
function formatBloblang(mappingString) {
  const lines = mappingString.split("\n");
  const formatted = lines
    .map((line) => {
      const trimmed = line.trim();
      if (trimmed === "" || trimmed.startsWith("#")) return trimmed;

      // Add consistent spacing around operators
      return trimmed
        .replace(/\s*=\s*/g, " = ")
        .replace(/\s*\.\s*/g, ".")
        .replace(/\s*\(\s*/g, "(")
        .replace(/\s*\)\s*/g, ")");
    })
    .join("\n");

  return formatted;
}

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
function lintJSON(jsonString) {
  try {
    JSON.parse(jsonString);
    return { valid: true, message: "Valid JSON" };
  } catch (e) {
    return { valid: false, message: `Invalid JSON: ${e.message}` };
  }
}

function lintBloblang(mappingString) {
  const lines = mappingString.split("\n");
  const errors = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line === "" || line.startsWith("#")) continue;
  }

  if (errors.length > 0) {
    return { valid: false, message: errors[0] };
  }

  return { valid: true, message: "Valid Syntax" };
}

// Linter Update Functions
function updateInputLinter(input) {
  const lint = lintJSON(input);
  const indicator = document.getElementById("inputLint");
  if (indicator) {
    indicator.textContent = lint.message;
    indicator.className = `lint-indicator ${lint.valid ? "valid" : "invalid"}`;
  }
  return lint;
}

function updateMappingLinter(mapping, errorMessage = null) {
  const lint = errorMessage
    ? { valid: false, message: "Invalid Syntax" }
    : lintBloblang(mapping);

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

  const isJSON = isValidJSON(output);
  if (isJSON) {
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

// Global action handlers for onclick events
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

function formatMapping() {
  if (window.playground) {
    const formatted = formatBloblang(window.playground.editor.getMapping());
    window.playground.editor.setMapping(formatted);
    updateMappingLinter(formatted);
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

// Error Message Creation
function createErrorMessage(title, message, details = null) {
  return `
    <div class="error-message">
      <div class="error-title">${title}</div>
      <div>${message}</div>
      ${details ? `<div class="error-details">${details}</div>` : ""}
    </div>
  `;
}
