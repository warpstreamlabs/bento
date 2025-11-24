/**
 * UI Utility Functions
 * Pure helpers with no business logic
 */

// Syntax Highlighting - For display purposes only, not validation
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

// Clipboard Operations
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

// File Download
function downloadFile(content, filename, contentType = "text/plain") {
  const blob = new Blob([content], { type: contentType });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.URL.revokeObjectURL(url);
}

// JSON utilities
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
