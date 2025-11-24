/**
 * API for Bloblang operations
 * Automatically routes to WASM or server based on execution mode
 */
class BloblangAPI {
  constructor(mode, wasmApi = null) {
    this.mode = mode; // 'wasm' or 'server'
    this.wasmApi = wasmApi; // window.bloblangApi when using WASM
  }

  async execute(input, mapping) {
    if (this.mode === "wasm") {
      return this.wasmApi.execute(input, mapping);
    } else {
      const response = await fetch("/execute", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ input, mapping }),
      });
      return response.json();
    }
  }

  async format(mapping) {
    if (this.mode === "wasm") {
      return this.wasmApi.format(mapping);
    } else {
      const response = await fetch("/format", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mapping }),
      });
      return response.json();
    }
  }

  async validate(mapping) {
    if (this.mode === "wasm") {
      return this.wasmApi.validate(mapping);
    } else {
      const response = await fetch("/validate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mapping }),
      });
      return response.json();
    }
  }

  async autocomplete(request) {
    if (this.mode === "wasm") {
      const requestJSON = JSON.stringify(request);
      return this.wasmApi.autocomplete(requestJSON);
    } else {
      const response = await fetch("/autocomplete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(request),
      });
      return response.json();
    }
  }

  getSyntax() {
    if (this.mode === "wasm") {
      return this.wasmApi.syntax();
    } else {
      // Server mode: syntax injected in HTML as window.BLOBLANG_SYNTAX
      return window.BLOBLANG_SYNTAX;
    }
  }
}
