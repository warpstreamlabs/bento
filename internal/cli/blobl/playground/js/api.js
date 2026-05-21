/**
 * Route Bloblang operations to WASM or server based on execution mode
 */
class BloblangAPI {
  constructor(mode, wasmApi = null) {
    this.mode = mode; // 'wasm' or 'server'
    this.wasmApi = wasmApi; // window.bloblangApi when using WASM
  }

  async execute(input, mapping) {
    if (this.mode === "wasm") {
      return this.wasmApi.execute(input, mapping);
    }
    
    const response = await fetch("/execute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ input, mapping }),
    });
    
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return response.json();
  }

  async format(mapping) {
    if (this.mode === "wasm") {
      const result = this.wasmApi.format(mapping);
      if (result && result.error) {
        throw new Error(result.error)
      };
      return result;
    }

    const response = await fetch("/format", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mapping }),
    });

    if (!response.ok) {
      throw new Error(await response.text());
    }
    return response.json();
  }

  async autocomplete(request) {
    if (this.mode === "wasm") {
      const requestJSON = JSON.stringify(request);
      const result = this.wasmApi.autocomplete(requestJSON);
      if (result && result.error) {
        throw new Error(result.error)
      };
      return result;
    }
  
    const response = await fetch("/autocomplete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    });
    
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return response.json();
  }

  getSyntax() {
    if (this.mode === "wasm") {
      return this.wasmApi.syntax();
    }
  
    // Server mode: syntax injected in HTML as window.BLOBLANG_SYNTAX
    return window.BLOBLANG_SYNTAX;
  }
}
