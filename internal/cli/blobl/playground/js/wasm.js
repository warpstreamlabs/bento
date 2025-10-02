class WasmManager {
  constructor() {
    this.available = false;
    this.failed = false;
    this.go = null;
    this.api = null; // Will hold window.bloblangApi

    // Track available WASM functions
    this.functions = {
      execute: false,
      syntax: false,
      format: false,
    };

    // Check if Go WASM support is available
    if (typeof Go !== "undefined") {
      try {
        this.go = new Go();
      } catch (error) {
        console.warn("Failed to initialize Go WASM runtime:", error.message);
        this.failed = true;
      }
    } else {
      console.warn(
        "Go WASM runtime not available - wasm_exec.js not loaded or Go not defined"
      );
      this.failed = true;
    }
  }

  async load() {
    // If Go runtime failed to initialize, don't attempt WASM loading
    if (this.failed || !this.go) {
      console.warn("Skipping WASM load - Go runtime not available");
      return;
    }

    try {
      const wasmPath = ["playground.wasm"];
      const result = await this.loadWasmFromPath(wasmPath);
      this.go.run(result.instance);

      // Wait for WASM module to signal readiness
      await this.waitForWasmReady();

      // Register and validate all WASM functions
      this.registerFunctions();

      this.available = true;
      return;
    } catch (error) {
      console.warn("Failed to load WASM from", wasmPath, ":", error.message);
    }

    console.warn("WASM failed to load from all paths");
    this.failed = true;
    this.available = false;
  }

  async loadWasmFromPath(path) {
    const response = await fetch(path);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return await WebAssembly.instantiateStreaming(
      response,
      this.go.importObject
    );
  }

  async waitForWasmReady() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("WASM initialization timeout"));
      }, 5000); // 5 second timeout

      const checkReady = () => {
        if (window.wasmReady) {
          clearTimeout(timeout);
          resolve();
        } else {
          setTimeout(checkReady, 100);
        }
      };

      checkReady();
    });
  }

  // Register available WASM functions
  registerFunctions() {
    // Check if the bloblang API is available
    if (window.bloblangApi && typeof window.bloblangApi === "object") {
      this.api = window.bloblangApi;
      this.functions.execute = typeof this.api.execute === "function";
      this.functions.syntax = typeof this.api.syntax === "function";
      this.functions.format = typeof this.api.format === "function";
    } else {
      console.warn("window.bloblangApi API not available");
    }
  }

  // Check if a specific function is available
  isAvailable(functionName) {
    return this.available && !this.failed && this.functions[functionName];
  }

  // Check WASM and specific function availability
  assertAvailable(functionKey, errorMsg) {
    if (this.failed) {
      throw new Error(
        "WASM not loaded. Bloblang functionality is unavailable."
      );
    }
    if (!this.available) {
      throw new Error("WASM not available. Please wait for initialization.");
    }
    if (!this.functions[functionKey]) {
      throw new Error(
        errorMsg || `${functionKey} not available in WASM context.`
      );
    }
  }

  // Get all available functions
  getAvailableFunctions() {
    return Object.entries(this.functions)
      .filter(([, available]) => available)
      .map(([name]) => name);
  }

  // Registered Go functions
  execute(input, mapping) {
    this.assertAvailable(
      "execute",
      "Bloblang functionality not available in WASM context."
    );
    return this.api.execute(input, mapping);
  }

  getSyntax() {
    this.assertAvailable(
      "syntax",
      "Syntax functionality not available in WASM context."
    );
    return this.api.syntax();
  }

  formatMapping(mapping) {
    this.assertAvailable(
      "format",
      "Formatter functionality not available in WASM context."
    );
    return this.api.format(mapping);
  }
}
