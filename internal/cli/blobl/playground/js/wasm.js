class WasmManager {
  constructor() {
    this.available = false;
    this.failed = false;
    this.go = null;

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

    const wasmPaths = [
      "playground.wasm",
      "./playground.wasm",
      "/wasm/playground.wasm",
    ];

    for (const path of wasmPaths) {
      try {
        const result = await WebAssembly.instantiateStreaming(
          fetch(path),
          this.go.importObject
        );

        this.go.run(result.instance);
        
        // Wait for WASM module to signal readiness
        await this.waitForWasmReady();
        
        this.available = true;
        return;
      } catch (error) {
        console.warn("Failed to load WASM from", path, ":", error.message);
      }
    }

    console.warn("WASM failed to load");
    this.failed = true;
    this.available = false;
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

  execute(input, mapping) {
    if (this.failed) {
      throw new Error(
        "WASM not loaded. Bloblang functionality is unavailable."
      );
    }

    if (!this.available) {
      throw new Error("WASM not available. Please wait for initialization.");
    }

    if (window.executeBloblang) {
      return window.executeBloblang(input, mapping);
    } else {
      throw new Error("Bloblang functionality not available in WASM context.");
    }
  }
}
