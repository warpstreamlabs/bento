/**
 * Manages WebAssembly module loading for the Bloblang playground.
 * Handles initialization of the Go WASM runtime and loading of the compiled WASM module.
 */
class WasmManager {
  static WASM_PATH = "playground.wasm";
  static READY_TIMEOUT_MS = 5000;
  static READY_CHECK_INTERVAL_MS = 100;

  constructor() {
    this.isLoaded = false;
    this.hasFailed = false;
    this.goRuntime = this.#initializeGoRuntime();
  }

  /**
   * Loads and initializes the WebAssembly module.
   * @returns {Promise<void>}
   */
  async load() {
    if (this.hasFailed || !this.goRuntime) {
      console.warn("Skipping WASM load - Go runtime not available");
      return;
    }

    try {
      const wasmModule = await this.#fetchAndInstantiate();
      this.goRuntime.run(wasmModule.instance);
      await this.#waitForReady();
      this.isLoaded = true;
    } catch (error) {
      console.warn("WASM failed to load:", error.message);
      this.hasFailed = true;
      this.isLoaded = false;
    }
  }

  /**
   * Initializes the Go WebAssembly runtime.
   * @returns {Go|null} The Go runtime instance, or null if initialization failed
   */
  #initializeGoRuntime() {
    if (typeof Go === "undefined") {
      console.warn("Go WASM runtime not available - wasm_exec.js not loaded");
      this.hasFailed = true;
      return null;
    }

    try {
      return new Go();
    } catch (error) {
      console.warn("Failed to initialize Go WASM runtime:", error.message);
      this.hasFailed = true;
      return null;
    }
  }

  /**
   * Fetches and instantiates the WebAssembly module.
   * @returns {Promise<WebAssembly.Instance>}
   */
  async #fetchAndInstantiate() {
    const response = await fetch(WasmManager.WASM_PATH);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return await WebAssembly.instantiateStreaming(
      response,
      this.goRuntime.importObject
    );
  }

  /**
   * Waits for the WASM module to signal readiness via window.wasmReady.
   * @returns {Promise<void>}
   */
  async #waitForReady() {
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error("WASM initialization timeout"));
      }, WasmManager.READY_TIMEOUT_MS);

      const checkReady = () => {
        if (window.wasmReady) {
          clearTimeout(timeoutId);
          resolve();
        } else {
          setTimeout(checkReady, WasmManager.READY_CHECK_INTERVAL_MS);
        }
      };

      checkReady();
    });
  }
}
