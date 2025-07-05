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
      "playground.wasm.gz",
      "./playground.wasm.gz",
      "playground.wasm",
      "./playground.wasm",
    ];

    for (const path of wasmPaths) {
      try {
        const result = await this.loadWasmFromPath(path);
        this.go.run(result.instance);

        // Wait for WASM module to signal readiness
        await this.waitForWasmReady();

        this.available = true;
        return;
      } catch (error) {
        console.warn("Failed to load WASM from", path, ":", error.message);
      }
    }

    console.warn("WASM failed to load from all paths");
    console.warn(
      "Note: This browser may not support gzip decompression."
    );
    this.failed = true;
    this.available = false;
  }

  async loadWasmFromPath(path) {
    const response = await fetch(path);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    // Check if the file is gzip compressed
    const isGzip = path.endsWith(".gz");

    if (isGzip) {
      // For compressed files, we need to decompress them first
      const compressedBuffer = await response.arrayBuffer();
      const decompressedBuffer = await this.decompressGzipWasm(
        compressedBuffer
      );
      return await WebAssembly.instantiate(
        decompressedBuffer,
        this.go.importObject
      );
    } else {
      // For uncompressed files, use streaming (more efficient)
      return await WebAssembly.instantiateStreaming(
        response,
        this.go.importObject
      );
    }
  }

  async decompressGzipWasm(compressedBuffer) {
    // Try DecompressionStream first (modern browsers)
    if (typeof DecompressionStream !== "undefined") {
      try {
        const ds = new DecompressionStream("gzip");
        const decompressedStream = new Response(
          compressedBuffer
        ).body.pipeThrough(ds);
        return await new Response(decompressedStream).arrayBuffer();
      } catch (error) {
        console.warn(
          "DecompressionStream failed, trying pako fallback:",
          error.message
        );
      }
    }

    // Fallback to pako library (broader browser support)
    if (typeof pako !== "undefined") {
      try {
        const decompressed = pako.inflate(new Uint8Array(compressedBuffer));
        return decompressed.buffer;
      } catch (error) {
        throw new Error(
          `Gzip decompression with pako failed: ${error.message}`
        );
      }
    }

    throw new Error(
      "Gzip decompression not supported (DecompressionStream and pako not available)"
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

  getSyntax() {
    if (this.failed) {
      throw new Error("WASM not loaded. Syntax data is unavailable.");
    }

    if (!this.available) {
      throw new Error("WASM not available. Please wait for initialization.");
    }

    if (window.getBloblangSyntax) {
      return window.getBloblangSyntax();
    } else {
      throw new Error("Syntax functionality not available in WASM context.");
    }
  }
}
