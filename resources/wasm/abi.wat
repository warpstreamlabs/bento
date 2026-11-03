;; The ABI between Bento and a plugin. Bento writes the input of a call into
;; memory allocated with the plugin's malloc, and calls an export with it as a
;; buffer: an i64 holding its address in the upper 32 bits, and its length in
;; the lower 32 bits. The export returns its output as a buffer too. Bento
;; copies the output, then frees it and the input with the plugin's free.
;;
;; These are signatures to compare with a module's exports, such as with
;; `wasm-tools print`, not a module that compiles.
(module
  ;; Required. The memory that buffers refer to.
  (memory (export "memory") 0)

  ;; Required. Allocates $size bytes, and returns their address, or 0 when the
  ;; allocation fails. Bento calls it for the input of each call, unless the
  ;; input is empty.
  (func (export "malloc") (param $size i32) (result i32))

  ;; Required. Frees the $length bytes at $address, which malloc returned, or
  ;; which a call returned as its output. Bento never calls it with 0. It may
  ;; take only $address, as C's free does.
  (func (export "free") (param $address i32) (param $length i32))

  ;; Optional. Initializes a WASI reactor when each instance is created.
  (func (export "_initialize"))

  ;; Optional. Configures an instance with its $config, a buffer of JSON. It
  ;; returns an empty buffer, or a buffer of a UTF-8 error message.
  (func (export "init_plugin") (param $config i64) (result i64))

  ;; Required, unless process is exported. Processes a $request, a buffer of an
  ;; encoded ProcessBatch.Request, and returns a buffer of an encoded
  ;; ProcessBatch.Response.
  (func (export "process_batch") (param $request i64) (result i64))

  ;; Required, unless process_batch is exported. Processes a $request, a buffer
  ;; of an encoded Process.Request, and returns a buffer of an encoded
  ;; Process.Response.
  (func (export "process") (param $request i64) (result i64))
)
