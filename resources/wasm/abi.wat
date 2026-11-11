;; The ABI between Bento and a plugin. Bento writes the input of a call into
;; memory allocated with the plugin's malloc, and calls an export with its
;; $address and $length. The export returns its output as an i64, with its
;; address in the high 32 bits and its length in the low 32 bits, where 0 is an
;; empty output. Bento reads the output, then frees it and the input with the
;; plugin's free.
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

  ;; Optional. Configures an instance with its config, as JSON. Its output is
  ;; empty, or a UTF-8 error message.
  (func (export "init_plugin") (param $address i32) (param $length i32) (result i64))

  ;; Required, unless process is exported. Processes an encoded
  ;; ProcessBatch.Request, and its output is an encoded ProcessBatch.Response.
  (func (export "process_batch") (param $address i32) (param $length i32) (result i64))

  ;; Required, unless process_batch is exported. Processes an encoded
  ;; Process.Request, and its output is an encoded Process.Response.
  (func (export "process") (param $address i32) (param $length i32) (result i64))
)
