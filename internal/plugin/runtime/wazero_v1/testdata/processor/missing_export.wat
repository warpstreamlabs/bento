;; The plugin exports no free.
(module
  (memory (export "memory") 1)
  (func (export "malloc") (param i32) (result i32)
    (i32.const 1024))
  (func (export "process") (param i32 i32) (result i64)
    (i64.const 0)))
