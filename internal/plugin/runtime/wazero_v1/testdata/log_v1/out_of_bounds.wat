;; Logs a message whose length runs past the end of its memory.
(module
  (import "bento_log_v1" "info" (func $info (param i32 i32)))
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "malloc") (param $n i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $n)))
    (local.get $p))
  (func (export "free") (param i32 i32))
  (func (export "process") (param i32 i32) (result i64)
    (call $info (i32.const 0) (i32.const 0xFFFFFFFF))
    (i64.const 0)))
