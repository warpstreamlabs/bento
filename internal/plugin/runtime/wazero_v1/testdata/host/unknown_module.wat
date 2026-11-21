;; Imports a function from a module that no manifest can enable.
(module
  (import "bento_unknown_v1" "call" (func $call))
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "malloc") (param $n i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $n)))
    (local.get $p))
  (func (export "free") (param i32 i32))
  (func (export "process") (param i32 i32) (result i64)
    (call $call)
    (i64.const 0)))
