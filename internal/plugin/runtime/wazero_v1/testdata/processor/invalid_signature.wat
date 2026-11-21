;; process returns an i32 rather than an i64.
(module
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "malloc") (param $n i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $n)))
    (local.get $p))
  (func (export "free") (param i32 i32))
  (func (export "process") (param i32 i32) (result i32)
    (i32.const 0)))
