// Wasm setjmp/longjmp needs the exception handling proposal, which wazero
// doesn't support. TeaVM only longjmps when a Java exception is thrown, so
// setjmp always returns 0 and a throw traps.
#pragma once
typedef int jmp_buf[1];
#define setjmp(buf) 0
#define longjmp(buf, val) __builtin_trap()
