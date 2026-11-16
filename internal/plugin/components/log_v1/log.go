//go:build wasm

package logv1

import "github.com/warpstreamlabs/bento/internal/plugin/wasm"

//go:wasmimport bento_log_v1 trace
func log_trace(ptr, len uint32)

//go:wasmimport bento_log_v1 debug
func log_debug(ptr, len uint32)

//go:wasmimport bento_log_v1 info
func log_info(ptr, len uint32)

//go:wasmimport bento_log_v1 warn
func log_warn(ptr, len uint32)

//go:wasmimport bento_log_v1 error
func log_error(ptr, len uint32)

func Trace(msg string) { log_trace(lower(msg)) }
func Debug(msg string) { log_debug(lower(msg)) }
func Info(msg string)  { log_info(lower(msg)) }
func Warn(msg string)  { log_warn(lower(msg)) }
func Error(msg string) { log_error(lower(msg)) }

func lower(msg string) (uint32, uint32) {
	buf := wasm.FromSlice([]byte(msg))
	return buf.Address(), buf.Length()
}
