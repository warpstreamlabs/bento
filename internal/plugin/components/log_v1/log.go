//go:build wasm

package logv1

import "github.com/warpstreamlabs/bento/internal/plugin/wasm"

//go:wasmimport bento_log_v1 trace
func log_trace(address, length uint32)

//go:wasmimport bento_log_v1 debug
func log_debug(address, length uint32)

//go:wasmimport bento_log_v1 info
func log_info(address, length uint32)

//go:wasmimport bento_log_v1 warn
func log_warn(address, length uint32)

//go:wasmimport bento_log_v1 error
func log_error(address, length uint32)

func Trace(msg string) { log_trace(lower(msg)) }
func Debug(msg string) { log_debug(lower(msg)) }
func Info(msg string)  { log_info(lower(msg)) }
func Warn(msg string)  { log_warn(lower(msg)) }
func Error(msg string) { log_error(lower(msg)) }

func lower(msg string) (uint32, uint32) {
	buf := wasm.FromSlice([]byte(msg))
	return buf.Address(), buf.Length()
}
