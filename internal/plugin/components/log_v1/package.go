//go:build !wasm

package logv1

// NOTE: These are all no-ops since we don't expect this package to be used outside of WASM builds.

func Trace(string) {}
func Debug(string) {}
func Info(string)  {}
func Warn(string)  {}
func Error(string) {}
