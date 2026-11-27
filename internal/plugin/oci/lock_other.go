//go:build !unix && !windows

package oci

import "os"

// Platforms without file locks, such as WASM, rely on a single process using
// the store at a time.

func lockFile(*os.File, bool) error { return nil }

func unlockFile(*os.File) error { return nil }
