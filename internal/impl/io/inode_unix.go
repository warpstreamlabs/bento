//go:build unix

package io

import (
	"os"
	"syscall"
)

// inodeOf extracts the inode from a FileInfo on Unix-like systems
// Returns (inode, true) if inode is available, (0, false) otherwise
func inodeOf(fi os.FileInfo) (uint64, bool) {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino, true
	}
	return 0, false
}
