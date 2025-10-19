//go:build !unix

package io

import "os"

// inodeOf extracts the inode from a FileInfo on non-Unix systems
// On Windows and other non-Unix systems, inodes are not available
// Returns (0, false) to indicate inode support is not available
// Rotation detection will fall back to size-based heuristics
func inodeOf(_ os.FileInfo) (uint64, bool) {
	return 0, false
}
