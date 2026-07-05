// System utilities that differ between OS implementations.
package diskcache

import (
	"io/fs"
	"syscall"
	"time"
)

// This will return correct values only if `fsutil behavior set disablelastaccess 0` is set.
// Tracking of last access time is disabled by default on Windows.
func FileInfoToAccessTime(info fs.FileInfo) time.Time {
	return time.Unix(0, info.Sys().(*syscall.Win32FileAttributeData).LastAccessTime.Nanoseconds())
}
