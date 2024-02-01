// System utilities that differ between OS implementations.
package diskcache

import (
	"os"
	"syscall"
	"time"
)

// This will return correct values only if `fsutil behavior set disablelastaccess 0` is set.
// Tracking of last access time is disabled by default on Windows.
func GetLastAccessTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, info.Sys().(*syscall.Win32FileAttributeData).LastAccessTime.Nanoseconds()), nil
}
