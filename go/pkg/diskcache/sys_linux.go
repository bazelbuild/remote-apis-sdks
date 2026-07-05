// System utilities that differ between OS implementations.
package diskcache

import (
	"io/fs"
	"syscall"
	"time"
)

func FileInfoToAccessTime(info fs.FileInfo) time.Time {
	return time.Unix(info.Sys().(*syscall.Stat_t).Atim.Unix())
}
