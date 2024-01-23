// Utility to get the last accessed time on Darwin.
package diskcache

import (
	"os"
	"syscall"
	"time"
)

func GetLastAccessTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(info.Sys().(*syscall.Stat_t).Atimespec.Unix()), nil
}
