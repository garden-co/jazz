//go:build unix

package jazz

import (
	"os"
	"syscall"
)

func ownedByUs(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}
