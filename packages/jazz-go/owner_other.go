//go:build !unix

package jazz

import "os"

func ownedByUs(os.FileInfo) bool { return true }
