package jazz

import (
	"bytes"
	"os"
	"testing"
)

// The module ships jazz-ffi's header, so it builds from the module cache:
// in the Jazz repository, it is the crate's, byte for byte.
func TestTheShippedHeaderIsJazzFFIs(t *testing.T) {
	crate, err := os.ReadFile("../../crates/jazz-ffi/include/jazz_ffi.h")
	if err != nil {
		t.Skip("not in the Jazz repository")
	}
	shipped, err := os.ReadFile("include/jazz_ffi.h")
	if err != nil || !bytes.Equal(crate, shipped) {
		t.Fatal("include/jazz_ffi.h differs from crates/jazz-ffi/include/jazz_ffi.h: copy it again")
	}
}
