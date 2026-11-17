package jazz

import (
	"errors"
	"testing"
)

func TestApplyEdits(t *testing.T) {
	got, err := ApplyEdits("héllo world", Splice(0, 1, "H"), Splice(7, 5, "Go"))
	if err != nil || got != "Héllo Go" {
		t.Fatalf("%q %v", got, err)
	}
	// "é" is two bytes: an offset between them splits it.
	if _, err := ApplyEdits("héllo", Splice(2, 0, "x")); !errors.Is(err, ErrRefused) {
		t.Fatalf("mid-character: %v", err)
	}
	if _, err := ApplyEdits("hi", Splice(1, 5, "")); !errors.Is(err, ErrRefused) {
		t.Fatalf("out of range: %v", err)
	}
}
