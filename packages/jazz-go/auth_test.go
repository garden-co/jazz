package jazz

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeviceAccountKeepsItsKey(t *testing.T) {
	key, err := GenerateDeviceKey()
	if err != nil || !strings.HasPrefix(key, "jazz-auth-v1:") {
		t.Fatalf("%q %v", key, err)
	}
	store := FileStore(filepath.Join(t.TempDir(), "keys", "device"))
	first, err := DeviceAccount(store).authJSON()
	if err != nil {
		t.Fatal(err)
	}
	// Signing up stored the key; the next open signs in with it.
	again, err := DeviceAccount(store).authJSON()
	if err != nil || first["deviceKey"] != again["deviceKey"] || first["deviceKey"] == "" {
		t.Fatalf("%v %v %v", first, again, err)
	}
}

func TestALocalAppHasNoSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := Open(ctx, Config{App: filesApp, Storage: StorageMemory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if s, err := db.Session(ctx); err != nil || s != nil {
		t.Fatalf("%+v %v", s, err)
	}
}
