package jazz

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func needsFFI(t *testing.T) {
	t.Helper()
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
}

// An empty or blank key file is a damaged one, never "no key yet": taking
// it so would replace the account it was meant to keep.
func TestFileStoreNeverReplacesAKeptFile(t *testing.T) {
	needsFFI(t)
	for name, content := range map[string]string{"empty": "", "blank": " \n\t\n"} {
		path := filepath.Join(t.TempDir(), "device-key")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := DeviceAccount(FileStore(path)).authJSON(); err == nil {
			t.Errorf("%s key file: no error", name)
		}
		if got, err := os.ReadFile(path); err != nil || string(got) != content {
			t.Errorf("%s key file was replaced (%v)", name, err)
		}
	}
}

// A dangling link's target may be on a volume that isn't mounted.
func TestFileStoreKeepsADanglingLink(t *testing.T) {
	needsFFI(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "device-key")
	if err := os.Symlink(filepath.Join(dir, "missing"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := DeviceAccount(FileStore(path)).authJSON(); err == nil {
		t.Error("dangling link: no error")
	}
}

// A save replaces the file whole, so a reader (or a crash) never meets a
// partial key.
func TestFileStoreSavesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "device-key")
	store := FileStore(path)
	if err := store.Save("first"); err != nil {
		t.Fatal(err)
	}
	before, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if err := store.Save("second"); err != nil {
		t.Fatal(err)
	}
	old := make([]byte, 64)
	n, _ := before.Read(old)
	if string(old[:n]) != "first\n" {
		t.Errorf("the file was rewritten in place: a reader of the old one got %q", old[:n])
	}
	if got, err := store.Load(); err != nil || got != "second" {
		t.Errorf("Load after Save: %q, %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Errorf("Save left %d files (%v)", len(entries), err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("the key file's mode: %v", err)
	}
}

func TestFileStoreSavesThroughALink(t *testing.T) {
	dir := t.TempDir()
	target, link := filepath.Join(dir, "kept"), filepath.Join(dir, "device-key")
	if err := os.WriteFile(target, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := FileStore(link).Save("second"); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "second\n" {
		t.Errorf("the link's target: %q, %v", got, err)
	}
}

func TestFileStoreRefusesAnEmptyKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "device-key")
	if FileStore(path).Save(" ") == nil {
		t.Error("saved an empty key")
	}
}

// Openers racing to create the account end up as the one the file keeps.
func TestDeviceAccountCreatesOneKey(t *testing.T) {
	needsFFI(t)
	for round := 0; round < 20; round++ {
		store := FileStore(filepath.Join(t.TempDir(), "device-key"))
		const openers = 8
		var got [openers]string
		var errs [openers]error
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range got {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				auth, err := DeviceAccount(store).authJSON()
				if errs[i] = err; err == nil {
					got[i], _ = auth["deviceKey"].(string)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		kept, err := store.Load()
		if err != nil {
			t.Fatal(err)
		}
		for i := range got {
			if errs[i] != nil || got[i] != kept {
				t.Fatalf("round %d: opener %d doesn't act as the kept account (%v)", round, i, errs[i])
			}
		}
	}
}

func TestOpenKeepsADamagedKey(t *testing.T) {
	needsFFI(t)
	path := filepath.Join(t.TempDir(), "device-key")
	if err := os.WriteFile(path, []byte("jazz-auth-v1:cut"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := Open(ctx, Config{
		App: filesApp, AppID: "8f3c2a7e-1b4d-4c9e-9a2f-6d5e4c3b2a10",
		ServerURL: "http://127.0.0.1:9/jazz", Storage: StorageMemory,
		Auth: DeviceAccount(FileStore(path)),
	})
	if err == nil {
		db.Close()
		t.Error("a cut key: no error")
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "jazz-auth-v1:cut" {
		t.Errorf("a cut key was replaced (%v)", err)
	}
}

// A file another opener creates between Load's read and its Lstat is the
// key, not an error.
func TestFileStoreLoadsAKeyCreatedMeanwhile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "device-key")
	written := path + ".written"
	if err := os.WriteFile(written, []byte("key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		linked := make(chan error, 1)
		go func() { linked <- os.Link(written, path) }()
		for {
			key, err := FileStore(path).Load()
			if err != nil {
				t.Fatalf("round %d: %v", i, err)
			}
			if key != "" {
				break
			}
		}
		if err := <-linked; err != nil {
			t.Fatal(err)
		}
	}
}
