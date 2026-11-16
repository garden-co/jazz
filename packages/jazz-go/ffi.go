package jazz

// The jazz-ffi C ABI (include/jazz_ffi.h), called without cgo: jazz-go
// loads libjazz_ffi's shared library at run time with purego, so building
// an app needs no C compiler and CGO_ENABLED=0 works.
//
// The library comes from, in order: JAZZ_FFI_LIBRARY (a path); the copy
// this module embeds for the platform (release builds, see embed_*.go),
// extracted once into the user cache dir; or, inside the Jazz repository,
// target/release or target/debug.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// embeddedLibrary is this platform's libjazz_ffi, when the module ships
// one (set by an embed_<os>_<arch>.go file).
var embeddedLibrary []byte

var ffi struct {
	once sync.Once
	err  error

	open             func(config *byte, err *uintptr) uintptr
	openUser         func(config *byte, cb uintptr, ctx uintptr, err *uintptr) uintptr
	call             func(h uintptr, method, args *byte, err *uintptr) uintptr
	subscribe        func(h uintptr, query *byte, cb uintptr, ctx uintptr, err *uintptr) uintptr
	unsubscribe      func(sub uintptr)
	watch            func(h uintptr, what *byte, cb uintptr, ctx uintptr, err *uintptr) uintptr
	unwatch          func(w uintptr)
	close            func(h uintptr)
	stringFree       func(s uintptr)
	generateKey      func(err *uintptr) uintptr
	endpointNew      func(h uintptr, cb uintptr, ctx uintptr, options *byte, err *uintptr) uintptr
	endpointHandle   func(e uintptr, request *byte, err *uintptr) uintptr
	endpointIsSocket func(e uintptr, path *byte) int32
	endpointForUser  func(e uintptr, request *byte, err *uintptr) uintptr
	endpointSocket   func(e uintptr, upgrade *byte, onMessage, onClose uintptr, ctx uintptr, err *uintptr) uintptr
	socketPush       func(s uintptr, data unsafe.Pointer, n uintptr, err *uintptr) int32
	socketFree       func(s uintptr)
	endpointFree     func(e uintptr)
	malloc           func(n uintptr) uintptr

	// The C callbacks, made once: each gets a Go id as its ctx.
	subscriptionCallback, tokenCallback, authCallback, socketMessage, socketClosed uintptr
}

// loadFFI loads the library and binds the C ABI, once.
func loadFFI() error {
	ffi.once.Do(func() { ffi.err = bind() })
	return ffi.err
}

func bind() error {
	path, err := libraryPath()
	if err != nil {
		return err
	}
	lib, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return fmt.Errorf("jazz: loading %s: %w", path, err)
	}
	libc, err := purego.Dlopen(libcName(), purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return fmt.Errorf("jazz: loading libc: %w", err)
	}
	for name, fn := range map[string]any{
		"jazz_open": &ffi.open, "jazz_open_user": &ffi.openUser, "jazz_call": &ffi.call,
		"jazz_subscribe": &ffi.subscribe, "jazz_unsubscribe": &ffi.unsubscribe,
		"jazz_watch": &ffi.watch, "jazz_unwatch": &ffi.unwatch, "jazz_close": &ffi.close,
		"jazz_string_free": &ffi.stringFree, "jazz_generate_device_key": &ffi.generateKey,
		"jazz_endpoint_new": &ffi.endpointNew, "jazz_endpoint_handle": &ffi.endpointHandle,
		"jazz_endpoint_is_socket": &ffi.endpointIsSocket, "jazz_endpoint_for_user": &ffi.endpointForUser,
		"jazz_endpoint_socket": &ffi.endpointSocket, "jazz_socket_push": &ffi.socketPush,
		"jazz_socket_free": &ffi.socketFree, "jazz_endpoint_free": &ffi.endpointFree,
	} {
		sym, err := purego.Dlsym(lib, name)
		if err != nil {
			return fmt.Errorf("jazz: %s has no %s (an older libjazz_ffi?)", path, name)
		}
		purego.RegisterFunc(fn, sym)
	}
	purego.RegisterLibFunc(&ffi.malloc, libc, "malloc")
	ffi.subscriptionCallback = purego.NewCallback(onSubscription)
	ffi.tokenCallback = purego.NewCallback(onToken)
	ffi.authCallback = purego.NewCallback(onAuth)
	ffi.socketMessage = purego.NewCallback(onSocketMessage)
	ffi.socketClosed = purego.NewCallback(onSocketClosed)
	return nil
}

func libcName() string {
	if runtime.GOOS == "darwin" {
		return "/usr/lib/libSystem.B.dylib"
	}
	return "libc.so.6"
}

func libraryFile() string {
	if runtime.GOOS == "darwin" {
		return "libjazz_ffi.dylib"
	}
	return "libjazz_ffi.so"
}

func libraryPath() (string, error) {
	if path := os.Getenv("JAZZ_FFI_LIBRARY"); path != "" {
		return path, nil
	}
	if len(embeddedLibrary) > 0 {
		return extractEmbedded()
	}
	// Inside the Jazz repository: this file is packages/jazz-go/ffi.go.
	if _, here, _, ok := runtime.Caller(0); ok {
		root := filepath.Join(filepath.Dir(here), "..", "..", "target")
		for _, profile := range []string{"release", "debug"} {
			path := filepath.Join(root, profile, libraryFile())
			if _, err := os.Stat(path); err == nil {
				return path, nil
			}
		}
	}
	return "", fmt.Errorf("jazz: no libjazz_ffi for %s/%s: this jazz-go build ships none; set JAZZ_FFI_LIBRARY to a %s from the release (or cargo build -p jazz-ffi)",
		runtime.GOOS, runtime.GOARCH, libraryFile())
}

// extractEmbedded writes the embedded library once into the user cache
// dir, named by its hash, and returns its path. The directory must be the
// user's own and not writable by others, so no one else can swap the
// library between the check and the load; there is no shared fallback.
func extractEmbedded() (string, error) {
	sum := sha256.Sum256(embeddedLibrary)
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("jazz: no user cache dir to keep Jazz's core in (%v): set JAZZ_FFI_LIBRARY", err)
	}
	dir := filepath.Join(cache, "jazz-go", hex.EncodeToString(sum[:8]))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := private(filepath.Join(cache, "jazz-go")); err != nil {
		return "", err
	}
	if err := private(dir); err != nil {
		return "", err
	}
	path := filepath.Join(dir, libraryFile())
	if f, err := os.Open(path); err == nil {
		b, readErr := io.ReadAll(f)
		f.Close()
		if readErr == nil && sha256.Sum256(b) == sum {
			return path, nil
		}
	}
	tmp, err := os.CreateTemp(dir, ".lib-*")
	if err != nil {
		return "", err
	}
	if _, err := tmp.Write(embeddedLibrary); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Chmod(0o500); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	tmp.Close()
	if err := os.Rename(tmp.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		os.Remove(tmp.Name())
		return "", err
	}
	return path, nil
}

// private refuses a directory others may write to (or that isn't ours),
// tightening one we own.
func private(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("jazz: %s is not a directory", dir)
	}
	if !ownedByUs(info) {
		return fmt.Errorf("jazz: %s isn't yours: refusing to load Jazz's core from it (set JAZZ_FFI_LIBRARY)", dir)
	}
	if info.Mode().Perm()&0o022 != 0 {
		if err := os.Chmod(dir, info.Mode().Perm()&^0o022); err != nil {
			return fmt.Errorf("jazz: %s is writable by others: %v", dir, err)
		}
	}
	return nil
}

// cstr is s as a NUL-terminated string: keep the slice alive (it is) for
// the call.
func cstr(s string) *byte {
	b := make([]byte, len(s)+1)
	copy(b, s)
	return &b[0]
}

// goString copies the NUL-terminated string at p.
func goString(p uintptr) string {
	if p == 0 {
		return ""
	}
	n := 0
	for *(*byte)(unsafe.Add(unsafe.Pointer(nil), p+uintptr(n))) != 0 {
		n++
	}
	return string(unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(nil), p)), n))
}

// goBytes copies n bytes at p.
func goBytes(p, n uintptr) []byte {
	if p == 0 || n == 0 {
		return nil
	}
	return append([]byte(nil), unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(nil), p)), n)...)
}

// mallocString is s copied into C memory jazz-ffi frees (callback answers).
func mallocString(s string) uintptr {
	p := ffi.malloc(uintptr(len(s) + 1))
	if p == 0 {
		return 0
	}
	dst := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(nil), p)), len(s)+1)
	copy(dst, s)
	dst[len(s)] = 0
	return p
}

// takeString is the string jazz-ffi returned at p, which it frees.
func takeString(p uintptr) (string, bool) {
	if p == 0 {
		return "", false
	}
	s := goString(p)
	ffi.stringFree(p)
	return s, true
}

// takeError is the error jazz-ffi stored, freeing it.
func takeError(p uintptr) error {
	text, ok := takeString(p)
	if !ok {
		return nil
	}
	return parseError(text)
}
