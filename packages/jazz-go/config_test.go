package jazz

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestConfigNeverPrintsItsSecrets(t *testing.T) {
	c := Config{AppID: "a", ServerURL: "https://x", Auth: DeviceKey("jazz-auth-v1:s3cret-key")}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(format, c)
		if strings.Contains(out, "s3cret") {
			t.Errorf("%s printed a secret: %s", format, out)
		}
	}
	if out := fmt.Sprintf("%v %#v", c.Auth, c.Auth); strings.Contains(out, "s3cret") {
		t.Errorf("auth printed its key: %s", out)
	}
}

func TestPlaintextToARemoteHostIsRefused(t *testing.T) {
	for url, ok := range map[string]bool{
		"https://app.example/jazz": true, "wss://app.example/jazz": true,
		"http://127.0.0.1:8080/jazz": true, "http://localhost:3000/jazz": true, "ws://[::1]:1/jazz": true,
		"http://app.example/jazz": false, "ws://10.0.0.5/jazz": false, "ftp://x": false,
	} {
		err := Config{ServerURL: url}.checkTransport()
		if ok != (err == nil) {
			t.Errorf("%s: %v", url, err)
		}
		if !ok && !errors.Is(err, ErrRefused) {
			t.Errorf("%s: %v is not ErrRefused", url, err)
		}
	}
	if err := (Config{ServerURL: "http://app.internal/jazz", AllowPlaintext: true}).checkTransport(); err != nil {
		t.Errorf("AllowPlaintext: %v", err)
	}
}
