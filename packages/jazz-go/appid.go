package jazz

import (
	"crypto/sha1"
	"fmt"
	"net/url"
	"strings"
)

// EndpointAppID is the app id a TypeScript client (createJazzClient)
// derives from the serverUrl it is given when it passes no scope: the
// UUIDv5, in the DNS namespace, of the endpoint's URL read as http(s)
// (ws as http, wss as https), its origin and path without trailing
// slashes. Run the Jazz server and your Go backend with it
// (Config.AppID), so those clients reach your app.
func EndpointAppID(serverURL string) (string, error) {
	scope, err := endpointScope(serverURL)
	if err != nil {
		return "", err
	}
	return uuidV5(dnsNamespace, scope), nil
}

// endpointScope is TS's endpointScope: origin + path, trailing slashes
// trimmed.
func endpointScope(serverURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("jazz: invalid server URL %q: expected http://, https://, ws:// or wss://", serverURL)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "ws":
		scheme = "http"
	case "wss":
		scheme = "https"
	case "http", "https":
	default:
		return "", fmt.Errorf("jazz: invalid server URL %q: expected http://, https://, ws:// or wss://", serverURL)
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port := u.Port(); port != "" && !(scheme == "http" && port == "80") && !(scheme == "https" && port == "443") {
		host += ":" + port
	}
	return scheme + "://" + host + strings.TrimRight(u.EscapedPath(), "/"), nil
}

// dnsNamespace is RFC 4122's DNS namespace (6ba7b810-9dad-11d1-80b4-00c04fd430c8).
var dnsNamespace = [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

func uuidV5(namespace [16]byte, name string) string {
	h := sha1.New()
	h.Write(namespace[:])
	h.Write([]byte(name))
	var u [16]byte
	copy(u[:], h.Sum(nil))
	u[6] = u[6]&0x0f | 0x50
	u[8] = u[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}
