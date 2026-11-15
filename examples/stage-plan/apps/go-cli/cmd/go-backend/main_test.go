package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func mint(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	head := enc(map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"}) + "." + enc(claims)
	digest := sha256.Sum256([]byte(head))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return head + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestVerifierChecksJWTs(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "kid": "k1",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()
	v := &verifier{issuer: "https://auth.example", audience: "stage-plan", jwksURL: jwks.URL, leeway: time.Second}
	now := time.Now().Unix()
	good := map[string]any{"iss": "https://auth.example", "aud": "stage-plan", "sub": "alice", "exp": now + 600}
	with := func(change map[string]any) map[string]any {
		c := map[string]any{}
		for k, x := range good {
			c[k] = x
		}
		for k, x := range change {
			if x == nil {
				delete(c, k)
			} else {
				c[k] = x
			}
		}
		return c
	}
	if user, _, _, err := v.verify(mint(t, key, "k1", good)); err != nil || user != "alice" {
		t.Fatalf("a valid token: %q %v", user, err)
	}
	for name, token := range map[string]string{
		"expired":        mint(t, key, "k1", with(map[string]any{"exp": now - 60})),
		"wrong issuer":   mint(t, key, "k1", with(map[string]any{"iss": "https://evil.example"})),
		"wrong audience": mint(t, key, "k1", with(map[string]any{"aud": "other"})),
		"no sub":         mint(t, key, "k1", with(map[string]any{"sub": nil})),
		"no exp":         mint(t, key, "k1", with(map[string]any{"exp": nil})),
		"not yet":        mint(t, key, "k1", with(map[string]any{"nbf": now + 600})),
		"bad signature":  mint(t, other, "k1", good),
		"unknown key":    mint(t, key, "k2", good),
		"not a jwt":      "token-alice",
		"alg none":       base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","kid":"k1"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://auth.example","sub":"alice","exp":9999999999}`)) + ".",
	} {
		if user, _, _, err := v.verify(token); err == nil {
			t.Errorf("%s: accepted as %q", name, user)
		}
	}
}
