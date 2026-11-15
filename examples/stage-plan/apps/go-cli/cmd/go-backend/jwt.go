package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// verifier checks bearer JWTs against an issuer's JWKS: RS256 or ES256
// signatures, iss, aud (when set), exp and nbf (with leeway), and a
// non-empty sub, the user's id.
type verifier struct {
	issuer, audience, jwksURL string
	leeway                    time.Duration

	mu      sync.Mutex
	keys    map[string]crypto.PublicKey
	fetched time.Time
}

type claims struct {
	Subject   string          `json:"sub"`
	Issuer    string          `json:"iss"`
	Audience  json.RawMessage `json:"aud"`
	ExpiresAt *int64          `json:"exp"`
	NotBefore *int64          `json:"nbf"`
}

func (v *verifier) verify(token string) (string, map[string]any, time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", nil, time.Time{}, errors.New("not a JWT")
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := decodePart(parts[0], &header); err != nil {
		return "", nil, time.Time{}, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", nil, time.Time{}, errors.New("bad signature encoding")
	}
	key, err := v.key(header.Kid)
	if err != nil {
		return "", nil, time.Time{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch header.Alg {
	case "RS256":
		k, ok := key.(*rsa.PublicKey)
		if !ok || rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], signature) != nil {
			return "", nil, time.Time{}, errors.New("bad signature")
		}
	case "ES256":
		k, ok := key.(*ecdsa.PublicKey)
		if !ok || len(signature) != 64 ||
			!ecdsa.Verify(k, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
			return "", nil, time.Time{}, errors.New("bad signature")
		}
	default:
		return "", nil, time.Time{}, fmt.Errorf("unsupported alg %q", header.Alg)
	}
	var c claims
	if err := decodePart(parts[1], &c); err != nil {
		return "", nil, time.Time{}, err
	}
	var all map[string]any
	_ = decodePart(parts[1], &all)
	now := time.Now()
	switch {
	case c.Issuer != v.issuer:
		return "", nil, time.Time{}, errors.New("wrong issuer")
	case c.ExpiresAt == nil:
		return "", nil, time.Time{}, errors.New("no exp")
	case now.After(time.Unix(*c.ExpiresAt, 0).Add(v.leeway)):
		return "", nil, time.Time{}, errors.New("expired")
	case c.NotBefore != nil && now.Add(v.leeway).Before(time.Unix(*c.NotBefore, 0)):
		return "", nil, time.Time{}, errors.New("not yet valid")
	case c.Subject == "":
		return "", nil, time.Time{}, errors.New("no sub")
	case v.audience != "" && !hasAudience(c.Audience, v.audience):
		return "", nil, time.Time{}, errors.New("wrong audience")
	}
	return c.Subject, all, time.Unix(*c.ExpiresAt, 0), nil
}

func decodePart(part string, out any) error {
	b, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return errors.New("bad JWT encoding")
	}
	return json.Unmarshal(b, out)
}

func hasAudience(raw json.RawMessage, want string) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one == want
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, a := range many {
			if a == want {
				return true
			}
		}
	}
	return false
}

// key is the JWKS key kid names, fetching the set again (at most every
// 30 s) when it doesn't know kid.
func (v *verifier) key(kid string) (crypto.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	if time.Since(v.fetched) < 30*time.Second && v.keys != nil {
		return nil, errors.New("unknown key")
	}
	keys, err := fetchJWKS(v.jwksURL)
	v.fetched = time.Now()
	if err != nil {
		return nil, err
	}
	v.keys = keys
	if k, ok := keys[kid]; ok {
		return k, nil
	}
	return nil, errors.New("unknown key")
}

func fetchJWKS(url string) (map[string]crypto.PublicKey, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var set struct {
		Keys []struct {
			Kty, Kid, N, E, Crv, X, Y string
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, err
	}
	keys := map[string]crypto.PublicKey{}
	for _, k := range set.Keys {
		switch k.Kty {
		case "RSA":
			n, err1 := base64.RawURLEncoding.DecodeString(k.N)
			e, err2 := base64.RawURLEncoding.DecodeString(k.E)
			if err1 != nil || err2 != nil {
				continue
			}
			keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		case "EC":
			if k.Crv != "P-256" {
				continue
			}
			x, err1 := base64.RawURLEncoding.DecodeString(k.X)
			y, err2 := base64.RawURLEncoding.DecodeString(k.Y)
			if err1 != nil || err2 != nil {
				continue
			}
			keys[k.Kid] = &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
		}
	}
	return keys, nil
}
