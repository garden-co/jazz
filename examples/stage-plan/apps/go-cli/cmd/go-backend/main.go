// Command go-backend is StagePlan's backend in Go: it publishes the app's
// permissions to the Jazz server and serves the app's Jazz endpoint at
// /jazz, through which end users' clients (TypeScript, Swift, Kotlin, Go)
// sign in with a JWT from your auth provider, or as a device account.
//
// Configuration, from the environment:
//
//	JAZZ_SERVER_URL      the Jazz server (core), e.g. http://127.0.0.1:1625
//	JAZZ_APP_ID          the app's id
//	JAZZ_BACKEND_SECRET  the server's backend secret
//	JAZZ_ADMIN_SECRET    the server's admin secret
//	JWT_ISSUER           the issuer whose JWTs sign users in (iss)
//	JWKS_URL             where that issuer's signing keys are (RS256, ES256)
//	JWT_AUDIENCE         the audience JWTs must name (aud), if set
//	DEVICE_ACCOUNTS      "off" to refuse local-first device accounts
//	PORT                 where to listen (default 8080)
//	JAZZ_PERMISSIONS_FILE  the app's jazz.permissions.json (default: the
//	                     one beside jazz.app.json, ../../jazz.permissions.json
//	                     from this directory, or ./jazz.permissions.json)
//
// A JWT's sub is the user's id. It prints the endpoint's URL once it
// listens.
//
// The app id: a TypeScript client (createJazzClient) is scoped by its
// serverUrl unless it passes scope, so it derives an app id from the URL it
// uses (scheme, host, port, path). Run the core and go-backend with that id,
// or give the client scope: "<JAZZ_APP_ID>".
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func env(name string) string {
	v := os.Getenv(name)
	if v == "" && name != "JWT_AUDIENCE" {
		log.Fatalf("%s is required", name)
	}
	return v
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	permissions, err := readPermissions()
	if err != nil {
		return err
	}
	backend, err := jazzapp.Open(ctx, jazz.Config{
		Permissions:   permissions,
		AppID:         env("JAZZ_APP_ID"),
		ServerURL:     env("JAZZ_SERVER_URL"),
		BackendSecret: env("JAZZ_BACKEND_SECRET"),
		AdminSecret:   env("JAZZ_ADMIN_SECRET"),
		Storage:       jazz.StorageMemory,
		Session:       &jazz.Session{Subject: "stage-plan"},
	})
	if err != nil {
		return err
	}
	defer backend.Close()
	jwt := &verifier{issuer: env("JWT_ISSUER"), jwksURL: env("JWKS_URL"), audience: env("JWT_AUDIENCE"), leeway: 30 * time.Second}
	devices := os.Getenv("DEVICE_ACCOUNTS") != "off"
	endpoint, err := jazz.NewEndpoint(backend, func(r *http.Request) (*jazz.EndpointUser, error) {
		bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || bearer == "" {
			if devices {
				return jazz.AnyDeviceAccount, nil
			}
			return nil, nil
		}
		user, claims, expires, err := jwt.verify(bearer)
		if err != nil {
			return nil, nil // refused: no one
		}
		return &jazz.EndpointUser{UserID: user, Claims: claims, ExpiresAt: expires}, nil
	}, jazz.EndpointOptions{})
	if err != nil {
		return err
	}
	defer endpoint.Close()
	mux := http.NewServeMux()
	mux.Handle("/jazz/", endpoint)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	fmt.Printf("http://%s/jazz\n", listener.Addr())
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	if err := server.Serve(listener); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func readPermissions() (string, error) {
	paths := []string{os.Getenv("JAZZ_PERMISSIONS_FILE"), "jazz.permissions.json", "../../jazz.permissions.json"}
	for _, path := range paths {
		if path == "" {
			continue
		}
		if b, err := os.ReadFile(path); err == nil {
			return string(b), nil
		}
	}
	return "", fmt.Errorf("no jazz.permissions.json: set JAZZ_PERMISSIONS_FILE")
}
