package main

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/evanw/esbuild/pkg/api"
)

// web/src holds the TSX sources; web/ is a pnpm workspace package, so
// node_modules can land alongside but must not be embedded — the embed
// directive scopes to web/src to keep the binary small.
//
//go:embed web/src
var webFS embed.FS

const webRoot = "web/src"

type uiBundle struct {
	indexHTML []byte
	mainJS    []byte
}

// buildUIBundle unpacks the embedded TSX sources to a temp dir and bundles
// them with esbuild. React/ReactDOM/ReactQuery are kept external — the
// browser resolves them via the import-map in index.html (pointing at
// esm.sh) so we never need a node_modules at runtime.
func buildUIBundle() (*uiBundle, error) {
	indexHTML, err := webFS.ReadFile(webRoot + "/index.html")
	if err != nil {
		return nil, fmt.Errorf("read embedded index.html: %w", err)
	}

	tmp, err := os.MkdirTemp("", "local-telemetry-web-*")
	if err != nil {
		return nil, fmt.Errorf("temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	if err := fs.WalkDir(webFS, webRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(webRoot, path)
		target := filepath.Join(tmp, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := webFS.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	}); err != nil {
		return nil, fmt.Errorf("unpack web/src: %w", err)
	}

	result := api.Build(api.BuildOptions{
		EntryPoints: []string{filepath.Join(tmp, "main.tsx")},
		Bundle:      true,
		Format:      api.FormatESModule,
		JSX:         api.JSXAutomatic,
		External: []string{
			"react",
			"react/jsx-runtime",
			"react-dom/client",
			"@tanstack/react-query",
		},
		Loader: map[string]api.Loader{
			".tsx": api.LoaderTSX,
			".ts":  api.LoaderTS,
		},
		Target:   api.ES2020,
		Write:    false,
		LogLevel: api.LogLevelSilent,
	})
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("esbuild: %s", result.Errors[0].Text)
	}
	if len(result.OutputFiles) == 0 {
		return nil, fmt.Errorf("esbuild produced no output")
	}
	return &uiBundle{indexHTML: indexHTML, mainJS: result.OutputFiles[0].Contents}, nil
}

func registerUIHandlers(mux *http.ServeMux, bundle *uiBundle) {
	mux.HandleFunc("/main.js", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(bundle.mainJS)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(bundle.indexHTML)
	})
}
