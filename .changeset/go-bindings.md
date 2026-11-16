---
"jazz-tools": patch
---

`jazz-tools generate` writes Go bindings (`jazzapp/jazz_app.go`) beside a `go.mod` that requires `github.com/garden-co/jazz/packages/jazz-go/v2`, or with `--lang go`. The Go module (`jazz-go/v2`, no cgo) opens an app locally or as an end user through the main backend's `/jazz`.
