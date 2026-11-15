---
"jazz-tools": patch
---

`jazz-tools generate` writes Go bindings (`jazzapp/jazz_app.go`) beside a `go.mod` that requires `github.com/garden-co/jazz/packages/jazz-go`, or with `--lang go`. Apps that declare computed fields or transforms are not supported in Go yet.
