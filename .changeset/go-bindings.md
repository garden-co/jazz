---
"jazz-tools": patch
---

`jazz-tools generate` writes Go bindings (`jazzapp/jazz_app.go`) beside a `go.mod` that requires `github.com/garden-co/jazz/packages/jazz-go/v2`, or with `--lang go`. The Go module (`jazz-go/v2`) opens an app as a client, an end user or a backend, and a backend serves its end users at `/jazz` with `jazz.NewEndpoint`.
