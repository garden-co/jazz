# Driver protocol

A driver is an adopter's program, one per language, that does what it is told on the public API
and reports what that API did. It judges nothing: expectations live in `suite/`.

One JSON object per line. The first line out is `{"ready":true,"pid":<pid>}`. Each request
`{"id":<n>,"op":"<op>",…}` runs concurrently with the others and gets one answer:
`{"id":n,"ok":true,"v":<value>}` or `{"id":n,"ok":false,"kind":"<kind>","msg":"<message>"}`.
`kind` is the language's own name for the error (Rust's `Error` variant, Swift's `JazzError` case,
Kotlin's exception class, Go's `Error.Kind`), read at run time so a new kind needs no driver change.
Kinds starting `driver.` are the driver's own: `driver.cancelled`, `driver.unsupported`, `driver.bad`.
At end of input the process exits without closing anything.

| op | arguments | value |
| --- | --- | --- |
| `genkey` | | a new device key |
| `open` | `h dir url app`, `key` or `token`, `storage=persistent\|memory`, `local` (no server, no account) | null |
| `account` | `h` | account id |
| `write` | `h title owner tier=local\|global` | row id |
| `read` | `h tier=local\|remote` | titles, sorted |
| `sync` | `h` | null, once waitForSync returns |
| `close` | `h`, `ms?` (close with that timeout, where the language has one) | null |
| `delete` `signout` `disconnect` `reconnect` | `h` | null |
| `cancel` | `target` (a request id) | null; the target then answers as the language does |
| `sopen` | `h dir url app start=backend\|device\|signed-out`, `token?` | null |
| `stoken` | `h token` (`""` is signed out) | null; what the bearer provider gives from now on |
| `ssnap` | `h` | `{status,clientEpoch,error}` |
| `sjazz` | `h as` | null; handle `as` is the signed-in user's client |
| `screate` `sreopen` `srefresh` `ssignout` `sclose` | `h` | null |

`h` is the caller's name for a handle. A closed handle stays named, to test use after close.
A language without an op answers `driver.unsupported`, which the suite reports as NOT_COVERED.
