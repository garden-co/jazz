---
"jazz-tools": patch
---

The Go module (`jazz-go/v2`) has `jazz.OpenSession`, a session that follows who is signed in, as TS's `JazzSession`: `Session.DB` for the user signed in now, `Watch` for its changes, and sign-in flows (`SignOut`, `Refresh`, `CreateDeviceAccount`, `RestoreDeviceAccount`, `LinkDeviceAccount`, `ExportDeviceKey`). New errors `jazz.ErrReplaced` and `jazz.ErrNoUser`.
