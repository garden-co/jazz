---
"jazz-tools": patch
---

Go: `ForUser` on a trusted backend client whose `Config.Permissions` isn't the bundle the main backend serves fails with `jazz.ErrBundleMismatch` (`Error.OwnBundle`, `Error.ServedBundle`), as TS's `bundle_mismatch`.
