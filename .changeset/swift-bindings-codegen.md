---
"jazz-tools": patch
---

`jazz-tools generate` writes Swift bindings (`JazzApp.swift`) for an app whose `Package.swift` beside `jazz.app.json` names `jazz-swift`: a `Codable` row struct, insert and patch structs and a typed query builder per table. Computed fields and transforms are refused as not supported in Swift yet.
