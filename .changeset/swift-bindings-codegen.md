---
"jazz-tools": patch
---

jazz-codegen gains a Swift back end (`JazzApp.swift`, not yet available from `jazz-tools generate`): a `Codable` row struct, insert and patch structs and a typed query builder per table. Computed fields and transforms are refused as not supported in Swift yet.
