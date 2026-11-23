---
"jazz-tools": patch
---

Jazz for Swift: `transaction` and `serializableTransaction` can be called from main-actor code, such as a SwiftUI view or view model. Their closures run on the caller's actor; in Swift 6 these calls used to fail to compile ("sending value of non-Sendable type … risks causing data races").
