---
"jazz-tools": patch
---

Jazz for Swift: every call is plain `throws` instead of `throws(JazzError)`, since Swift 6.1 (Xcode 16) crashes compiling callers of typed-throws async functions. What a call throws is still a `JazzError`, but callers that relied on the typed error now need `catch let error as JazzError`.
