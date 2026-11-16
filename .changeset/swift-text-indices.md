---
"jazz-tools": patch
---

Jazz for Swift: `JazzText` edits take Swift's own positions. `insert(_:at:)`, `delete(_:)` and `replace(_:with:)` take `String.Index` / `Range<String.Index>` of `current`; `replace(_:with:)` also takes an `NSRange` (UTF-16, as `UITextView` gives it), and `replace(utf16:with:)` raw UTF-16 offsets. The `Int` (UTF-16) overloads of `insert`, `delete` and `replace` are gone: pass `current`'s indices, or an `NSRange`.
