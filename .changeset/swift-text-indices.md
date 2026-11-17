---
"jazz-tools": patch
---

**Breaking:** `JazzText`'s `Int` (UTF-16) overloads of `insert(_:at:)`, `delete(_:)` and `replace(_:with:)` are removed. Jazz for Swift: `JazzText` edits take Swift's own positions. `insert(_:at:)`, `delete(_:)` and `replace(_:with:)` take `String.Index` / `Range<String.Index>` of `current`; `replace(_:with:)` also takes an `NSRange` (UTF-16, as `UITextView` gives it), and `replace(utf16:with:)` raw UTF-16 offsets; UTF-16 positions may split a character (a backspace removing one combining mark) and are refused only inside a surrogate pair. Pass `current`'s indices, or an `NSRange`.
