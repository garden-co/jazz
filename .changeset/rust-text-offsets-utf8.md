---
"jazz-tools": patch
---

**Rust: text offsets are UTF-8 bytes (`str` indices) throughout `jazz-app-support`.**

- **Splices:** `Page::text(from..to)` and `Splice::text(at, delete, "…")` build UTF-8 pages and splices for `jazz.splice`, `tx.splice` and `Edits::splice`. A UTF-8 offset inside a code point is now refused with the new `Error::TextOffset` instead of a generic write error (the core refuses it for every host, reading only the bytes at the splice's ends). `Page::TextUtf16` stays for interop.
- **Partial reads:** `ValuePart::text(from..to)` (and `ValuePart::bytes`) build a UTF-8 (byte) range for `Query::part`; `ValuePart::TextUtf16` stays for interop.
- **Subscription splices:** `ValueSplice::at`/`delete` are UTF-8 bytes on text columns, documented as the Rust fields; `at_utf16`/`delete_utf16` are for UTF-16 hosts.
- **Text anchors:** `Anchor::capture_utf8`/`resolve_utf8` and `Range::capture_utf8`/`resolve_utf8` take and return UTF-8 byte offsets, refusing an offset inside a code point with `NotCharBoundary`. The anchors are the same as the UTF-16 ones, so they are shared with TypeScript clients.
