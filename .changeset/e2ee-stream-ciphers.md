---
"jazz-tools": patch
"jazz-napi": patch
---

Add bounded browser/native authenticated stream ciphers with explicit framing, final-record authentication, cancellation, pull backpressure, cleanup, and independent interoperability fixtures. This does not implement encrypted large-value upload, storage publication, or locator authorization.

Expose `E2EeSodiumStream` as a native ESM named export. Early stream return requests upstream cleanup without awaiting it or propagating cleanup failures; use `AbortSignal` to interrupt an already pending read.

Aborting between stream pulls immediately clears the owned crypto state and requests upstream cleanup once, without waiting for the consumer to resume. Cancellation at the final-record or EOF boundary rejects rather than reporting successful completion.
