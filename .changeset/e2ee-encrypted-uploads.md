---
"jazz-tools": minor
"jazz-napi": minor
"jazz-rn": patch
---

Wire encrypted Bytea uploads and whole-value replacements through the ordinary
large-value substrate. Stage bounded ciphertext outside a database transaction,
then attach it in one exclusive publication with fresh epoch, device and
membership checks. Reject stale uploads without replaying their sources.

Add `Db.streamingTransaction` for mutation declarations that publish a new scope,
its exact initial recipients and first encrypted files together. Complete initial
recipient device delivery after authoritative acceptance; a denied later delivery
does not roll back accepted data and requires explicit reconciliation.

Version stream records independently from ordinary encrypted cells, including the
cipher adapter mechanism and authenticated context. Ordinary queries return
complete authenticated byte values and still materialise the whole file.

Uploads are root-view only. Branch targets, encrypted Text/JSON streams,
equality-indexed stream columns, scope-row streams and partial file edits remain
unsupported. Rejected transactions retain ciphertext for normal retry/discard;
proofless durable exclusive retries fail closed. Revocation does not erase
previously uploaded ciphertext or previously held keys.

Extend native, WASM and foreground stage/attach capabilities and codec contracts.
Foreground codec coverage is not a claim of React Native E2EE qualification.
