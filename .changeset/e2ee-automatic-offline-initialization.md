---
"jazz-tools": patch
"jazz-rn": patch
---

Initialize newly generated local accounts and encrypted spaces automatically, including while offline after the application has cached an authenticated schema catalogue. Persist provisional keys and complete exclusive transactions before reporting Local durability, then reconcile their original identities on reconnect without rerunning transaction callbacks or upload streams.

Local durability is not authority acceptance or verified account history. Imported accounts and explicit recipients without accepted public keys still require online readiness; unavailable recipient keys are rejected before consuming an upload stream. Preserve pending encrypted writes across persistent owner restarts, and bind catalogue caches and restored owners to their admitted application/account scope.

Keep persistent browser catalogue-cache publication with the durable worker.
Foreground and inspector peers retain cached identity access and delegate
readiness without racing the worker to publish older relay-local snapshots.

Authorize relayed exclusive writes as complete transactions so explicitly marked same-commit creation dependencies work without broadening ordinary existence checks or bypassing mergeable read-for-write policy.

Split persistent browser B-tree leaves by encoded byte size, including separator capacity, so uneven encrypted-history records do not overflow a child page despite fitting a valid split. Apply the same root-aware split selection to leaves created earlier in a batch, preserving failure rollback and reopen behavior. Existing stored data remains compatible.

Keep React Native initialisation payload types behind the existing foreground command API without adding a separate relay export.

Preserve ordinary transaction behavior on mixed encrypted/plaintext schemas.
Initialization-capable nullable upserts still seal and journal before local
acknowledgement. Keep device-only enrollment on accepted-history setup, admit the
local browser owner before checking catalogue readiness, and hydrate cold legacy
scope rows without replacing the transaction snapshot. Verified, promoted keys can
reopen against a fresh database; pending and unpromoted journal entries still
require their original owner's exact transaction evidence.

Transfer application wait ownership before deferred transaction submission can
deliver a rejection callback. Waited failures remain on their returned handles;
unwaited failures retain their once-only fallback notification.

Preserve checked point-snapshot observations after the core layer split.
Initialisation propagates storage and required transaction-audit field decoding
failures rather than treating an unreadable existing coordinate as absent.
Coverage projects global time without decoding full payloads per row version.
Legacy nonfallible snapshot observations keep their conservative exclusion.
