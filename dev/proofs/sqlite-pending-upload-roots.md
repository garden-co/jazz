# Pending streamed uploads on SQLite

A pending local upload now retains every large-value root named by its wire
operations until the authority returns its final fate. Previously only relayed
uploads had this independent hold. A small streamed value could become an inline
row cell, lose its streaming lease, and have its chunks reclaimed while its
pending upload still referenced them. Reconnecting then produced a missing-chunks
rejection despite a successful local write.

The upload journal, row mutation and root holds commit together. Maintenance and
reopening preserve the holds. Acceptance or rejection releases only the roots
for that original node and transaction, atomically with the fate. Reclamation
still respects other pending uploads, live row images and chunk parents.
All lookups use the existing node/transaction and root indexes; this adds no scan
of unrelated rows or pending writes and does not bypass authority validation.

For example, Alice streams a small encrypted image while offline, closes her
persistent database and reopens it. Maintenance cannot remove its pending chunks.
On reconnect, normal sync sends the chunks before the write, and Bob's authority
can accept it. If the row instead conflicts with an existing authority row, the
real rejection releases Alice's upload hold. Large values remain readable through
the ordinary row-root references after acceptance.

## Encoding and validation

This reuses `_jazz_lv_relay_root_v1` with its existing `(node, tx, root)` key,
column constraints and indexes for both local and relayed uploads. No table,
serialized field, wire tag or byte encoding changes. Existing relay journal and
fate byte fixtures continue to apply. Experimental pending writes whose chunks
were already reclaimed cannot be repaired by this change.

The public `large_value_sync` regressions cover inline and large streamed values,
ordinary and private owners, maintenance, durable reopen, real authority receipts,
and reclamation after acceptance and rejection. The existing relay and staged
streaming tests exercise the same holds through relayed and atomic uploads.

The SDK also reads a rejected write's retained typed fate before interpreting a
binding's diagnostic text. WASM and NAPI can format diagnostics differently;
authorization failures must still report `permission_denied`, while malformed
payload text containing a rejection name must retain its actual classification.
If the binding cannot return a receipt, the existing diagnostic fallback remains.
Local transaction-conflict handling is unchanged. New adapter tests cover this
boundary without changing existing error-code expectations.
