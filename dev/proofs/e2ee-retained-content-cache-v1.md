# Private retained-content cache v1

This is local private storage for content keys previously validated online. It is
not a wire message, membership certificate, accepted-query result or SELECT
coverage proof. An offline `ready` result means a bound retained content key is
usable. It says nothing about current membership, absent revocation, the latest
epoch, another recipient or permission to write.

The optional `retainedContentV1` field lives alongside the existing
`jazz-e2ee-local-devices-v2` device records. Its named format is
`jazz-e2ee-retained-content-v1`. Old stores without the field have no offline
content capability and must validate online. Unknown cache versions fail closed;
device credentials and unrelated private-store fields are not discarded.

The cache contains a revision, terminal locally known device denials, signed
device bindings and sealed content-key records. Each binding includes the exact
account/application registry scope, physical schema digest, device and key
mechanisms, and a positive local device-key row with paired state/content origins.
Each content record additionally binds a scope table, root and epoch, positive
root-row content/origins, and the table/column UUIDs used for the cell context.
Those UUIDs are prior validated content identities, not fresh offline catalogue
coverage. The current executing owner's ordinary local read path remains intact.

The key is sealed to the retained device key. The local device signs the entire
record, including its sealed key and bindings. JSON with keys sorted by UTF-16
code units independently of locale, prefixed by
the format name, supplies the signing and sealing contexts. The independent byte
fixture is `packages/jazz-tools/src/e2ee/fixtures/retained-content-context-v1.json`.
The signature protects this local record; it does not turn the device into an
authority. A same-cut observation or a Global submission receipt does not prove
that all revocations or membership rows were retained.

Only successful online accepted preparation and its actual Global confirmation
may populate this cache. Provisional initial-space keys are not persisted here.
The compiler's initial activation waits for Global before subsequent content
retention. A private-store failure after that point does not undo the core's
already accepted transaction.

Offline decryption selects explicitly retained epochs for the stored cell frame.
It does not choose a latest epoch from partial history, replay a membership set,
mint a root, repair a group, distribute a key or authorise a mutation. Missing or
changed positive bindings yield unknown/unavailable. Failed online authority is
not retried as equivalent local authority.

Known device revocation is terminal for that cached device owner. A known space
denial removes that address, and a local revision prevents an older asynchronous
online result from restoring it. Borrowed key copies are scoped to the active
owner, wiped on release and close, and checked again after asynchronous use.
Other streams and storage formats are unchanged.

## Private online anchor version 1

`jazz-e2ee-private-online-anchor-v1` is a separate private signing-context tag,
not C provenance. A genuine online accepted transaction captures the device-key
and space-root rows at one schema/cut, validates both paired origins, and waits
for Global before publishing owner-private anchors. Actual account/session,
keys, namespace catalogue IDs, exact row bodies and the live Db owner are bound.
No complete-history, current offline membership or authority grant follows.

The existing outer retained-content and provisional-key encodings remain v1.
Their signed anchor now admits this explicitly versioned variant; old C-backed
records retain identical bytes and remain readable. Older readers fail closed
on the new provenance tag. New readers require the matching owner-local proof;
a later owner must obtain its own genuine online validation. The byte fixture
`private-online-cache-anchor-v1.json` pins the added signing context.
A later same-content observation may advance its cut without changing the
immutable origin pair; each newly signed device/root bundle still requires
matching cuts. No old seal or wire/database encoding is rewritten.

## E10 provisional writes

The separate provisional-key record lets a supported offline operation stage
encrypted content with a prior-valid bound key. This is local pending work, not
current membership or Global acceptance. A removed recipient may retain that
key and read the provisional ciphertext; reconnect authority may reject it.
The retained-content record itself does not become a write-authority receipt.
Known denials, exact bindings, revision and owner-lifetime checks still apply.
