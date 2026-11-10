# Fresh catalogue coverage contract

`jazz.sqlite.catalogue-coverage.v1` adds one mode to the existing one-shot read route. It uses the current catalogue, stored names, namespace and name digest. It adds no storage layout or identity store.

A request is `SyncMessage::Read` with appended `ReadSpan::CatalogueOnce` (tag 3): fixed 16-byte schema version followed by effective identity/claims scope. Its carrier is `Query::from("").limit(0)` with empty binding, `include_deleted=false`, Remote tier, Current view and no resume coordinate. The carrier is never compiled or evaluated against rows. Any other carrier is refused. The existing wait field supplies the relay deadline.

The response is appended `SyncMessage::CatalogueCovered` (tag 21): request ID (unsigned LEB128), schema UUID, authority namespace and stored-name digest (each 16 raw bytes), then scope. Scope uses the existing canonical author UTF-8 string and claims map encoding. The independent `crates/jazz/fixtures/catalogue_coverage_v1.json` pins field order, tags, lengths and empty claims. Existing message and ReadSpan tags remain unchanged.

The authority validates the requested admitted schema and session. Only an authenticated SYSTEM connection can delegate a different effective scope. An ordinary connection must match the requested identity and claims. Existing Catalogue/Welcome output precedes the acknowledgement. A relay uses the ordinary one-shot answer slot and pending request lifetime, forwards the effective scope, and validates the received catalogue before forwarding the acknowledgement. It never constructs fresh coverage from cached metadata or ordinary Settled messages.

The non-async core admission method captures schema, actual peer/registered claims and deadline before returning a future. Completion checks that the admitted scope/schema still match and that the received namespace/name digest match the existing confirmed catalogue. Each preparation issues a fresh request even when the catalogue digest is unchanged. Cancellation, reconnect and delayed wakeups share the existing read machinery.

The acknowledgement contains no row cut, row evidence, query completeness or permission verdict. Catalogue coverage neither depends on retained row history nor grants row access. A schema/session preparation may resolve several table/column identities synchronously on the same handle after one coverage operation; those identities are ordinary stored-name identities, not reusable freshness tokens. Later operations need their own coverage.

The additive NAPI/WASM `coverCatalogue(timeoutMs, author?, claims?)` is an in-process ABI. Claims use the same JSON convention as ordinary Binding reads. Omitted identity selects the real opened core peer and registered claims; no JS identity inference or lookup-time claims registration occurs. Millisecond budgets must be nonnegative safe integers. Existing cached tableIdentity/columnIdentity methods retain their behaviour.

Use matching extension-capable clients, relays and authorities for this mode. Older peers do not recognise the appended tags and fail the request/link; there is no silent fallback to cached metadata. Ordinary compatible traffic and all durable formats remain unchanged.
