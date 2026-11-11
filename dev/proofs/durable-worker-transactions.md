# Durable worker transaction ownership

The SDK can select its authenticated durable browser worker before opening an explicitly opted-in serializable transaction. It sends the schema and opening context once, then waits for that worker to open the native batch before transaction preparation reads or stages writes. It never opens a foreground batch and transfers it later.

Each handle belongs to one admitted peer, native runtime and schema view. Admission captures the app, physical database, storage owner, authenticated session and claims. Operations check that binding before and after asynchronous work. Disconnecting from the remote server does not retire the local owner; replacing its session or closing its peer, link or storage does.

The worker keeps the native single-use seal. The page receives only its reserved original transaction identity. Publication uses the frozen native batch and its original policy, read set and snapshot semantics. A reservation, local persistence or an observed row does not prove Global acceptance, current membership, complete history or absence of revocation.

The fixed private command set covers opening, reads, accepted reads, ordinary row mutations, insertion coordinate preparation, seal, publication, cancellation and durability waits. It cannot evaluate caller-supplied functions. Queued values are copied at admission. An operation failure prevents publication of the remaining prefix. Cancelling an in-flight read rolls back its original batch rather than waiting behind that read.

An application wait registered before publication is carried to the worker and registered against the real native write before host progress. Ending a wait aborts its wait work; it does not change the write's fate. Peer retirement revokes handles synchronously and cleans up batches, seals and waits on the actual owner.

The worker-open readiness receipt confirms its completed physical open and authenticated peer admission only. Native journal eligibility and recovered-packet validation still run separately during before-begin admission. This receipt supplies no authority or completeness evidence.

Ordinary transactions without the explicit accepted-read opening capability use their existing foreground route, including offline operation. This SDK unit depends on the native owner-bound publication capability in #5110. It does not implement E2EE founder eligibility, membership policies, key retention or lifecycle promotion.

Waits registered while an opening prerequisite is pending attach to the selected worker before publication. Ended waits release active worker capacity without requiring another page request. Cancellation identities remain scoped to their existing published write and owner lifetime; at 4096 retained cancellations for one write, later waits on that write refuse, while independent publications remain available. An opening transaction accepts at most 64 registered wait intents.

For an implicit own-client opening, default operations keep that captured authenticated native owner. They do not convert the default into a page-realm explicit session token. Caller-supplied sessions remain explicit and subject to the original identity and claims checks. Sealed publication associates its reserved journal coordinate with the exact native write; native identity, policy, fate and acceptance are unchanged.
