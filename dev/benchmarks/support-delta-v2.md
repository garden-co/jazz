# Physical supporting-set delta experiment

Experimental successor to #2951; thesis and outcomes live in
[#2913](https://github.com/garden-co/jazz/issues/2913#issuecomment-5654399881).
This is not approved for merge. The implementation checkpoint may be red while
the format, producers, receivers and compatibility corpus are migrated together.

## Contract under implementation

Wire protocol v2 is the sole negotiated version. Initial/recovery snapshots
establish a fresh opaque 16-byte revision. Ordinary successors name the exact
predecessor and successor revisions and disjoint physical row/version additions
and removals. Compiler roles remain local. Multiple roles supporting one native
version produce one physical membership; removing one role alone cannot retract
it. Content and deletion witnesses remain distinct.

Snapshot and Delta are respectively postcard enum discriminants 0 and 1;
revision bytes are fixed arrays, not serializer-defined UUID strings. The
semantic envelope remains postcard, with the v2 negotiated frame boundary and
explicit byte-level fixtures. There is no v1 decoding or negotiated fallback.
Native storage encodings are unchanged.

No successor installs against the wrong predecessor, missing native additions,
or a different admitted authority usage. Changes become visible atomically.
Pending native-body repair retains dependent deltas; a new snapshot may supersede
them. Reconnect/reopen uses a fresh snapshot, not recovered transport authority.
Scope removal is neither global cached-row deletion nor an access-denial claim.

Authority fallback reissues the original admitted subscription, preserving its
binding and policy context. Preselection arrivals cannot settle it, and their
observed cut is a lower bound on the new snapshot. A stalled repair chain is
limited to 64 pending transitions per subscription; overflowing it requests a
fresh snapshot and retires superseded unsent repairs. An already-sent repair
remains correlated until its reply arrives, but cannot install superseded state.

An unchanged confirmation preserves its revision, so dropping an empty poll
does not break the next predecessor. A nonempty out-of-order or replayed delta
does not install against a different revision; recovery requires a snapshot.
This experiment does not add an unordered/retransmitting transport protocol.

The implementation must avoid full retained-map cloning for atomic staging and
full-manifest construction/comparison on ordinary delta updates. Initial and
recovery snapshots intentionally remain scope-sized.

## Measurement

Compare exact-source native todo inserts/updates and permissioned cold load with
parent `8021dffb31628a3f7f97ede47c30a421354aa13d`, keeping fixtures, allocator,
storage and sample counts unchanged. Confirm through CodSpeed and full GQL
profiles. Prediction: 10–20% lower sequential todo latency; no cold-load win is
promised. Format retention is a separate user decision after correctness and
performance evidence.

Initial matched local receipt: inserts −14.2%/−15.7%, updates −25.4%/−26.7%,
cold first sync +1.1%/+0.6% (paired three-sample ABBA medians). Exact binaries,
source hashes, validation status, caveats and further findings are recorded in
[#2913](https://github.com/garden-co/jazz/issues/2913#issuecomment-5654627498).
