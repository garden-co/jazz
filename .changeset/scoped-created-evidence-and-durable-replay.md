---
"jazz-tools": patch
"jazz-napi": patch
"jazz-rn": patch
---

Add `policy.<table>.existsIncludingCreated.where(...)` for INSERT policies that
need independently authorized parent rows created in the same complete exclusive
transaction. Preserve ordinary policy behavior, authenticated authorship, exact
absence checks and atomic rejection. Bound the proof and reject unseeded cycles;
a pending local row is not automatically authorized evidence.

Retain the original exclusive snapshot, point reads, absent reads and predicate
reads using the existing four-slot `jazz.exclusive-read-evidence.v1` encoding.
Transactions replay after restart with their original identity and observations,
including after settlement. Legacy transactions without complete proof remain
blocked rather than acquiring evidence from later observations.

Advance Jazz node storage to epoch 2. Native and browser open paths validate exact
epoch-1 storage without mutation, then atomically publish the new manifest and
admission receipt. Older readers reject the new epoch. Auxiliary account and
catalogue storage keep their existing profiles.

Keep browser storage under one physical-root owner through admission and reset.
Await lock release before successful reset notifications permit immediate reopen.
These core capabilities do not by themselves provide automatic offline E2EE
initialization or establish accepted membership.
