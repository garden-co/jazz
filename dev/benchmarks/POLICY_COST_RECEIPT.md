# Current-core policy cost receipt

`crates/jazz/benches/policy_cost_receipt.rs` extracts the stable policy lane
from draft #1170 without retaining its row-scale, route-fan-out, hydration, and
operator experiments.

## Question

What does adding common authorization branches cost when the dataset,
identities, query, page size, and result membership are held fixed? Do writes
that change authorization continue to produce the exact maintained page?

## Method

Each of five fresh in-memory databases contains the same deterministic 1,200
documents, two organizations, two teams, membership rows, organization-admin
rows, and 100 direct ACL rows. The query orders all authorized documents by
`updated_at DESC` and limits the result to 100.

The cumulative policy tiers are:

1. team membership;
2. organization admin;
3. direct document ACL;
4. public and published;
5. trusted admin claim.

Five identities exercise those paths in a fixed order. Public rows are a
subset of the member and organization-admin scope, so adding the public branch
does not change those principals' result pages. Five of the ACL reader's
highest-ranked rows deliberately overlap the public branch, enforcing
existential authorization before the finite window. The trusted administrator
overlaps every other branch.

An independent fixture oracle checks every ordered one-shot result and digest.
A retained subscription then reconstructs and checks the exact ordered page
after membership grant, revoke, restore, moving a winning document out of
scope, and moving it back. The benchmark exits unsuccessfully on any mismatch.

Run it with:

```sh
cargo bench --profile perf -p jazz --bench policy_cost_receipt --quiet
```

## Interpretation

Timings are directional single-run developer-machine measurements, not a
latency gate. The durable value is the fixed fixture, exact result digests, and
authorization lifecycle gate. Compare an access path only between tiers where
its expected result is identical; do not sum identity timings into a product
latency claim.

The initial extraction exposed two correctness prerequisites. #1238 makes
authorization proofs existential before finite windows, and #1239 preserves
omitted cells while composing a partial update for a writer who cannot read the
row. Both fixes are carried on this branch until they land independently.

## Initial result

One optimized run on the local development box produced the following
same-result comparisons:

| Access path        | First available tier | Five-branch tier | Added cost |
| ------------------ | -------------------: | ---------------: | ---------: |
| Team membership    |            16.816 ms |        18.217 ms |       8.3% |
| Organization admin |            15.999 ms |        17.726 ms |      10.8% |
| Direct ACL         |            13.689 ms |        14.605 ms |       6.7% |
| Public/published   |            13.639 ms |        13.934 ms |       2.2% |

All compared pages retained the same exact digest. The trusted-admin path took
20.310 ms for the same 100-row page returned by the ACL path; it is reported as
its own access path rather than compared with a tier where that claim was not
enabled.

The maintained lifecycle measured grant/revoke/restore at
6.635/6.208/5.699 ms and scope exit/re-entry at 1.356/4.022 ms. Every phase
matched the independent page oracle. These timings are illustrative; the
correctness booleans and digests are the retained contract.

## Acceptance

- every identity returns the oracle's exact ordered IDs at every tier;
- overlapping grants never consume `TopBy` slots;
- grant, revoke, and restore produce the exact maintained page;
- scope exit removes and refills the page exactly;
- scope re-entry restores the winner and evicts the refill exactly; and
- ordinary application joins retain bag semantics.
