# Plan: existential authorization and complete prepared routing

Status: two minimized public-API regressions and disposable corrections were
validated in an isolated clone. No production worktree change.

## A. Authorization must be existential before `ORDER BY / LIMIT`

### Invariant

A protected row is authorized when at least one grant path succeeds. Extra
matching grants must not duplicate the protected row or consume page slots.
`INV-RLS-9` says authorization requires “at least one” matching row; the
authorization boundary is a semijoin, not a bag-producing application join.

### Minimal evidence

Two documents ordered descending with `LIMIT 2`:

1. One membership; the newest document is also public/published.
2. Two active membership rows for the same user/team; both documents private.

Both one-shot and subscription reset return only the newest unique document,
not two. The duplicate authorization derivation occupies the second weighted
ordinal before the page is materialized.

This generalizes the realistic policy benchmark's public tier returning
`99 / 100`. It is not fixture-specific. Overlapping RBAC, ACL, public, admin,
or duplicate local-first membership rows can all produce the same result.

### Source

- `crates/jazz/src/node/query_eval.rs:3677-3682` lowers joins inside policy atom
  chains as `Inner`.
- `query_eval.rs:3949-3959` and `5520-5530` join policy alternatives back to
  protected rows as `Inner`.
- Positive grant weights therefore reach `TopBy`, which ranks weighted
  derivations rather than unique protected-row identities.

### Validated direction

In the isolated clone only:

- policy atom joins became route-aware `Semi`;
- both policy-union authorization joins became route-aware `Semi`;
- ordinary application joins remained `Inner`.

The two minimal repros then passed. Partial revoke stayed quiet while one grant
remained and removed the rows only after the last grant disappeared. Fresh
1,200-document policy tiers 4 and 5 both returned exact `100 / 100`, including
public/admin overlap.

Groove's existing semijoin groups the authorization side by join key plus route
fields and maintains one existential winner. The implementation should use
that semantic operator consistently rather than add a late `distinct` after
the page.

### Implementation

1. Separate policy-chain normalization from ordinary query-join normalization
   so policy joins explicitly request `NormalizedJoinMode::Semi`.
2. Change root and inherited policy-alternative authorization joins to `Semi`.
3. Audit direct, inherited, reachable, and branch-union authorization
   producers for existential semantics.
4. Preserve the complete claim/user-param route tuple in every semijoin group.
5. Document that bag multiplicity is valid inside ordinary query relations but
   must be thresholded at the authorization boundary.
6. Add authorization multiplicity counters so a protected-row weight greater
   than one cannot silently reach a finite window.

### Gates

- same-branch duplicate membership grants;
- membership + public, membership + ACL, ACL + admin overlaps;
- N -> N-1 grants emits no membership event; last revoke removes once;
- restore after last revoke adds once;
- overlap exactly at the Kth boundary;
- two identities and two bindings;
- randomized duplicate-grant/branch overlap differential oracle for one-shot
  and maintained results.

## B. A prepared graph embeds identity under a shared shape name

### Invariant

A prepared graph shared across identities must parameterize every claim value
that can affect membership. Per `crates/jazz/SPEC/14_lowering_to_groove.md`
§14.4, prepared identity includes the claim-path/binding-column signature, not
claim values. Values such as built-in `sub` belong in runtime binding rows and
route fields.

### Minimal evidence

Two teams and two users, with one membership/document per pair, reuse one shape:

```text
documents
  WHERE team = $team
  ORDER BY updated_at DESC, id DESC
  LIMIT 2
```

Sequential one-shots produce:

```text
(team A, user A) -> 1
(team B, user B) -> 0, expected 1
```

Reversing call order reverses the winner. No active subscription is required.
Claim-only multi-identity and same-identity/multi-team controls pass; the bug
requires both a changed claim context and an application parameter. Explicitly
unsubscribing the first one-shot Groove output after `recv()` does not repair
the second call, so this is independent of the lifecycle leak.

### Source

- `crates/jazz/src/node/query_eval.rs:7889-7945` prepares the outer graph under
  a stable binding-source shape derived from its parameter descriptor.
- The failing route trace shows that descriptor contains only user parameter
  `team`; `program.lowered.parameters.claim_params` is empty.
- The compiled authorization graph nevertheless contains the first identity's
  built-in `sub` UUID as a literal. Claim lowering obtains it from
  `PolicyContext::permission_subject`
  (`crates/jazz/src/node/query_engine/lowering.rs:4559-4587`).
- Missing from the descriptor and hidden routes is the generated
  `__jazz_claim_<sub path>` binding column.
- `normalize_operand_with_target_type` keeps the policy operand as a normalized
  claim (`query_eval.rs:2421-2447`), but
  `binding_claim_params_for_shape` (`query_eval.rs:9114-9141`) does not discover
  that form in the composed ordinary policy predicate before the binding
  descriptor is finalized.
- Both identities then receive the same Groove `PreparedShapeId`, but the
  shared shape's graph was compiled with the first subject literal.
- `query_binding_value_signature` at `query_eval.rs:9522-9528` correctly
  describes parameter names rather than values; the defect is that the
  authorization claim path/value never reached that prepared parameter domain.

### Diagnostic workaround

Forcing separate prepared identities made the repro pass in both orders and
made live-subscription-plus-one-shot checks pass. The existing
`direct_multi_identity_subscribe_reuses_shared_seeded_fragments_without_leaking`
test remained green in that probe.

That only proves shared prepared identity is the collision boundary. Keying
plans permanently by identity/claim value would conflict with the spec, grow
plans per user, and hide the missing parameterization. It is a fail-safe
fallback, not the target design.

### Implementation

1. Discover every composed policy claim path and type—including built-in
   `sub`—before finalizing the outer program's `ParameterDomain`.
2. Retarget the policy value source to the outer binding source rather than
   lowering a runtime claim as a graph literal.
3. Include both caller params and claim params in the binding descriptor;
   `binding_values_for_plan` already supplies claim values from the policy
   context.
4. Preserve corresponding claim route fields through authorization semijoins,
   `TopBy`, and the terminal's routed multisink filter.
5. Keep the prepared-shape identity based on shape, tier, policy identity, and
   one canonical user-name/type plus claim-path/type signature. Different
   values must bind the same graph.
6. Add a compiler assertion/explain field: a graph eligible for shared prepare
   contains no fixed identity or trusted-claim value affecting membership.
7. Until that assertion holds, fail closed by avoiding shared preparation for
   that graph. If an identity-segregated fallback is temporarily required,
   count it loudly and do not present it as the final sharing model.

### Gates

- two identities x two app bindings in all call orders;
- one identity x two bindings and two identities x one binding;
- equal and different custom claim values;
- direct, inherited, reachable, branch, and join policies;
- one-shot before/after subscriptions and mutations;
- cache-order permutation property: results never depend on first caller;
- compiled plan inspection: built-in `sub` and custom claims appear as binding
  params/routes, never membership-affecting literals;
- one prepared shape serves many identity values without graph growth;
- retain existing shared-fragment storage-read gates.

Audit any white-box test that requires different compiled graphs for different
identity values. If it conflicts with §14.4, surface that behavior decision
instead of silently rewriting the test; the intended replacement is shared
plan plus runtime value-isolation coverage.

## Positive controls

On the baseline, membership revoke/restore, Top-K rank changes, deleting a
winner, and moving a document between teams all produced exact maintained
deltas. Those controls bound these failures to authorization multiplicity and
mixed claim/parameter plan reuse.

## Landing order

Land existential policy semijoins first, then complete claim parameterization
and routing. Both are correctness prerequisites for selective policy access
paths and route-after-window optimization.

Tooling friction: an explain view should show authorization join mode and every
claim path, binding column, route field, and fixed literal in a prepared graph.
