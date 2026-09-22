# Unified deployment

Make `deploy` the only public operation for publishing schemas, migrations, and permissions. Publish the complete local migration history, rather than only the path from the server's active schema to the current schema.

## Plan

1. **Define one deployment request.**:

- Add an authenticated, app-scoped `POST /admin/deploy` containing the target schema hash, explicit permissions, schema snapshots, migrations, and the expected active deployment revision.
- Include the complete local graph manifest; omit artifact bodies already stored on the server. Return the activated revision and publication results.
- Missing referenced artifacts are errors, not silently skipped dependencies.

2. **Validate the whole graph before publication.**

- Resolve submitted and stored artifacts, verify hashes and migration endpoints, and run Rust schema, permission, and lens validation. Require an acyclic graph in which every included schema reaches the current schema through forward migrations; the current schema must be the sole terminal. **If concurrent migrations leave multiple branch tips, reject deployment and require the user to add migrations connecting every tip to the current `schema.ts`**, directly or through intermediate schemas. Report the unresolved tips and the target schema hash; do not choose one branch or automatically discard another. Multiple equivalent paths are allowed, but all branches must converge on that single terminal schema. Include the server's existing deployment history when checking convergence so stale checkouts cannot abandon a deployed branch.
- Schema activation only moves forward to the terminal schema; \***\*activating an earlier schema as a rollback is unsupported**. Bidirectional read projection does not authorize backward activation.
- Initial deployment permits one schema and no migrations.
- Permission-only deployment reuses the graph; compatible transitions need an explicit or automatically generated identity connection.
- For any parallel forward paths with the same source and destination, compose their migrations and require equivalent structural changes, including table/column identity mappings; merely ending at the same schema shape is insufficient. Accept equivalent paths rather than rejecting multiple paths outright. Reject conflicting definitions or non-equivalent paths with actionable diagnostics identifying the conflicting migrations.

3. **Make branch convergence preserve values.**

- A merged schema needs valid incoming migrations from both branch tips. Resolve column identities and validate incoming mappings together before fixing the target's physical mapping; do not infer identity from matching names or silently remove the existing conflict check. Define how this works for an already-published target, or reject it explicitly. Multiple paths must not silently disagree about projected values. Keep bidirectional lenses for reads, but do not use backward reachability to satisfy deployment convergence.

4. **Commit atomically and support retries.**

- Serialize deployments per app and compare the expected revision before mutation.
- Persist artifacts, permissions, and active-schema selection as one recoverable deployment, exposing it only after successful validation and commit. A failure or restart must leave either the previous deployment or the complete new deployment available.
- Identical retries succeed without creating another revision; stale conflicting requests fail.
- Replication and fresh-edge bootstrap must preserve the same complete-deployment boundary.

5. **Update all callers.**

- Make CLI and programmatic `deploy` collect every local migration and snapshot, discover missing artifact bodies, and send one request.
- Share graph checks with `validate`, while keeping server validation authoritative.
- Remove the separate schema, migration, and permission publishing HTTP endpoints and public callers; retain read endpoints and internal helpers as needed.
- Update dev watching, bindings, fixtures, examples, docs, and the changeset.

## Validation

Cover initial and permission-only deployment; missing artifacts; disconnected, cyclic, and multiple-terminal graphs; rejected backward activation; stale revisions and identical retries; multi-step history publication; equivalent parallel paths and conflicting parallel paths (including identical final shapes with different column mappings); invalid policies/lenses; persistence failures and restart recovery; replication and fresh-edge bootstrap. Add the two-branch merge regression with non-default values and subsequent writes from clients on both schemas, proving that the merged view preserves both branches' values. Run affected Rust/server and CLI/dev suites, then the canonical integration gates before landing.
