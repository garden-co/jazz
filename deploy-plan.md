# Unified deployment

Make `deploy` the only public operation for publishing schemas, migrations, and permissions. Publish the complete local migration history, rather than only the path from the server's active schema to the current schema.

## Plan

1. **Define a graph read and a single deployment write.**

- `GET /apps/{appId}/admin/migrations/graph` returns a consistent snapshot of the authority's stored graph and active schema hash. Use existing admin authentication. Edge servers forward to the authority.
- Return every stored schema hash and every migration's source and target hashes. Include isolated schemas so incomplete legacy history remains visible. Include the active schema hash, or null before the first deployment. No revision or concurrency token is required.
- `POST /apps/{appId}/admin/deploy` accepts the target schema hash, explicit permissions, only missing schema/migration bodies. No expected revision, deployment ID, idempotency key, or retry receipt.
- The CLI compares the complete local history with this inventory. For now, assume at most one lens per directed `(fromHash, toHash)` pair and identify migrations by that pair. Inventory comparison does not detect changed local operations for an existing pair.
- Missing endpoint bodies may be resolved from stored artifacts; missing endpoints in both places are errors. Omitting an artifact never deletes server history. Request array order has no meaning. Validate the union of stored and submitted history.

Illustrative JSON contracts (reuse existing compiled schema, lens, and permission encodings):

```ts
interface MigrationGraphResponse {
  activeSchemaHash: string | null;
  schemas: string[]; // full schema hashes
  migrations: Array<{ fromHash: string; toHash: string }>;
}

interface DeployRequest {
  targetSchemaHash: string;
  schemas: Array<{ hash: string; schema: CompiledSchema }>;
  migrations: Array<{ fromHash: string; toHash: string; forward: TableLens[] }>;
  permissions: CompiledPermissions; // required; {} explicitly denies all access
}

interface DeployResponse {
  changed: boolean;
  published: {
    schemas: string[];
    migrations: Array<{ fromHash: string; toHash: string }>;
  };
}
```

- Return structured validation errors with stable codes and relevant hashes/paths. Use `422` for invalid graph/schema/lens/permissions and `400` for malformed requests. A successful `200` means the authority durably committed and activated the complete deployment, not that all replicas have caught up.
- The graph endpoint also powers `jazz-tools migrations graph <appId>`, displaying directed edges, abbreviated hashes, and the active schema. Combine the remote graph with local snapshots, schema.ts, and migrations; mark schemas and edges that exist only locally or only on the server. Do not publish or save snapshots.

2. **Validate the whole graph before publication.**

- Resolve submitted and stored artifacts, verify hashes and migration endpoints, and run Rust schema, permission, and lens validation. Require an acyclic graph in which every included schema reaches the current schema through forward migrations; the current schema must be the sole terminal. **If concurrent migrations leave multiple branch tips, reject deployment and require the user to add migrations connecting every tip to the current `schema.ts`**, directly or through intermediate schemas. Report the unresolved tips and the target schema hash; do not choose one branch or automatically discard another. Multiple equivalent paths are allowed, but all branches must converge on that single terminal schema. Include the server's existing deployment history when checking convergence so stale checkouts cannot abandon a deployed branch.
- Schema activation only moves forward to the terminal schema; **activating an earlier schema as a rollback is unsupported**. Bidirectional read projection does not authorize backward activation.
- Initial deployment permits one schema and no migrations.
- Permission-only deployment reuses the graph. Compatible changes need no migration file: deployment connects compatible definitions with ordinary empty lenses, preserving existing schema hashes and stored encodings. Convergence ignores compatible steps, including compatible revisions of historical snapshots; structural steps must still move forward and agree across parallel paths. Branch-key defaults remain immutable. The graph response derives an optional `automatic: true` marker for compatible empty lenses so the CLI does not suggest a missing local migration. This marker describes a connection that requires no file, not persisted authorship provenance.
- For any parallel forward paths with the same source and destination, compose their migrations and require equivalent structural changes, including table/column identity mappings; merely ending at the same schema shape is insufficient. Accept equivalent paths rather than rejecting multiple paths outright. Reject conflicting definitions or non-equivalent paths with actionable diagnostics identifying the conflicting migrations.

3. **Make branch convergence preserve values.**

- A merged schema needs valid incoming migrations from both branch tips. Resolve column identities and validate incoming mappings together before fixing the target's physical mapping; do not infer identity from matching names or silently remove the existing conflict check. Define how this works for an already-published target, or reject it explicitly. Multiple paths must not silently disagree about projected values. Keep bidirectional lenses for reads, but do not use backward reachability to satisfy deployment convergence. When projecting an ancestor schema into the active target, select a value-preserving forward path rather than a shorter backward detour that drops and reintroduces branch-added columns.

4. **Serialize deployment; defer cross-storage failure handling.**

- For the current implementation, assume storage writes succeed. Complete all schema, lens, permission, graph, physical-mapping, and encoding validation before the first persistent write. Use the existing catalogue records; do not add a deployment recovery record or replay protocol. Cross-storage failure atomicity is deferred; the atomicity and uncertain-outcome requirements below describe the eventual design.

- Serialize deployments per app. Under the same serialization boundary, validate the combined catalogue against current server state and commit; validating against an earlier graph read is insufficient. Accept concurrent changes when the resulting deployment remains valid. Permissions from the last successful deployment take effect, including for concurrent permission-only deployments.
- Persist artifacts, permissions, and active-schema selection as one recoverable deployment, exposing it only after successful validation and commit. A failure or restart must leave either the previous deployment or the complete new deployment available.
- If validation fails because the server graph changed, or the network outcome is uncertain, the CLI re-fetches the graph and recomputes missing artifacts before attempting a fresh deploy. Bound retries; unresolved graph or policy conflicts require user action. Do not blindly resend or infer that a lost response means failure. A request whose artifacts, target, and permissions are already installed is a no-op and does not advance the active version.
- Replication and fresh-edge bootstrap must preserve the same complete-deployment boundary.

5. **Update all callers.**

- Make CLI and programmatic `deploy` collect every local migration and snapshot, discover missing artifact bodies, and send one request.
- Implement graph validation in Rust and reuse it through bindings for local checks. Offline validation only covers local history; server validation always covers the combined history.
- Remove the separate schema, migration, and permission publishing HTTP endpoints and public callers; retain read endpoints and internal helpers as needed.
- Update dev watching, bindings, fixtures, examples, docs, and the changeset.

## Validation

Cover initial and permission-only deployment; missing artifacts; disconnected, cyclic, and multiple-terminal graphs; rejected backward activation; concurrent graph changes validated against current server state, concurrent permission-only deployments, lost responses followed by graph refresh, and unchanged redeploys; graph inventory completeness and CLI rendering; multi-step history publication; equivalent parallel paths and conflicting parallel paths (including identical final shapes with different column mappings); invalid policies/lenses; persistence failures and restart recovery; replication and fresh-edge bootstrap. Keep `merged_schema_via_sibling_path_defaults_reintroduced_column_without_erasing_source` as evidence of the existing sibling-path projection behavior. The new deployment flow must reject its incomplete graph (the left branch cannot reach the merged target through forward migrations); the completed diamond must preserve the authored value instead of substituting a default. Add the two-branch merge regression with non-default values and subsequent writes from clients on both schemas, proving that the merged view preserves both branches' values. Run affected Rust/server and CLI/dev suites, then the canonical integration gates before landing.
