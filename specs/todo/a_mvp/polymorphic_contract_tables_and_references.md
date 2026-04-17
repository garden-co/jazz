# Polymorphic Contract Tables and References — TODO (MVP)

This spec adds first-class, closed-world polymorphism to Jazz without storing
table identifiers on every reference edge.

It assumes the current `main` support for caller-supplied create/upsert ids has
already landed. That support removes the main write-path blocker for
shared-identity base/subtype rows.

The intended model is not PostgreSQL-style inherited tables and not Rails-style
`type + id` generic refs. Instead:

- one **contract table** holds the shared identity and shared columns
- one **variant table** per concrete subtype holds variant-specific columns
- refs still point to exactly one table: the contract table
- the contract row stores the discriminator once

This is very close to what an app could now build in userland. The delta is
making it a schema-level concept so Jazz can coordinate writes, plan joins, and
shape results for includes and relation traversal.

## Related

- [App Surface — Status Quo](../../status-quo/ts_client.md)
- [Query Manager — Status Quo](../../status-quo/query_manager.md)
- [Schema Manager — Status Quo](../../status-quo/schema_manager.md)
- [Schema Files — Status Quo](../../status-quo/schema_files.md)
- [Opt-In Transactions, Replayable Reconciliation, and Strict Visibility](./opt_in_transactions_replayable_reconciliation.md)

## Why this exists

Today an app can approximate polymorphism by hand:

1. define a base table such as `entities`
2. define subtype tables such as `posts` and `images`
3. point refs at `entities`
4. coordinate shared ids manually

That gets surprisingly far, especially now that create/upsert can accept an
external id. But four gaps remain:

1. **No schema-level nominal contract.** Tables only “match” by convention.
2. **No coordinated polymorphic writes.** `db.insert(app.posts, ...)` cannot fan
   out to contract + variant writes as one first-class operation.
3. **No contract-aware relation planning.** Includes and hops see only ordinary
   one-table refs, not “contract plus matching variant”.
4. **No typed result shaping.** Jazz cannot currently expose one logical
   relation and return either narrowed variant rows or a uniform
   `kind + variant` result shape.

## Goals

- Keep refs single-target and schema-driven.
- Make polymorphic refs migration-lens friendly.
- Support first-class contract/variant table declarations in the TypeScript
  schema DSL and runtime schema metadata.
- Support coordinated writes for variant handles using one shared id.
- Support querying a contract as:
  - contract-only rows
  - one narrowed variant
  - one uniform `kind + variant` shape
- Reuse the existing query machinery as much as possible.

## Non-goals

- No open-world generic refs in MVP.
- No row-level `table_id + object_id` ref format in MVP.
- No structural “any table with these columns satisfies the contract” behavior.
- No nested contract hierarchies in MVP.
- No requirement to add a brand-new relation IR node if existing joins/includes
  are sufficient.

## Core model

### 1. Contracts are nominal and abstract

A contract table is an explicitly declared supertype. It is not inferred from a
matching column set.

Contracts are abstract in the sense that application code cannot insert a
contract row without also choosing a variant.

Reads and refs may target the contract. Inserts create concrete variants.

### 2. Variant rows share the contract row id

Each variant row has the same `id` as its contract row. Variant identity is
therefore:

- stable for refs
- stable for schema lenses
- independent from the concrete variant table name

### 3. The discriminator lives on the contract row

The contract row stores the variant tag once.

That tag is schema metadata plus row content on the target side, not ref-side
routing data on every referencing row.

This is what keeps contract refs stable across:

- variant table renames
- variant table splits/merges
- future storage/catalogue ids for tables

### 4. Contract refs remain ordinary refs

A polymorphic ref is still just:

```ts
targetId: s.ref("entities");
```

Jazz resolves the concrete variant by following that ref to the contract row,
reading the discriminator, and then joining or including the matching variant
table.

### 5. Contract and variant rows are jointly valid

A contract row is only considered a valid app-visible row if the matching
variant row exists for its discriminator.

A variant row is only considered a valid app-visible row if:

- the contract row exists
- the contract discriminator matches that variant tag

This invariant lets the runtime tolerate partial failure during coordinated
writes without exposing half-written polymorphic rows as valid query results.

## High-level API

### Schema declaration

```ts
import { schema as s } from "jazz-tools";

export const app = s.defineApp({
  entities: s.contract({
    ownerId: s.ref("users"),
    createdAt: s.timestamp(),
  }),

  posts: s.variant("entities", {
    tag: "post",
    columns: {
      title: s.string(),
      body: s.string(),
    },
  }),

  images: s.variant("entities", {
    tag: "image",
    columns: {
      url: s.string(),
      alt: s.string().optional(),
    },
  }),

  comments: s.table({
    targetId: s.ref("entities"),
    body: s.string(),
  }),
});
```

Rules:

- `s.contract(...)` declares the shared columns.
- `s.variant(contractTable, { tag, columns })` declares one concrete subtype.
- `tag` is required in MVP so table renames do not force discriminator rewrites.
- `s.ref("entities")` is the polymorphic ref. `s.ref("posts")` remains an
  ordinary concrete ref.

### Read API

#### Contract handle

`app.entities` is a real query handle.

Its default row shape is:

```ts
type Entity = {
  id: string;
  kind: "post" | "image";
  ownerId: string;
  createdAt: Date;
};
```

Default contract queries return only contract columns plus `kind`. They do not
eagerly materialize variant payload.

#### Variant handle

`app.posts` and `app.images` are also real query handles.

Their row shapes merge contract + variant columns:

```ts
type Post = {
  id: string;
  kind: "post";
  ownerId: string;
  createdAt: Date;
  title: string;
  body: string;
};
```

#### Narrowing a contract query

Contract handles gain:

```ts
app.entities.asVariant("post");
```

This returns a narrowed query handle equivalent to querying `app.posts`.

Examples:

```ts
await db.all(app.entities.asVariant("post").where({ title: { contains: "Jazz" } }));
await db.all(app.comments.hopTo("target").asVariant("image"));
```

#### Full discriminated-union shape

Contract handles also gain:

```ts
app.entities.includeVariant();
```

This widens the contract query from “contract columns only” to the full
contract-plus-variant row shape, flattened at the top level and typed as a
discriminated union:

```ts
type EntityWithVariant =
  | {
      id: string;
      kind: "post";
      ownerId: string;
      createdAt: Date;
      title: string;
      body: string;
    }
  | {
      id: string;
      kind: "image";
      ownerId: string;
      createdAt: Date;
      url: string;
      alt?: string | null;
    };
```

So TypeScript narrowing works directly on the returned row:

```ts
const entity = await db.one(app.entities.includeVariant().where({ id }));

if (entity?.kind === "post") {
  entity.title;
}
```

This is the MVP answer to “load the full concrete row while preserving one
logical contract handle”.

#### Includes and reverse relations

Refs to a contract should work in includes without introducing a new ref type:

```ts
await db.all(
  app.comments.include({
    target: (target) => target.includeVariant(),
  }),
);
```

Variant handles also inherit relations that target the contract. For example, if
`comments.targetId` references `entities`, then `app.posts` should still expose
the reverse relation from the contract side:

```ts
await db.all(app.posts.include({ commentsViaTarget: true }));
```

### Write API

#### Insert

Inserts happen through variant handles:

```ts
const post = db.insert(app.posts, {
  ownerId: alice.id,
  createdAt: new Date(),
  title: "Hello",
  body: "World",
});

const image = db.insert(
  app.images,
  {
    ownerId: alice.id,
    createdAt: new Date(),
    url: "https://...",
  },
  { id: externalUuidV7 },
);
```

`db.insert(app.entities, ...)` is invalid in MVP because contracts are abstract.

#### Update

Updates may target either:

- a variant handle, for common + variant columns
- a contract handle, for common columns only

Examples:

```ts
db.update(app.posts, post.id, { title: "Hello again" });
db.update(app.entities, post.id, { ownerId: bob.id });
```

Changing `kind` through update is forbidden in MVP.

#### Delete

Deleting either the contract or the variant handle deletes the logical entity:

```ts
db.delete(app.entities, post.id);
db.delete(app.posts, post.id);
```

The runtime removes both the contract row and the matching variant row.

### Policy surface

Ordinary table policies still apply.

MVP rule:

- querying a narrowed variant must satisfy both the contract policy and the
  variant table policy
- querying the plain contract must satisfy the contract policy
- materializing full variant columns via `includeVariant()` or a variant include
  must additionally satisfy the concrete variant table policy for the chosen row

This keeps authorization table-first while preserving the idea that a contract
row alone is not the full object.

## Schema and runtime representation

### Schema AST / wire shape

`Column.references` remains unchanged:

- refs still point to one table name

Polymorphism is table-level metadata, so the schema needs new table metadata on
both the TypeScript and runtime sides.

Illustrative wire shape:

```ts
interface TableSchema {
  columns: ColumnDescriptor[];
  policies?: TablePolicies;
  contract?: {
    variant_tags: string[];
  };
  variant_of?: {
    contract_table: string;
    tag: string;
  };
}
```

Equivalent Rust metadata must be added to `TableSchema`.

Validation rules:

- a table cannot be both `contract` and `variant_of`
- variant tags are unique per contract
- a contract may only be referenced by declared variants
- nested variants are disallowed in MVP

## Necessary low-level implementation changes

### 1. Schema DSL, AST, and encoding

Files touched:

- `packages/jazz-tools/src/typed-app.ts`
- `packages/jazz-tools/src/schema.ts`
- `packages/jazz-tools/src/codegen/schema-reader.ts`
- `packages/jazz-tools/src/drivers/types.ts`
- `crates/jazz-tools/src/query_manager/types/schema.rs`
- `crates/jazz-tools/src/schema_manager/encoding.rs`

Changes:

- add `s.contract(...)` and `s.variant(...)`
- carry contract/variant metadata through schema compilation and runtime schema
  encoding
- include this metadata in schema hashes and schema diffing

### 2. Type generation and query-builder surface

Files touched:

- `packages/jazz-tools/src/typed-app.ts`
- `packages/jazz-tools/src/index.ts`

Changes:

- contract handles expose common columns + union `kind`
- variant handles expose merged contract + variant rows
- `includeVariant()` returns a top-level discriminated union of merged
  contract-plus-variant rows rather than nesting variant fields under one
  property
- add `asVariant(tag)` and `includeVariant()`
- synthesize inherited contract relations onto variant handles

### 3. Coordinated variant writes

Files touched:

- `packages/jazz-tools/src/runtime/db.ts`
- `packages/jazz-tools/src/runtime/client.ts`
- `crates/jazz-tools/src/runtime_core/writes.rs`
- `crates/jazz-tools/src/runtime_tokio.rs`
- `crates/jazz-tools/src/schema_manager/manager.rs`
- `crates/jazz-tools/src/query_manager/writes.rs`

Changes:

- `db.insert(app.posts, ...)` becomes a coordinated logical write:
  1. choose id, using caller-supplied UUIDv7 when present
  2. write contract row with discriminator
  3. write variant row with same id
- `db.update(app.posts, ...)` splits fields into contract-table and variant-table
  updates
- `db.delete(app.entities, id)` and `db.delete(app.posts, id)` remove both halves
- subscriber delivery must not expose intermediate half-written states for one
  logical polymorphic write

This does not require the full public transaction API, but it does require one
internal multi-row write helper rather than leaving coordination to app code.

### 4. Relation analysis

Files touched:

- `packages/jazz-tools/src/codegen/relation-analyzer.ts`

Changes:

- keep existing FK-derived relations from `references`
- synthesize singular contract-to-variant and variant-to-contract relations
- synthesize inherited reverse relations from contract refs onto variant handles

Example:

- `comments.targetId -> entities`
- contract `entities` gets `commentsViaTarget`
- `posts` should also expose `commentsViaTarget`

### 5. Query lowering for narrowed variants

Files touched:

- `packages/jazz-tools/src/runtime/query-adapter.ts`
- `packages/jazz-tools/src/ir.ts`
- `crates/jazz-tools/src/query_manager/relation_ir.rs`

Changes:

- `asVariant("post")` lowers to ordinary contract-to-variant joins plus a
  discriminator check
- plain contract queries filter out invalid contract rows that lack the matching
  variant row
- MVP should prefer lowering to existing `Join`, `Filter`, and `Project` nodes
  rather than introducing a dedicated polymorphic IR node

### 6. Includes and result shaping

Files touched:

- `packages/jazz-tools/src/runtime/query-adapter.ts`
- `packages/jazz-tools/src/runtime/row-transformer.ts`

Changes:

- `include({ target: true })` against a contract ref returns contract columns
  only
- `includeVariant()` expands to hidden singular includes for each declared
  variant and then flattens the matching variant columns onto the top-level row
  in the row transformer
- the runtime output type for `includeVariant()` is therefore a discriminated
  union keyed by `kind`, not `{ kind, variant: ... }`
- because current relation IR does not have a `Union` node, MVP should compile
  polymorphic payload hydration through existing include/subquery machinery

This keeps the physical plan simple at the cost of one hidden include per
variant, which is acceptable for the expected small closed-world variant sets.

### 7. Policy evaluation

Files touched:

- `packages/jazz-tools/src/permissions/index.ts`
- `packages/jazz-tools/src/schema-permissions.ts`
- `crates/jazz-tools/src/query_manager/policy.rs`
- `crates/jazz-tools/src/query_manager/graph_nodes/policy_eval.rs`

Changes:

- contract relations remain ordinary relation inputs for policy DSL
- narrowed variant reads must compose contract + variant policy checks
- contract queries that materialize a variant payload must enforce the chosen
  variant policy before exposing variant data

### 8. Migrations and lenses

Files touched:

- `packages/jazz-tools/src/migrations.ts`
- `crates/jazz-tools/src/schema_manager/diff.rs`
- `crates/jazz-tools/src/schema_manager/lens.rs`
- `crates/jazz-tools/src/schema_manager/manager.rs`

Changes:

- schema diffs must compare contract/variant metadata
- table rename of a variant table must not force inbound ref rewrites because
  refs point to the contract
- renaming a variant tag requires a lens over the contract discriminator values
- moving a column between contract and variant tables is a normal lens
  transformation, but only on the target side

This is the main migration advantage of the contract model over ref-side
`table_id + object_id`.

## Before / After

### Before

- apps can hand-roll `entities + posts + images`
- refs can point to `entities`
- callers can now coordinate ids manually
- Jazz does not understand that shape as one logical polymorphic relation

### After

- schema declares contracts and variants explicitly
- refs remain ordinary single-target refs to contracts
- variant inserts/updates/deletes are coordinated by the runtime
- contract queries can stay cheap, narrow to one variant, or materialize a full
  top-level discriminated union row
- migration lenses only need to reason about target-side discriminator and
  variant layout, not per-ref table identifiers

## Open questions

- Should contract queries expose the discriminator as `kind`, `type`, or a
  generated property backed by a reserved `_jazz_*` column?
- Should direct refs to variant tables remain allowed in MVP, or should Jazz
  steer all polymorphic models through contract refs only?
- Do we want contract-level `includeVariant()` only, or also variant-specific
  sugar such as `includeAs("post")` on contract refs?
- Is the internal coordinated multi-row write helper sufficient for MVP, or do
  we want to make this feature wait for the broader transaction/batch API?
