# TypeScript Nested Relation Mutations — TODO (MVP)

Let high-level TypeScript mutations use the same relation names and nesting shape as `include(...)`, so app code can create and update related rows without dropping down to raw foreign-key columns for every step.

Today the read side already has a clean relation tree:

- relation names come from the same metadata that powers `include(...)`
- nested includes already work for forward scalar, forward array, and reverse array relations
- the high-level write side still only accepts flat column objects

That mismatch means the API teaches two different mental models:

- reads talk in relation names like `project`, `assignees`, `todosViaProject`
- writes talk in storage columns like `projectId` and `assigneesIds`

This MVP makes relation names first-class on writes too.

## Goals

- Use the same relation names as `include(...)` for nested writes.
- Support nested `create` and `update` in `db.insert(...)` and `db.update(...)`.
- Keep flat column writes working.
- Preserve the current local-first synchronous mutation model.
- Lower nested writes to ordinary row inserts/updates so the runtime does not need a new graph-write protocol yet.
- Make later transaction support able to reuse the same planned mutation tree.

## Non-goals (MVP)

- No delete / disconnect / upsert operators yet.
- No all-or-nothing multi-row rollback.
- No new runtime transaction primitive.
- No attempt to make reverse-array writes replace the full child set.
- No query-builder-shaped write payloads. This is relation-shaped data, not query JSON.

## Design Summary

High-level write inputs gain optional relation keys.

- Base columns still use the current flat row shape.
- Relation keys use the same names as `include(...)`.
- Relation payloads only accept `create` and `update`.

Example:

```ts
const project = db.insert(app.projects, {
  name: "Launch",
  todosViaProject: {
    create: [
      {
        title: "Write docs",
        done: false,
        tags: ["docs"],
        owner: {
          create: {
            name: "alice",
            friendsIds: [],
          },
        },
      },
    ],
  },
});

db.update(app.todos, todo.id, {
  project: {
    update: {
      id: project.id,
      data: {
        name: "Launch v2",
      },
    },
  },
  assignees: {
    update: [{ id: alice.id, data: { name: "Alice A." } }, { id: bob.id }],
    create: [{ name: "charlie", friendsIds: [] }],
  },
});
```

The important rule is that relation semantics follow where the relation is physically stored.

## Relation Kinds

### 1. Forward scalar relation

Example: `todos.projectId -> projects`, relation name `project`

Payload:

```ts
project?: {
  create?: CreateOf<typeof app.projects>;
  update?: {
    id: string;
    data?: UpdateOf<typeof app.projects>;
  };
}
```

Semantics:

- `create` inserts a new related row, then sets the parent FK column to the new id.
- `update` updates the provided row id and also sets the parent FK column to that id.
- `update: { id }` is valid and acts like "reattach/connect this existing row".
- Omitting the relation key leaves the FK untouched.
- Clearing the relation still uses the raw FK column (`projectId: null`) in MVP.

### 2. Forward array relation

Example: `todos.assigneesIds -> users[]`, relation name `assignees`

Payload:

```ts
assignees?: {
  update?: Array<{
    id: string;
    data?: UpdateOf<typeof app.users>;
  }>;
  create?: Array<CreateOf<typeof app.users>>;
}
```

Semantics:

- The parent owns the full relation through its UUID array column.
- In both `insert(...)` and `update(...)`, this payload describes the full next array value.
- `update` items contribute their `id`s in the listed order.
- `create` items contribute newly created ids after the `update` ids, in the listed order.
- After child writes finish, the parent array FK column is set to:
  - `[...updateIds, ...createdIds]`
- Any previously related ids not mentioned are detached.
- `update: [{ id }]` is valid and means "keep/attach this existing row in the array".

This replacement behavior is intentional. We can do it synchronously because the parent stores the whole set.

### 3. Reverse array relation

Example: `projects <- todos.projectId`, relation name `todosViaProject`

Payload:

```ts
todosViaProject?: {
  update?: Array<{
    id: string;
    data?: UpdateOf<typeof app.todos>;
  }>;
  create?: Array<CreateOf<typeof app.todos>>;
}
```

Semantics:

- The parent does not own a full child-id set.
- `create` inserts new child rows with the reverse FK bound to the parent id.
- `update` updates the provided child row ids and also writes the reverse FK column to the parent id.
- `update: [{ id }]` is valid and means "attach/reparent this existing row under this parent".
- Existing related rows not mentioned remain untouched.

This is additive/targeted, not replacement. Without a read or transaction layer, replacing the full reverse child set would be misleading.

## Write Types

The current exported `InsertOf<TTable>` should stay as the flat physical-row shape.

Add two new high-level helper types:

```ts
type CreateOf<TTable>;
type UpdateOf<TTable>;
```

Meaning:

- `InsertOf<TTable>`: raw column init shape
- `CreateOf<TTable>`: raw columns plus nested relation `create` / `update` payloads
- `UpdateOf<TTable>`: partial raw columns plus nested relation `create` / `update` payloads

`CreateOf<TTable>` must treat forward relation payloads as alternate ways to satisfy the
underlying FK column requirements.

Example:

```ts
// `projectId` is required in the physical row shape
type TodoInsert = InsertOf<typeof app.todos>;

// But `CreateOf<typeof app.todos>` should allow either:
db.insert(app.todos, {
  title: "Raw FK",
  done: false,
  tags: [],
  projectId: someProjectId,
});

db.insert(app.todos, {
  title: "Nested FK",
  done: false,
  tags: [],
  project: {
    create: { name: "Project from nested write" },
  },
});
```

In other words:

- required forward FK columns remain required unless their relation payload is present
- nested relation payloads are not just additive fields; they are alternative sources for FK values

`db.insert(...)` should accept `CreateOf<TTable>`.

`db.update(...)` should accept `UpdateOf<TTable>`.

This keeps the physical row type available for low-level needs while making the high-level API explicit about graph-shaped inputs.

## Conflicts and Validation

The planner should reject ambiguous payloads before performing any write.

### Same relation expressed twice

Reject if a mutation mixes a nested relation payload with the raw FK column(s) that drive that same relation.

Examples:

```ts
db.insert(app.todos, {
  projectId: existingProject.id,
  project: { create: { name: "Conflicting" } },
});

db.update(app.todos, todo.id, {
  assigneesIds: [alice.id],
  assignees: { update: [{ id: bob.id }] },
});
```

### Nested relation/operator conflicts

Reject if a single relation payload tries to use incompatible forms at once.

Examples:

- both `create` and a raw nested FK field that targets the same relation
- malformed scalar-vs-array payload shape
- unknown relation name

### Mutation-path errors

Errors should include the nested path, for example:

- `project.create.owner.create`
- `todosViaProject.update[1].owner.update`

That matters because multi-row write plans can fail after validation but during execution.

## Lowering Model

Nested relation writes do not require a new runtime mutation primitive in MVP.

Instead, `db.insert(...)` / `db.update(...)` first compile the input into an ordered list of ordinary row writes.

Conceptually:

```ts
type PlannedWrite =
  | { kind: "insert"; table: string; data: Record<string, unknown> }
  | { kind: "update"; table: string; id: string; data: Record<string, unknown> };
```

The planner uses the same relation metadata that powers `include(...)` today.

## Insert Lowering

Given `db.insert(app.todos, data)`:

1. Split raw columns from relation payloads.
2. For forward scalar relations:
   - lower nested child writes first
   - capture the resulting child id
   - write the parent's FK column from that id
3. For forward array relations:
   - lower listed child writes first
   - collect `[...updateIds, ...createdIds]`
   - write the parent UUID array column from that list
4. Insert the parent row.
5. For reverse array relations:
   - lower child writes after the parent exists
   - inject the reverse FK column with the parent id

This means the insert planner is naturally recursive.

## Update Lowering

Given `db.update(app.todos, todoId, data)`:

1. Split raw columns from relation payloads.
2. Lower forward scalar relation payloads:
   - `create` child first, capture id, then include FK patch in the root update
   - `update` child first, then include FK patch pointing at the provided id
3. Lower forward array relation payloads:
   - apply listed child updates
   - create listed new children
   - include the derived full UUID array in the root update
4. Apply the root update once with:
   - raw root column patches
   - any FK patches derived from forward relations
5. Lower reverse array relation payloads:
   - child `create`: inject the reverse FK column with the parent id
   - child `update`: update child row and force the reverse FK column to the parent id

This ordering keeps parent-owned relation columns derived in one place and gives reverse child writes the final parent identity.

## Sync Semantics and Partial Failure

This project's high-level mutations are synchronous and local-first today. Nested writes should preserve that.

That means:

- the full plan is validated up front as much as possible
- individual low-level writes execute synchronously in order
- if write `N` fails, writes `1..N-1` are not rolled back in MVP

This is acceptable for MVP because it is honest about the current execution model. The API must not imply transactions we do not yet have.

The user-facing contract should say:

- a nested mutation is an ordered local write batch
- each underlying row write is still individually atomic
- the overall nested mutation is not atomic yet

## Why This Is Still Worth Doing Before Transactions

Even without rollback, this API still buys us a better model:

- app code speaks in relations on both reads and writes
- nested write intent is centralized in one payload instead of spread across manual imperative steps
- the lowering step becomes a reusable intermediate representation for later transaction support

When transaction support exists, the same planned writes can run inside one transaction without changing the public TypeScript shape.

## Returning Values

MVP should keep the existing return contract:

- `db.insert(...)` returns the inserted root row
- `db.update(...)` returns `void`

Nested writes should not implicitly query and hydrate included graphs.

If we want "write and return included graph", that should be a separate follow-up with an explicit `include`/`returning` option. It is orthogonal to relation-shaped input.

## Example End-to-End Shapes

### Insert through a reverse relation

```ts
db.insert(app.projects, {
  name: "Website",
  todosViaProject: {
    create: [
      {
        title: "Homepage",
        done: false,
        tags: ["web"],
      },
      {
        title: "Pricing",
        done: false,
        tags: ["web"],
      },
    ],
  },
});
```

Lowers to:

1. insert `projects`
2. insert `todos` with `projectId = <new project id>`
3. insert `todos` with `projectId = <new project id>`

### Update a forward scalar relation

```ts
db.update(app.todos, todo.id, {
  project: {
    update: {
      id: existingProject.id,
      data: { name: "Renamed Project" },
    },
  },
});
```

Lowers to:

1. update `projects(existingProject.id)`
2. update `todos(todo.id)` with `projectId = existingProject.id`

### Replace a forward array relation

```ts
db.update(app.todos, todo.id, {
  assignees: {
    update: [{ id: alice.id }],
    create: [{ name: "bob", friendsIds: [] }],
  },
});
```

Lowers to:

1. insert `users(bob)`
2. update `todos(todo.id)` with `assigneesIds = [alice.id, bob.id]`

## Open Questions

- Should `db.update(...)` expose the planned writes in dev mode for easier debugging?
- Should nested mutation errors include the low-level write index as well as the path?
- Do we want a future `strictRelations` mode that verifies reverse-array membership through a pre-read once we have an async mutation path?
- Should `CreateOf<TTable>` and `UpdateOf<TTable>` be exported from `schema` alongside `InsertOf<TTable>`?
- Once transactions exist, do we keep partial-failure sync writes as the default, or add a separate transactional graph-write API?
