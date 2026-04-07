# Co-located Explicit Permissions In `schema.ts`

## Problem

Jazz currently splits app authoring across `schema.ts` for structure and optional `permissions.ts` for row-level security. That makes permissions feel like a follow-up step instead of part of the schema itself: a developer can add a new table, validate the app, and only later remember to define access rules somewhere else. The current runtime semantics are also more permissive than the desired model: when a table or operation has no policy, reads and writes are effectively allowed rather than denied. We want the authoring model to make permissions visible from the start, colocate them with the schema, and make the default behavior fail closed: if an app does not declare an allow rule, Jazz should deny that operation.

We do not want this to turn permissions into structural migrations. Permission edits should still be publishable independently of schema hashes and migration edges, even though the source of truth lives in the same `schema.ts` file.

This spec also preserves Jazz's trusted-client split: deny-by-default applies to ordinary user-scoped clients, but privileged admin/backend clients continue to bypass row-level policies.

## Solution

`schema.ts` becomes the only supported authoring file for both structure and current permissions.

The design uses a hybrid DSL:

- Each table can declare a local `permissions` block for straightforward per-table rules.
- `defineApp(...)` can also declare one app-level `permissions` block for cross-table, inherited, claim-heavy, or centrally organized rules.
- Both sources compile into one permissions bundle.
- Rules for the same table and operation keep today's OR semantics: multiple allow rules widen access only for that specific operation.
- Missing rules deny access.
- A table with no local `permissions` block is fully denied unless the app-level block grants specific operations.

### Breadboards

#### 1. Authoring flow

Developer experience should look like this:

1. A developer adds or edits tables in `schema.ts`.
2. They can attach simple rules directly next to a table.
3. If a rule needs `allowedTo`, `exists`, recursive inheritance, or a multi-table view of the app, they can add it in the same file at the app level.
4. `jazz-tools validate` loads `schema.ts`, extracts both structure and permissions, and validates both.
5. If a table ends up with no explicit permissions after combining table-local and app-level rules, `jazz-tools validate` prints a warning explaining that the table will be deny-all for policy-scoped clients and inviting the developer to make that intent explicit.
6. There is no `permissions.ts` to keep in sync.

#### 2. Publication flow

```text
schema.ts
  |-- structural extraction --> wasmSchema --> schema hash / migrations
  `-- permissions extraction -> compiled permissions -> permissions status / push
```

Operationally, the split stays intact:

1. Structural changes still affect the structural schema hash.
2. Permission-only edits do not affect the structural schema hash.
3. `jazz-tools permissions status` and `jazz-tools permissions push` compile permissions from `schema.ts` and target the resolved structural hash, just like today.
4. If `schema.ts` contains a structural change whose hash has not been stored on the server yet, `permissions push` fails and tells the developer to publish the structural schema first.
5. Publishing a structural schema without then publishing permissions is a valid but fail-closed intermediate state.

#### 3. Runtime flow

The runtime behavior must change to match the new contract:

1. If a table has no read rule, queries return no rows from that table.
2. If a table has no insert, update, or delete rule, those writes are rejected.
3. If a structural schema exists but no permissions head has been published for it yet, the server behaves as if authorization is required but unavailable:
   - reads return nothing
   - writes are rejected after the existing schema/authorization wait path
4. A table only becomes readable or writable when one or more explicit allow rules are present for that operation.
5. A table does not need an explicit empty `permissions` block to be denied; absence of a block already means deny-all until some rule is granted elsewhere in `schema.ts`.
6. Trusted clients using admin or backend authentication bypass row-level policies entirely and keep their existing privileged behavior.

This is the semantic center of the spec. The change is not only "same file"; it is also "missing policy means deny."

### Fat Marker Sketch

The implementation should separate authoring, compilation, and publication:

```text
author writes schema.ts
        |
        +--> typed schema definition ----------------------+
        |                                                  |
        |                                            schema hash
        |                                                  |
        |                                            migrations
        |
        +--> local table permission blocks ----------------+
        |                                                  |
        +--> app-level permission block -------------------+--> compiled permissions bundle
                                                           |
                                                     permissions head publish
```

Important boundaries:

- Structural extraction must ignore permission declarations completely.
- Permissions compilation may depend on the structural schema for relation analysis and validation, but it must not perturb the structural hash.
- Loader and CLI output should report both artifacts as coming from `schema.ts`.
- Policy enforcement semantics only apply to policy-scoped clients. Trusted admin/backend roles continue to take the bypass path.

### Core API Shape

This spec chooses a concrete hybrid shape so the work stays single-PR and buildable:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table(
    {
      name: s.string(),
      ownerId: s.string(),
    },
    {
      permissions: ({ policy, session }) => {
        policy.allowRead.always();
        policy.allowDelete.where({ ownerId: session.user_id });
      },
    },
  ),

  todos: s.table(
    {
      title: s.string(),
      ownerId: s.string(),
      projectId: s.ref("projects").optional(),
    },
    {
      permissions: ({ policy, allowedTo, anyOf, session }) => {
        policy.allowRead.where(anyOf([{ ownerId: session.user_id }, allowedTo.read("projectId")]));
        policy.allowInsert.where({ ownerId: session.user_id });
        policy.allowUpdate
          .whereOld({ ownerId: session.user_id })
          .whereNew({ ownerId: session.user_id });
      },
    },
  ),
};

type AppSchema = s.Schema<typeof schema>;

export const app: s.App<AppSchema> = s.defineApp(schema, {
  permissions: ({ policy, allowedTo }) => {
    policy.todos.allowDelete.where(allowedTo.delete("projectId"));
  },
});
```

The API rules behind this shape are:

- `s.table(columns, { permissions })` is the standard way to define local rules.
- Table-local blocks use `policy.allowRead`, `policy.allowInsert`, `policy.allowUpdate`, and `policy.allowDelete` so the syntax stays aligned with app-level permissions.
- In a table-local block, `policy` is already scoped to the current table; in an app-level block, `policy.<table>` selects the table first.
- Table-local blocks still receive the normal helper context (`session`, `anyOf`, `allOf`, `allowedTo`, and `policy`); the table-local `policy` object is scoped sugar, not a weaker DSL.
- `s.defineApp(schema, { permissions })` is optional and augments the same compiled permissions bundle.
- App-level permissions keep the current `policy.<table>.allowRead...` shape to preserve expressive power for cross-table policies.
- Local and app-level rules can both target the same table and operation; they OR-merge into the final bundle.
- Omitting an operation is equivalent to deny for that operation.
- Omitting a table-local `permissions` block is equivalent to deny for all operations on that table unless an app-level rule grants one.
- Omitting a table from both local and app-level permissions is equivalent to deny-all for that table.

### Loader, CLI, and Catalogue Changes

`packages/jazz-tools` should treat `schema.ts` as the sole root:

- `loadCompiledSchema(...)` loads only `schema.ts` or `src/schema.ts`.
- It extracts:
  - structural schema
  - compiled permissions
  - one source path for both: `schema.ts`
- `permissions.ts` is a validation error, not a fallback.
- Legacy low-level `table(name, columns, thirdArg)` should not silently come back as a second authoring style. The supported path is the typed `s.table(...)` / `s.defineApp(...)` API.

CLI behavior should become:

- `jazz-tools validate`
  - `Loaded structural schema from .../schema.ts.`
  - `Loaded current permissions from .../schema.ts.`
  - `Permission-only changes do not create schema hashes or require migrations.`
  - If a table has no explicit compiled permissions, print a warning rather than fail validation. The warning should explain:
    - this table will be deny-all for user/anonymous policy-scoped clients
    - admin/backend clients still bypass policies
    - the developer should add explicit rules if the deny-all outcome is intentional or accidental
- `jazz-tools permissions status|push`
  - resolve structure from `schema.ts`
  - compile permissions from `schema.ts`
  - keep today's server-side bundle/head lifecycle
- `jazz-tools schema export`
  - exports structural schema only

### Runtime Semantics Changes

The runtime must stop treating missing policies as allow-all. Specifically:

- Select:
  - no explicit read rule means zero visible rows
- Insert:
  - no explicit insert rule means reject insert
- Update:
  - no explicit update `using` or `with_check` means reject update
- Delete:
  - no explicit delete rule means reject delete

Trusted-role exception:

- Clients authenticated through the admin secret keep full bypass behavior.
- Clients authenticated through the backend secret keep bypass behavior for row access.
- Deny-by-default applies to user/anonymous policy-scoped traffic, not to trusted admin/backend paths.

In other words, deny-by-default must be implemented in the Rust authorization path, not merely in TypeScript compilation. Publishing an empty permissions bundle for a schema must result in deny-all behavior, not open access.

## Rabbit Holes

- The current Rust runtime treats absent policies as permissive in key read and write paths. If we only move the DSL into `schema.ts` and do not change those paths, the feature will look correct in TypeScript while remaining wrong at enforcement time.
- Structural hashing must ignore permission declarations even though they live in the same file. A naïve implementation that serializes the whole typed app will accidentally turn permission edits into schema hash changes.
- `schema.ts` currently has more than one supported loading path (`collected` DSL, exported schema definition, exported app). The new extraction logic must stay deterministic across those paths and avoid import-cache/state leakage.
- Missing published permissions heads need a clear fail-closed story. If the server keeps serving structural schemas without requiring an authorization schema, the new authoring model still has a permissive gap at rollout time.
- The fail-closed rollout behavior is intentionally stricter than today: a schema can be published before permissions, but that state must remain dark. Tooling and docs need to make that intermediate state obvious so teams do not mistake it for breakage.
- The bypass carve-out must stay explicit in the runtime code paths. If deny-by-default is implemented too high in the stack, it may accidentally block trusted admin/backend clients that today intentionally bypass ReBAC.
- Warning quality matters: if `validate` only checks for a table-local block, it will produce false positives for tables covered exclusively by app-level rules. The warning must run on the final compiled permissions map.
- The hybrid API can become confusing if local and app-level rules have different capabilities or different merge semantics. The spec assumes one compiled bundle and one OR-merge model.

## No-gos

- No compatibility bridge for `permissions.ts`.
- No schema-hash versioning of permission-only changes.
- No new migration artifact for permission-only edits.
- No attempt to preserve the old "missing policy means allow" semantics.
- No second inline-permissions DSL for the low-level side-effect `table(...)` API.
- No removal of the existing admin/backend trusted-client bypass behavior.

## Testing Strategy

Favor integration-first coverage over helper-level unit tests.

TypeScript / tooling:

- Extend `packages/jazz-tools/src/cli.test.ts` with same-file loading cases:
  - validate loads structure and permissions from the same `schema.ts`
  - permission-only edits do not alter exported structural schema
  - `permissions.ts` present alongside `schema.ts` fails validation
  - tables with only local permissions, only app-level permissions, and both all compile correctly
  - validate warns when a table has no explicit compiled permissions
  - validate does not warn when a table has no local block but is covered by app-level permissions

Rust runtime / catalogue:

- Add SchemaManager or RuntimeCore-level tests for deny-by-default behavior with realistic actors (`alice`, `bob`):
  - table with no read rule returns no rows
  - table with no write rules rejects insert/update/delete
  - local rule plus app-level rule OR-merge for the same operation
  - published empty permissions bundle results in deny-all, not allow-all
  - structural schema present with no permissions head fails closed until a head is published
  - admin-authenticated client bypasses read/write policy checks
  - backend-authenticated client bypasses read/write policy checks

Use short ASCII flow sketches in the multi-step runtime tests where helpful. Example:

```text
alice publishes schema
alice does not publish permissions head yet
bob subscribes  -> sees nothing
bob writes row  -> denied
alice publishes permissions head
bob retries     -> policy result now depends on explicit allow rules
```

## Example Conversion

Below is a representative slice of `examples/chat-react`, shown as a side-by-side migration.

- Left: today's split across `schema.ts` and `permissions.ts`
- Right: the new single-file `schema.ts`

This example intentionally uses only the `chats`, `chatMembers`, and `messages` tables so the local-vs-app-level split is easy to read.

<table>
  <tr>
    <th align="left">Before: <code>schema.ts</code> + <code>permissions.ts</code></th>
    <th align="left">After: single <code>schema.ts</code></th>
  </tr>
  <tr>
    <td valign="top">

```ts
// schema.ts
import { schema as s } from "jazz-tools";

const schema = {
  chats: s.table({
    name: s.string().optional(),
    isPublic: s.boolean(),
    createdBy: s.string(),
    joinCode: s.string().optional(),
  }),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    userId: s.string(),
    joinCode: s.string().optional(),
  }),
  messages: s.table({
    chatId: s.ref("chats"),
    text: s.string(),
    senderId: s.ref("profiles"),
    createdAt: s.timestamp(),
  }),
};

export const app = s.defineApp(schema);

// permissions.ts
import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, anyOf, allowedTo }) => {
  const userIsChatMember = (chatId) =>
    policy.chatMembers.exists.where({ chatId, userId: session.user_id });

  policy.chats.allowRead.where((chat) =>
    anyOf([
      { isPublic: true },
      userIsChatMember(chat.id),
      { joinCode: session["claims.join_code"] },
    ]),
  );
  policy.chats.allowInsert.where({ createdBy: session.user_id });
  policy.chats.allowUpdate.where((chat) => userIsChatMember(chat.id));

  policy.chatMembers.allowRead.where((member) =>
    anyOf([{ userId: session.user_id }, userIsChatMember(member.chatId)]),
  );
  policy.chatMembers.allowInsert.where({ userId: session.user_id });
  policy.chatMembers.allowDelete.where({ userId: session.user_id });

  policy.messages.allowRead.where(allowedTo.read("chatId"));
  policy.messages.allowInsert.where((message) => userIsChatMember(message.chatId));
  policy.messages.allowDelete.where({ senderId: session.user_id });
});
```

</td>
<td valign="top">

```ts
// schema.ts
import { schema as s } from "jazz-tools";

const schema = {
  chats: s.table(
    {
      name: s.string().optional(),
      isPublic: s.boolean(),
      createdBy: s.string(),
      joinCode: s.string().optional(),
    },
    {
      permissions: ({ policy, session, anyOf }) => {
        policy.allowRead.where((chat) =>
          anyOf([{ isPublic: true }, { joinCode: session["claims.join_code"] }]),
        );
        policy.allowInsert.where({ createdBy: session.user_id });
      },
    },
  ),

  chatMembers: s.table(
    {
      chatId: s.ref("chats"),
      userId: s.string(),
      joinCode: s.string().optional(),
    },
    {
      permissions: ({ policy, session }) => {
        policy.allowInsert.where({ userId: session.user_id });
        policy.allowDelete.where({ userId: session.user_id });
      },
    },
  ),

  messages: s.table(
    {
      chatId: s.ref("chats"),
      text: s.string(),
      senderId: s.ref("profiles"),
      createdAt: s.timestamp(),
    },
    {
      permissions: ({ policy, allowedTo, session }) => {
        policy.allowRead.where(allowedTo.read("chatId"));
        policy.allowDelete.where({ senderId: session.user_id });
      },
    },
  ),
};

export const app = s.defineApp(schema, {
  permissions: ({ policy, session, anyOf }) => {
    const userIsChatMember = (chatId) =>
      policy.chatMembers.exists.where({ chatId, userId: session.user_id });

    policy.chats.allowRead.where((chat) => userIsChatMember(chat.id));
    policy.chats.allowUpdate.where((chat) => userIsChatMember(chat.id));

    policy.chatMembers.allowRead.where((member) =>
      anyOf([{ userId: session.user_id }, userIsChatMember(member.chatId)]),
    );

    policy.messages.allowInsert.where((message) => userIsChatMember(message.chatId));
  },
});
```

</td>
  </tr>
</table>
