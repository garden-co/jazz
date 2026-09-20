---
"jazz-tools": patch
"create-jazz": patch
---

Jazz now uses explicit table-local relationship declarations instead of inferred relationship names. `s.table(columns, relations)` now requires a relationship map (use `{}` when empty). Store references as `s.uuid()` or UUID arrays, declare forward navigation with `s.rel(targetTable, column)`, and declare reverse navigation with `s.reverse(sourceTable, forwardRelationName)`.

Jazz no longer adds automatic reverse relationships or derives relationship names from reference-name suffixes and pluralization. Declarations are validated in TypeScript and at runtime, including cross-table targets, reserved names, and conflicting reference targets. Equivalent declarations preserve existing core reference metadata and storage identity. The examples, starters, permissions, and migration tooling now use the explicit API.

### Migrating an existing app

Follow this order when upgrading from alpha.55 to alpha.56:

Run these commands from the app package directory. The examples assume its schema is in `src`; replace `src` with the actual schema directory in every command. Repeat the checks for each independently defined app schema, keeping separate snapshots.

1. **Before upgrading Jazz packages**, export the compiled schema and record the CLI hash with the existing alpha.55 packages and schema. Keep these files outside the schema directory and preserve them through the upgrade:

   ```sh
   pnpm exec jazz-tools schema export --schema-dir src > schema-before-alpha56.json
   pnpm exec jazz-tools schema hash --schema-dir src > schema-before-alpha56.hash.txt
   ```

2. Upgrade the app's Jazz packages together to alpha.56, then convert the whole app to the schema form below. Preserve the underlying schema identity and update query and permission callers together.
3. Export again and record the upgraded CLI hash:

   ```sh
   pnpm exec jazz-tools schema export --schema-dir src > schema-after-alpha56.json
   pnpm exec jazz-tools schema hash --schema-dir src > schema-after-alpha56.hash.txt
   ```

   **Compare the exported structure, not the two CLI hash strings.** Alpha.56 also corrects the CLI's hashing of defaults to match the server. A schema containing defaults can therefore display a different CLI hash without changing its stored identity. The CLI prints only a 12-character short hash; preserve both receipts for diagnosis, but do not require cross-version equality.

   Run this comparison using Node. It removes only table-local navigation metadata (`relations`), which does not enter storage identity, and checks everything else. It preserves column order, defaults, reference targets, indexes, branch keys, and any other exported metadata; object property order does not matter.

   ```sh
   node --input-type=module <<'NODE'
   import { readFileSync } from "node:fs";
   import { deepStrictEqual } from "node:assert";

   function structure(file) {
     const schema = JSON.parse(readFileSync(file, "utf8"));
     for (const table of Object.values(schema)) delete table.relations;
     return schema;
   }

   deepStrictEqual(
     structure("schema-after-alpha56.json"),
     structure("schema-before-alpha56.json"),
     "Stored schema changed: inspect the conversion before deploying; do not reset data.",
   );
   console.log("Stored schema structure is unchanged.");
   NODE
   ```

   If this comparison fails, stop before deploying and inspect the conversion. Do not dismiss a structural difference as the hash-calculator correction, remove additional fields to make it pass, or reset existing data. If you already upgraded without a snapshot, recover the original schema and alpha.55 package versions in a separate checkout and export there; do not reconstruct the baseline from the converted schema.

4. Run the app's TypeScript checks (for example, `pnpm exec tsc --noEmit` where that is the app's check command) and `pnpm exec jazz-tools validate --schema-dir src`. Exercise the app's queries and permissions against its existing data before deploying. Republish permissions if you changed their authored traversals. Structural equality does not prove that every query and permission caller is correct.

Every `s.table` now requires two arguments: stored columns and explicitly named relationships. Pass `{}` when there are no relationships. Replace `s.ref(target)` with `s.uuid()` and move the target information into a forward relationship declaration. **To keep the same schema identity and existing data, preserve every stored column name, modifier, default, optionality, array shape, and reference target.** Add a matching forward relationship for every former reference column, even if no query currently uses it. This conversion needs no data rewrite or database reset.

Before:

```ts
const schema = s.defineSchema({
  users: s.table({ name: s.string() }),
  posts: s.table({
    title: s.string(),
    authorId: s.ref("users"),
    editorId: s.ref("users").optional(),
    reviewerIds: s.array(s.ref("users")).default([]),
  }),
});
```

After, with the same schema identity:

```ts
const schema = s.defineSchema({
  posts: s.table(
    {
      title: s.string(),
      authorId: s.uuid(),
      editorId: s.uuid().optional(),
      reviewerIds: s.array(s.uuid()).default([]),
    },
    {
      author: s.rel("users", "authorId"),
      editor: s.rel("users", "editorId"),
      reviewers: s.rel("users", "reviewerIds"),
    },
  ),
  users: s.table(
    { name: s.string() },
    {
      postsViaAuthor: s.reverse("posts", "author"),
      postsViaEditor: s.reverse("posts", "editor"),
      postsViaReviewers: s.reverse("posts", "reviewers"),
    },
  ),
});
```

The required `authorId`, optional `editorId`, and array `reviewerIds` keep their names and still point to `users`. The array also keeps its `[]` default.

- Declare a forward relation for **every former reference column**, including references not currently used by an include.
- Declare reverse relations explicitly if your queries or permissions use them. The second argument to `s.reverse` is the **forward relationship name**, not its UUID column. No reverse navigation is added automatically.
- You can keep previous navigation names, as above, or choose names such as `authoredPosts`. For example, renaming `author` to `writer` keeps `authorId` unchanged, but requires `s.reverse("posts", "writer")` and `.include({ writer: true })` in place of the old names. Update `hopTo`, relation-based query filters, and permission `hopTo` calls too, including names used in untyped query objects.
- Relationship names cannot shadow columns, `id`, reserved `$...` fields, or reserved prototype names. If an old include replaced a same-named UUID column, preserve that stored column and choose a distinct relationship name instead.
- Keep `{}` on tables without relationships, including tables in test fixtures, migration schema witnesses, and dynamically generated schema source. Migration operations such as `s.add.ref(...)` and `s.drop.ref(...)` remain separate APIs; do not mechanically replace those operations.
- Update schemas shared by browser, server, and React Native consumers. Complete the structural comparison, TypeScript, validation, and app checks above before deploying.

### Migrating with an agent

Point your coding agent at this guide and the release notes, and give it this instruction:

> Follow the before/after export-and-comparison commands in this guide. Capture the original alpha.55 schema export and CLI hash before upgrading packages. Switch the whole app to the new schema form, preserving all stored schema metadata and updating query and permission callers. Export with alpha.56 and compare the snapshots after removing only each table's `relations` metadata. Do not require the alpha.55 and alpha.56 CLI hash strings to match: alpha.56 fixes default-value hashing. Stop before deployment if the structural comparison fails. Run TypeScript checks, `pnpm exec jazz-tools validate --schema-dir src`, and existing-data query and permission checks. Never reset data to make the upgrade pass.

Have it use the actual app schema directory as described above and check every schema consumer (browser, server, React Native, tests, migration schema witnesses, and generated schema sources). Preserve all column definitions and reference targets, indexes, and branch keys. Update permission traversals together with the declared relationship names. Keep existing stores and pending writes.

### Schema identity and existing data

The underlying mapping, for example `posts.authorId → users`, already participates in schema identity and is used by core query validation and indexing. Both the old `s.ref("users")` and the new `s.rel("users", "authorId")` produce that same mapping.

An equivalent conversion that preserves column definitions and all reference targets requires no stored-data rewrite or database reset. Keep existing stores and pending writes; do not clear browser data as an upgrade shortcut. Relationship aliases and reverse navigation declarations do not themselves enter the core schema hash: renaming `author` to `writer` only changes the authored query API, provided you also update reverse declarations and callers.

Replacing a former reference with a plain UUID **without** its forward declaration removes reference metadata; changing its target changes that metadata. Those are actual schema changes, not the API-only conversion described here. Do not use a database reset as a migration shortcut or assume an empty migration can retarget references.

The docs, maintained examples, starter templates, permission examples, and schema-export tooling have been updated to this API. Historical fixture producers pinned to older published packages intentionally retain their original API.
