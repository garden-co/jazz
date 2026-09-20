---
"jazz-tools": patch
"create-jazz": patch
---

Replace inferred relationship names with explicit table-local declarations. `s.table(columns, relations)` now requires a relationship map (use `{}` when empty). Store references as `s.uuid()` or UUID arrays, declare forward navigation with `s.rel(targetTable, column)`, and declare reverse navigation with `s.reverse(sourceTable, forwardRelationName)`.

Remove automatic reverse relationships, reference-name suffix restrictions, and pluralization. Validate declarations in TypeScript and at runtime, including cross-table targets, reserved names, and conflicting reference targets. Preserve existing core reference metadata and storage identity when migrating equivalent declarations. Update examples, starters, permissions, and migration tooling to the explicit API.

### Migrating an existing app

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
- You can keep previous navigation names, as above, or choose names such as `authoredPosts`. For example, renaming `author` to `writer` keeps `authorId` unchanged, but requires `s.reverse("posts", "writer")` and `.include({ writer: true })` in place of the old names. Update `hopTo`, relation-based filters, and permission traversals too, including names used in untyped query objects.
- Relationship names cannot shadow columns, `id`, reserved `$...` fields, or reserved prototype names. If an old include replaced a same-named UUID column, preserve that stored column and choose a distinct relationship name instead.
- Keep `{}` on tables without relationships, including tables in test fixtures, migration schema witnesses, and dynamically generated schema source. Migration operations such as `s.add.ref(...)` and `s.drop.ref(...)` remain separate APIs; do not mechanically replace those operations.
- Update schemas shared by browser, server, and React Native consumers, then run TypeScript checking and `jazz-tools validate --schema-dir <your-schema-directory>`. Republish permissions if you changed their authored traversals.

### Migrating with an agent

Point your coding agent at this guide and ask it to migrate the whole app while preserving schema identity. Have it check every schema consumer (browser, server, React Native, tests, and generated schema sources), preserve the column definitions and targets above, update query and permission traversals together, and run the app's TypeScript checks plus `jazz-tools validate --schema-dir <your-schema-directory>`. Keep existing stores and pending writes; do not reset data to make the migration pass.

### Schema identity and existing data

The underlying mapping, for example `posts.authorId → users`, already participates in schema identity and is used by core query validation and indexing. Both the old `s.ref("users")` and the new `s.rel("users", "authorId")` produce that same mapping.

An equivalent conversion that preserves column definitions and all reference targets requires no stored-data rewrite or database reset. Keep existing stores and pending writes; do not clear browser data as an upgrade shortcut. Relationship aliases and reverse navigation declarations do not themselves enter the core schema hash: renaming `author` to `writer` only changes the authored query API, provided you also update reverse declarations and callers.

Replacing a former reference with a plain UUID **without** its forward declaration removes reference metadata; changing its target changes that metadata. Those are actual schema changes, not the API-only conversion described here. Do not use a database reset as a migration shortcut or assume an empty migration can retarget references.

The docs, maintained examples, starter templates, permission examples, and schema-export tooling have been updated to this API. Historical fixture producers pinned to older published packages intentionally retain their original API.
