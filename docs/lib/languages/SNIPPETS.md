# Multi-language snippet spec

Every tab in a `jazz-environment` group shows **the same example in a different language**: the
same app, the same scenario and the same shape. Write every language's regions to this spec, and
keep to it when you change one. [README.md](README.md) covers adding the tabs to pages.

## Regions

A region is `// #region <topic>-<lang>` … `// #endregion <topic>-<lang>`, where `<topic>` is a
name from the table below and `<lang>` is `ts`, `effect`, `rust`, `swift`, `kotlin` or `go`: for
example `reading-oneshot-swift`. Pages include regions by that name, so don't rename them.

## The app

Every language uses the docs todo app: `todos` and `projects`, as declared in
[`examples/docs/todo-client-localfirst-ts/schema.ts`](../../../examples/docs/todo-client-localfirst-ts/schema.ts).
Don't copy the schema into a non-TS example: re-export it from the `schema.ts` beside that example's
manifest, then run `npx jazz-tools generate`.

```ts
export {
  schema,
  permissions,
} from "<relative path>/examples/docs/todo-client-localfirst-ts/schema";
```

| table      | columns                                                                                    | relations                                                                |
| ---------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| `todos`    | `title`, `done`, `priority?` (int), `description?`, `owner_id?`, `parentId?`, `projectId?` | `parent` → todos, `project` → projects, `children` (reverse of `parent`) |
| `projects` | `name`                                                                                     | `todos` (reverse of `todos.project`)                                     |

## The shape

- **One complete function per region**, named after what it does, in the language's own case
  (`readOpenTodos`, `read_open_todos`, `ReadOpenTodos`). Don't cut a body out of a larger
  function, where the reader can't see where its variables came from.
- **The exception is `where-*`:** those regions are a few statements, each a query on its own,
  inside one function in the source. The variables they use (`db`, a search term, ids) are that
  function's parameters.
- **Parameters:** the client (`db`, `jazz`, or `ctx, db` in Go) and any ids the scenario names.
- **Return the result.** Don't print it, except where the TypeScript region does: the error
  regions report a rejection's code and reason.
- **Comments:** where they help, especially for what's particular to that language.
- **Keep to the scenario:** no extra reads or writes around it.
- **Inserts:** set the columns the TypeScript region sets. Its `owner_id` and `projectId` are
  example ids; use your example's own constants for them.

## The scenarios

The TypeScript region in `examples/docs/todo-client-localfirst-ts/src/docs-snippets.ts` is the
reference. The table says what each one does; match it.

| topic                              | scenario                                                                                                                                                  |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `reading-oneshot`                  | all open todos (`done = false`)                                                                                                                           |
| `reading-one`                      | the todo with id `id`, or none                                                                                                                            |
| `reading-subscriptions`            | subscribe to open todos, get the whole result on each change, and unsubscribe. A language with both whole results and deltas shows whole results          |
| `reading-filters`                  | `done = false` and `title` contains `"docs"`                                                                                                              |
| `reading-sorting`                  | open todos by `title`, ascending                                                                                                                          |
| `reading-pagination`               | page `page` (counted from 1) of `pageSize` (20) open todos, by `title` then `id`, ascending                                                               |
| `reading-durability-tier`          | open todos at the `Remote` tier                                                                                                                           |
| `reading-includes`                 | open todos with their `project`, and their `parent`'s `project`                                                                                           |
| `reading-reverse-relation`         | projects, each with its open todos (`include`)                                                                                                            |
| `reading-select`                   | open todos' `title`, with their project's `name`                                                                                                          |
| `reading-require-includes`         | rows whose required reference must resolve (`requireIncludes`). TypeScript uses a separate orders/customers recipe app                                    |
| `reading-recursive`                | a query gathering, from each open todo, its chain of parents, up to 10 deep                                                                               |
| `where-eq-ne`                      | `done = false`; `title != "Draft"`; `id` in two ids                                                                                                       |
| `where-numeric`                    | `$createdAt` after a week ago; `priority >= 3`; `priority < 10`                                                                                           |
| `where-contains`                   | `title` contains a search term                                                                                                                            |
| `where-null`                       | `parentId` is null; `parentId` is not null                                                                                                                |
| `where-and`                        | `done = true` and `projectId` is not null                                                                                                                 |
| `where-order-limit`                | open todos by `$createdAt`, ascending, limit 50                                                                                                           |
| `where-subscription`               | subscribe to open todos                                                                                                                                   |
| `writing-crud`                     | insert `"Write docs"` (not done, with `owner_id` and `projectId`); set `todoId`'s `done` to `true`; delete `todoId`                                       |
| `writing-nullable-update`          | set `todoId`'s `owner_id` to null; leave `description` unchanged                                                                                          |
| `writing-upsert`                   | upsert `importedTodoId` as `{ title: "Imported task", done: false }`, and wait for `global`                                                               |
| `writing-restore`                  | delete `todoId`, read it back including deleted rows, and restore it as `"Restored task"`                                                                 |
| `writing-durability-tier`          | insert, update and delete one todo, each waiting for `global`                                                                                             |
| `writing-mutation-errors`          | insert `"Ship review fixes"`, wait for `global`, and report a rejection's code and reason                                                                 |
| `writing-transaction`              | in one transaction: insert `"Write transaction docs"`, set `existingTodoId` done, read the new todo back; wait for `global`; return its id                |
| `writing-manual-transaction`       | begin a transaction, insert `"Review staged changes"`, then roll back if `shouldCancel`, or commit and wait for `global`                                  |
| `writing-transaction-errors`       | a transaction setting `todoId` done; wait for `global`, and report a rejection's code and reason                                                          |
| `writing-serializable-transaction` | serializably: read `todoId`, fail if it's missing, set it done; return its id                                                                             |
| `dry-run-permissions`              | permission advice, in four functions: can the user read `todoId`; which todos they may delete; which they may update; can they insert a todo with `title` |

Where a language can't do a topic, leave its region out, and the page leaves its tab out. Don't
substitute a different scenario.
