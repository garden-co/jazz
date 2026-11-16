# Multi-language snippet spec

Every tab in a `jazz-environment` group shows **the same example in a different language**:
the same app, the same scenario and the same shape. Write a new region for every language to
this spec, and match it when you change an existing one.

## The app

Every language uses the docs todo app: `todos` and `projects`, as declared in
[`examples/docs/todo-client-localfirst-ts/schema.ts`](../../../examples/docs/todo-client-localfirst-ts/schema.ts).
Don't copy the schema into a non-TS example. Re-export it from the `schema.ts` beside that example's
manifest, then run `npx jazz-tools generate`:

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

- **One complete function per region**, named after what it does, in the language's own case:
  `readOpenTodos`, `read_open_todos`, `ReadOpenTodos`. Don't cut a body out of a larger function;
  the reader can't see where `show` or `jazz` came from.
- **Parameters:** the client (`db`, `jazz`, `ctx, db` in Go) and any ids the scenario names.
- **Return the result.** Don't print it.
- **Comments:** only where they help, for what's particular to that language (for example, how a
  nullable patch field is written).
- Each region shows only its scenario, nothing more: no extra reads or writes around it.

## The scenarios

The TypeScript region is the reference; match its behaviour.

| region                             | scenario                                                                                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `reading-oneshot`                  | all open todos (`done = false`)                                                                                                                   |
| `reading-subscriptions`            | subscribe to open todos, get the whole result on each change, then unsubscribe. A language with both whole results and deltas shows whole results |
| `reading-filters`                  | `done = false` and `title` contains `"docs"`                                                                                                      |
| `reading-sorting`                  | open todos by `title`, ascending                                                                                                                  |
| `reading-pagination`               | page `page` (counted from 1) of `pageSize` (20) open todos, by `title` then `id`, ascending                                                       |
| `reading-durability-tier`          | open todos at the `Remote` tier                                                                                                                   |
| `reading-includes`                 | open todos with their `project`, and their `parent`'s `project`                                                                                   |
| `reading-reverse-relation`         | projects, each with its open todos (`include`)                                                                                                    |
| `reading-select`                   | open todos' `title`, with their project's `name`                                                                                                  |
| `where-eq-ne`                      | `done = false`; `title != "Draft"`; `id` in two ids                                                                                               |
| `where-numeric`                    | `priority >= 3`; `priority < 10`                                                                                                                  |
| `where-contains`                   | `title` contains a search term                                                                                                                    |
| `where-null`                       | `parentId` is null; `parentId` is not null                                                                                                        |
| `writing-crud`                     | insert `{ title: "Write docs", done: false }`; set `todoId`'s `done` to `true`; delete `todoId`                                                   |
| `writing-nullable-update`          | set `todoId`'s `owner_id` to null; leave `description` unchanged                                                                                  |
| `writing-upsert`                   | upsert `importedTodoId` as `{ title: "Imported task", done: false }`, wait for `global`                                                           |
| `writing-durability-tier`          | insert, update, delete one todo, each waiting for `global`                                                                                        |
| `writing-transaction`              | in one transaction: insert `"Write transaction docs"`, set `existingTodoId` done, read the new todo back; wait for `global`; return its id        |
| `writing-transaction-errors`       | a transaction setting `todoId` done; catch the server's rejection and report its code and reason                                                  |
| `writing-serializable-transaction` | serializably: read `todoId`, fail if missing, set it done; return its id                                                                          |
| `writing-mutation-errors`          | insert `"Ship review fixes"`, wait for `global`, catch the server's rejection                                                                     |
| `dry-run-permissions`              | permission advice: can the user read `todoId`; can they update its title; can they insert a todo with `title`                                     |

Where a language can't do a scenario, leave its region out; the docs show its "Not supported yet"
list instead. Don't substitute a different scenario.
