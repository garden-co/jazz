# jazz-tools

TypeScript SDK, framework bindings, and CLI tools for **Jazz 2**.

## Jazz Classic API errors

Jazz 2 is a local-first relational database with tables, queries, and row-level
permission policies. **Jazz Classic (0.x) code does not work with this package.
This is not a rename-only migration.**

If you encounter `JAZZ_CLASSIC_API_REMOVED`, stop using Classic examples and read
the [Jazz 2 overview](https://jazz.tools/docs) or
[full agent-facing documentation](https://jazz.tools/llms-full.txt).
For example, define tables rather than CoValues:

```ts
import { schema as s } from "jazz-tools";

export const app = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
});
```

Use `db.insert`, `db.all` and `db.subscribeAll` for writes and queries.
React, Vue and Solid expose reactive reads through `useAll` / `useOne` in their
respective entrypoints; Svelte uses `QuerySubscription` / `QuerySubscriptionOne`.
Model access through row-level permission policies, not Classic `Group` objects.
See [client setup](https://jazz.tools/docs/getting-started/client-setup) for creating
the database and configuring authentication.

During the Jazz 2 transition, selected Classic names remain exported **only to
provide actionable errors**, not compatibility:

| Import path               | Diagnostic-only names                                                                                            |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `jazz-tools`              | `co`, `z`, `CoMap`, `CoList`, `CoFeed`, `CoPlainText`, `CoRichText`, `FileStream`, `Account`, `Group`, `Profile` |
| `jazz-tools/react`        | `useCoState`, `useAccount`, `useSuspenseCoState`, `useSuspenseAccount`, `JazzReactProvider`                      |
| `jazz-tools/react-core`   | `useCoState`, `useAccount`, `useSuspenseCoState`, `useSuspenseAccount`                                           |
| `jazz-tools/react-native` | The same four hooks and `JazzReactNativeProvider`                                                                |
| `jazz-tools/expo`         | The same four hooks and `JazzExpoProvider`                                                                       |
| `jazz-tools/svelte`       | `CoState`, `AccountCoState`, `InviteListener`, `SyncConnectionStatus`                                            |
| `jazz-tools/vue`          | `useCoState`, `useAccount`, `useAccountOrGuest`, `useJazzContext`, `useAcceptInvite`                             |

These declarations reject Classic usage in TypeScript. In JavaScript or
transpile-only builds, Classic operations throw synchronously with migration
guidance in development and production. Merely importing a diagnostic export
does not throw; a Classic provider throws when rendered, not when its React
element is created. Existing platform/peer-dependency requirements still apply.
Other unsupported exports and historical subpaths retain normal import errors.

Standard function metadata (`prototype`, `name`, and `length`) remains readable
so tooling such as Next.js Fast Refresh can inspect exports without breaking
valid Jazz 2 imports. TypeScript still rejects extending a Classic value. In
JavaScript, declaring a subclass may be inert; calling its inherited constructor
or accessing Classic static operations raises the diagnostic. A constructor that
deliberately bypasses the Classic base does not execute that base's diagnostic.

Svelte's `JazzSvelteProvider` and Vue's `JazzProvider` are still supported Jazz 2
components, and Svelte's `getJazzContext` remains supported. These providers take
`config`, not Classic `sync` or `AccountSchema` props. Supplying those old props
(including `accountSchema` / `account-schema` spellings, even with an undefined
value) produces the same diagnostic, including when introduced during an update.
Do not rename the provider or treat this as a mechanical prop migration: read the
Jazz 2 setup guide. The providers' supported prop types remain unchanged.
Framework error handlers and boundaries may intercept the exception. In particular,
Vue's production default logs component errors rather than rejecting the SSR
render promise; `app.config.errorHandler` receives the diagnostic. Rejected
provider setup does not render its fallback or descendants or create a client.

Solid's current binding remains supported; no speculative Classic Solid exports
are added. Next.js, SvelteKit and Nuxt use their underlying React, Svelte and Vue
bindings respectively.

These diagnostics apply when using the installed Jazz 2 package. They cannot
intercept separate legacy packages such as `jazz-vue`, `jazz-svelte` or
`jazz-react` that bring their own Classic dependencies.

## Usage

```bash
npx jazz-tools@alpha server
```

To use a specific prerelease:

```bash
npx jazz-tools@2.0.0-alpha.0 server
```

## Supported binary targets

- macOS arm64
- macOS x64
- Linux x64
- Linux arm64

If your platform is not supported in the npm package, install with Cargo from source.

## React Native alpha boundary

The `jazz-tools/react-native` entry point currently exposes compile-level binding
scaffolding only. Persistent React Native/Expo databases are not available in
this alpha: the default persistent configuration and the proposal-only
`sqliteStorage` option both fail before opening a driver. Explicit memory mode
has only been exercised by Node-based wiring tests, not Metro/Hermes or a device,
and is not a supported persistence alternative.

## Schema discovery and Jazz SQL (preview)

`jazz-tools sql` compiles a small SQL dialect into the existing Jazz SDK query
and mutation APIs. It runs in Node with `jazz-napi`. SQL is never executed by the
database engine, and unsupported syntax fails before any session opens.

There are two nouns, each with one default source and one explicit opt-in:

- `schema` describes a schema. It reads the local `./schema.ts` by default and
  needs no server or credentials.
- `sql` reads and writes rows. It uses the deployed schema by default
  (`--app-id`, `--server-url`, and `--admin-secret` required) and can load a
  local `schema.ts` with `--schema-dir` instead.

```sh
# Discovery: local first, no server, no credentials.
jazz-tools schema tables
jazz-tools schema describe todos
jazz-tools schema tables --schema-dir ./my-app --format json

# The same information read from a stored schema version.
export JAZZ_APP_ID=my-app
export JAZZ_SERVER_URL=https://your-jazz-server.example
export JAZZ_ADMIN_SECRET=your-admin-secret
jazz-tools schema tables --schema-hash current
jazz-tools sql 'SHOW TABLES'
jazz-tools sql 'DESCRIBE todos'

# Reads are the default. Remote schema loading still needs the admin secret.
export JAZZ_BACKEND_SECRET=your-backend-secret
jazz-tools sql 'SELECT id, title FROM todos WHERE done = FALSE ORDER BY title LIMIT 20'

# Mutations always require --write. UPDATE/DELETE need an exact WHERE id = 'uuid'.
jazz-tools sql "INSERT INTO todos (title, done) VALUES ('Review draft', FALSE)" --write --format json
jazz-tools sql "UPDATE todos SET done = TRUE WHERE id = '00000000-0000-0000-0000-000000000001'" --write
jazz-tools sql "DELETE FROM todos WHERE id = '00000000-0000-0000-0000-000000000001'" --write

# An existing user's permissions instead of backend access.
jazz-tools sql 'SELECT * FROM todos' --jwt "$USER_JWT"

# One statement from a file or stdin; --sql is an alternative to positional SQL.
printf '%s\n' 'SELECT title FROM todos;' | jazz-tools sql --file - --format json
```

### Machine-readable by default

`--format table` is only the default when stdout is a terminal. Piped or
captured output defaults to `json`, so scripts and agents never parse a
truncated, human-formatted table. `--format jsonl` emits one object per row.
BIGINTs are decimal strings, timestamps are ISO strings, and bytes are number
arrays. `--max-cell-width` (default 80, `0` disables) bounds only table cells.

Failures carry a stable code and a documented exit code instead of prose:

```json
{
  "error": {
    "code": "UNKNOWN_TABLE",
    "message": "Unknown table \"todo\"",
    "hint": "Did you mean \"todos\"?",
    "exitCode": 2
  }
}
```

```
0 success                3 missing or ambiguous credentials
1 unexpected failure     4 denied by the server
2 usage or invalid input 5 timeout (a write may already have committed)
                         6 the row already exists
```

### Inspect before you run

`--explain` resolves the schema, validates the statement, and prints the plan
without opening a data session:

```sh
jazz-tools sql "DELETE FROM todos WHERE id = '...'" --explain --format json
jazz-tools sql 'SELECT rank FROM todos WHERE rank >= 1' --explain
```

`--capabilities` prints the accepted dialect, the operators valid per column
type, and the current limits, with no statement, schema, or credentials:

```sh
jazz-tools sql --capabilities --format json
```

`--verbose` writes the resolved context (app, server, auth mode, schema source,
and schema hash) as one JSON line on stderr, keeping stdout clean.

### Credentials

Row access requires `--backend-secret` / `JAZZ_BACKEND_SECRET` for unrestricted
access, or `--jwt` / `JAZZ_JWT_TOKEN` for an **already registered** user. An
admin secret grants catalogue access only and never row access. Explicit flags
override the environment, blank values count as unset, and supplying both
credentials is an error. JWT login does not create accounts or fall back to
backend authority. Prefer environment variables over flags: argv is visible to
other processes.

### Supported grammar

```sql
SHOW TABLES;
DESCRIBE table;
SELECT * | column [, column ...] FROM table
  [WHERE column operator literal [AND column operator literal ...]]
  [ORDER BY column [ASC | DESC] [, column [ASC | DESC] ...]]
  [LIMIT count] [OFFSET count];
INSERT INTO table (column [, column ...]) VALUES (literal [, literal ...])
  [WITH ID SEED 'seed'];
UPDATE table SET column = literal [, column = literal ...] WHERE id = 'uuid';
DELETE FROM table WHERE id = 'uuid';
```

Keywords ignore case; table and column names are case-sensitive. Double quotes
quote identifiers, single quotes quote strings, and doubling the quote escapes
it (`'O''Brien'`). `--` and non-nested `/* */` comments are accepted. One
statement, with an optional trailing semicolon, per invocation. Invalid syntax
never reaches the engine and never opens a session.

- Comparisons: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, plus `IS NULL` and
  `IS NOT NULL`. The column type must support the operator: numeric and
  timestamp ordering works, text ordering does not. Use `IS NULL`, never
  `= NULL`. Null tests require a nullable column.
- Literals: strings, `TRUE`, `FALSE`, `NULL`, signed 32-bit integers, exact
  signed 64-bit BIGINTs, finite doubles, enum strings, and UUID strings.
  Timestamps accept integer Unix milliseconds or UTC strings in
  `YYYY-MM-DDTHH:mm:ss[.SSS]Z` form. There is no implicit string-to-number
  coercion.
- Projection includes exactly the requested columns; `*` includes `id` and
  ordinary non-sparse columns. Use an explicit `ORDER BY` with a unique
  tie-breaker for repeatable pagination. LIMIT/OFFSET are nonnegative safe
  integers, including `LIMIT 0`.
- INSERT accepts one row. Required fields must be supplied unless they have a
  schema default. UPDATE/DELETE accept only an exact `WHERE id = 'uuid'` and
  first read that row, so JWT callers need read permission as well as write
  permission. A missing or invisible row returns `affectedRows: 0`.
- Writes retain Jazz's merge/concurrency semantics. The lookup and the mutation
  are separate operations, never a SQL transaction or compare-and-swap: write
  results report `atomic: false`. Success waits for global acknowledgement and
  returns `operation`, `affectedRows`, `id`, `idSource`, `txId`, `atomic`, and
  `safeToRetry`.
- `WITH ID SEED '<seed>'` pins the row id from a seed. A retried INSERT then
  fails with `ALREADY_EXISTS` (exit 6) instead of writing a duplicate row, so
  the failure itself tells you the earlier attempt committed.

Reads require a remote response; there is no offline cache fallback. Sessions
use temporary in-memory storage and close after each command. `--timeout`
(default 30000 milliseconds) bounds the entire command, including input,
connection, acknowledgement, and shutdown. Failure exits nonzero and diagnostics
go to stderr; stdout stays clean. The CLI never retries writes, and a timed-out
write may already have committed.

This slice excludes joins, OR/parenthesised expressions, aliases, aggregates,
functions, parameters, schema changes, multi-row writes, SQL transactions,
branch-keyed tables, provenance paths, structured-value literals
(JSON/arrays/bytes/payload enums), and an interactive shell. Use the SDK for
these. `jazz-tools sql --capabilities` is the machine-readable list.
