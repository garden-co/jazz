# jazz-tools

TypeScript SDK, framework bindings, and CLI tools for **Jazz 2**.

## Jazz Classic API errors

Jazz 2 is a local-first relational database with tables, queries, and row-level
permission policies. **Jazz Classic (0.x) code does not work with this package.
This is not a rename-only migration.**

If you encounter `JAZZ_CLASSIC_API_REMOVED`, stop using Classic examples and read
the [Jazz 2 overview](https://jazz.tools/docs) or
[full agent-facing documentation](https://jazz.tools/llms-full.txt).
For example, declare tables rather than CoValues, in `jazz.app.json`:

```json
{
  "format": 1,
  "tables": { "todos": { "columns": { "title": "string", "done": "boolean" } } }
}
```

`npx jazz-tools generate` checks it (and the app's `jazz.permissions.json`)
and writes `jazz.generated.ts`, the app's typed bindings:
`import { app } from "./jazz.generated"`.

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

Svelte's and Vue's `JazzProvider` are still supported Jazz 2
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
npx jazz-tools generate          # check jazz.app.json / jazz.permissions.json, write jazz.generated.ts
npx jazz-tools migrations create --name <name>   # record a schema change
npx jazz-tools upgrade           # convert an alpha.64 app (schema.ts, permissions.ts, migrations/)
```

## Supported binary targets

- macOS arm64
- macOS x64
- Linux x64
- Linux arm64

If your platform is not supported in the npm package, install with Cargo from source.

## React Native alpha boundary

The `jazz-tools/react-native` entry point requires a matching installed
`jazz-rn` native artifact. Persistent databases use native account admission
or a platform-issued `nativeRelay` capability. Storage is owned by the native
runtime; there is no JavaScript SQLite driver option. Explicit memory mode is
not supported by the React Native entrypoint.

## Relations

Relations are declared explicitly in a table's `relations` in `jazz.app.json`.
Store row IDs in `uuid` columns (or `{ "array": "uuid" }`), name a forward
relation with `{ "rel": "<table>", "column": "<column>" }`, and name a reverse
traversal with `{ "reverse": "<table>", "relation": "<forward relation>" }`.
There is no suffix convention and no automatic reverse relation: a UUID column
without a relation declaration stays an ordinary UUID column. See
[Defining tables](https://jazz.tools/docs/schemas/defining-tables) for the full rules.

```json
{
  "format": 1,
  "tables": {
    "people": {
      "columns": { "name": "string" },
      "relations": { "records": { "reverse": "records", "relation": "people" } }
    },
    "records": {
      "columns": { "personIds": { "array": "uuid" }, "addressId": "uuid?" },
      "relations": {
        "people": { "rel": "people", "column": "personIds" },
        "address": { "rel": "people", "column": "addressId" }
      }
    }
  }
}
```

```ts
import type { RowOf } from "jazz-tools";
import { app } from "./jazz.generated";

export const recordsWithPeople = app.records.include({ people: true, address: true });
export type RecordWithPeople = RowOf<typeof recordsWithPeople>;
// people: Array<{ id: string; name: string }>
// address: { id: string; name: string } | null

export const peopleWithRecords = app.people.include({ records: true });
export const relatedPeople = app.records.hopTo("people");
```

`RowOf` preserves scalar/array cardinality. `.requireIncludes()` refines a
non-nullable scalar reference's included row from `Row | null` to `Row` and
requires its match; nullable references such as `address` stay nullable. Required
array includes require all referenced matches and remain arrays. Reverse includes
remain arrays. Relation names come straight from `jazz.app.json`, through the
bindings `jazz-tools generate` writes; `generate --check` in CI catches stale ones.
