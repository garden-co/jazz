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

### Relation naming convention

Relations use a bounded identifier convention, without generated application or
library code. Scalar reference names strip a final `Id` or `_id`. A final `Ids`
or `_ids` additionally pluralizes the remaining name. Names without these exact
suffixes stay unchanged, including `status` and `analysis`. Reverse names remain
`<sourceTable>Via<CapitalizedForwardName>`.

Pluralization recognizes terminal `person/people`, `child/children`, `mouse/mice`,
`goose/geese`, `tooth/teeth`, `foot/feet`, `analysis/analyses`, `status/statuses`,
`alias/aliases`, and `bus/buses`. Equipment, news, information, software, data, media,
series, species, fish, and sheep are invariant. These dictionary suffixes match
lowercase, Titlecase, or UPPERCASE, preserving any prefix (`ownerPersonIds` →
`ownerPeople`, `owner_person_ids` → `owner_people`). Existing dictionary plurals
stay unchanged. Otherwise consonant + `y` becomes `ies`; `ss`, `sh`, `ch`, `x`,
and `z` gain `es`; an existing final `s` stays; all other names gain `s`.
Regular endings are uppercase only when the entire stem is uppercase. Empty
stems stay empty (normal relation collision validation still applies). This is
not a general English inflector: unusual words or mixed-case dictionary spellings
follow the regular rules. Runtime aliases and literal TypeScript names use the
same rules; a widened `string` remains `string`.

This replaces the previous runtime English inflector and simpler TypeScript
rules. Review include keys, reverse relation names, and permission hops when
upgrading: names outside this convention may change. Stored column names,
reference targets, schema hashes, and existing data are unchanged; no storage
migration is involved. Ambiguous aliases and aliases that shadow another stored
column continue to be rejected.

### Inferring relation APIs from authored schemas

Keep the schema, app, and permissions in separate modules if helpful. No generated
relation file or build step is required:

```ts
// schema.ts
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  people: s.table({ name: s.string() }),
  records: s.table({
    personIds: s.array(s.ref("people")),
    address: s.ref("people").optional(),
  }),
});
```

```ts
// app.ts
import { schema as s } from "jazz-tools";
import { schema } from "./schema.js";

export const app: s.App<typeof schema> = s.defineApp(schema);
export const recordsWithPeople = app.records.include({ people: true, address: true });
export type RecordWithPeople = s.RowOf<typeof recordsWithPeople>;
// people: Array<{ id: string; name: string }>
// address: { id: string; name: string } | null

export const peopleWithRecords = app.people.include({ recordsViaPeople: true });
export const relatedPeople = app.records.hopTo("people");
export const relatedRecords = app.people.hopTo("recordsViaPeople");
```

```ts
// permissions.ts
import { schema as s } from "jazz-tools";
import { app } from "./app.js";

export default s.definePermissions(app, ({ policy }) => [policy.people.allowRead.where({})]);
```

The normal `jazz-tools validate --schema-dir .` command loads `schema.ts` and
`permissions.ts`; permissions can import the independently authored app.
A raw definition also works: declare `type AppSchema = s.Schema<typeof definition>`
and `const app: s.App<AppSchema> = s.defineApp(definition)`.

`RowOf` preserves scalar/array cardinality. `.requireIncludes()` refines a
non-nullable scalar reference's included row from `Row | null` to `Row` and
requires its match; nullable references such as `address` stay nullable. Required
array includes require all referenced matches and remain arrays. Reverse includes
remain arrays. These APIs infer their relation names directly from the authored
schema, so there is no separate catalogue to generate, pass, or check for staleness.

For a smaller typed surface, use
`s.defineSliceableApp(schema).slice("records", "people")`. Only selected tables
and relations between them are exposed in TypeScript; references outside a slice
remain scalar IDs or ID arrays. Slices retain the full structural schema for
query planning and schema identity. A slice is a typing convenience, not an
authorization boundary; permission policies still control access.
