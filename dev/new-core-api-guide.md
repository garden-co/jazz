# Jazz 2 new-core API guide

Snapshot: commit `b29b48d1a6b2ca4061ca2e7a661e4f3d79dd8de2` from
`origin/codex/jazz-core-engine-swap` (2026-08-19).

This guide is for application developers, maintainers and SDK authors who need
to check the new core's API at this commit. Start with the application API or
the tooling and extension API, then use the complete catalogue for exact
signatures.

## How to use this guide

- For TypeScript, search for the import path or exported name. A symbol is part
  of the package API only if the package manifest and a public entry file
  export it.
- If you are building an application, start with [Application API](#application-api).
  If you are extending Jazz or maintaining its tooling, start with
  [Tooling and extension API](#tooling-and-extension-api).
- Use the [complete TypeScript catalogue](#complete-typescript-api-catalogue)
  when auditing exports. It contains application APIs and specialist exports.
- TypeScript types that exist only to carry generic inference or validate an
  inferred object shape are deliberately omitted. Types meant for explicit
  annotations, such as `RowOf`, `InsertOf` and `WhereOf`, remain listed.
- For Rust application code, start with `jazz::db::Db<S>`,
  `jazz::tools::public_schema` and the selected `jazz::tools` re-exports. A
  `pub` item may still be an engine or server implementation detail.
- For NAPI, WASM and UniFFI, use the consumer declarations in the binding
  sections. The generated declaration shipped with a release is authoritative.
- Follow the narrowest label on an export. Labels such as `@internal`,
  browser-only, devtools, testing and advanced/unstable override the general
  status of the import path.
- Use identity and attribution methods only in trusted serving code. They are
  not client capabilities.

## Application API

Use these entry points to build browser, mobile, server, Rust or native
applications. The complete catalogue also includes their supporting types and
exact signatures.

| API area                     | Import or crate                                                                  | Use it for                                                         |
| ---------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Framework-neutral TypeScript | `jazz-tools`                                                                     | Define schemas, create typed apps and use permissions              |
| React                        | `jazz-tools/react`                                                               | Use Jazz in a React application                                    |
| React Native and Expo        | `jazz-tools/react-native`, `jazz-tools/expo`, `jazz-tools/expo/polyfills`        | Configure native providers and authentication                      |
| Vue, Svelte and Solid        | `jazz-tools/vue`, `jazz-tools/svelte`, `jazz-tools/solid`                        | Use the binding for your UI framework                              |
| Framework-free browser       | `jazz-tools/client`                                                              | Create a browser client without a framework binding                |
| Server and authentication    | `jazz-tools/backend`, `jazz-tools/better-auth-adapter`, `jazz-tools/permissions` | Create backend contexts, authenticate requests and define policies |
| Account recovery             | `jazz-tools/passphrase`, `jazz-tools/passkey-backup`                             | Back up and restore local authentication secrets                   |
| Rust application API         | `jazz::db`, `jazz::tools`                                                        | Open a database and work with schemas, queries and sessions        |
| Native bindings              | `jazz-napi`, `jazz-wasm`, `jazz-rn`                                              | Call the generated host APIs                                       |

### Choose a TypeScript entry point

- Use `jazz-tools/react` for an application-facing React API. Use
  `jazz-tools/react-core` only when you need its lower-level primitives.
- Make sure your bundler honours package export conditions. Solid, Svelte and
  React Native select different builds for different runtimes.
- Treat `jazz-tools/shared` as advanced and unstable. Its subscription-store
  types are exported implementation tools. Treat `dev`, `dev/*`, `testing`
  and `_dev/schema-hash` as development and test APIs.
- Import `jazz-tools/expo/polyfills` for its side effect. It has no named
  exports and installs `ReadableStream` when React Native does not provide it.
- The high-level React Native provider rejects persistent storage in this
  alpha. The low-level native SQLite API is a separate binding.
- Do not deep-import source-only helpers. Use only the framework helpers
  exported by the package entry points listed below.

## Tooling and extension API

Use these surfaces when you are extending Jazz, building a framework binding,
working on its runtime or maintaining development tools. They are public or
shipped in this snapshot, but they are not the default application API.

| Work                            | Entry point or export family                                                                  | Notes                                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Lower-level framework bindings  | `jazz-tools/react-core`                                                                       | Primitives used to build React integrations                                                         |
| Advanced subscription bindings  | `jazz-tools/shared`                                                                           | Unstable subscription-store tools for binding authors                                               |
| Runtime and storage integration | Root exports from `src/runtime` and `src/drivers`                                             | Driver contracts, wire types, subscription machinery and diagnostics                                |
| Schema compilation and lowering | Root exports from `src/schema.ts`, `src/ir.ts`, `src/codegen` and `src/schema-permissions.ts` | Schema ASTs, relational IR and conversion helpers                                                   |
| Permission lowering             | Low-level exports from `jazz-tools/permissions`                                               | Compiler-facing relation and policy helpers; use the high-level permission builders in applications |
| Development and testing         | `jazz-tools/dev*`, `jazz-tools/testing`, `jazz-tools/_dev/schema-hash`, `create-jazz`         | Build plugins, inspector support, test helpers and project scaffolding                              |
| Rust engine and transport       | `groove`, public Rust internals and native transport crates                                   | Storage, incremental views, protocols and server infrastructure                                     |

The complete catalogue retains these exports so the audit matches the shipped
surface. Entries marked internal/dev, deprecated, proposal-only or unavailable
are evidence of that surface, not recommendations for application code.

## Complete TypeScript API catalogue

This guide covers `jazz-tools@2.0.0-alpha.53`. Source paths are relative to the
repository. The package is an alpha. If an exported
item is marked internal, devtools or testing, use it only for that stated
purpose. Within the scope described above, the catalogue records every retained
entry rather than presenting a suggested import list. Inference-only support
types remain omitted.

### Available import paths

The package manifest exposes these paths:

- Application: `.`, `./client`, `./react`, `./react-native`, `./solid`,
  `./svelte`, `./vue`, `./expo` and `./expo/polyfills`.
- Server and policies: `./backend`, `./better-auth-adapter` and
  `./permissions`.
- Authentication recovery: `./passphrase` and `./passkey-backup`.
- Framework and binding extensions: `./react-core` and `./shared`.
- Development and testing: `./testing`, `./dev`, `./dev/next`, `./dev/vite`,
  `./dev/expo`, `./dev/sveltekit` and `./_dev/schema-hash`.
- Package metadata: `./package.json`.

The declaration and runtime files for these paths are under `dist`.

The root import also exports symbols from `src/drivers/index.ts`,
`src/runtime/index.ts` and `src/permissions/index.ts`. The package does not
provide direct `drivers` or `runtime` subpaths.

### Support status

- The root import, framework bindings, backend, authentication adapter,
  recovery and permissions paths are supported alpha APIs.
- `jazz-tools/shared` is advanced, unstable and not covered by semantic
  versioning.
- `jazz-tools/testing`, `jazz-tools/dev*` and
  `jazz-tools/_dev/schema-hash` are for development and testing, not
  production runtime code.
- `jazz-tools/expo/polyfills` has no named exports. Import it to install a React
  Native `ReadableStream` global when the runtime does not provide one.

### `jazz-tools` root exports

- **ActiveQuerySubscriptionTrace** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface ActiveQuerySubscriptionTrace
  Status: internal diagnostic data for devtools. Do not use it as application API.
  Public members:
  - **id** — packages/jazz-tools/src/runtime/db.ts
    id: string;
  - **query** — packages/jazz-tools/src/runtime/db.ts
    query: string;
  - **table** — packages/jazz-tools/src/runtime/db.ts
    table: string;
  - **branches** — packages/jazz-tools/src/runtime/db.ts
    branches: string[];
  - **tier** — packages/jazz-tools/src/runtime/db.ts
    tier: DurabilityTier;
  - **propagation** — packages/jazz-tools/src/runtime/db.ts
    propagation: QueryPropagation;
  - **createdAt** — packages/jazz-tools/src/runtime/db.ts
    createdAt: string;
  - **stack** — packages/jazz-tools/src/runtime/db.ts
    stack?: string;
- **AddOp** (interface) — packages/jazz-tools/src/schema.ts
  export interface AddOp<TSqlType extends SqlType = SqlType, TDefault = unknown>
  Purpose: Describes an `add` migration operation.
  Public members:
  - **\_type** — packages/jazz-tools/src/schema.ts
    \_type: "add";
  - **sqlType** — packages/jazz-tools/src/schema.ts
    sqlType: TSqlType;
  - **default** — packages/jazz-tools/src/schema.ts
    default: TDefault;
- **allOf** (function) — packages/jazz-tools/src/permissions/index.ts
  (conditions: readonly unknown[]): Condition
- **AllowedToContext** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface AllowedToContext
  Public members:
  - **read** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **insert** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **update** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **delete** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **readReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **insertReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **updateReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **deleteReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
- **anyOf** (function) — packages/jazz-tools/src/permissions/index.ts
  (conditions: readonly unknown[]): Condition
- **App** (type) — packages/jazz-tools/src/typed-app.ts
  export type App<TSchema extends SchemaLike> = Simplify<{
  [TTable in TableName<TSchema>]: Table<TTable, TSchema>;
  } & {
  union<TTable extends string>(relations: readonly RelationSeedQuery<TTable>[]): TypedTableQueryBuilder<any, any, any, any>;
  wasmSchema: WasmSchema;
  }>;
- **AppContext** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface AppContext
  Purpose: Configuration for connecting to Jazz.
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Application identifier (used for isolation) _/
    appId: string;
  - **clientId** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Optional client ID (generated if not provided) _/
    clientId?: string;
  - **schema** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Schema definition _/
    schema: WasmSchema;
  - **serverUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Optional server URL for sync _/
    serverUrl?: string;
  - **runtimeSources** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Optional runtime source overrides for WASM loading. _/
    runtimeSources?: RuntimeSourcesConfig;
  - **driver** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Storage driver mode (defaults to persistent). _/
    driver?: StorageDriver;
  - **env** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Environment (e.g., "dev", "prod") _/
    env?: string;
  - **userBranch** — packages/jazz-tools/src/runtime/context.ts
    /\*_ User branch name (default: "main") _/
    userBranch?: string;
  - **jwtToken** — packages/jazz-tools/src/runtime/context.ts
    // Authentication fields
    /\*\*
    - JWT token for frontend authentication.
    - Sent as \`Authorization: Bearer <token>\`.
      \*/
      jwtToken?: string;
  - **cookieSession** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Mirrored session used for local permission evaluation when auth rides on
    - an HttpOnly cookie instead of a JS-readable bearer token.
      \*/
      cookieSession?: Session;
  - **backendSecret** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Backend secret for session impersonation.
    - Enables backend session-scoped operations as any user.
      \*/
      backendSecret?: string;
  - **adminSecret** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Admin secret for privileged sync and \`/admin/\*\` catalogue endpoints.
    - On \`/ws\`, a valid admin secret authenticates this client as the backend.
      \*/
      adminSecret?: string;
  - **tier** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Durability tier identity for this node (or identities for multi-role nodes).
    - Set for server nodes to enable durability notifications.
    - Clients typically leave this undefined.
      \*/
      tier?: "local" | "edge" | "global" | Array<"local" | "edge" | "global">;
  - **defaultDurabilityTier** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Default durability tier for reads and writes when no explicit tier is provided.
      \*/
      defaultDurabilityTier?: "local" | "edge" | "global";
- **applySubscriptionDelta** (function) — packages/jazz-tools/src/runtime/subscription-manager.ts
  <T extends { id: string; }>(current: T[], delta: SubscriptionDelta<T>): T[]
  Purpose: Canonical reducer for subscription streams. Consumers own the materialized
- **AuthConfig** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface AuthConfig
  Purpose: Authentication configuration for connecting to a Jazz server.
  Public members:
  - **jwt_token** — packages/jazz-tools/src/runtime/client.ts
    /\*_ JWT bearer token for user authentication. _/
    jwt_token?: string;
  - **backend_secret** — packages/jazz-tools/src/runtime/client.ts
    /\*_ Backend service secret for server-to-server calls. _/
    backend_secret?: string;
  - **admin_secret** — packages/jazz-tools/src/runtime/client.ts
    /\*_ Admin secret for privileged sync and \`/admin/_\` catalogue operations. \*/
    admin_secret?: string;
  - **backend_session** — packages/jazz-tools/src/runtime/client.ts
    /\*_ Opaque session payload forwarded by a backend proxy. _/
    backend_session?: unknown;
- **AuthFailureReason** (type) — packages/jazz-tools/src/runtime/auth-state.ts
  export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";
- **AuthSecretStore** (interface) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export interface AuthSecretStore
  Purpose: Interface for platform-appropriate auth secret persistence.
  Public members:
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string>
- **AuthState** (interface) — packages/jazz-tools/src/runtime/auth-state.ts
  export interface AuthState
  Public members:
  - **authMode** — packages/jazz-tools/src/runtime/auth-state.ts
    authMode: AuthMode;
  - **session** — packages/jazz-tools/src/runtime/auth-state.ts
    session: Session | null;
  - **error** — packages/jazz-tools/src/runtime/auth-state.ts
    error?: AuthFailureReason;
- **BrowserAuthSecretStore** (class) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export class BrowserAuthSecretStore implements AuthSecretStore
  Purpose: AuthSecretStore backed by localStorage.
  Public members:
  - **constructor** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): BrowserAuthSecretStore
  - **getDefault** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): BrowserAuthSecretStore
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string>
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string, options?: BrowserAuthSecretStoreOptions): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<string>
- **BrowserAuthSecretStoreOptions** (interface) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export interface BrowserAuthSecretStoreOptions
  Public members:
  - **key** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ localStorage key name (default: "jazz-auth-secret") _/
    key?: string;
  - **appId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional app identifier to namespace the default key. _/
    appId?: string;
  - **userId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional principal identifier to isolate secrets per user. _/
    userId?: string | null;
  - **sessionId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional session identifier for per-session isolation. _/
    sessionId?: string | null;
  - **storage** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Override storage backend (for testing) _/
    storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
- **col** (const) — packages/jazz-tools/src/dsl.ts
  const col: { string: () => StringColumn; boolean: () => BooleanColumn; int: () => IntColumn; bigint: () => BigIntColumn; timestamp: () => TimestampColumn; float: () => FloatColumn; bytes: () => BytesColumn; json: { (): JsonColumn; <Schema extends StandardJSONSchemaV1<unknown, unknown>>(schema: Schema): JsonColumn<StandardJSONSchemaV1.InferOutput<Schema>>; (schema: JsonSchema): JsonColumn; }; enum: { <const Variants extends readonly [string, ...string[]]>(...variants: Variants): EnumColumn<Variants>; <const Cases extends Record<string, Record<string, AnyTypedColumnBuilder>>>(cases: Cases): EnumCasesColumn<{ [Name in keyof Cases & string]: { name: Name; fields: { [Field in keyof Cases[Name] & string]: Column & { name: Field; sqlType: ColumnBuilderSqlType<Cases[Name][Field]>; }; }[keyof Cases[Name] & string][]; }; }[keyof Cases & string][]>; }; ref: <const TargetTable extends string>(targetTable: TargetTable) => RefColumn<TargetTable>; array: <Builder extends AnyTypedColumnBuilder>(element: Builder) => ArrayColumn<ColumnBuilderSqlType<Builder>, false, ColumnBuilderReferences<Builder>>; add: AddBuilder; drop: DropBuilder; rename: (oldName: string) => RenameOp; renameFrom: <const TOldName extends string>(oldName: TOldName) => RenameOp<TOldName>; }
- **Column** (interface) — packages/jazz-tools/src/schema.ts
  export interface Column
  Public members:
  - **name** — packages/jazz-tools/src/schema.ts
    name: string;
  - **sqlType** — packages/jazz-tools/src/schema.ts
    sqlType: SqlType;
  - **nullable** — packages/jazz-tools/src/schema.ts
    nullable: boolean;
  - **default** — packages/jazz-tools/src/schema.ts
    default?: unknown;
  - **references** — packages/jazz-tools/src/schema.ts
    references?: string; // Target table name for foreign key
  - **mergeStrategy** — packages/jazz-tools/src/schema.ts
    mergeStrategy?: ColumnMergeStrategy;
- **ColumnDescriptor** (interface) — packages/jazz-tools/src/drivers/types.ts
  export interface ColumnDescriptor
  Public members:
  - **name** — packages/jazz-tools/src/drivers/types.ts
    name: string;
  - **column_type** — packages/jazz-tools/src/drivers/types.ts
    column_type: ColumnType;
  - **nullable** — packages/jazz-tools/src/drivers/types.ts
    nullable: boolean;
  - **sparse** — packages/jazz-tools/src/drivers/types.ts
    /\*_ Physical current-row carriers may omit this wildcard field. _/
    sparse?: boolean;
  - **default** — packages/jazz-tools/src/drivers/types.ts
    default?: Value;
  - **references** — packages/jazz-tools/src/drivers/types.ts
    references?: string;
  - **merge_strategy** — packages/jazz-tools/src/drivers/types.ts
    merge_strategy?: ColumnMergeStrategy;
- **ColumnMergeStrategy** (type) — packages/jazz-tools/src/schema.ts
  export type ColumnMergeStrategy = "counter" | "g-set";
- **ColumnMergeStrategyName** (type) — packages/jazz-tools/src/schema.ts
  export type ColumnMergeStrategyName = ColumnMergeStrategy | "lww";
- **ColumnTransform** (interface) — packages/jazz-tools/src/dsl.ts
  export interface ColumnTransform<Stored = unknown, View = unknown>
  Public members:
  - **from** — packages/jazz-tools/src/dsl.ts
    (value: Stored): View
  - **to** — packages/jazz-tools/src/dsl.ts
    (value: View): Stored
- **ColumnType** (type) — packages/jazz-tools/src/drivers/types.ts
  export type ColumnType = {
  type: "Integer";
  } | {
  type: "BigInt";
  } | {
  type: "Double";
  } | {
  type: "Boolean";
  } | {
  type: "Text";
  } | {
  type: "Json";
  schema?: Record<string, unknown>;
  } | {
  type: "Enum";
  variants: string[];
  } | {
  type: "EnumPayload";
  cases: Array<{
  name: string;
  fields: ColumnDescriptor[];
  }>;
  } | {
  type: "Timestamp";
  } | {
  type: "Uuid";
  } | {
  type: "Bytea";
  } | {
  type: "Array";
  element: ColumnType;
  } | {
  type: "Row";
  columns: ColumnDescriptor[];
  };
- **CompiledPermissions** (type) — packages/jazz-tools/src/permissions/index.ts
  export type CompiledPermissions = Record<string, TablePolicies>;
- **createDb** (function) — packages/jazz-tools/src/runtime/db.ts
  (config: DbConfig): Promise<Db>
- **InsertOptions** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface InsertOptions extends TimestampOverrideOptions
  Public members:
  - **id** — packages/jazz-tools/src/runtime/client.ts
    id?: string;
- **createSessionContext** (function — exported but marked internal/dev) — packages/jazz-tools/src/permissions/index.ts
  (): SessionContext
  Status: Internal.
- **Db** (class) — packages/jazz-tools/src/runtime/db.ts
  export class Db
  Purpose: High-level database interface for typed queries and mutations.
  Public members:
  - **initLocalFirstAuth** — packages/jazz-tools/src/runtime/db.ts
    (seed: string, ttlSeconds: number, refresh?: boolean): void
  - **create** — packages/jazz-tools/src/runtime/db.ts
    (config: DbConfig, runtimeSource: AnyRuntimeSource): Db
    Internal factory; use the exported `createDb(config)` entrypoint instead.
  - **createWithBrowserWorker** — packages/jazz-tools/src/runtime/db.ts
    (config: DbConfig, runtimeSource: AnyRuntimeSource): Promise<Db>
    `@internal`, browser-only worker construction; not a backend/server setup method.
  - **updateAuthToken** — packages/jazz-tools/src/runtime/db.ts
    (jwtToken: string | null): void
  - **updateCookieSession** — packages/jazz-tools/src/runtime/db.ts
    (cookieSession: Session | null): void
  - **getAuthState** — packages/jazz-tools/src/runtime/db.ts
    (): AuthState
  - **getLocalFirstIdentityProof** — packages/jazz-tools/src/runtime/db.ts
    (options?: { ttlSeconds?: number; audience?: string; }): string | null
  - **onAuthChanged** — packages/jazz-tools/src/runtime/db.ts
    (listener: (state: AuthState) => void): () => void
  - **onMutationError** — packages/jazz-tools/src/runtime/db.ts
    (listener: (event: MutationErrorEvent) => void): () => void
  - **getConfig** — packages/jazz-tools/src/runtime/db.ts
    (): DbConfig
  - **setDevMode** — packages/jazz-tools/src/runtime/db.ts
    (enabled: boolean): void
  - **disconnect** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
  - **reconnect** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
  - **getActiveQuerySubscriptions** — packages/jazz-tools/src/runtime/db.ts
    (): ActiveQuerySubscriptionTrace[]
    `@internal`, devtools/inspector diagnostics only.
  - **onActiveQuerySubscriptionsChange** — packages/jazz-tools/src/runtime/db.ts
    (listener: ActiveQuerySubscriptionTraceListener): () => void
    `@internal`, devtools/inspector diagnostics only.
  - **getRuntimeSchema** — packages/jazz-tools/src/runtime/db.ts
    (): WasmSchema | null
    Devtools/inspector runtime-schema accessor; not a general schema API.
  - **insert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): WriteResult<T>
  - **restore** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Init, options?: RestoreOptions): WriteResult<T>
  - **upsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): WriteHandle
  - **update** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): WriteHandle
  - **delete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, options?: DeleteOptions): WriteHandle
  - **canInsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionAdvice>
  - **canRead** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice>
  - **canUpdate** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): Promise<PermissionAdvice>
  - **canDelete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice>
  - **beginTransaction** — packages/jazz-tools/src/runtime/db.ts
    (): Transaction<"mergeable">
  - **beginExclusiveTransaction** — packages/jazz-tools/src/runtime/db.ts
    (): Transaction<"exclusive">
  - **transaction** — packages/jazz-tools/src/runtime/db.ts
    <TResult>(callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>): Promise<WriteResult<Awaited<TResult>>>
  - **exclusiveTransaction** — packages/jazz-tools/src/runtime/db.ts
    <TResult>(callback: (tx: TransactionScope<"exclusive">) => TResult | Promise<TResult>): Promise<ExclusiveWriteResult<Awaited<TResult>>>
  - **deleteClientStorage** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
    Browser-only OPFS/local-storage maintenance; not server-runtime setup.
  - **logout** — packages/jazz-tools/src/runtime/db.ts
    (options?: LogoutOptions): Promise<void>
  - **all** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>
  - **one** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>
  - **subscribeAll** — packages/jazz-tools/src/runtime/db.ts
    <T extends { id: string; }>(query: QueryBuilder<T>, callback: (delta: SubscriptionDelta<T>) => void, options?: QueryOptions, session?: Session): () => void
  - **shutdown** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
- **DbConfig** (type) — packages/jazz-tools/src/runtime/db.ts
  export type DbConfig = BaseDbConfig & (
  | { secret?: string; jwtToken?: never; cookieSession?: never }
  | { secret?: never; jwtToken?: string; cookieSession?: never }
  | { secret?: never; jwtToken?: never; cookieSession?: Session }
  );
  Purpose: Configuration for creating a Db instance. `secret`, `jwtToken` and
  `cookieSession` are mutually exclusive authentication modes.
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Application identifier (used for isolation) _/
    appId: string;
  - **driver** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Storage driver mode (defaults to persistent). _/
    driver?: StorageDriver;
  - **serverUrl** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional server URL for sync _/
    serverUrl?: string;
  - **runtimeSources** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional runtime source overrides for WASM loading. _/
    runtimeSources?: RuntimeSourcesConfig;
  - **env** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Environment (e.g., "dev", "prod") _/
    env?: string;
  - **userBranch** — packages/jazz-tools/src/runtime/db.ts
    /\*_ User branch name (default: "main") _/
    userBranch?: string;
  - **jwtToken** — packages/jazz-tools/src/runtime/db.ts
    /\*_ JWT token for server authentication _/
    jwtToken?: string;
  - **cookieSession** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Mirrored session for local permission evaluation when sync auth uses cookies. _/
    cookieSession?: Session;
  - **adminSecret** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Admin secret for catalogue sync _/
    adminSecret?: string;
  - **backendSecret** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Backend secret for backend-scoped sync auth with cookieSession. _/
    backendSecret?: string;
  - **dbName** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Database name for OPFS persistence (browser only, default: appId) _/
    dbName?: string;
  - **initialSyncFlushEvery** — packages/jazz-tools/src/runtime/db.ts
    /\*\*
    - Initial-sync durability boundary, in writes (default: 512 for clients).
    - A crash can lose up to M - 1 writes since the previous boundary; older
    - boundaries recover from the storage WAL.
      \*/
      initialSyncFlushEvery?: number;
  - **logLevel** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional WASM tracing level for benchmark/debug scenarios (default: "warn"). _/
    logLevel?: WasmLogLevel;
  - **telemetryCollectorUrl** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional OTLP/HTTP collector URL for WASM trace telemetry. _/
    telemetryCollectorUrl?: string;
  - **devMode** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Enable runtime tracing for DevTools-only diagnostics. _/
    devMode?: boolean;
  - **secret** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Local-first auth via a local seed. _/
    secret?: string;
- **defineApp** (function) — packages/jazz-tools/src/typed-app.ts
  <const TSchema extends Schema<any>>(definition: TSchema): App<TSchema>
  Purpose: Creates a typed app from a schema definition.
- **DefinedMigration** (interface) — packages/jazz-tools/src/migrations.ts
  export interface DefinedMigration<
  TFrom extends SchemaLike = SchemaLike,
  TTo extends SchemaLike = SchemaLike,
  > Public members:
  - **from** — packages/jazz-tools/src/migrations.ts
    readonly from: TFrom;
  - **to** — packages/jazz-tools/src/migrations.ts
    readonly to: TTo;
  - **forward** — packages/jazz-tools/src/migrations.ts
    readonly forward: Lens[];
- **DefinedSchema** (type) — packages/jazz-tools/src/typed-app.ts
  export type DefinedSchema<TSchema extends SchemaDefinition = SchemaDefinition> = Schema<TSchema>;
- **DefinedTable** (class) — packages/jazz-tools/src/typed-app.ts
  export class DefinedTable<TColumns extends TableDefinition = TableDefinition>
  Purpose: Adds table modifiers such as `indexOnly(...)` without changing the
  runtime column schema.
  Public members:
  - **\_\_jazzTableDefinition** — packages/jazz-tools/src/typed-app.ts
    public readonly \_\_jazzTableDefinition = true as const;
  - **constructor** — packages/jazz-tools/src/typed-app.ts
    <TColumns extends TableDefinition = TableDefinition>(columns: TColumns, indexedColumns?: readonly Extract<keyof TColumns, string>[] | undefined): DefinedTable<TColumns>
  - **indexOnly** — packages/jazz-tools/src/typed-app.ts
    <const TColumnsForIndex extends readonly [Extract<keyof TColumns, string>, ...Extract<keyof TColumns, string>[]]>(columns: TColumnsForIndex): DefinedTable<TColumns>
- **defineMigration** (function) — packages/jazz-tools/src/migrations.ts
  <const TFrom extends SchemaLike, const TTo extends SchemaLike, const TRenameTables extends RenameTableShape<TFrom, TTo> | undefined = undefined, const TCreateTables extends AddedTableShape<TFrom, TTo> | undefined = undefined, const TDropTables extends RemovedTableShape<TFrom, TTo> | undefined = undefined, const TMigrate extends MigrationShape<TFrom, TTo, TRenameTables> | undefined = undefined>(config: { fromHash?: string; toHash?: string; from: TFrom; to: TTo; renameTables?: TRenameTables; createTables?: TCreateTables; dropTables?: TDropTables; migrate?: TMigrate; } & ValidateMigrationConfig<TFrom, TTo, TRenameTables, TCreateTables, TDropTables, TMigrate>): DefinedMigration<TFrom, TTo>
  Purpose: Defines a migration between two schemas.
- **definePermissions** (function) — packages/jazz-tools/src/permissions/index.ts
  <TApp extends AppLike>(app: TApp, factory: (ctx: PolicyContext<TApp>) => void): CompiledPermissions
- **defineSchema** (function) — packages/jazz-tools/src/typed-app.ts
  <const TSchema extends SchemaDefinition>(definition: TSchema & ValidateSchemaRefs<TSchema>): Schema<TSchema>
- **defineSliceableApp** (function) — packages/jazz-tools/src/typed-app.ts
  <const TSchema extends Schema<any>>(definition: TSchema): SliceableApp<TSchema>
  Purpose: Defines an app that can expose schema slices.
- **DropOp** (interface) — packages/jazz-tools/src/schema.ts
  export interface DropOp<TSqlType extends SqlType = SqlType, TBackwardsDefault = unknown>
  Public members:
  - **\_type** — packages/jazz-tools/src/schema.ts
    \_type: "drop";
  - **sqlType** — packages/jazz-tools/src/schema.ts
    sqlType: TSqlType;
  - **backwardsDefault** — packages/jazz-tools/src/schema.ts
    backwardsDefault: TBackwardsDefault;
- **DurabilityTier** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Persistence tier for durability guarantees.
  -
  - - \`local\`: Persisted in local durable storage
  - - \`edge\`: Persisted at edge server
  - - \`global\`: Persisted at global server
      \*/
      export type DurabilityTier = "local" | "edge" | "global";
      Purpose: Persistence tier for durability guarantees.
- **fetchSchemaHashes** (function) — packages/jazz-tools/src/runtime/schema-fetch.ts
  (serverUrl: string, options: FetchStoredSchemasOptions): Promise<{ hashes: string[]; schemas: StoredSchemaHash[]; }>
- **fetchServerSubscriptions** (function) — packages/jazz-tools/src/runtime/introspection-fetch.ts
  (serverUrl: string, options: FetchServerSubscriptionsOptions): Promise<IntrospectionSubscriptionResponse>
- **FetchServerSubscriptionsOptions** (interface) — packages/jazz-tools/src/runtime/introspection-fetch.ts
  export interface FetchServerSubscriptionsOptions
  Public members:
  - **adminSecret** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    adminSecret: string;
  - **appId** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    appId: string;
- **fetchStoredPermissions** (function) — packages/jazz-tools/src/runtime/schema-fetch.ts
  (serverUrl: string, options: FetchStoredPermissionsOptions): Promise<StoredPermissionsResponse>
- **FetchStoredPermissionsOptions** (interface) — packages/jazz-tools/src/runtime/schema-fetch.ts
  export interface FetchStoredPermissionsOptions
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/schema-fetch.ts
    appId: string;
  - **adminSecret** — packages/jazz-tools/src/runtime/schema-fetch.ts
    adminSecret: string;
- **fetchStoredWasmSchema** (function) — packages/jazz-tools/src/runtime/schema-fetch.ts
  (serverUrl: string, options: FetchStoredWasmSchemaOptions): Promise<{ schema: WasmSchema; publishedAt: number | null; }>
- **FetchStoredWasmSchemaOptions** (interface) — packages/jazz-tools/src/runtime/schema-fetch.ts
  export interface FetchStoredWasmSchemaOptions
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/schema-fetch.ts
    appId: string;
  - **adminSecret** — packages/jazz-tools/src/runtime/schema-fetch.ts
    adminSecret: string;
  - **schemaHash** — packages/jazz-tools/src/runtime/schema-fetch.ts
    schemaHash: string;
- **generateAuthSecret** (function) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  (): string
  Purpose: Generate a new 32-byte auth secret as a base64url string.
- **getCollectedMigration** (function) — packages/jazz-tools/src/dsl.ts
  (): Lens | null
- **getCollectedSchema** (function) — packages/jazz-tools/src/dsl.ts
  (): Schema
- **getSupportedWhereOperatorsForColumn** (function) — packages/jazz-tools/src/where-operators.ts
  (column: WhereOperatorColumn): WhereOperator[]
- **getSupportedWhereOperatorsForSchemaColumn** (function) — packages/jazz-tools/src/where-operators.ts
  (fieldName: string, column: ColumnDescriptor | undefined): WhereOperator[] | undefined
- **InsertOf** (type) — packages/jazz-tools/src/typed-app.ts
  export type InsertOf<TTable> = TTable extends {
  readonly \_initType: infer TInit;
  } ? TInit : never;
- **InsertValues** (type) — packages/jazz-tools/src/drivers/types.ts
  export type InsertValues = Record<string, Value>;
- **INSPECTOR_HOST_GLOBAL** (const — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
  const INSPECTOR_HOST_GLOBAL: "\_\_jazzInspectorHost"
- **INSPECTOR_SUBSCRIPTIONS_MESSAGE** (const — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
  const INSPECTOR_SUBSCRIPTIONS_MESSAGE: "jazz-inspector:subscriptions"
- **InspectorHostDb** (type — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/host-bridge.ts
  export type InspectorHostDb = Db;
- **InspectorSubscription** (type — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
  /\*_ Active subscription as sent to the overlay — the trace minus the JS stack. _/
  export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;
  Purpose: Active subscription as sent to the overlay — the trace minus the JS stack.
- **InspectorSubscriptionsMessage** (interface — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
  export interface InspectorSubscriptionsMessage
  Public members:
  - **type** — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
    type: typeof INSPECTOR_SUBSCRIPTIONS_MESSAGE;
  - **list** — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
    list: InspectorSubscription[];
- **installInspectorHost** (function — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/host-bridge.ts
  (db: Db, iframeWindow: Window, origin: string): () => void
  Purpose: Publish the same-origin host handle and the one-way active-subscription feed.
- **IntrospectionSubscriptionGroup** (interface) — packages/jazz-tools/src/runtime/introspection-fetch.ts
  export interface IntrospectionSubscriptionGroup
  Public members:
  - **groupKey** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    groupKey: string;
  - **count** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    count: number;
  - **table** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    table: string;
  - **query** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    query: string;
  - **branches** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    branches: string[];
  - **propagation** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    propagation: QueryPropagation;
- **IntrospectionSubscriptionResponse** (interface) — packages/jazz-tools/src/runtime/introspection-fetch.ts
  export interface IntrospectionSubscriptionResponse
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    appId: string;
  - **generatedAt** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    generatedAt: number;
  - **queries** — packages/jazz-tools/src/runtime/introspection-fetch.ts
    queries: IntrospectionSubscriptionGroup[];
- **JazzInspectorHost** (interface — exported but marked internal/dev) — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
  export interface JazzInspectorHost
  Purpose: Read-once handle the host publishes on `window` for the same-origin overlay.
  Public members:
  - **getConnectionConfig** — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
    (): DbConfig
  - **getWasmSchema** — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
    (): WasmSchema
  - **getActiveSubscriptions** — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
    (): InspectorSubscription[]
- **JsonSchema** (type) — packages/jazz-tools/src/schema.ts
  export type JsonSchema = Exclude<JSONSchema, boolean>;
- **JsonValue** (type) — packages/jazz-tools/src/schema.ts
  export type JsonValue = JsonPrimitive | {
  [key: string]: JsonValue;
  } | JsonValue[];
- **Lens** (type) — packages/jazz-tools/src/schema.ts
  export type Lens = TableLens;
- **LensOp** (type) — packages/jazz-tools/src/schema.ts
  export type LensOp = IntroduceLensOp | DropLensOp | RenameLensOp;
- **LensOpType** (type) — packages/jazz-tools/src/schema.ts
  export type LensOpType = LensOp["type"];
- **loadWasmModule** (function) — packages/jazz-tools/src/runtime/client.ts
  (runtime?: RuntimeSourcesConfig): Promise<WasmModule>
  Purpose: Load and initialize the WASM module.
- **LocalTransactionRecord** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface LocalTransactionRecord
  Public members:
  - **batchId** — packages/jazz-tools/src/runtime/client.ts
    batchId: BatchId;
  - **kind** — packages/jazz-tools/src/runtime/client.ts
    kind: TransactionKind;
  - **sealed** — packages/jazz-tools/src/runtime/client.ts
    sealed: boolean;
  - **latestSettlement** — packages/jazz-tools/src/runtime/client.ts
    latestSettlement: TransactionFate | null;
  - **encodedRecord** — packages/jazz-tools/src/runtime/client.ts
    encodedRecord?: Uint8Array;
- **LocalUpdatesMode** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Controls when a write is visible to subscriptions.
  -
  - - With \`"immediate"\`, your own local writes appear in the subscription while it's still waiting for
  - the tier to confirm the initial snapshot (only once the subscription has settled at least once).
  - - With \`"deferred"\`, all delivery is held until the tier confirms.
  - Default is \`"immediate"\`.
    \*/
    export type LocalUpdatesMode = "immediate" | "deferred";
    Purpose: Controls when a write is visible to subscriptions.
- **LogoutOptions** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface LogoutOptions
  Public members:
  - **wipeData** — packages/jazz-tools/src/runtime/db.ts
    wipeData?: boolean;
- **migrate** (function) — packages/jazz-tools/src/dsl.ts
  (tableName: string, ops: Record<string, MigrationOp>): void
- **MigrationOp** (type) — packages/jazz-tools/src/schema.ts
  export type MigrationOp = AddOp | DropOp | RenameOp;
- **MutationErrorEvent** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface MutationErrorEvent
  Purpose: Describes a rejected write emitted by `JazzClient.onMutationError`.
  Public members:
  - **code** — packages/jazz-tools/src/runtime/client.ts
    code: string;
  - **reason** — packages/jazz-tools/src/runtime/client.ts
    reason: string;
  - **transaction** — packages/jazz-tools/src/runtime/client.ts
    transaction: LocalTransactionRecord;
- **NativeRowDelta** (interface) — packages/jazz-tools/src/drivers/types.ts
  export interface NativeRowDelta
  Public members:
  - **\_\_jazzNativeRowDelta** — packages/jazz-tools/src/drivers/types.ts
    \_\_jazzNativeRowDelta: true;
  - **reset** — packages/jazz-tools/src/drivers/types.ts
    reset?: boolean;
  - **added** — packages/jazz-tools/src/drivers/types.ts
    added: Uint8Array;
  - **removed** — packages/jazz-tools/src/drivers/types.ts
    removed: Uint8Array;
  - **updated** — packages/jazz-tools/src/drivers/types.ts
    updated: Uint8Array;
  - **addedCount** — packages/jazz-tools/src/drivers/types.ts
    addedCount: number;
  - **removedCount** — packages/jazz-tools/src/drivers/types.ts
    removedCount: number;
  - **updatedCount** — packages/jazz-tools/src/drivers/types.ts
    updatedCount: number;
  - **addedOccurrenceKeys** — packages/jazz-tools/src/drivers/types.ts
    addedOccurrenceKeys?: Uint8Array[];
  - **updatedOccurrenceKeys** — packages/jazz-tools/src/drivers/types.ts
    updatedOccurrenceKeys?: Uint8Array[];
  - **removedOccurrenceKeys** — packages/jazz-tools/src/drivers/types.ts
    removedOccurrenceKeys?: Uint8Array[];
  - **terminalLayouts** — packages/jazz-tools/src/drivers/types.ts
    terminalLayouts?: NativeTerminalRootLayout[];
  - **terminalOperations** — packages/jazz-tools/src/drivers/types.ts
    terminalOperations?: NativeTerminalOperation[];
- **OperationPolicy** (interface) — packages/jazz-tools/src/schema.ts
  export interface OperationPolicy
  Public members:
  - **using** — packages/jazz-tools/src/schema.ts
    using?: PolicyExpr;
  - **with_check** — packages/jazz-tools/src/schema.ts
    with_check?: PolicyExpr;
- **PermissionAdvice** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Advisory result for a permission preflight. \`allowed\` and \`denied\` are
  - final only when a trusted-serving authority evaluated the request;
  - \`unknown\` means that a local replica or unavailable authority cannot decide.
    \*/
    export type PermissionAdvice = "allowed" | "denied" | "unknown";
    Purpose: Advisory result for a permission preflight. `allowed` and `denied` are
- **PermissionRelation** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface PermissionRelation
  Public members:
  - **where** — packages/jazz-tools/src/permissions/index.ts
    (input: unknown): PermissionRelation
  - **join** — packages/jazz-tools/src/permissions/index.ts
    (target: RelationJoinTarget, on: { left: string; right: string; }): PermissionRelation
  - **select** — packages/jazz-tools/src/permissions/index.ts
    (columns: Record<string, string>): PermissionRelation
  - **hopTo** — packages/jazz-tools/src/permissions/index.ts
    (relation: string): PermissionRelation
  - **gather** — packages/jazz-tools/src/permissions/index.ts
    (options: { start?: Record<string, unknown> | PermissionRelation; step: (ctx: { current: RecursiveCurrentValue; }) => PermissionRelation; maxDepth?: number; }): PermissionRelation
  - **reachable_via** — packages/jazz-tools/src/permissions/index.ts
    (access_table: string, access_row_column: string, access_team_column: string, from: SessionRefValue, edge_table: string, edge_member_column: string, edge_parent_column: string, edge_filters?: Record<string, unknown>): ReachableSeedBuilder
  - **reachable_via_with_access_filters** — packages/jazz-tools/src/permissions/index.ts
    (access_table: string, access_row_column: string, access_team_column: string, from: SessionRefValue, access_filters: Record<string, unknown>, edge_table: string, edge_member_column: string, edge_parent_column: string, edge_filters?: Record<string, unknown>): ReachableSeedBuilder
- **PersistedWriteRejectedError** (class) — packages/jazz-tools/src/runtime/client.ts
  export class PersistedWriteRejectedError extends Error
  Purpose: Error returned when a write fails to be persisted at a given durability tier.
  Public members:
  - **name** — packages/jazz-tools/src/runtime/client.ts
    readonly name = "PersistedWriteRejectedError";
  - **constructor** — packages/jazz-tools/src/runtime/client.ts
    (batchId: BatchId, code: string, reason: string): PersistedWriteRejectedError
- **PolicyCmpOp** (type) — packages/jazz-tools/src/schema.ts
  export type PolicyCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";
- **PolicyContext** (type) — packages/jazz-tools/src/permissions/index.ts
  export type PolicyContext<TApp extends AppLike> = {
  policy: {
  [K in TableKey<TApp>]: TablePolicyBuilder<WhereFor<QueryBuilderFor<TApp, K>>, RowFor<QueryBuilderFor<TApp, K>>>;
  } & {
  exists(relation: PermissionRelation): ExistsRelationCondition;
  union(relations: readonly PermissionRelation[]): PermissionRelation;
  };
  anyOf: (conditions: readonly unknown[]) => Condition;
  allOf: (conditions: readonly unknown[]) => Condition;
  isCreator: Condition;
  allowedTo: AllowedToContext;
  session: SessionContext;
  };
- **PolicyExpr** (type) — packages/jazz-tools/src/schema.ts
  export type PolicyExpr = {
  type: "Cmp";
  column: string;
  op: PolicyCmpOp;
  value: PolicyValue;
  } | {
  type: "SessionCmp";
  path: string[];
  op: PolicyCmpOp;
  value: PolicyLiteralValue;
  } | {
  type: "IsNull";
  column: string;
  } | {
  type: "SessionIsNull";
  path: string[];
  } | {
  type: "IsNotNull";
  column: string;
  } | {
  type: "SessionIsNotNull";
  path: string[];
  } | {
  type: "Contains";
  column: string;
  value: PolicyValue;
  } | {
  type: "SessionContains";
  path: string[];
  value: PolicyLiteralValue;
  } | {
  type: "In";
  column: string;
  session_path: string[];
  } | {
  type: "InList";
  column: string;
  values: PolicyValue[];
  } | {
  type: "SessionInList";
  path: string[];
  values: PolicyLiteralValue[];
  } | {
  type: "Exists";
  table: string;
  condition: PolicyExpr;
  } | {
  type: "ExistsRel";
  rel: RelExpr;
  } | {
  type: "Inherits";
  operation: PolicyOperation;
  via_column: string;
  max_depth?: number;
  } | {
  type: "InheritsReferencing";
  operation: PolicyOperation;
  source_table: string;
  via_column: string;
  max_depth?: number;
  } | {
  type: "And";
  exprs: PolicyExpr[];
  } | {
  type: "Or";
  exprs: PolicyExpr[];
  } | {
  type: "Not";
  expr: PolicyExpr;
  } | {
  type: "True";
  } | {
  type: "False";
  };
- **PolicyExprV2** (type) — packages/jazz-tools/src/ir.ts
  export type PolicyExprV2 = {
  Predicate: RelPredicateExpr;
  } | {
  ExistsRel: {
  rel: RelExpr;
  };
  } | {
  Inherits: {
  operation: PolicyOperationV2;
  via_column: string;
  max_depth?: number;
  };
  } | {
  And: PolicyExprV2[];
  } | {
  Or: PolicyExprV2[];
  } | {
  Not: PolicyExprV2;
  } | "True" | "False";
- **PolicyOperation** (type) — packages/jazz-tools/src/schema.ts
  export type PolicyOperation = "Select" | "Insert" | "Update" | "Delete";
- **PolicyOperationV2** (type) — packages/jazz-tools/src/ir.ts
  export type PolicyOperationV2 = "Select" | "Insert" | "Update" | "Delete";
- **PolicyValue** (type) — packages/jazz-tools/src/schema.ts
  export type PolicyValue = {
  type: "Literal";
  value: unknown;
  } | {
  type: "SessionRef";
  path: string[];
  };
- **publishStoredPermissions** (function) — packages/jazz-tools/src/runtime/schema-fetch.ts
  (serverUrl: string, options: PublishStoredPermissionsOptions): Promise<{ head: StoredPermissionsHead | null; }>
- **PublishStoredPermissionsOptions** (interface) — packages/jazz-tools/src/runtime/schema-fetch.ts
  export interface PublishStoredPermissionsOptions
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/schema-fetch.ts
    appId: string;
  - **adminSecret** — packages/jazz-tools/src/runtime/schema-fetch.ts
    adminSecret: string;
  - **schemaHash** — packages/jazz-tools/src/runtime/schema-fetch.ts
    schemaHash: string;
  - **permissions** — packages/jazz-tools/src/runtime/schema-fetch.ts
    permissions: CompiledPermissionsMap;
  - **expectedParentBundleObjectId** — packages/jazz-tools/src/runtime/schema-fetch.ts
    expectedParentBundleObjectId?: string | null;
- **publishStoredSchema** (function) — packages/jazz-tools/src/runtime/schema-fetch.ts
  (serverUrl: string, options: PublishStoredSchemaOptions): Promise<{ objectId: string; hash: string; }>
- **PublishStoredSchemaOptions** (interface) — packages/jazz-tools/src/runtime/schema-fetch.ts
  export interface PublishStoredSchemaOptions
  Public members:
  - **appId** — packages/jazz-tools/src/runtime/schema-fetch.ts
    appId: string;
  - **adminSecret** — packages/jazz-tools/src/runtime/schema-fetch.ts
    adminSecret: string;
  - **schema** — packages/jazz-tools/src/runtime/schema-fetch.ts
    schema: WasmSchema;
- **Query** (interface) — packages/jazz-tools/src/typed-app.ts
  export interface Query<
  TTable extends string,
  TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {},
  TSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>> = any,
  TSchema extends SchemaLike = SchemaLike,
  TRequired extends boolean = false,
  > extends TypedTableQueryBuilder<SchemaMeta<TTable, TSchema>, TInclude, TSelection, TRequired>
  > Public members:
  - **where** — packages/jazz-tools/src/typed-app.ts
    (conditions: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>): Query<TTable, TInclude, TSelection, TSchema, TRequired>
  - **select** — packages/jazz-tools/src/typed-app.ts
    <NewSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>>>(columns_0: NewSelection, ...columns: NewSelection[]): Query<TTable, TInclude, NewSelection, TSchema, TRequired>
  - **include** — packages/jazz-tools/src/typed-app.ts
    <NewInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>>>(relations: NewInclude): Query<TTable, TInclude & NewInclude, TSelection, TSchema, TRequired>
  - **requireIncludes** — packages/jazz-tools/src/typed-app.ts
    (): Query<TTable, TInclude, TSelection, TSchema, true>
  - **orderBy** — packages/jazz-tools/src/typed-app.ts
    (column: TableOrderableFromMeta<SchemaMeta<TTable, TSchema>>, direction?: "asc" | "desc"): Query<TTable, TInclude, TSelection, TSchema, TRequired>
  - **limit** — packages/jazz-tools/src/typed-app.ts
    (n: number): Query<TTable, TInclude, TSelection, TSchema, TRequired>
  - **offset** — packages/jazz-tools/src/typed-app.ts
    (n: number): Query<TTable, TInclude, TSelection, TSchema, TRequired>
  - **includeDeleted** — packages/jazz-tools/src/typed-app.ts
    (): Query<TTable, TInclude, TSelection, TSchema, TRequired>
  - **hopTo** — packages/jazz-tools/src/typed-app.ts
    <TRelation extends RelationNameFromMeta<SchemaMeta<TTable, TSchema>>>(relation: TRelation): Query<RelationTargetFromMeta<SchemaMeta<TTable, TSchema>, TRelation>["name"], {}, DefaultTableSelection<RelationTargetFromMeta<SchemaMeta<TTable, TSchema>, TRelation>>, TSchema, TRequired>
  - **gather** — packages/jazz-tools/src/typed-app.ts
    (options: { start?: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>; step: (ctx: { current: string; }) => QueryBuilder<unknown>; maxDepth?: number; }): Query<TTable, TInclude, TSelection, TSchema, TRequired>
- **QueryBuilder** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface QueryBuilder<T>
  Purpose: Interface that QueryBuilder classes implement.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name for this query _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference for translation and transformation _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed query handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_build** — packages/jazz-tools/src/runtime/db.ts
    (): string
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
- **QueryExecutionOptions** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface QueryExecutionOptions
  Public members:
  - **tier** — packages/jazz-tools/src/runtime/client.ts
    tier?: DurabilityTier;
  - **localUpdates** — packages/jazz-tools/src/runtime/client.ts
    localUpdates?: LocalUpdatesMode;
  - **propagation** — packages/jazz-tools/src/runtime/client.ts
    propagation?: QueryPropagation;
  - **visibility** — packages/jazz-tools/src/runtime/client.ts
    visibility?: QueryVisibility;
- **QueryHandle** (type) — packages/jazz-tools/src/typed-app.ts
  export type QueryHandle<TTable extends string, TSchema extends SchemaLike, TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {}, TSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>> = DefaultTableSelection<SchemaMeta<TTable, TSchema>>> = Query<TTable, TInclude, TSelection, TSchema>;
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **QueryPropagation** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Controls where the subscription reads data from.
  -
  - - With \`"full"\`, the subscription is sent to upstream servers, which push matching data back.
  - - With \`"local-only"\`, only local storage is queried and no server communication happens.
      \*/
      export type QueryPropagation = "full" | "local-only";
      Purpose: Controls where the subscription reads data from.
- **QueryVisibility** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Whether this query should be shown in the inspector.
  - Useful for helpers and framework internals that create subscriptions
  - but should stay out of the DB inspector.
  - Defaults to \`"public"\`.
    \*/
    export type QueryVisibility = "public" | "hidden_from_live_query_list";
    Purpose: Whether this query should be shown in the inspector.
- **ReachableSeedBuilder** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface ReachableSeedBuilder
  Public members:
  - **seeded_by** — packages/jazz-tools/src/permissions/index.ts
    (seed_table: string, user_column: string, claim_path: string, team_column: string): PermissionRelation
- **relationExistsToPolicy** (function) — packages/jazz-tools/src/permissions/index.ts
  (relation: PermissionRelation): PolicyExpr
- **relationToIr** (function) — packages/jazz-tools/src/permissions/index.ts
  (relation: PermissionRelation): RelExpr
- **RelColumnRef** (type) — packages/jazz-tools/src/ir.ts
  export type RelColumnRef = {
  scope?: string;
  column: string;
  };
- **RelExpr** (type) — packages/jazz-tools/src/ir.ts
  export type RelExpr = {
  TableScan: {
  table: string;
  alias?: string;
  };
  } | {
  Filter: {
  input: RelExpr;
  predicate: RelPredicateExpr;
  };
  } | {
  Union: {
  inputs: RelExpr[];
  };
  } | {
  Join: {
  left: RelExpr;
  right: RelExpr;
  on: RelJoinCondition[];
  join_kind: RelJoinKind;
  };
  } | {
  Project: {
  input: RelExpr;
  columns: RelProjectColumn[];
  };
  } | {
  Gather: {
  seed: RelExpr;
  step: RelExpr;
  frontier_key: RelKeyRef;
  bound: RelRecursionBound;
  dedupe_key: RelKeyRef[];
  };
  } | {
  Distinct: {
  input: RelExpr;
  key: RelKeyRef[];
  };
  } | {
  OrderBy: {
  input: RelExpr;
  terms: RelOrderByExpr[];
  };
  } | {
  Offset: {
  input: RelExpr;
  offset: number;
  };
  } | {
  Limit: {
  input: RelExpr;
  limit: number;
  };
  };
- **RelJoinCondition** (type) — packages/jazz-tools/src/ir.ts
  export type RelJoinCondition = {
  left: RelColumnRef;
  right: RelColumnRef;
  };
- **RelJoinKind** (type) — packages/jazz-tools/src/ir.ts
  export type RelJoinKind = "Inner" | "Left";
- **RelKeyRef** (type) — packages/jazz-tools/src/ir.ts
  export type RelKeyRef = {
  Column: RelColumnRef;
  } | {
  RowId: RelRowIdRef;
  };
- **RelOrderByExpr** (type) — packages/jazz-tools/src/ir.ts
  export type RelOrderByExpr = {
  column: RelColumnRef;
  direction: RelOrderDirection;
  };
- **RelOrderDirection** (type) — packages/jazz-tools/src/ir.ts
  export type RelOrderDirection = "Asc" | "Desc";
- **RelPredicateCmpOp** (type) — packages/jazz-tools/src/ir.ts
  export type RelPredicateCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";
- **RelPredicateExpr** (type) — packages/jazz-tools/src/ir.ts
  export type RelPredicateExpr = {
  Cmp: {
  left: RelColumnRef;
  op: RelPredicateCmpOp;
  right: RelValueRef;
  };
  } | {
  IsNull: {
  column: RelColumnRef;
  };
  } | {
  IsNotNull: {
  column: RelColumnRef;
  };
  } | {
  In: {
  left: RelColumnRef;
  values: RelValueRef[];
  };
  } | {
  Contains: {
  left: RelColumnRef;
  right: RelValueRef;
  };
  } | {
  EnumMatch: {
  column: RelColumnRef;
  case: string;
  payload: RelPredicateExpr;
  };
  } | {
  And: RelPredicateExpr[];
  } | {
  Or: RelPredicateExpr[];
  } | {
  Not: RelPredicateExpr;
  } | "True" | "False";
- **RelProjectColumn** (type) — packages/jazz-tools/src/ir.ts
  export type RelProjectColumn = {
  alias: string;
  expr: RelProjectExpr;
  };
- **RelProjectExpr** (type) — packages/jazz-tools/src/ir.ts
  export type RelProjectExpr = {
  Column: RelColumnRef;
  } | {
  RowId: RelRowIdRef;
  };
- **RelRowIdRef** (type) — packages/jazz-tools/src/ir.ts
  export type RelRowIdRef = "Current" | "Outer" | "Frontier";
- **RelValueRef** (type) — packages/jazz-tools/src/ir.ts
  export type RelValueRef = {
  Literal: unknown;
  } | {
  Param: string;
  } | {
  SessionRef: string[];
  } | {
  OuterColumn: RelColumnRef;
  } | {
  FrontierColumn: RelColumnRef;
  } | {
  RowId: RelRowIdRef;
  };
- **RenameOp** (interface) — packages/jazz-tools/src/schema.ts
  export interface RenameOp<TOldName extends string = string>
  Public members:
  - **\_type** — packages/jazz-tools/src/schema.ts
    \_type: "rename";
  - **oldName** — packages/jazz-tools/src/schema.ts
    oldName: TOldName;
- **renameTableFrom** (function) — packages/jazz-tools/src/migrations.ts
  <const TOldName extends string>(oldName: TOldName): RenameTableFromOp<TOldName>
- **RenameTableFromOp** (interface) — packages/jazz-tools/src/schema.ts
  export interface RenameTableFromOp<TOldName extends string = string>
  Public members:
  - **\_type** — packages/jazz-tools/src/schema.ts
    \_type: "renameTable";
  - **oldName** — packages/jazz-tools/src/schema.ts
    oldName: TOldName;
- **resetCollectedState** (function) — packages/jazz-tools/src/dsl.ts
  (): void
- **RestoreOptions** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface RestoreOptions extends TimestampOverrideOptions
- **Row** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface Row
  Purpose: Query row result.
  Public members:
  - **id** — packages/jazz-tools/src/runtime/client.ts
    id: string;
  - **values** — packages/jazz-tools/src/runtime/client.ts
    values: Value[];
- **RowChangeKind** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  const RowChangeKind: { readonly Added: 0; readonly Removed: 1; readonly Updated: 2; }
- **RowContext** (type) — packages/jazz-tools/src/permissions/index.ts
  export type RowContext<Row> = {
  [K in keyof Row & string]: RowRefValue;
  };
- **RowDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type RowDelta<T> = {
  kind: RowChangeKind["Added"];
  id: string;
  index: number;
  item: T;
  } | {
  kind: RowChangeKind["Removed"];
  id: string;
  index: number;
  } | {
  kind: RowChangeKind["Updated"];
  id: string;
  index: number;
  item?: T;
  };
- **RowOf** (type) — packages/jazz-tools/src/typed-app.ts
  export type RowOf<TTable> = TTable extends {
  readonly \_rowType: infer TRow;
  } ? TRow : never;
- **RowRefValue** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface RowRefValue
  Public members:
  - **\_\_jazzPermissionKind** — packages/jazz-tools/src/permissions/index.ts
    readonly \_\_jazzPermissionKind: "row-ref";
  - **column** — packages/jazz-tools/src/permissions/index.ts
    readonly column: string;
- **RuntimeSourcesConfig** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface RuntimeSourcesConfig
  Purpose: Runtime source overrides for Jazz WASM and worker startup.
  Public members:
  - **baseUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Base URL for Jazz runtime files.
    -
    - When set, Jazz derives \`jazz_wasm_bg.wasm\` and the browser broker worker.
      \*/
      baseUrl?: string;
  - **wasmUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the WASM binary. Overrides \`baseUrl\`. _/
    wasmUrl?: string;
  - **brokerWorkerUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the browser broker SharedWorker entry script. Overrides \`baseUrl\`. _/
    brokerWorkerUrl?: string;
  - **wasmSource** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit in-memory WASM source bytes. Overrides URL-based resolution. _/
    wasmSource?: BufferSource;
  - **wasmModule** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit compiled WASM module. Highest-precedence bootstrap input. _/
    wasmModule?: WebAssembly.Module;
- **schema** (namespace) — packages/jazz-tools/src/index.ts
  const schema: RuntimeSchemaNamespace
- **Schema** (interface) — packages/jazz-tools/src/typed-app.ts
  export interface Schema<TSchema extends SchemaDefinition = SchemaDefinition>
  Public members:
  - **[definedSchemaBrand]** — packages/jazz-tools/src/typed-app.ts
    readonly [definedSchemaBrand]: CompactSchema<TSchema>;
- **SchemaAst** (interface) — packages/jazz-tools/src/schema.ts
  export interface Schema
  Public members:
  - **tables** — packages/jazz-tools/src/schema.ts
    tables: Table[];
- **SchemaAstTable** (interface) — packages/jazz-tools/src/schema.ts
  export interface Table
  Public members:
  - **name** — packages/jazz-tools/src/schema.ts
    name: string;
  - **columns** — packages/jazz-tools/src/schema.ts
    columns: Column[];
  - **indexedColumns** — packages/jazz-tools/src/schema.ts
    indexedColumns?: string[];
  - **policies** — packages/jazz-tools/src/schema.ts
    policies?: TablePolicies;
- **SchemaDefinition** (type) — packages/jazz-tools/src/typed-app.ts
  export type SchemaDefinition = Record<string, TableSource>;
- **schemaToWasm** (function) — packages/jazz-tools/src/codegen/schema-reader.ts
  (schema: Schema): WasmSchema
  Purpose: Convert a TS DSL Schema to WasmSchema format.
- **serializeActiveSubscriptions** (function) — packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts
  (traces: readonly ActiveQuerySubscriptionTrace[]): InspectorSubscription[]
- **Session** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface Session
  Purpose: Session context for policy evaluation.
  Public members:
  - **user_id** — packages/jazz-tools/src/runtime/context.ts
    /\*_ User identifier _/
    user_id: string;
  - **claims** — packages/jazz-tools/src/runtime/context.ts
    /\*_ User-defined claims (roles, teams, etc.) _/
    claims: Record<string, unknown>;
  - **authMode** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Auth mode — derived from the JWT's \`iss\` claim. _/
    authMode: AuthMode;
- **SessionContext** (type) — packages/jazz-tools/src/permissions/index.ts
  export type SessionContext = Record<string, SessionRefValue> & {
  readonly user_id: SessionRefValue;
  readonly userId: SessionRefValue;
  readonly authMode: SessionRefValue;
  where: SessionWhereBuilder;
  };
- **SessionRefValue** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface SessionRefValue
  Public members:
  - **\_\_jazzPermissionKind** — packages/jazz-tools/src/permissions/index.ts
    readonly \_\_jazzPermissionKind: "session-ref";
  - **path** — packages/jazz-tools/src/permissions/index.ts
    readonly path: string[];
- **SliceableApp** (interface) — packages/jazz-tools/src/typed-app.ts
  export interface SliceableApp<TSchema extends SchemaLike>
  Public members:
  - **wasmSchema** — packages/jazz-tools/src/typed-app.ts
    readonly wasmSchema: WasmSchema;
  - **slice** — packages/jazz-tools/src/typed-app.ts
    <const TTables extends readonly [TableName<TSchema>, ...TableName<TSchema>[]]>(...tables: TTables): App<SchemaSlice<TSchema, TTables>>
- **SqlType** (type) — packages/jazz-tools/src/schema.ts
  export type SqlType = ScalarSqlType | ArraySqlType | EnumSqlType | JsonSqlType<unknown>;
- **StorageDriver** (type) — packages/jazz-tools/src/drivers/types.ts
  // ============================================================================
  // Storage Driver Interface
  // ============================================================================
  /\*\*
  - Interface for storage backend implementations.
  -
  - - \`persistent\`: local persistence enabled (OPFS in browser, Fjall in backend)
  - - \`memory\`: non-persistent in-memory runtime only
      _/
      export type StorageDriver = {
      type: "persistent";
      /\*\* Browser OPFS namespace when persistence is enabled (default: appId). _/
      dbName?: string;
      } | {
      type: "memory";
      };
      Purpose: Selects persistent or in-memory storage.
- **StoredPermissionsResponse** (interface) — packages/jazz-tools/src/runtime/schema-fetch.ts
  export interface StoredPermissionsResponse
  Public members:
  - **head** — packages/jazz-tools/src/runtime/schema-fetch.ts
    head: StoredPermissionsHead | null;
  - **permissions** — packages/jazz-tools/src/runtime/schema-fetch.ts
    permissions: Record<string, TablePolicies> | null;
- **StoredSchemaHash** (interface) — packages/jazz-tools/src/runtime/schema-fetch.ts
  export interface StoredSchemaHash
  Public members:
  - **hash** — packages/jazz-tools/src/runtime/schema-fetch.ts
    hash: string;
  - **publishedAt** — packages/jazz-tools/src/runtime/schema-fetch.ts
    publishedAt: number | null;
- **SubscriptionCallback** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Subscription callback type.
    \*/
    export type SubscriptionCallback = (delta: SubscriptionWireDelta) => void;
    Purpose: Subscription callback type.
- **SubscriptionDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type SubscriptionDelta<T> = {
  /** Complete result after applying this delta, when available. \*/
  all?: T[];
  /** Ordered list of changes for this delta. _/
  delta: RowDelta<T>[];
  reset?: false;
  } | {
  /\*\* Complete replacement result after applying this reset delta. _/
  all: T[];
  /** Ordered list of changes for this delta. \*/
  delta: RowDelta<T>[];
  /** True when this delta replaces all previously observed state. \*/
  reset: true;
  };
- **SubscriptionWireDelta** (type) — packages/jazz-tools/src/drivers/types.ts
  export type SubscriptionWireDelta = RowDelta | NativeRowDelta;
- **table** (function) — packages/jazz-tools/src/dsl.ts
  <const T extends Record<string, ColumnBuilder>>(name: string, columns: EnforceReferenceColumnNames<T>): void
- **Table** (interface) — packages/jazz-tools/src/typed-app.ts
  export interface Table<TTable extends string, TSchema extends SchemaLike> extends Query<
  TTable,
  {},
  DefaultTableSelection<SchemaMeta<TTable, TSchema>>,
  TSchema
  >
- **TableDefinition** (type) — packages/jazz-tools/src/typed-app.ts
  export type TableDefinition = Record<string, AnyTypedColumnBuilder>;
- **TableHandle** (type) — packages/jazz-tools/src/typed-app.ts
  export type TableHandle<TTable extends string, TSchema extends SchemaLike> = Table<TTable, TSchema>;
- **TableLens** (interface) — packages/jazz-tools/src/schema.ts
  export interface TableLens
  Public members:
  - **table** — packages/jazz-tools/src/schema.ts
    table: string;
  - **added** — packages/jazz-tools/src/schema.ts
    added?: boolean;
  - **removed** — packages/jazz-tools/src/schema.ts
    removed?: boolean;
  - **renamedFrom** — packages/jazz-tools/src/schema.ts
    renamedFrom?: string;
  - **operations** — packages/jazz-tools/src/schema.ts
    operations: LensOp[];
- **TablePolicies** (interface) — packages/jazz-tools/src/schema.ts
  export interface TablePolicies
  Public members:
  - **select** — packages/jazz-tools/src/schema.ts
    select?: OperationPolicy;
  - **insert** — packages/jazz-tools/src/schema.ts
    insert?: OperationPolicy;
  - **update** — packages/jazz-tools/src/schema.ts
    update?: OperationPolicy;
  - **delete** — packages/jazz-tools/src/schema.ts
    delete?: OperationPolicy;
- **TableProxy** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface TableProxy<T, Init>
  Purpose: Generated table constants implement this interface.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed table handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
  - **\_initType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer Init from usage _/
    readonly \_initType: Init;
- **TableSchema** (interface) — packages/jazz-tools/src/drivers/types.ts
  export interface TableSchema
  Public members:
  - **columns** — packages/jazz-tools/src/drivers/types.ts
    columns: ColumnDescriptor[];
  - **indexed_columns** — packages/jazz-tools/src/drivers/types.ts
    indexed_columns?: string[];
  - **policies** — packages/jazz-tools/src/drivers/types.ts
    policies?: TablePolicies;
- **Transaction** (class) — packages/jazz-tools/src/runtime/db.ts
  export class Transaction<TKind extends TransactionKind = TransactionKind>
  Purpose: Groups writes into a mergeable or exclusive transaction. See
  `TransactionKind`.
  Public members:
  - **constructor** — packages/jazz-tools/src/runtime/db.ts
    <TKind extends TransactionKind = TransactionKind>(kind: TKind, resolveClient: (schema: WasmSchema) => JazzClient, session?: Session | undefined, attribution?: string | undefined, ownerClient?: JazzClient): Transaction<TKind>
  - **openBatchId** — packages/jazz-tools/src/runtime/db.ts
    (): OpenBatchId
  - **commit** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<TransactionCommitHandle<TKind>>
  - **rollback** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<boolean>
  - **insert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): T
  - **restore** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Init, options?: RestoreOptions): T
  - **upsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): void
  - **update** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void
  - **delete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): void
  - **all** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>
  - **one** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>
- **TransactionFate** (type) — packages/jazz-tools/src/runtime/client.ts
  export type TransactionFate = {
  kind: "missing";
  batchId: BatchId;
  } | {
  kind: "rejected";
  batchId: BatchId;
  code: string;
  reason: string;
  } | {
  kind: "accepted";
  batchId: BatchId;
  confirmedTier: DurabilityTier;
  };
- **TransactionKind** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Selects the transaction semantics used for grouped writes.
  -
  - - \`mergeable\`: eventually-consistent writes that merge with concurrent writes.
  - - \`exclusive\`: serializable writes that are validated as one unit by the authority.
      \*/
      export type TransactionKind = "mergeable" | "exclusive";
      Purpose: Selects the transaction semantics used for grouped writes.
- **TransactionScope** (type) — packages/jazz-tools/src/runtime/db.ts
  /\*\*
  - Transaction object available inside {@link Db.transaction}'s callback.
    \*/
    export type TransactionScope<TKind extends TransactionKind = TransactionKind> = Scoped<Transaction<TKind>>;
    Purpose: The transaction object passed to a `Db.transaction` callback.
- **TypedApp** (type) — packages/jazz-tools/src/typed-app.ts
  export type TypedApp<TSchema extends SchemaLike> = App<TSchema>;
- **TypedTableQueryBuilder** (class) — packages/jazz-tools/src/typed-app.ts
  export class TypedTableQueryBuilder<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
  > implements QueryBuilder<SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection, TRequired>>
  > Public members:
  - **\_table** — packages/jazz-tools/src/typed-app.ts
    readonly \_table: TableNameFromMeta<TMeta>;
  - **\_schema** — packages/jazz-tools/src/typed-app.ts
    readonly \_schema: WasmSchema;
  - **\_rowType** — packages/jazz-tools/src/typed-app.ts
    declare readonly \_rowType: SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection, TRequired>;
  - **\_initType** — packages/jazz-tools/src/typed-app.ts
    declare readonly \_initType: TableInitFromMeta<TMeta>;
  - **\_columnTransforms** — packages/jazz-tools/src/typed-app.ts
    \_columnTransforms?: ColumnTransformMap;
  - **constructor** — packages/jazz-tools/src/typed-app.ts
    <TMeta extends AnyTableMeta, TInclude extends BuilderInclude<TMeta> = {}, TSelection extends TableSelectableFromMeta<TMeta> = Extract<keyof TableRowFromMeta<TMeta>, string>, TRequired extends boolean = false>(table: TableNameFromMeta<TMeta>, schema: WasmSchema, columnTransforms?: ColumnTransformMap): TypedTableQueryBuilder<TMeta, TInclude, TSelection, TRequired>
  - **where** — packages/jazz-tools/src/typed-app.ts
    (conditions: TableWhereFromMeta<TMeta>): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired>
  - **select** — packages/jazz-tools/src/typed-app.ts
    <NewSelection extends TableSelectableFromMeta<TMeta>>(columns_0: NewSelection, ...columns: NewSelection[]): MetaQueryHandle<TMeta, TInclude, NewSelection, TRequired>
  - **include** — packages/jazz-tools/src/typed-app.ts
    <NewInclude extends BuilderInclude<TMeta>>(relations: NewInclude): MetaQueryHandle<TMeta, TInclude & NewInclude, TSelection, TRequired>
  - **requireIncludes** — packages/jazz-tools/src/typed-app.ts
    (): MetaQueryHandle<TMeta, TInclude, TSelection, true>
  - **orderBy** — packages/jazz-tools/src/typed-app.ts
    (column: TableOrderableFromMeta<TMeta>, direction?: "asc" | "desc"): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired>
  - **limit** — packages/jazz-tools/src/typed-app.ts
    (n: number): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired>
  - **offset** — packages/jazz-tools/src/typed-app.ts
    (n: number): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired>
  - **includeDeleted** — packages/jazz-tools/src/typed-app.ts
    (): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired>
  - **hopTo** — packages/jazz-tools/src/typed-app.ts
    <TRelation extends RelationNameFromMeta<TMeta>>(relation: TRelation): MetaQueryHandle<RelationTargetFromMeta<TMeta, TRelation>, {}, DefaultTableSelection<RelationTargetFromMeta<TMeta, TRelation>>, TRequired>
  - **gather** — packages/jazz-tools/src/typed-app.ts
    (options: { start?: TableWhereFromMeta<TMeta>; step: (ctx: { current: string; }) => QueryBuilder<unknown>; maxDepth?: number; }): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired>
  - **\_build** — packages/jazz-tools/src/typed-app.ts
    (): string
  - **toJSON** — packages/jazz-tools/src/typed-app.ts
    (): unknown
  - **\_serializeRelation** — packages/jazz-tools/src/typed-app.ts
    (): BuiltRelation
- **UpdateOptions** (interface) — packages/jazz-tools/src/runtime/client.ts
  export interface UpdateOptions extends TimestampOverrideOptions
- **Value** (type) — packages/jazz-tools/src/drivers/types.ts
  /\*\*
  - Shared TS value and FFI boundary types used by the Jazz runtimes.
  -
  - \`Value\` is the logical runtime-facing value shape used throughout the TS client.
  - \`FFIValue\` names that same shape when values are crossing into a specific runtime
  - adapter or native binding. These are naming aliases only; runtime adapters can
  - translate them at transport boundaries without forcing client-side copies.
    \*/
    export type Value = {
    type: "Integer";
    value: number;
    } | {
    type: "BigInt";
    value: bigint | number;
    } | {
    type: "Double";
    value: number;
    } | {
    type: "Boolean";
    value: boolean;
    } | {
    type: "Text";
    value: string;
    } | {
    type: "Timestamp";
    value: number;
    } | {
    type: "Uuid";
    value: string;
    } | {
    type: "Bytea";
    value: Uint8Array;
    } | {
    type: "Array";
    value: Value[];
    } | {
    type: "Row";
    value: {
    id?: string;
    values: Value[];
    };
    } | {
    type: "Enum";
    value: {
    case: string;
    values: Value[];
    };
    } | {
    type: "Null";
    };
    Purpose: Shared TS value and FFI boundary types used by the Jazz runtimes.
- **WasmModule** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - WASM module type for sync client creation.
  - This is the type of the jazz-wasm module after dynamic import.
    \*/
    export type WasmModule = typeof import("jazz-wasm");
    Purpose: WASM module type for sync client creation.
- **WasmRow** (interface) — packages/jazz-tools/src/drivers/types.ts
  export interface WasmRow
  Public members:
  - **id** — packages/jazz-tools/src/drivers/types.ts
    id: string;
  - **values** — packages/jazz-tools/src/drivers/types.ts
    values: Value[];
- **WasmSchema** (type) — packages/jazz-tools/src/drivers/types.ts
  export type WasmSchema = Schema;
- **WhereInputOrCallback** (type) — packages/jazz-tools/src/permissions/index.ts
  export type WhereInputOrCallback<WhereInput, Row> = WhereInput | ((row: RowContext<Row>) => WhereInput | Condition);
- **WhereOf** (type) — packages/jazz-tools/src/typed-app.ts
  export type WhereOf<TQuery> = TQuery extends {
  where(input: infer TWhere): unknown;
  } ? TWhere : never;
- **WhereOperator** (type) — packages/jazz-tools/src/where-operators.ts
  export type WhereOperator = "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "contains" | "in" | "isNull";
- **WhereOperatorColumn** (interface) — packages/jazz-tools/src/where-operators.ts
  export interface WhereOperatorColumn
  Public members:
  - **name** — packages/jazz-tools/src/where-operators.ts
    name: string;
  - **columnType** — packages/jazz-tools/src/where-operators.ts
    columnType: ColumnType;
  - **nullable** — packages/jazz-tools/src/where-operators.ts
    nullable: boolean;
  - **references** — packages/jazz-tools/src/where-operators.ts
    references?: string;
  - **implicitId** — packages/jazz-tools/src/where-operators.ts
    implicitId?: boolean;
- **WireRowDelta** (type) — packages/jazz-tools/src/drivers/types.ts
  export type RowDelta = WireRowChange[];
- **WriteHandle** (class) — packages/jazz-tools/src/runtime/client.ts
  export class WriteHandle<T = void>
  Purpose: Returned by upsert, update, delete, and transaction operations.
  Public members:
  - **batchId** — packages/jazz-tools/src/runtime/client.ts
    readonly batchId: Promise<BatchId>;
  - **constructor** — packages/jazz-tools/src/runtime/client.ts
    <T = void>(batchId: BatchId | Promise<BatchId>, client: JazzClient): WriteHandle<T>
  - **wait** — packages/jazz-tools/src/runtime/client.ts
    (options: { tier: DurabilityTier; }): Promise<T>
- **WriteResult** (class) — packages/jazz-tools/src/runtime/client.ts
  export class WriteResult<T> extends WriteHandle<T>
  Purpose: Returned by insert operations and auto-committed transactions.
  Public members:
  - **constructor** — packages/jazz-tools/src/runtime/client.ts
    <T>(value: T, batchId: BatchId | Promise<BatchId>, client: JazzClient): WriteResult<T>
  - **wait** — packages/jazz-tools/src/runtime/client.ts
    (options: { tier: DurabilityTier; }): Promise<T>
  - **mapValue** — packages/jazz-tools/src/runtime/client.ts
    <U>(transformValue: (value: T) => U): WriteResult<U>

### API by import path

#### `jazz-tools/_dev/schema-hash` — `_dev/schema-hash.ts`

- **HASH** (const — exported but marked internal/dev) — packages/jazz-tools/src/\_dev/schema-hash.ts
  const HASH: ""

#### `jazz-tools/backend` — `backend/index.ts`

This entry point re-exports the shared `Db` class. In server code, create the
database through `createJazzContext(config)` and use `JazzContext.asBackend()`
or `JazzContext.forSession()`. The shared
`Db.createWithBrowserWorker` and `Db.deleteClientStorage` entries below remain
browser-only/internal and are not backend server-runtime setup methods.

- **BackendContextConfig** (type) — packages/jazz-tools/src/backend/create-jazz-context.ts
  export type BackendContextConfig = Omit<AppContext, "schema" | "driver" | "clientId" | "tier"> & {
  /** Server runtime driver mode and storage location. \*/
  driver: BackendDriver;
  /** Optional node durability tier identity. _/
  tier?: "local" | "edge" | "global";
  /\*\* JWKS endpoint used to verify external bearer JWTs in \`forRequest()\`. _/
  jwksUrl?: string;
  /** Single JWK object or PEM/JWK string used to verify external bearer JWTs in \`forRequest()\`. \*/
  jwtPublicKey?: BackendJwtPublicKey;
  /** Whether local-first bearer JWTs are accepted in \`forRequest()\`. Defaults to \`true\`. \*/
  allowLocalFirstAuth?: boolean;
  } & BackendContextSchemaConfig;
- **BackendJwtPublicKey** (type) — packages/jazz-tools/src/backend/create-jazz-context.ts
  export type BackendJwtPublicKey = JWK | string;
- **BackendQuerySchemaSource** (type) — packages/jazz-tools/src/backend/create-jazz-context.ts
  export type BackendQuerySchemaSource = QuerySchemaSource;
- **BackendRequestAuthConfig** (interface) — packages/jazz-tools/src/backend/request-auth.ts
  export interface BackendRequestAuthConfig
  Public members:
  - **appId** — packages/jazz-tools/src/backend/request-auth.ts
    appId: string;
  - **jwksUrl** — packages/jazz-tools/src/backend/request-auth.ts
    jwksUrl?: string;
  - **jwtPublicKey** — packages/jazz-tools/src/backend/request-auth.ts
    jwtPublicKey?: BackendJwtPublicKey;
  - **allowLocalFirstAuth** — packages/jazz-tools/src/backend/request-auth.ts
    allowLocalFirstAuth?: boolean;
- **BackendSchemaInput** (type) — packages/jazz-tools/src/backend/create-jazz-context.ts
  export type BackendSchemaInput = SchemaSourceInput;
- **BackendSchemaSource** (type) — packages/jazz-tools/src/backend/create-jazz-context.ts
  export type BackendSchemaSource = WasmSchemaSource;
- **createJazzContext** (function) — packages/jazz-tools/src/backend/create-jazz-context.ts
  (config: BackendContextConfig): JazzContext
- **Db** (class) — packages/jazz-tools/src/runtime/db.ts
  export class Db
  Purpose: High-level database interface for typed queries and mutations.
  Public members:
  - **initLocalFirstAuth** — packages/jazz-tools/src/runtime/db.ts
    (seed: string, ttlSeconds: number, refresh?: boolean): void
  - **create** — packages/jazz-tools/src/runtime/db.ts
    (config: DbConfig, runtimeSource: AnyRuntimeSource): Db
    Internal factory; use the exported `createDb(config)` entrypoint instead.
  - **createWithBrowserWorker** — packages/jazz-tools/src/runtime/db.ts
    (config: DbConfig, runtimeSource: AnyRuntimeSource): Promise<Db>
    `@internal`, browser-only worker construction; not a backend/server setup method.
  - **updateAuthToken** — packages/jazz-tools/src/runtime/db.ts
    (jwtToken: string | null): void
  - **updateCookieSession** — packages/jazz-tools/src/runtime/db.ts
    (cookieSession: Session | null): void
  - **getAuthState** — packages/jazz-tools/src/runtime/db.ts
    (): AuthState
  - **getLocalFirstIdentityProof** — packages/jazz-tools/src/runtime/db.ts
    (options?: { ttlSeconds?: number; audience?: string; }): string | null
  - **onAuthChanged** — packages/jazz-tools/src/runtime/db.ts
    (listener: (state: AuthState) => void): () => void
  - **onMutationError** — packages/jazz-tools/src/runtime/db.ts
    (listener: (event: MutationErrorEvent) => void): () => void
  - **getConfig** — packages/jazz-tools/src/runtime/db.ts
    (): DbConfig
  - **setDevMode** — packages/jazz-tools/src/runtime/db.ts
    (enabled: boolean): void
  - **disconnect** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
  - **reconnect** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
  - **getActiveQuerySubscriptions** — packages/jazz-tools/src/runtime/db.ts
    (): ActiveQuerySubscriptionTrace[]
    `@internal`, devtools/inspector diagnostics only.
  - **onActiveQuerySubscriptionsChange** — packages/jazz-tools/src/runtime/db.ts
    (listener: ActiveQuerySubscriptionTraceListener): () => void
    `@internal`, devtools/inspector diagnostics only.
  - **getRuntimeSchema** — packages/jazz-tools/src/runtime/db.ts
    (): WasmSchema | null
    Devtools/inspector runtime-schema accessor; not a general schema API.
  - **insert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): WriteResult<T>
  - **restore** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Init, options?: RestoreOptions): WriteResult<T>
  - **upsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): WriteHandle
  - **update** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): WriteHandle
  - **delete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, options?: DeleteOptions): WriteHandle
  - **canInsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionAdvice>
  - **canRead** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice>
  - **canUpdate** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): Promise<PermissionAdvice>
  - **canDelete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice>
  - **beginTransaction** — packages/jazz-tools/src/runtime/db.ts
    (): Transaction<"mergeable">
  - **beginExclusiveTransaction** — packages/jazz-tools/src/runtime/db.ts
    (): Transaction<"exclusive">
  - **transaction** — packages/jazz-tools/src/runtime/db.ts
    <TResult>(callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>): Promise<WriteResult<Awaited<TResult>>>
  - **exclusiveTransaction** — packages/jazz-tools/src/runtime/db.ts
    <TResult>(callback: (tx: TransactionScope<"exclusive">) => TResult | Promise<TResult>): Promise<ExclusiveWriteResult<Awaited<TResult>>>
  - **deleteClientStorage** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
    Browser-only OPFS/local-storage maintenance; not server-runtime setup.
  - **logout** — packages/jazz-tools/src/runtime/db.ts
    (options?: LogoutOptions): Promise<void>
  - **all** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>
  - **one** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>
  - **subscribeAll** — packages/jazz-tools/src/runtime/db.ts
    <T extends { id: string; }>(query: QueryBuilder<T>, callback: (delta: SubscriptionDelta<T>) => void, options?: QueryOptions, session?: Session): () => void
  - **shutdown** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
- **JazzContext** (class) — packages/jazz-tools/src/backend/create-jazz-context.ts
  export class JazzContext
  Purpose: Server-side Jazz context with lazy runtime setup.
  Public members:
  - **constructor** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (config: BackendContextConfig): JazzContext
  - **db** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (source?: BackendSchemaInput): Db
  - **asBackend** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (source?: BackendSchemaInput): Db
  - **withAttribution** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (principalId: string, source?: BackendSchemaInput): Db
  - **forRequest** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (request: RequestLike, source?: BackendSchemaInput): Promise<Db>
  - **withAttributionForSession** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (session: Session, source?: BackendSchemaInput): Db
  - **withAttributionForRequest** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (request: RequestLike, source?: BackendSchemaInput): Promise<Db>
  - **forSession** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (session: Session, source?: BackendSchemaInput): Db
  - **flush** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (): void
  - **shutdown** — packages/jazz-tools/src/backend/create-jazz-context.ts
    (): Promise<void>
- **QueryBuilder** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface QueryBuilder<T>
  Purpose: Interface that QueryBuilder classes implement.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name for this query _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference for translation and transformation _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed query handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_build** — packages/jazz-tools/src/runtime/db.ts
    (): string
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **resolveRequestSession** (function) — packages/jazz-tools/src/backend/request-auth.ts
  (request: RequestLike, config: BackendRequestAuthConfig): Promise<Session>
- **Session** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface Session
  Purpose: Session context for policy evaluation.
  Public members:
  - **user_id** — packages/jazz-tools/src/runtime/context.ts
    /\*_ User identifier _/
    user_id: string;
  - **claims** — packages/jazz-tools/src/runtime/context.ts
    /\*_ User-defined claims (roles, teams, etc.) _/
    claims: Record<string, unknown>;
  - **authMode** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Auth mode — derived from the JWT's \`iss\` claim. _/
    authMode: AuthMode;
- **TableProxy** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface TableProxy<T, Init>
  Purpose: Generated table constants implement this interface.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed table handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
  - **\_initType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer Init from usage _/
    readonly \_initType: Init;
- **WasmSchema** (type) — packages/jazz-tools/src/drivers/types.ts
  export type WasmSchema = Schema;

#### `jazz-tools/better-auth-adapter` — `better-auth-adapter/index.ts`

- **jazzAdapter** (const) — packages/jazz-tools/src/better-auth-adapter/index.ts
  const jazzAdapter: (config: {
  debugLogs?: DBAdapterDebugLogOption;
  usePlural?: boolean;
  prefix?: string;
  db: () => Db;
  schema: BackendSchemaInput;
  }) => any
  `JazzAdapterConfig` is an inline, non-exported interface. `prefix` defaults
  to `"better_auth_"` when omitted. The returned adapter is the Better Auth
  database adapter factory result.

#### `jazz-tools/client` — `client/index.ts`

- **applySubscriptionDelta** (function) — packages/jazz-tools/src/runtime/subscription-manager.ts
  <T extends { id: string; }>(current: T[], delta: SubscriptionDelta<T>): T[]
  Purpose: Canonical reducer for subscription streams. Consumers own the materialized
- **AuthSecretStore** (interface) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export interface AuthSecretStore
  Purpose: Interface for platform-appropriate auth secret persistence.
  Public members:
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string>
- **BrowserAuthSecretStore** (class) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export class BrowserAuthSecretStore implements AuthSecretStore
  Purpose: AuthSecretStore backed by localStorage.
  Public members:
  - **constructor** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): BrowserAuthSecretStore
  - **getDefault** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): BrowserAuthSecretStore
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string>
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string, options?: BrowserAuthSecretStoreOptions): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<string>
- **BrowserAuthSecretStoreOptions** (interface) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export interface BrowserAuthSecretStoreOptions
  Public members:
  - **key** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ localStorage key name (default: "jazz-auth-secret") _/
    key?: string;
  - **appId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional app identifier to namespace the default key. _/
    appId?: string;
  - **userId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional principal identifier to isolate secrets per user. _/
    userId?: string | null;
  - **sessionId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional session identifier for per-session isolation. _/
    sessionId?: string | null;
  - **storage** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Override storage backend (for testing) _/
    storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
- **CacheEntryHandle** (type — exported but marked internal/dev) — packages/jazz-tools/src/subscriptions-orchestrator.ts
  export type CacheEntryHandle<T extends {
  id: string;
  }> = {
  readonly state: UseAllState<T>;
  readonly status: UseAllState<T>["status"];
  readonly promise: TrackedPromise<T[]>;
  readonly error: unknown;
  subscribe(callbacks: QueryEntryCallbacks<T>): () => void;
  };
- **createJazzClient** (function) — packages/jazz-tools/src/web/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **getSubscriptionStore** (function — exported but marked internal/dev) — packages/jazz-tools/src/subscription-store-internal.ts
  (client: object): SubscriptionStore
- **JazzClient** (interface) — packages/jazz-tools/src/web/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/web/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/web/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/web/create-jazz-client.ts
    (): Promise<void>
- **JazzClientConfig** (type) — packages/jazz-tools/src/web/create-jazz-client.ts
  export type JazzClientConfig = DbConfig;
- **QueryBuilder** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface QueryBuilder<T>
  Purpose: Interface that QueryBuilder classes implement.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name for this query _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference for translation and transformation _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed query handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_build** — packages/jazz-tools/src/runtime/db.ts
    (): string
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **RowChangeKind** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  const RowChangeKind: { readonly Added: 0; readonly Removed: 1; readonly Updated: 2; }
- **RowDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type RowDelta<T> = {
  kind: RowChangeKind["Added"];
  id: string;
  index: number;
  item: T;
  } | {
  kind: RowChangeKind["Removed"];
  id: string;
  index: number;
  } | {
  kind: RowChangeKind["Updated"];
  id: string;
  index: number;
  item?: T;
  };
- **subscribeAll** (function) — packages/jazz-tools/src/client/index.ts
  <T extends { id: string; }>(client: object, query: QueryBuilder<T>, callback: (delta: SubscriptionDelta<T>) => void, options?: QueryOptions): () => void
- **SubscriptionDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type SubscriptionDelta<T> = {
  /** Complete result after applying this delta, when available. \*/
  all?: T[];
  /** Ordered list of changes for this delta. _/
  delta: RowDelta<T>[];
  reset?: false;
  } | {
  /\*\* Complete replacement result after applying this reset delta. _/
  all: T[];
  /** Ordered list of changes for this delta. \*/
  delta: RowDelta<T>[];
  /** True when this delta replaces all previously observed state. \*/
  reset: true;
  };
- **UseAllState** (type — exported but marked internal/dev) — packages/jazz-tools/src/subscriptions-orchestrator.ts
  export type UseAllState<T extends {
  id: string;
  }> = UseAllStatePending<T> | UseAllStatefulfilledData<T> | UseAllStateError<T>;

#### `jazz-tools/dev/expo` — `dev/expo.ts`

- **\_\_resetJazzPluginForTests** (function — exported but marked internal/dev) — packages/jazz-tools/src/dev/expo.ts
  (): Promise<void>
- **ExpoConfigLike** (interface) — packages/jazz-tools/src/dev/expo.ts
  export interface ExpoConfigLike
  Public members:
  - **extra** — packages/jazz-tools/src/dev/expo.ts
    extra?: Record<string, unknown>;
- **JazzPluginOptions** (interface) — packages/jazz-tools/src/dev/vite.ts
  export interface JazzPluginOptions
  Public members:
  - **server** — packages/jazz-tools/src/dev/vite.ts
    server?: boolean | string | JazzServerOptions;
  - **adminSecret** — packages/jazz-tools/src/dev/vite.ts
    adminSecret?: string;
  - **schemaDir** — packages/jazz-tools/src/dev/vite.ts
    schemaDir?: string;
  - **appId** — packages/jazz-tools/src/dev/vite.ts
    appId?: string;
  - **telemetry** — packages/jazz-tools/src/dev/vite.ts
    telemetry?: TelemetryOptions;
  - **inspector** — packages/jazz-tools/src/dev/vite.ts
    /\*\*
    - The in-app inspector overlay (a floating toggle that opens the embedded
    - inspector) is served during dev by default. Set to \`false\` to disable it.
      \*/
      inspector?: boolean;
- **JazzServerOptions** (interface) — packages/jazz-tools/src/dev/vite.ts
  export interface JazzServerOptions
  Public members:
  - **port** — packages/jazz-tools/src/dev/vite.ts
    port?: number;
  - **adminSecret** — packages/jazz-tools/src/dev/vite.ts
    adminSecret?: string;
  - **appId** — packages/jazz-tools/src/dev/vite.ts
    appId?: string;
  - **allowLocalFirstAuth** — packages/jazz-tools/src/dev/vite.ts
    allowLocalFirstAuth?: boolean;
  - **dataDir** — packages/jazz-tools/src/dev/vite.ts
    dataDir?: string;
  - **inMemory** — packages/jazz-tools/src/dev/vite.ts
    inMemory?: boolean;
  - **jwksUrl** — packages/jazz-tools/src/dev/vite.ts
    jwksUrl?: string;
- **withJazz** (function) — packages/jazz-tools/src/dev/expo.ts
  (expoConfig: ExpoConfigLike, options?: JazzPluginOptions): Promise<ExpoConfigLike>

#### `jazz-tools/dev` — `dev/index.ts`

- **deploy** (function) — packages/jazz-tools/src/dev/catalogue.ts
  (options: DeployOptions): Promise<DeployResult>
  Purpose: Publishes a schema and optional permissions.
- **DeployOptions** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface DeployOptions extends CatalogueServerOptions
  Public members:
  - **schema** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Current schema. Will only be published if not already stored on the server.
      \*/
      schema: SchemaSourceInput;
  - **permissions** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Permissions to publish. Omitting this param restricts \`deploy\` to only publish the schema.
      \*/
      permissions?: CompiledPermissionsMap;
  - **migration** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Migration between the current server schema and the new schema.
    - Only published if there's no existing migration between these schemas.
    - In order to publish migrations, provide {@link permissions} as well.
      \*/
      migration?: DefinedMigration;
  - **noVerify** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Set to \`true\` to publish permissions even if a migration is missing between
    - the current server schema and the new schema.
      \*/
      noVerify?: boolean;
- **DeployResult** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface DeployResult
  Public members:
  - **schema** — packages/jazz-tools/src/dev/catalogue.ts
    schema: DeploySchemaResult;
  - **migration** — packages/jazz-tools/src/dev/catalogue.ts
    migration?: DeployMigrationResult;
  - **permissions** — packages/jazz-tools/src/dev/catalogue.ts
    permissions?: PushPermissionsResult;
  - **warnings** — packages/jazz-tools/src/dev/catalogue.ts
    warnings: string[];
- **DeploySchemaResult** (type) — packages/jazz-tools/src/dev/catalogue.ts
  export type DeploySchemaResult = PushSchemaResult | {
  hash: string;
  schemaFile?: string;
  status: "already-stored";
  };
- **jazzPlugin** (function) — packages/jazz-tools/src/dev/vite.ts
  (options?: JazzPluginOptions): { name: string; config(config: { ssr?: { external?: true | string[]; }; optimizeDeps?: { exclude?: string[]; }; }): { resolve?: { alias: { find: RegExp; replacement: string; }[]; } | undefined; optimizeDeps: { exclude: string[]; }; ssr: { external: true | string[]; }; }; configureServer(viteServer: ViteDevServer): Promise<void>; }
- **JazzPluginOptions** (interface) — packages/jazz-tools/src/dev/vite.ts
  export interface JazzPluginOptions
  Public members:
  - **server** — packages/jazz-tools/src/dev/vite.ts
    server?: boolean | string | JazzServerOptions;
  - **adminSecret** — packages/jazz-tools/src/dev/vite.ts
    adminSecret?: string;
  - **schemaDir** — packages/jazz-tools/src/dev/vite.ts
    schemaDir?: string;
  - **appId** — packages/jazz-tools/src/dev/vite.ts
    appId?: string;
  - **telemetry** — packages/jazz-tools/src/dev/vite.ts
    telemetry?: TelemetryOptions;
  - **inspector** — packages/jazz-tools/src/dev/vite.ts
    /\*\*
    - The in-app inspector overlay (a floating toggle that opens the embedded
    - inspector) is served during dev by default. Set to \`false\` to disable it.
      \*/
      inspector?: boolean;
- **JazzServerOptions** (interface) — packages/jazz-tools/src/dev/vite.ts
  export interface JazzServerOptions
  Public members:
  - **port** — packages/jazz-tools/src/dev/vite.ts
    port?: number;
  - **adminSecret** — packages/jazz-tools/src/dev/vite.ts
    adminSecret?: string;
  - **appId** — packages/jazz-tools/src/dev/vite.ts
    appId?: string;
  - **allowLocalFirstAuth** — packages/jazz-tools/src/dev/vite.ts
    allowLocalFirstAuth?: boolean;
  - **dataDir** — packages/jazz-tools/src/dev/vite.ts
    dataDir?: string;
  - **inMemory** — packages/jazz-tools/src/dev/vite.ts
    inMemory?: boolean;
  - **jwksUrl** — packages/jazz-tools/src/dev/vite.ts
    jwksUrl?: string;
- **jazzSvelteKit** (function) — packages/jazz-tools/src/dev/sveltekit.ts
  (options?: JazzPluginOptions): { name: string; enforce: "pre"; config(config: ViteUserConfigLike, env?: ViteConfigEnvLike): { resolve?: { alias: { find: RegExp; replacement: string; }[]; } | undefined; optimizeDeps: { exclude: string[]; }; ssr: { external: true | string[]; }; } | Promise<{ resolve?: { alias: { find: RegExp; replacement: string; }[]; } | undefined; optimizeDeps: { exclude: string[]; }; ssr: { external: true | string[]; }; }>; configureServer(viteServer: ViteDevServer): Promise<void>; }
- **LocalJazzServerHandle** (interface) — packages/jazz-tools/src/dev/dev-server.ts
  export interface LocalJazzServerHandle
  Public members:
  - **appId** — packages/jazz-tools/src/dev/dev-server.ts
    appId: string;
  - **port** — packages/jazz-tools/src/dev/dev-server.ts
    port: number;
  - **url** — packages/jazz-tools/src/dev/dev-server.ts
    url: string;
  - **dataDir** — packages/jazz-tools/src/dev/dev-server.ts
    dataDir: string;
  - **adminSecret** — packages/jazz-tools/src/dev/dev-server.ts
    adminSecret: string;
  - **backendSecret** — packages/jazz-tools/src/dev/dev-server.ts
    backendSecret: string;
  - **stop** — packages/jazz-tools/src/dev/dev-server.ts
    stop: () => Promise<void>;
- **pushMigration** (function) — packages/jazz-tools/src/dev/catalogue.ts
  (options: PushMigrationOptions): Promise<PushMigrationResult>
  Purpose: Publishes the migration that connects two schemas.
- **PushMigrationOptions** (type) — packages/jazz-tools/src/dev/catalogue.ts
  export type PushMigrationOptions = CatalogueServerOptions & ({
  migration: DefinedMigration;
  fromHash?: string;
  toHash?: string;
  } | {
  fromHash: string;
  toHash: string;
  migration?: undefined;
  });
- **PushMigrationResult** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface PushMigrationResult
  Public members:
  - **fromHash** — packages/jazz-tools/src/dev/catalogue.ts
    fromHash: string;
  - **toHash** — packages/jazz-tools/src/dev/catalogue.ts
    toHash: string;
  - **status** — packages/jazz-tools/src/dev/catalogue.ts
    status: "published";
  - **objectId** — packages/jazz-tools/src/dev/catalogue.ts
    objectId?: string;
- **pushPermissions** (function) — packages/jazz-tools/src/dev/catalogue.ts
  (options: PushPermissionsOptions): Promise<PushPermissionsResult>
  Purpose: The target schema must already be identified by `options.schemaHash`.
- **PushPermissionsOptions** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface PushPermissionsOptions extends CatalogueServerOptions
  Public members:
  - **schemaHash** — packages/jazz-tools/src/dev/catalogue.ts
    schemaHash: string;
  - **permissions** — packages/jazz-tools/src/dev/catalogue.ts
    permissions: CompiledPermissionsMap;
- **PushPermissionsResult** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface PushPermissionsResult
  Public members:
  - **schemaHash** — packages/jazz-tools/src/dev/catalogue.ts
    schemaHash: string;
  - **permissionsFile** — packages/jazz-tools/src/dev/catalogue.ts
    permissionsFile?: string;
  - **previousHead** — packages/jazz-tools/src/dev/catalogue.ts
    previousHead: StoredPermissionsHead | null;
  - **head** — packages/jazz-tools/src/dev/catalogue.ts
    head: StoredPermissionsHead | null;
- **pushSchema** (function) — packages/jazz-tools/src/dev/catalogue.ts
  (options: PushSchemaOptions): Promise<PushSchemaResult>
  Purpose: Publishes a schema to the Jazz server.
- **PushSchemaOptions** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface PushSchemaOptions extends CatalogueServerOptions
  Public members:
  - **schema** — packages/jazz-tools/src/dev/catalogue.ts
    schema: SchemaSourceInput;
- **PushSchemaResult** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface PushSchemaResult
  Public members:
  - **hash** — packages/jazz-tools/src/dev/catalogue.ts
    hash: string;
  - **schemaFile** — packages/jazz-tools/src/dev/catalogue.ts
    schemaFile?: string;
  - **status** — packages/jazz-tools/src/dev/catalogue.ts
    status: "published";
  - **objectId** — packages/jazz-tools/src/dev/catalogue.ts
    objectId?: string;
- **SchemaSourceInput** (type) — packages/jazz-tools/src/schema-source.ts
  export type SchemaSourceInput = WasmSchema | WasmSchemaSource | QuerySchemaSource;
- **SchemaWatcherOptions** (interface) — packages/jazz-tools/src/dev/schema-watcher.ts
  export interface SchemaWatcherOptions
  Public members:
  - **schemaDir** — packages/jazz-tools/src/dev/schema-watcher.ts
    schemaDir: string;
  - **serverUrl** — packages/jazz-tools/src/dev/schema-watcher.ts
    serverUrl: string;
  - **appId** — packages/jazz-tools/src/dev/schema-watcher.ts
    appId: string;
  - **adminSecret** — packages/jazz-tools/src/dev/schema-watcher.ts
    adminSecret: string;
  - **onPush** — packages/jazz-tools/src/dev/schema-watcher.ts
    onPush?: (hash: string) => void;
  - **onError** — packages/jazz-tools/src/dev/schema-watcher.ts
    onError?: (error: Error) => void;
- **startLocalJazzServer** (function) — packages/jazz-tools/src/dev/dev-server.ts
  (options?: StartLocalJazzServerOptions): Promise<LocalJazzServerHandle>
  Purpose: idempotent `stop()` method that shuts the server down and releases owned
- **StartLocalJazzServerOptions** (interface) — packages/jazz-tools/src/dev/dev-server.ts
  export interface StartLocalJazzServerOptions
  Public members:
  - **appId** — packages/jazz-tools/src/dev/dev-server.ts
    appId?: string;
  - **port** — packages/jazz-tools/src/dev/dev-server.ts
    port?: number;
  - **dataDir** — packages/jazz-tools/src/dev/dev-server.ts
    dataDir?: string;
  - **inMemory** — packages/jazz-tools/src/dev/dev-server.ts
    inMemory?: boolean;
  - **jwksUrl** — packages/jazz-tools/src/dev/dev-server.ts
    jwksUrl?: string;
  - **backendSecret** — packages/jazz-tools/src/dev/dev-server.ts
    backendSecret?: string;
  - **adminSecret** — packages/jazz-tools/src/dev/dev-server.ts
    adminSecret?: string;
  - **upstreamUrl** — packages/jazz-tools/src/dev/dev-server.ts
    upstreamUrl?: string;
  - **allowLocalFirstAuth** — packages/jazz-tools/src/dev/dev-server.ts
    allowLocalFirstAuth?: boolean;
  - **telemetryCollectorUrl** — packages/jazz-tools/src/dev/dev-server.ts
    telemetryCollectorUrl?: string;
  - **enableLogs** — packages/jazz-tools/src/dev/dev-server.ts
    enableLogs?: boolean;
  - **schema** — packages/jazz-tools/src/dev/dev-server.ts
    schema?: Uint8Array;
- **watchSchema** (function) — packages/jazz-tools/src/dev/schema-watcher.ts
  (options: SchemaWatcherOptions): { close: () => void; }
- **withJazz** (function) — packages/jazz-tools/src/dev/next.ts
  (nextConfig?: NextConfigInput, options?: NextJazzPluginOptions): NextConfigFactory
- **withJazzExpo** (function) — packages/jazz-tools/src/dev/expo.ts
  (expoConfig: ExpoConfigLike, options?: JazzPluginOptions): Promise<ExpoConfigLike>

#### `jazz-tools/dev/next` — `dev/next.ts`

- **\_\_resetJazzNextPluginForTests** (function — exported but marked internal/dev) — packages/jazz-tools/src/dev/next.ts
  (): Promise<void>
- **NextConfigLike** (interface) — packages/jazz-tools/src/dev/next.ts
  export interface NextConfigLike
  Public members:
  - **env** — packages/jazz-tools/src/dev/next.ts
    env?: Record<string, string | undefined>;
  - **serverExternalPackages** — packages/jazz-tools/src/dev/next.ts
    serverExternalPackages?: string[];
- **NextJazzPluginOptions** (interface) — packages/jazz-tools/src/dev/next.ts
  export interface NextJazzPluginOptions extends JazzPluginOptions
  Public members:
  - **server** — packages/jazz-tools/src/dev/next.ts
    server?: boolean | string | NextJazzServerOptions;
  - **appRoot** — packages/jazz-tools/src/dev/next.ts
    appRoot?: string;
- **NextJazzServerOptions** (interface) — packages/jazz-tools/src/dev/next.ts
  export interface NextJazzServerOptions extends JazzServerOptions
  Public members:
  - **backendSecret** — packages/jazz-tools/src/dev/next.ts
    backendSecret?: string;
- **withJazz** (function) — packages/jazz-tools/src/dev/next.ts
  (nextConfig?: NextConfigInput, options?: NextJazzPluginOptions): NextConfigFactory

#### `jazz-tools/dev/sveltekit` — `dev/sveltekit.ts`

- **\_\_resetJazzSvelteKitPluginForTests** (function — exported but marked internal/dev) — packages/jazz-tools/src/dev/sveltekit.ts
  (): Promise<void>
- **JazzPluginOptions** (interface) — packages/jazz-tools/src/dev/sveltekit.ts
  export interface JazzPluginOptions extends Omit<BaseJazzPluginOptions, "server">
  Public members:
  - **server** — packages/jazz-tools/src/dev/sveltekit.ts
    server?: boolean | string | JazzServerOptions;
- **JazzServerOptions** (interface) — packages/jazz-tools/src/dev/sveltekit.ts
  export interface JazzServerOptions extends BaseJazzServerOptions
  Purpose: Adds SvelteKit server options to the shared Vite plugin options.
  The plugin puts `backendSecret` in `process.env` for server routes and hooks.
  Public members:
  - **backendSecret** — packages/jazz-tools/src/dev/sveltekit.ts
    backendSecret?: string;
- **jazzSvelteKit** (function) — packages/jazz-tools/src/dev/sveltekit.ts
  (options?: JazzPluginOptions): { name: string; enforce: "pre"; config(config: ViteUserConfigLike, env?: ViteConfigEnvLike): { resolve?: { alias: { find: RegExp; replacement: string; }[]; } | undefined; optimizeDeps: { exclude: string[]; }; ssr: { external: true | string[]; }; } | Promise<{ resolve?: { alias: { find: RegExp; replacement: string; }[]; } | undefined; optimizeDeps: { exclude: string[]; }; ssr: { external: true | string[]; }; }>; configureServer(viteServer: ViteDevServer): Promise<void>; }

#### `jazz-tools/dev/vite` — `dev/vite.ts`

- **jazzPlugin** (function) — packages/jazz-tools/src/dev/vite.ts
  (options?: JazzPluginOptions): { name: string; config(config: { ssr?: { external?: true | string[]; }; optimizeDeps?: { exclude?: string[]; }; }): { resolve?: { alias: { find: RegExp; replacement: string; }[]; } | undefined; optimizeDeps: { exclude: string[]; }; ssr: { external: true | string[]; }; }; configureServer(viteServer: ViteDevServer): Promise<void>; }
- **JazzPluginOptions** (interface) — packages/jazz-tools/src/dev/vite.ts
  export interface JazzPluginOptions
  Public members:
  - **server** — packages/jazz-tools/src/dev/vite.ts
    server?: boolean | string | JazzServerOptions;
  - **adminSecret** — packages/jazz-tools/src/dev/vite.ts
    adminSecret?: string;
  - **schemaDir** — packages/jazz-tools/src/dev/vite.ts
    schemaDir?: string;
  - **appId** — packages/jazz-tools/src/dev/vite.ts
    appId?: string;
  - **telemetry** — packages/jazz-tools/src/dev/vite.ts
    telemetry?: TelemetryOptions;
  - **inspector** — packages/jazz-tools/src/dev/vite.ts
    /\*\*
    - The in-app inspector overlay (a floating toggle that opens the embedded
    - inspector) is served during dev by default. Set to \`false\` to disable it.
      \*/
      inspector?: boolean;
- **JazzServerOptions** (interface) — packages/jazz-tools/src/dev/vite.ts
  export interface JazzServerOptions
  Public members:
  - **port** — packages/jazz-tools/src/dev/vite.ts
    port?: number;
  - **adminSecret** — packages/jazz-tools/src/dev/vite.ts
    adminSecret?: string;
  - **appId** — packages/jazz-tools/src/dev/vite.ts
    appId?: string;
  - **allowLocalFirstAuth** — packages/jazz-tools/src/dev/vite.ts
    allowLocalFirstAuth?: boolean;
  - **dataDir** — packages/jazz-tools/src/dev/vite.ts
    dataDir?: string;
  - **inMemory** — packages/jazz-tools/src/dev/vite.ts
    inMemory?: boolean;
  - **jwksUrl** — packages/jazz-tools/src/dev/vite.ts
    jwksUrl?: string;
- **resolveJazzWasmEntry** (function — exported but marked internal/dev) — packages/jazz-tools/src/dev/vite.ts
  (): string | null
  Purpose: Resolves `jazz-wasm` from `jazz-tools` so Vite can skip esbuild
  pre-bundling and pnpm users do not need a direct `jazz-wasm` dependency.
- **ViteDevServer** (interface — exported but marked internal/dev) — packages/jazz-tools/src/dev/vite.ts
  export interface ViteDevServer
  Purpose: Defines the small part of Vite's server API used by Jazz plugins,
  without adding Vite's types as a dependency.
  Public members:
  - **config** — packages/jazz-tools/src/dev/vite.ts
    config: {
    root: string;
    command: string;
    env?: Record<string, string>;
    server?: {
    port?: number;
    host?: string | boolean;
    https?: unknown;
    };
    };
  - **httpServer** — packages/jazz-tools/src/dev/vite.ts
    httpServer: {
    once(event: string, cb: () => void): void;
    } | null;
  - **middlewares** — packages/jazz-tools/src/dev/vite.ts
    middlewares?: OverlayDevServer["middlewares"];
  - **ws** — packages/jazz-tools/src/dev/vite.ts
    ws: {
    send(payload: {
    type: string;
    err?: {
    message: string;
    stack?: string;
    };
    }): void;
    };
  - **restart** — packages/jazz-tools/src/dev/vite.ts
    (forceOptimize?: boolean): Promise<void>

#### `jazz-tools/expo` — `expo/index.ts`

- **expoAuthSecretStore** (const) — packages/jazz-tools/src/expo/auth-secret-store.ts
  const expoAuthSecretStore: AuthSecretStore
- **ExpoAuthSecretStore** (class) — packages/jazz-tools/src/expo/auth-secret-store.ts
  export class ExpoAuthSecretStore implements AuthSecretStore
  Public members:
  - **constructor** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (options?: ExpoAuthSecretStoreOptions): ExpoAuthSecretStore
  - **getDefault** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (options?: ExpoAuthSecretStoreOptions): ExpoAuthSecretStore
  - **loadSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (): Promise<string>
  - **loadSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (options?: ExpoAuthSecretStoreOptions): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (secret: string, options?: ExpoAuthSecretStoreOptions): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (options?: ExpoAuthSecretStoreOptions): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (options?: ExpoAuthSecretStoreOptions): Promise<string>
- **ExpoAuthSecretStoreOptions** (interface) — packages/jazz-tools/src/expo/auth-secret-store.ts
  export interface ExpoAuthSecretStoreOptions
  Public members:
  - **key** — packages/jazz-tools/src/expo/auth-secret-store.ts
    /\*_ SecureStore key name (default: "jazz-auth-secret"). _/
    key?: string;
  - **appId** — packages/jazz-tools/src/expo/auth-secret-store.ts
    /\*_ Optional app identifier to namespace the default key. _/
    appId?: string;
  - **userId** — packages/jazz-tools/src/expo/auth-secret-store.ts
    /\*_ Optional principal identifier to isolate secrets per user. _/
    userId?: string | null;
  - **sessionId** — packages/jazz-tools/src/expo/auth-secret-store.ts
    /\*_ Optional session identifier for per-session isolation. _/
    sessionId?: string | null;
  - **secureStore** — packages/jazz-tools/src/expo/auth-secret-store.ts
    /\*_ Override SecureStore backend for tests and host adapters. _/
    secureStore?: ExpoSecureStoreLike;
- **ExpoSecureStoreLike** (interface) — packages/jazz-tools/src/expo/auth-secret-store.ts
  export interface ExpoSecureStoreLike
  Public members:
  - **getItemAsync** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (key: string): Promise<string | null>
  - **setItemAsync** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (key: string, value: string): Promise<void>
  - **deleteItemAsync** — packages/jazz-tools/src/expo/auth-secret-store.ts
    (key: string): Promise<void>
- **LocalFirstAuth** (interface) — packages/jazz-tools/src/react-core/use-local-first-auth.ts
  export interface LocalFirstAuth
  Public members:
  - **secret** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    secret: string | null;
  - **isLoading** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    isLoading: boolean;
  - **login** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (secret: string): Promise<void>
  - **signOut** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (): Promise<void>
- **useLocalFirstAuth** (function) — packages/jazz-tools/src/expo/use-local-first-auth.ts
  (options?: UseLocalFirstAuthOptions): LocalFirstAuth
- **UseLocalFirstAuthOptions** (type) — packages/jazz-tools/src/expo/use-local-first-auth.ts
  export type UseLocalFirstAuthOptions = Pick<ExpoAuthSecretStoreOptions, "key" | "appId" | "userId" | "sessionId">;

#### `jazz-tools/expo/polyfills` — `expo/polyfills.ts`

No named runtime or type exports. Importing this side-effect-only module
installs a React Native `ReadableStream` global, using `web-streams-polyfill`
when the global is not already available (`packages/jazz-tools/src/expo/polyfills.ts`).

#### `jazz-tools/passkey-backup` — `passkey-backup.ts`

Browser-only WebAuthn recovery. `backup` uses the decoded 32-byte Jazz secret
directly as `publicKey.user.id`, creates a resident platform credential with
required user verification, and `restore` requires user-present and
user-verified authenticator flags before returning the credential user handle.
This is a passkey recovery mechanism, not a Jazz encryption/key-wrapping layer
or a general encrypted-at-rest store (`packages/jazz-tools/src/runtime/passkey-backup.ts`).

- **BrowserPasskeyBackup** (class) — packages/jazz-tools/src/runtime/passkey-backup.ts
  export class BrowserPasskeyBackup
  Public members:
  - **constructor** — packages/jazz-tools/src/runtime/passkey-backup.ts
    (options: BrowserPasskeyBackupOptions): BrowserPasskeyBackup
  - **backup** — packages/jazz-tools/src/runtime/passkey-backup.ts
    (secret: string, displayName: string): Promise<void>
  - **restore** — packages/jazz-tools/src/runtime/passkey-backup.ts
    (): Promise<string>
- **BrowserPasskeyBackupOptions** (interface) — packages/jazz-tools/src/runtime/passkey-backup.ts
  export interface BrowserPasskeyBackupOptions
  Public members:
  - **appName** — packages/jazz-tools/src/runtime/passkey-backup.ts
    appName: string;
  - **appHostname** — packages/jazz-tools/src/runtime/passkey-backup.ts
    /\*\*
    - Relying-party ID for the passkey credential. Defaults to \`location.hostname\`.
    - Must be stable across environments for cross-device recovery to work.
      \*/
      appHostname?: string;
- **PasskeyBackupError** (class) — packages/jazz-tools/src/runtime/passkey-backup.ts
  export class PasskeyBackupError extends Error
  Public members:
  - **name** — packages/jazz-tools/src/runtime/passkey-backup.ts
    readonly name = "PasskeyBackupError";
  - **code** — packages/jazz-tools/src/runtime/passkey-backup.ts
    readonly code: PasskeyBackupErrorCode;
  - **constructor** — packages/jazz-tools/src/runtime/passkey-backup.ts
    (code: PasskeyBackupErrorCode, cause?: unknown): PasskeyBackupError
- **PasskeyBackupErrorCode** (type) — packages/jazz-tools/src/runtime/passkey-backup.ts
  export type PasskeyBackupErrorCode = "not-supported" | "invalid-secret" | "create-failed" | "get-failed" | "no-credential" | "invalid-credential" | "verification-failed";

#### `jazz-tools/passphrase` — `passphrase.ts`

- **RecoveryPhrase** (const) — packages/jazz-tools/src/runtime/recovery-phrase.ts
  const RecoveryPhrase: { fromSecret(secret: string): string; toSecret(phrase: string): string; }
- **RecoveryPhraseError** (class) — packages/jazz-tools/src/runtime/recovery-phrase.ts
  export class RecoveryPhraseError extends Error
  Public members:
  - **code** — packages/jazz-tools/src/runtime/recovery-phrase.ts
    readonly code: RecoveryPhraseErrorCode;
  - **constructor** — packages/jazz-tools/src/runtime/recovery-phrase.ts
    (code: RecoveryPhraseErrorCode, message: string): RecoveryPhraseError

#### `jazz-tools/permissions` — `permissions/index.ts`

- **allOf** (function) — packages/jazz-tools/src/permissions/index.ts
  (conditions: readonly unknown[]): Condition
- **AllowedToContext** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface AllowedToContext
  Public members:
  - **read** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **insert** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **update** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **delete** — packages/jazz-tools/src/permissions/index.ts
    (fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **readReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **insertReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **updateReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
  - **deleteReferencing** — packages/jazz-tools/src/permissions/index.ts
    (sourceTable: RelationJoinTarget, fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr
- **anyOf** (function) — packages/jazz-tools/src/permissions/index.ts
  (conditions: readonly unknown[]): Condition
- **CompiledPermissions** (type) — packages/jazz-tools/src/permissions/index.ts
  export type CompiledPermissions = Record<string, TablePolicies>;
- **createSessionContext** (function — exported but marked internal/dev) — packages/jazz-tools/src/permissions/index.ts
  (): SessionContext
  Status: Internal.
- **definePermissions** (function) — packages/jazz-tools/src/permissions/index.ts
  <TApp extends AppLike>(app: TApp, factory: (ctx: PolicyContext<TApp>) => void): CompiledPermissions
- **PermissionRelation** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface PermissionRelation
  Public members:
  - **where** — packages/jazz-tools/src/permissions/index.ts
    (input: unknown): PermissionRelation
  - **join** — packages/jazz-tools/src/permissions/index.ts
    (target: RelationJoinTarget, on: { left: string; right: string; }): PermissionRelation
  - **select** — packages/jazz-tools/src/permissions/index.ts
    (columns: Record<string, string>): PermissionRelation
  - **hopTo** — packages/jazz-tools/src/permissions/index.ts
    (relation: string): PermissionRelation
  - **gather** — packages/jazz-tools/src/permissions/index.ts
    (options: { start?: Record<string, unknown> | PermissionRelation; step: (ctx: { current: RecursiveCurrentValue; }) => PermissionRelation; maxDepth?: number; }): PermissionRelation
  - **reachable_via** — packages/jazz-tools/src/permissions/index.ts
    (access_table: string, access_row_column: string, access_team_column: string, from: SessionRefValue, edge_table: string, edge_member_column: string, edge_parent_column: string, edge_filters?: Record<string, unknown>): ReachableSeedBuilder
  - **reachable_via_with_access_filters** — packages/jazz-tools/src/permissions/index.ts
    (access_table: string, access_row_column: string, access_team_column: string, from: SessionRefValue, access_filters: Record<string, unknown>, edge_table: string, edge_member_column: string, edge_parent_column: string, edge_filters?: Record<string, unknown>): ReachableSeedBuilder
- **PolicyContext** (type) — packages/jazz-tools/src/permissions/index.ts
  export type PolicyContext<TApp extends AppLike> = {
  policy: {
  [K in TableKey<TApp>]: TablePolicyBuilder<WhereFor<QueryBuilderFor<TApp, K>>, RowFor<QueryBuilderFor<TApp, K>>>;
  } & {
  exists(relation: PermissionRelation): ExistsRelationCondition;
  union(relations: readonly PermissionRelation[]): PermissionRelation;
  };
  anyOf: (conditions: readonly unknown[]) => Condition;
  allOf: (conditions: readonly unknown[]) => Condition;
  isCreator: Condition;
  allowedTo: AllowedToContext;
  session: SessionContext;
  };
- **ReachableSeedBuilder** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface ReachableSeedBuilder
  Public members:
  - **seeded_by** — packages/jazz-tools/src/permissions/index.ts
    (seed_table: string, user_column: string, claim_path: string, team_column: string): PermissionRelation
- **relationExistsToPolicy** (function) — packages/jazz-tools/src/permissions/index.ts
  (relation: PermissionRelation): PolicyExpr
- **relationToIr** (function) — packages/jazz-tools/src/permissions/index.ts
  (relation: PermissionRelation): RelExpr
- **RowContext** (type) — packages/jazz-tools/src/permissions/index.ts
  export type RowContext<Row> = {
  [K in keyof Row & string]: RowRefValue;
  };
- **RowRefValue** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface RowRefValue
  Public members:
  - **\_\_jazzPermissionKind** — packages/jazz-tools/src/permissions/index.ts
    readonly \_\_jazzPermissionKind: "row-ref";
  - **column** — packages/jazz-tools/src/permissions/index.ts
    readonly column: string;
- **SessionContext** (type) — packages/jazz-tools/src/permissions/index.ts
  export type SessionContext = Record<string, SessionRefValue> & {
  readonly user_id: SessionRefValue;
  readonly userId: SessionRefValue;
  readonly authMode: SessionRefValue;
  where: SessionWhereBuilder;
  };
- **SessionRefValue** (interface) — packages/jazz-tools/src/permissions/index.ts
  export interface SessionRefValue
  Public members:
  - **\_\_jazzPermissionKind** — packages/jazz-tools/src/permissions/index.ts
    readonly \_\_jazzPermissionKind: "session-ref";
  - **path** — packages/jazz-tools/src/permissions/index.ts
    readonly path: string[];
- **WhereInputOrCallback** (type) — packages/jazz-tools/src/permissions/index.ts
  export type WhereInputOrCallback<WhereInput, Row> = WhereInput | ((row: RowContext<Row>) => WhereInput | Condition);

#### `jazz-tools/react-core` — `react-core/index.ts`

- **AuthStateInfo** (interface) — packages/jazz-tools/src/react-core/use-auth-state.ts
  export interface AuthStateInfo
  Public members:
  - **authMode** — packages/jazz-tools/src/react-core/use-auth-state.ts
    authMode: AuthMode;
  - **userId** — packages/jazz-tools/src/react-core/use-auth-state.ts
    userId: string | null;
  - **claims** — packages/jazz-tools/src/react-core/use-auth-state.ts
    claims: Record<string, unknown>;
  - **error** — packages/jazz-tools/src/react-core/use-auth-state.ts
    error?: AuthFailureReason;
- **createJazzClient** (function) — packages/jazz-tools/src/web/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **createUseLocalFirstAuth** (function) — packages/jazz-tools/src/react-core/use-local-first-auth.ts
  (store: AuthSecretStore): () => LocalFirstAuth
- **DurabilityTier** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Persistence tier for durability guarantees.
  -
  - - \`local\`: Persisted in local durable storage
  - - \`edge\`: Persisted at edge server
  - - \`global\`: Persisted at global server
      \*/
      export type DurabilityTier = "local" | "edge" | "global";
      Purpose: Persistence tier for durability guarantees.
- **JazzClient** (interface) — packages/jazz-tools/src/web/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/web/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/web/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/web/create-jazz-client.ts
    (): Promise<void>
- **JazzClientProvider** (function) — packages/jazz-tools/src/react-core/provider.tsx
  ({ client: clientPromise, onJWTExpired, children, }: JazzClientProviderProps): any
  Purpose: Makes a Jazz client available to children components through a React context.
- **JazzClientProviderProps** (type) — packages/jazz-tools/src/react-core/provider.tsx
  export type JazzClientProviderProps = {
  client: Promise<CoreJazzClient> | CoreJazzClient;
  onJWTExpired?: JwtRefreshFn;
  children: ReactNode;
  };
- **JazzProvider** (function) — packages/jazz-tools/src/react-core/provider.tsx
  ({ config, fallback, children, createJazzClient, onJWTExpired, }: JazzProviderProps): any
  Purpose: Default Jazz provider. Creates a Jazz client and makes it available to children
- **JazzProviderProps** (type) — packages/jazz-tools/src/react-core/provider.tsx
  export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  createJazzClient: CreateJazzClient;
  onJWTExpired?: JwtRefreshFn;
  };
- **LocalFirstAuth** (interface) — packages/jazz-tools/src/react-core/use-local-first-auth.ts
  export interface LocalFirstAuth
  Public members:
  - **secret** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    secret: string | null;
  - **isLoading** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    isLoading: boolean;
  - **login** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (secret: string): Promise<void>
  - **signOut** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (): Promise<void>
- **QueryBuilder** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface QueryBuilder<T>
  Purpose: Interface that QueryBuilder classes implement.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name for this query _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference for translation and transformation _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed query handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_build** — packages/jazz-tools/src/runtime/db.ts
    (): string
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **RowDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type RowDelta<T> = {
  kind: RowChangeKind["Added"];
  id: string;
  index: number;
  item: T;
  } | {
  kind: RowChangeKind["Removed"];
  id: string;
  index: number;
  } | {
  kind: RowChangeKind["Updated"];
  id: string;
  index: number;
  item?: T;
  };
- **SubscriptionDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type SubscriptionDelta<T> = {
  /** Complete result after applying this delta, when available. \*/
  all?: T[];
  /** Ordered list of changes for this delta. _/
  delta: RowDelta<T>[];
  reset?: false;
  } | {
  /\*\* Complete replacement result after applying this reset delta. _/
  all: T[];
  /** Ordered list of changes for this delta. \*/
  delta: RowDelta<T>[];
  /** True when this delta replaces all previously observed state. \*/
  reset: true;
  };
- **TableProxy** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface TableProxy<T, Init>
  Purpose: Generated table constants implement this interface.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed table handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
  - **\_initType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer Init from usage _/
    readonly \_initType: Init;
- **useAll** (function) — packages/jazz-tools/src/react-core/use-all.ts
  <T extends { id: string; } = { id: string; }>(): UseAllNoQueryResult
  <T extends { id: string; } = { id: string; }>(query: undefined, options?: QueryOptions): UseAllNoQueryResult
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): UseAllResult<T>
  <T extends { id: string; }>(query: QueryBuilder<T> | undefined, options?: QueryOptions): UseAllResult<T> | UseAllNoQueryResult
  `UseAllNoQueryResult` is an inferred return shape and is not a named export.
  Purpose: - `data` is `undefined` until the query resolves or if the query fails.
- **useAllSuspense** (function) — packages/jazz-tools/src/react-core/use-all.ts
  <T extends { id: string; }>(query?: QueryBuilder<T>, options?: QueryOptions): T[]
  Purpose: Suspends until the query is executed.
- **useOne** (function) — packages/jazz-tools/src/react-core/use-one.ts
  <T extends { id: string; } = { id: string; }>(): UseOneNoQueryResult
  <T extends { id: string; } = { id: string; }>(query: undefined, options?: QueryOptions): UseOneNoQueryResult
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): UseOneResult<T>
  <T extends { id: string; }>(query: QueryBuilder<T> | undefined, options?: QueryOptions): UseOneResult<T> | UseOneNoQueryResult
  Purpose: Subscribes to the first matching row. `data` is `undefined` while
  loading, the row when found, or `null` after an empty result.
- **useOneSuspense** (function) — packages/jazz-tools/src/react-core/use-one.ts
  <T extends { id: string; }>(query?: QueryBuilder<T>, options?: QueryOptions): T | null
  Purpose: Suspends until the first-row query resolves.
- **useAuthState** (function) — packages/jazz-tools/src/react-core/use-auth-state.ts
  (): AuthStateInfo
- **useDb** (function) — packages/jazz-tools/src/react-core/provider.tsx
  <TDb = unknown>(): TDb
  Purpose: Returns the Jazz `Db` used to read and write data.
- **useJazzClient** (function) — packages/jazz-tools/src/react-core/provider.tsx
  (): CoreJazzClient
- **useSession** (function) — packages/jazz-tools/src/react-core/provider.tsx
  (): Session | null
  Purpose: Returns the current `Session`, including the user's ID, claims and
  authentication mode.

#### `jazz-tools/react-native` — `react-native/index.ts`

The shared `Db` entries in this section are re-exported runtime types. The
high-level React Native provider does not make the `@internal`
`createWithBrowserWorker` method available as a supported native construction
path; it is browser-only. `deleteClientStorage` is likewise browser-only
storage maintenance, not a native/server setup method.

- **createDb** (function) — packages/jazz-tools/src/react-native/create-db.ts
  (config: ReactNativeDbConfig): Promise<Db>
- **createJazzClient** (function) — packages/jazz-tools/src/react-native/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **Db** (class) — packages/jazz-tools/src/runtime/db.ts
  export class Db
  Purpose: High-level database interface for typed queries and mutations.
  Public members:
  - **initLocalFirstAuth** — packages/jazz-tools/src/runtime/db.ts
    (seed: string, ttlSeconds: number, refresh?: boolean): void
  - **create** — packages/jazz-tools/src/runtime/db.ts
    (config: DbConfig, runtimeSource: AnyRuntimeSource): Db
    Internal factory; use the exported `createDb(config)` entrypoint instead.
  - **createWithBrowserWorker** — packages/jazz-tools/src/runtime/db.ts
    (config: DbConfig, runtimeSource: AnyRuntimeSource): Promise<Db>
    `@internal`, browser-only worker construction; not a React Native setup method.
  - **updateAuthToken** — packages/jazz-tools/src/runtime/db.ts
    (jwtToken: string | null): void
  - **updateCookieSession** — packages/jazz-tools/src/runtime/db.ts
    (cookieSession: Session | null): void
  - **getAuthState** — packages/jazz-tools/src/runtime/db.ts
    (): AuthState
  - **getLocalFirstIdentityProof** — packages/jazz-tools/src/runtime/db.ts
    (options?: { ttlSeconds?: number; audience?: string; }): string | null
  - **onAuthChanged** — packages/jazz-tools/src/runtime/db.ts
    (listener: (state: AuthState) => void): () => void
  - **onMutationError** — packages/jazz-tools/src/runtime/db.ts
    (listener: (event: MutationErrorEvent) => void): () => void
  - **getConfig** — packages/jazz-tools/src/runtime/db.ts
    (): DbConfig
  - **setDevMode** — packages/jazz-tools/src/runtime/db.ts
    (enabled: boolean): void
  - **disconnect** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
  - **reconnect** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
  - **getActiveQuerySubscriptions** — packages/jazz-tools/src/runtime/db.ts
    (): ActiveQuerySubscriptionTrace[]
    `@internal`, devtools/inspector diagnostics only.
  - **onActiveQuerySubscriptionsChange** — packages/jazz-tools/src/runtime/db.ts
    (listener: ActiveQuerySubscriptionTraceListener): () => void
    `@internal`, devtools/inspector diagnostics only.
  - **getRuntimeSchema** — packages/jazz-tools/src/runtime/db.ts
    (): WasmSchema | null
    Devtools/inspector runtime-schema accessor; not a general schema API.
  - **insert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): WriteResult<T>
  - **restore** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Init, options?: RestoreOptions): WriteResult<T>
  - **upsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): WriteHandle
  - **update** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>, options?: UpdateOptions): WriteHandle
  - **delete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, options?: DeleteOptions): WriteHandle
  - **canInsert** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionAdvice>
  - **canRead** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice>
  - **canUpdate** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): Promise<PermissionAdvice>
  - **canDelete** — packages/jazz-tools/src/runtime/db.ts
    <T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice>
  - **beginTransaction** — packages/jazz-tools/src/runtime/db.ts
    (): Transaction<"mergeable">
  - **beginExclusiveTransaction** — packages/jazz-tools/src/runtime/db.ts
    (): Transaction<"exclusive">
  - **transaction** — packages/jazz-tools/src/runtime/db.ts
    <TResult>(callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>): Promise<WriteResult<Awaited<TResult>>>
  - **exclusiveTransaction** — packages/jazz-tools/src/runtime/db.ts
    <TResult>(callback: (tx: TransactionScope<"exclusive">) => TResult | Promise<TResult>): Promise<ExclusiveWriteResult<Awaited<TResult>>>
  - **deleteClientStorage** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
    Browser-only OPFS/local-storage maintenance; not a React Native setup method.
  - **logout** — packages/jazz-tools/src/runtime/db.ts
    (options?: LogoutOptions): Promise<void>
  - **all** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>
  - **one** — packages/jazz-tools/src/runtime/db.ts
    <T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>
  - **subscribeAll** — packages/jazz-tools/src/runtime/db.ts
    <T extends { id: string; }>(query: QueryBuilder<T>, callback: (delta: SubscriptionDelta<T>) => void, options?: QueryOptions, session?: Session): () => void
  - **shutdown** — packages/jazz-tools/src/runtime/db.ts
    (): Promise<void>
- **DbConfig** (type) — packages/jazz-tools/src/react-native/create-db.ts
  export type DbConfig = ReactNativeDbConfig;
- **JazzClient** (interface) — packages/jazz-tools/src/react-native/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/react-native/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/react-native/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/react-native/create-jazz-client.ts
    (): Promise<void>
- **JazzClientProvider** (function) — packages/jazz-tools/src/react-core/provider.tsx
  ({ client: clientPromise, onJWTExpired, children, }: JazzClientProviderProps): any
  Purpose: Makes a Jazz client available to children components through a React context.
- **JazzClientProviderProps** (type) — packages/jazz-tools/src/react-core/provider.tsx
  export type JazzClientProviderProps = {
  client: Promise<CoreJazzClient> | CoreJazzClient;
  onJWTExpired?: JwtRefreshFn;
  children: ReactNode;
  };
- **JazzProvider** (function) — packages/jazz-tools/src/react-native/provider.tsx
  ({ config, fallback, children, onJWTExpired }: JazzProviderProps): any
- **JazzProviderProps** (type) — packages/jazz-tools/src/react-native/provider.tsx
  export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
  };
- **LocalFirstAuth** (interface) — packages/jazz-tools/src/react-core/use-local-first-auth.ts
  export interface LocalFirstAuth
  Public members:
  - **secret** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    secret: string | null;
  - **isLoading** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    isLoading: boolean;
  - **login** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (secret: string): Promise<void>
  - **signOut** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (): Promise<void>
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR** (const) — packages/jazz-tools/src/react-native/storage.ts
  const REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR: "React Native persistent storage is not available in this alpha; memory mode is unverified scaffolding, not device-supported persistence"
- **REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR** (const) — packages/jazz-tools/src/react-native/storage.ts
  const REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR: "ReactNativeDbConfig.sqliteStorage is proposal-only and cannot be used by the v2 runtime; remove sqliteStorage (memory mode remains unverified scaffolding)"
- **REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR** (const) — packages/jazz-tools/src/react-native/storage.ts
  const REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR: "React Native SQLite storage is not implemented in this alpha; the v2 runtime rejects sqliteStorage before opening a driver"
- **ReactNativeSqliteConnection** (interface) — packages/jazz-tools/src/react-native/storage.ts
  export interface ReactNativeSqliteConnection extends ReactNativeSqliteTransaction
  Public members:
  - **transaction** — packages/jazz-tools/src/react-native/storage.ts
    <T>(callback: (transaction: ReactNativeSqliteTransaction) => Promise<T> | T): Promise<T>
  - **close** — packages/jazz-tools/src/react-native/storage.ts
    (): Promise<void>
- **ReactNativeSqliteStorageDriver** (interface) — packages/jazz-tools/src/react-native/storage.ts
  export interface ReactNativeSqliteStorageDriver
  Purpose: ordered-KV runtime is available. Proposal-only storage ABI. The v2 runtime cannot consume this driver and
  Public members:
  - **type** — packages/jazz-tools/src/react-native/storage.ts
    readonly type: "react-native-sqlite";
  - **open** — packages/jazz-tools/src/react-native/storage.ts
    (databaseName: string): Promise<ReactNativeSqliteConnection>
  - **deleteDatabase** — packages/jazz-tools/src/react-native/storage.ts
    (databaseName: string): Promise<void>
- **ReactNativeSqliteTransaction** (interface) — packages/jazz-tools/src/react-native/storage.ts
  export interface ReactNativeSqliteTransaction
  Public members:
  - **execute** — packages/jazz-tools/src/react-native/storage.ts
    (sql: string, params?: readonly unknown[]): Promise<void>
  - **query** — packages/jazz-tools/src/react-native/storage.ts
    <T = unknown>(sql: string, params?: readonly unknown[]): Promise<readonly T[]>
- **RuntimeSourcesConfig** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface RuntimeSourcesConfig
  Purpose: Runtime source overrides for Jazz WASM and worker startup.
  Public members:
  - **baseUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Base URL for Jazz runtime files.
    -
    - When set, Jazz derives \`jazz_wasm_bg.wasm\` and the browser broker worker.
      \*/
      baseUrl?: string;
  - **wasmUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the WASM binary. Overrides \`baseUrl\`. _/
    wasmUrl?: string;
  - **brokerWorkerUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the browser broker SharedWorker entry script. Overrides \`baseUrl\`. _/
    brokerWorkerUrl?: string;
  - **wasmSource** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit in-memory WASM source bytes. Overrides URL-based resolution. _/
    wasmSource?: BufferSource;
  - **wasmModule** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit compiled WASM module. Highest-precedence bootstrap input. _/
    wasmModule?: WebAssembly.Module;
- **schema** (namespace) — packages/jazz-tools/src/index.ts
  const schema: RuntimeSchemaNamespace
- **UnimplementedSqliteStorageDriver** (class) — packages/jazz-tools/src/react-native/storage.ts
  export class UnimplementedSqliteStorageDriver implements ReactNativeSqliteStorageDriver
  Public members:
  - **type** — packages/jazz-tools/src/react-native/storage.ts
    readonly type = "react-native-sqlite" as const;
  - **open** — packages/jazz-tools/src/react-native/storage.ts
    (\_databaseName: string): Promise<ReactNativeSqliteConnection>
  - **deleteDatabase** — packages/jazz-tools/src/react-native/storage.ts
    (\_databaseName: string): Promise<void>
- **useAll** (function) — packages/jazz-tools/src/react-core/use-all.ts
  <T extends { id: string; } = { id: string; }>(): UseAllNoQueryResult
  <T extends { id: string; } = { id: string; }>(query: undefined, options?: QueryOptions): UseAllNoQueryResult
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): UseAllResult<T>
  <T extends { id: string; }>(query: QueryBuilder<T> | undefined, options?: QueryOptions): UseAllResult<T> | UseAllNoQueryResult
  `UseAllNoQueryResult` is an inferred return shape and is not a named export.
  Purpose: - `data` is `undefined` until the query resolves or if the query fails.
- **useAllSuspense** (function) — packages/jazz-tools/src/react-core/use-all.ts
  <T extends { id: string; }>(query?: QueryBuilder<T>, options?: QueryOptions): T[]
  Purpose: Suspends until the query is executed.
- **useOne** (function) — packages/jazz-tools/src/react-core/use-one.ts
  <T extends { id: string; } = { id: string; }>(): UseOneNoQueryResult
  <T extends { id: string; } = { id: string; }>(query: undefined, options?: QueryOptions): UseOneNoQueryResult
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): UseOneResult<T>
  <T extends { id: string; }>(query: QueryBuilder<T> | undefined, options?: QueryOptions): UseOneResult<T> | UseOneNoQueryResult
  Purpose: Subscribes to the first matching row. `data` is `undefined` while
  loading, the row when found, or `null` after an empty result.
- **useOneSuspense** (function) — packages/jazz-tools/src/react-core/use-one.ts
  <T extends { id: string; }>(query?: QueryBuilder<T>, options?: QueryOptions): T | null
  Purpose: Suspends until the first-row query resolves.
- **useDb** (function) — packages/jazz-tools/src/react-native/provider.tsx
  (): Db
- **useJazzClient** (function) — packages/jazz-tools/src/react-native/provider.tsx
  (): JazzClientContextValue
- **useLocalFirstAuth** (function) — packages/jazz-tools/src/react-native/use-local-first-auth.ts
  (options?: UseLocalFirstAuthOptions): import("./use-local-first-auth.js").LocalFirstAuth
- **UseLocalFirstAuthOptions** (type) — packages/jazz-tools/src/react-native/use-local-first-auth.ts
  export type UseLocalFirstAuthOptions = Pick<BrowserAuthSecretStoreOptions, "key" | "appId" | "userId" | "sessionId"> & {
  store?: AuthSecretStore;
  };
- **useSession** (function) — packages/jazz-tools/src/react-core/provider.tsx
  (): Session | null
  Purpose: Returns the current `Session`, including the user's ID, claims and
  authentication mode.

#### `jazz-tools/react` — `react/index.ts`

- **AuthStateInfo** (interface) — packages/jazz-tools/src/react-core/use-auth-state.ts
  export interface AuthStateInfo
  Public members:
  - **authMode** — packages/jazz-tools/src/react-core/use-auth-state.ts
    authMode: AuthMode;
  - **userId** — packages/jazz-tools/src/react-core/use-auth-state.ts
    userId: string | null;
  - **claims** — packages/jazz-tools/src/react-core/use-auth-state.ts
    claims: Record<string, unknown>;
  - **error** — packages/jazz-tools/src/react-core/use-auth-state.ts
    error?: AuthFailureReason;
- **createJazzClient** (function) — packages/jazz-tools/src/web/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **JazzClient** (interface) — packages/jazz-tools/src/web/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/web/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/web/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/web/create-jazz-client.ts
    (): Promise<void>
- **JazzClientProvider** (function) — packages/jazz-tools/src/react-core/provider.tsx
  ({ client: clientPromise, onJWTExpired, children, }: JazzClientProviderProps): any
  Purpose: Makes a Jazz client available to children components through a React context.
- **JazzClientProviderProps** (type) — packages/jazz-tools/src/react-core/provider.tsx
  export type JazzClientProviderProps = {
  client: Promise<CoreJazzClient> | CoreJazzClient;
  onJWTExpired?: JwtRefreshFn;
  children: ReactNode;
  };
- **JazzProvider** (function) — packages/jazz-tools/src/react/provider.tsx
  (props: JazzProviderProps): any
- **JazzProviderProps** (type) — packages/jazz-tools/src/react/provider.tsx
  export type JazzProviderProps = {
  config: DbConfig;
  auth?: undefined;
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
  /** Dev-only: auto-open the inspector overlay. Default true. \*/
  autoAttachDevTools?: boolean;
  } | {
  config: Omit<DbConfig, "secret" | "jwtToken" | "cookieSession">;
  auth: "local-first";
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
  /** Dev-only: auto-open the inspector overlay. Default true. \*/
  autoAttachDevTools?: boolean;
  };
  Purpose: Creates a configured client. With `auth="local-first"`, the
  provider owns secret creation and shows `fallback` until the client is ready.
- **LocalFirstAuth** (interface) — packages/jazz-tools/src/react-core/use-local-first-auth.ts
  export interface LocalFirstAuth
  Public members:
  - **secret** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    secret: string | null;
  - **isLoading** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    isLoading: boolean;
  - **login** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (secret: string): Promise<void>
  - **signOut** — packages/jazz-tools/src/react-core/use-local-first-auth.ts
    (): Promise<void>
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **RuntimeSourcesConfig** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface RuntimeSourcesConfig
  Purpose: Runtime source overrides for Jazz WASM and worker startup.
  Public members:
  - **baseUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Base URL for Jazz runtime files.
    -
    - When set, Jazz derives \`jazz_wasm_bg.wasm\` and the browser broker worker.
      \*/
      baseUrl?: string;
  - **wasmUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the WASM binary. Overrides \`baseUrl\`. _/
    wasmUrl?: string;
  - **brokerWorkerUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the browser broker SharedWorker entry script. Overrides \`baseUrl\`. _/
    brokerWorkerUrl?: string;
  - **wasmSource** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit in-memory WASM source bytes. Overrides URL-based resolution. _/
    wasmSource?: BufferSource;
  - **wasmModule** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit compiled WASM module. Highest-precedence bootstrap input. _/
    wasmModule?: WebAssembly.Module;
- **useAll** (function) — packages/jazz-tools/src/react-core/use-all.ts
  <T extends { id: string; } = { id: string; }>(): UseAllNoQueryResult
  <T extends { id: string; } = { id: string; }>(query: undefined, options?: QueryOptions): UseAllNoQueryResult
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): UseAllResult<T>
  <T extends { id: string; }>(query: QueryBuilder<T> | undefined, options?: QueryOptions): UseAllResult<T> | UseAllNoQueryResult
  `UseAllNoQueryResult` is an inferred return shape and is not a named export.
  Purpose: - `data` is `undefined` until the query resolves or if the query fails.
- **useAllSuspense** (function) — packages/jazz-tools/src/react-core/use-all.ts
  <T extends { id: string; }>(query?: QueryBuilder<T>, options?: QueryOptions): T[]
  Purpose: Suspends until the query is executed.
- **useOne** (function) — packages/jazz-tools/src/react-core/use-one.ts
  <T extends { id: string; } = { id: string; }>(): UseOneNoQueryResult
  <T extends { id: string; } = { id: string; }>(query: undefined, options?: QueryOptions): UseOneNoQueryResult
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): UseOneResult<T>
  <T extends { id: string; }>(query: QueryBuilder<T> | undefined, options?: QueryOptions): UseOneResult<T> | UseOneNoQueryResult
  Purpose: Subscribes to the first matching row. `data` is `undefined` while
  loading, the row when found, or `null` after an empty result.
- **useOneSuspense** (function) — packages/jazz-tools/src/react-core/use-one.ts
  <T extends { id: string; }>(query?: QueryBuilder<T>, options?: QueryOptions): T | null
  Purpose: Suspends until the first-row query resolves.
- **useAuthState** (function) — packages/jazz-tools/src/react-core/use-auth-state.ts
  (): AuthStateInfo
- **useDb** (function) — packages/jazz-tools/src/react/provider.tsx
  (): CreatedJazzClient["db"]
  Purpose: Returns the Jazz `Db` used to read and write data.
- **useJazzClient** (function) — packages/jazz-tools/src/react/provider.tsx
  (): JazzClientContextValue
- **useLocalFirstAuth** (function) — packages/jazz-tools/src/react/use-local-first-auth.ts
  (options?: UseLocalFirstAuthOptions): import("./use-local-first-auth.js").LocalFirstAuth
- **UseLocalFirstAuthOptions** (type) — packages/jazz-tools/src/react/use-local-first-auth.ts
  export type UseLocalFirstAuthOptions = Pick<BrowserAuthSecretStoreOptions, "key" | "appId" | "userId" | "sessionId">;
- **useSession** (function) — packages/jazz-tools/src/react-core/provider.tsx
  (): Session | null
  Purpose: Returns the current `Session`, including the user's ID, claims and
  authentication mode.

#### `jazz-tools/shared` — `shared/index.ts`

- **applyDelta** (function) — packages/jazz-tools/src/reconcile-array.ts
  <T extends { id: string; }>(target: T[], delta: SubscriptionDelta<T>): void
  Purpose: Apply a subscription delta to a reactive array, deep-merging only
- **applySubscriptionDelta** (function) — packages/jazz-tools/src/runtime/subscription-manager.ts
  <T extends { id: string; }>(current: T[], delta: SubscriptionDelta<T>): T[]
  Purpose: Canonical reducer for subscription streams. Consumers own the materialized
- **CacheEntryHandle** (type — exported but marked internal/dev) — packages/jazz-tools/src/subscriptions-orchestrator.ts
  export type CacheEntryHandle<T extends {
  id: string;
  }> = {
  readonly state: UseAllState<T>;
  readonly status: UseAllState<T>["status"];
  readonly promise: TrackedPromise<T[]>;
  readonly error: unknown;
  subscribe(callbacks: QueryEntryCallbacks<T>): () => void;
  };
- **QueryBuilder** (interface) — packages/jazz-tools/src/runtime/db.ts
  export interface QueryBuilder<T>
  Purpose: Interface that QueryBuilder classes implement.
  Public members:
  - **\_table** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Table name for this query _/
    readonly \_table: string;
  - **\_schema** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Schema reference for translation and transformation _/
    readonly \_schema: WasmSchema;
  - **\_columnTransforms** — packages/jazz-tools/src/runtime/db.ts
    /\*_ Optional TypeScript-only per-column transforms carried by typed query handles. _/
    readonly \_columnTransforms?: ColumnTransformMap;
  - **\_build** — packages/jazz-tools/src/runtime/db.ts
    (): string
  - **\_rowType** — packages/jazz-tools/src/runtime/db.ts
    /\*_ @internal Phantom brand — enables TypeScript to infer T from usage _/
    readonly \_rowType: T;
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **reconcileArray** (function) — packages/jazz-tools/src/reconcile-array.ts
  <T extends { id: string; }>(target: T[], source: T[]): void
  Purpose: Reconcile a target array in-place to match a source array,
- **RowChangeKind** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  const RowChangeKind: { readonly Added: 0; readonly Removed: 1; readonly Updated: 2; }
- **RowDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type RowDelta<T> = {
  kind: RowChangeKind["Added"];
  id: string;
  index: number;
  item: T;
  } | {
  kind: RowChangeKind["Removed"];
  id: string;
  index: number;
  } | {
  kind: RowChangeKind["Updated"];
  id: string;
  index: number;
  item?: T;
  };
- **SubscriptionDelta** (type) — packages/jazz-tools/src/runtime/subscription-manager.ts
  export type SubscriptionDelta<T> = {
  /** Complete result after applying this delta, when available. \*/
  all?: T[];
  /** Ordered list of changes for this delta. _/
  delta: RowDelta<T>[];
  reset?: false;
  } | {
  /\*\* Complete replacement result after applying this reset delta. _/
  all: T[];
  /** Ordered list of changes for this delta. \*/
  delta: RowDelta<T>[];
  /** True when this delta replaces all previously observed state. \*/
  reset: true;
  };
- **UseAllState** (type — exported but marked internal/dev) — packages/jazz-tools/src/subscriptions-orchestrator.ts
  export type UseAllState<T extends {
  id: string;
  }> = UseAllStatePending<T> | UseAllStatefulfilledData<T> | UseAllStateError<T>;

#### `jazz-tools/solid` — `solid/index.ts`

- **createJazzClient** (function) — packages/jazz-tools/src/web/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **createSolidJazzClient** (function) — packages/jazz-tools/src/solid/create-solid-jazz-client.ts
  (config: Accessor<DbConfig>): PendingSolidJazzClient
- **DurabilityTier** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Persistence tier for durability guarantees.
  -
  - - \`local\`: Persisted in local durable storage
  - - \`edge\`: Persisted at edge server
  - - \`global\`: Persisted at global server
      \*/
      export type DurabilityTier = "local" | "edge" | "global";
      Purpose: Persistence tier for durability guarantees.
- **JazzClient** (interface) — packages/jazz-tools/src/web/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/web/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/web/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/web/create-jazz-client.ts
    (): Promise<void>
- **JazzProvider** (function) — packages/jazz-tools/src/solid/provider.tsx
  (props: JazzProviderProps): any
- **JazzProviderProps** (type) — packages/jazz-tools/src/solid/provider.tsx
  export type JazzProviderProps = {
  client: PendingSolidJazzClient;
  fallback?: JSX.Element;
  children: JSX.Element;
  autoAttachDevTools?: boolean;
  };
- **PendingSolidJazzClient** (type) — packages/jazz-tools/src/solid/create-solid-jazz-client.ts
  export type PendingSolidJazzClient = {
  readonly db: Db | undefined;
  readonly session: Session | null;
  readonly authState: AuthState | null;
  shutdown(): Promise<void>;
  readonly loading: boolean;
  readonly error: unknown;
  readonly state: unknown;
  };
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **RuntimeSourcesConfig** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface RuntimeSourcesConfig
  Purpose: Runtime source overrides for Jazz WASM and worker startup.
  Public members:
  - **baseUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Base URL for Jazz runtime files.
    -
    - When set, Jazz derives \`jazz_wasm_bg.wasm\` and the browser broker worker.
      \*/
      baseUrl?: string;
  - **wasmUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the WASM binary. Overrides \`baseUrl\`. _/
    wasmUrl?: string;
  - **brokerWorkerUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the browser broker SharedWorker entry script. Overrides \`baseUrl\`. _/
    brokerWorkerUrl?: string;
  - **wasmSource** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit in-memory WASM source bytes. Overrides URL-based resolution. _/
    wasmSource?: BufferSource;
  - **wasmModule** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit compiled WASM module. Highest-precedence bootstrap input. _/
    wasmModule?: WebAssembly.Module;
- **SolidJazzClient** (type) — packages/jazz-tools/src/solid/create-solid-jazz-client.ts
  export type SolidJazzClient = Prettify<PendingSolidJazzClient & {
  db: Db;
  }>;
- **useAll** (function) — packages/jazz-tools/src/solid/use-all.ts
  <T extends { id: string; }>(args: Accessor<{ query: QueryBuilder<T> | undefined; options?: QueryOptions | undefined; }>): UseAllResult<T>
- **useOne** (function) — packages/jazz-tools/src/solid/use-one.ts
  <T extends { id: string; }>(args: Accessor<{ query: QueryBuilder<T> | undefined; options?: QueryOptions | undefined; }>): UseOneResult<T>
  Purpose: Subscribes to the first matching row. `data` is `undefined` while
  loading, the row when found, or `null` after an empty result.
- **useAuthState** (function) — packages/jazz-tools/src/solid/provider.tsx
  (): () => import("../index.js").AuthState | null
- **useDb** (function) — packages/jazz-tools/src/solid/provider.tsx
  <TDb = Db>(): Accessor<TDb>
- **useJazzClient** (function) — packages/jazz-tools/src/solid/provider.tsx
  (): JazzClientContextValue
- **useLocalFirstAuth** (function) — packages/jazz-tools/src/solid/use-local-first-auth.ts
  (store?: Accessor<AuthSecretStore>): { readonly secret: any; readonly isLoading: any; readonly error: any; login(nextSecret: string): Promise<void>; signOut(): Promise<void>; }
- **useSession** (function) — packages/jazz-tools/src/solid/provider.tsx
  (): Accessor<Session | null>

#### `jazz-tools/svelte` — `svelte/index.ts`

- **AuthSecretStore** (interface) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export interface AuthSecretStore
  Purpose: Interface for platform-appropriate auth secret persistence.
  Public members:
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string>
- **BrowserAuthSecretStore** (class) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export class BrowserAuthSecretStore implements AuthSecretStore
  Purpose: AuthSecretStore backed by localStorage.
  Public members:
  - **constructor** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): BrowserAuthSecretStore
  - **getDefault** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): BrowserAuthSecretStore
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (): Promise<string>
  - **loadSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<string | null>
  - **saveSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (secret: string, options?: BrowserAuthSecretStoreOptions): Promise<void>
  - **clearSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<void>
  - **getOrCreateSecret** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    (options?: BrowserAuthSecretStoreOptions): Promise<string>
- **BrowserAuthSecretStoreOptions** (interface) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  export interface BrowserAuthSecretStoreOptions
  Public members:
  - **key** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ localStorage key name (default: "jazz-auth-secret") _/
    key?: string;
  - **appId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional app identifier to namespace the default key. _/
    appId?: string;
  - **userId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional principal identifier to isolate secrets per user. _/
    userId?: string | null;
  - **sessionId** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Optional session identifier for per-session isolation. _/
    sessionId?: string | null;
  - **storage** — packages/jazz-tools/src/runtime/auth-secret-store.ts
    /\*_ Override storage backend (for testing) _/
    storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
- **createJazzClient** (function) — packages/jazz-tools/src/web/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **DurabilityTier** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Persistence tier for durability guarantees.
  -
  - - \`local\`: Persisted in local durable storage
  - - \`edge\`: Persisted at edge server
  - - \`global\`: Persisted at global server
      \*/
      export type DurabilityTier = "local" | "edge" | "global";
      Purpose: Persistence tier for durability guarantees.
- **generateAuthSecret** (function) — packages/jazz-tools/src/runtime/auth-secret-store.ts
  (): string
  Purpose: Generate a new 32-byte auth secret as a base64url string.
- **getDb** (function) — packages/jazz-tools/src/svelte/context.svelte.ts
  (): Db
  Purpose: Returns the Jazz `Db` used to read and write data.
- **getJazzContext** (function) — packages/jazz-tools/src/svelte/context.svelte.ts
  (): JazzContext
  Purpose: Returns the current Jazz context, including its `Db` and session
  snapshot.
- **getSession** (function) — packages/jazz-tools/src/svelte/context.svelte.ts
  (): { readonly current: Session | null; }
  Purpose: Subscribes to the current Jazz `Session`.
- **JazzClient** (interface) — packages/jazz-tools/src/web/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/web/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/web/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/web/create-jazz-client.ts
    (): Promise<void>
- **JazzContext** (interface) — packages/jazz-tools/src/svelte/context.svelte.ts
  export interface JazzContext
  Public members:
  - **db** — packages/jazz-tools/src/svelte/context.svelte.ts
    db: Db | null;
  - **session** — packages/jazz-tools/src/svelte/context.svelte.ts
    session: Session | null;
  - **subscriptionStore** — packages/jazz-tools/src/svelte/context.svelte.ts
    /\*_ @internal Used by framework bindings; not part of the app-facing client API. _/
    subscriptionStore: SubscriptionStore | null;
- **JazzSvelteProvider** (component) — packages/jazz-tools/src/svelte/JazzSvelteProvider.svelte
  Component<{
  client: JazzClient | Promise<JazzClient>;
  children: Snippet<[{ db: Db }]>;
  fallback?: Snippet;
  autoAttachDevTools?: boolean;
  }>
- **LocalFirstAuth** (class) — packages/jazz-tools/src/svelte/local-first-auth.svelte.ts
  export class LocalFirstAuth
  Purpose: Manages a local-first authentication secret. It uses the shared
  browser store by default; pass a custom store to isolate storage.
  Public members:
  - **secret** — packages/jazz-tools/src/svelte/local-first-auth.svelte.ts
    secret: string | null = $state(null);
  - **isLoading** — packages/jazz-tools/src/svelte/local-first-auth.svelte.ts
    isLoading: boolean = $state(true);
  - **login** — packages/jazz-tools/src/svelte/local-first-auth.svelte.ts
    login: (secret: string) => Promise<void>;
  - **signOut** — packages/jazz-tools/src/svelte/local-first-auth.svelte.ts
    signOut: () => Promise<void>;
  - **constructor** — packages/jazz-tools/src/svelte/local-first-auth.svelte.ts
    (store?: AuthSecretStore): LocalFirstAuth
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **QuerySubscription** (class) — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
  export class QuerySubscription<T extends { id: string }> extends QuerySubscriptionBase<T, T[]>
  Purpose: Reactive multi-row query subscription. Results are available through `.current`.
  Public members:
  - **current** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    current: T[] | undefined = $state();
  - **isLoading** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    isLoading: boolean = $state(true);
  - **error** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    error: Error | null = $state(null);
  - **constructor** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    <T extends { id: string; }>(query: MaybeGetter<QueryBuilder<T> | undefined>, options?: MaybeGetter<QueryOptions | undefined>): QuerySubscription<T>
- **QuerySubscriptionOne** (class) — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
  export class QuerySubscriptionOne<T extends { id: string }> extends QuerySubscriptionBase<T, T | null>
  Purpose: Reactive single-row query subscription. `.current` is `undefined`
  while loading, the first matching row when found, or `null` after an empty result.
  Public members:
  - **current** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    current: T | null | undefined = $state();
  - **isLoading** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    isLoading: boolean = $state(true);
  - **error** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    error: Error | null = $state(null);
  - **constructor** — packages/jazz-tools/src/svelte/query-subscription.svelte.ts
    <T extends { id: string; }>(query: MaybeGetter<QueryBuilder<T> | undefined>, options?: MaybeGetter<QueryOptions | undefined>): QuerySubscriptionOne<T>
- **RuntimeSourcesConfig** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface RuntimeSourcesConfig
  Purpose: Runtime source overrides for Jazz WASM and worker startup.
  Public members:
  - **baseUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Base URL for Jazz runtime files.
    -
    - When set, Jazz derives \`jazz_wasm_bg.wasm\` and the browser broker worker.
      \*/
      baseUrl?: string;
  - **wasmUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the WASM binary. Overrides \`baseUrl\`. _/
    wasmUrl?: string;
  - **brokerWorkerUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the browser broker SharedWorker entry script. Overrides \`baseUrl\`. _/
    brokerWorkerUrl?: string;
  - **wasmSource** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit in-memory WASM source bytes. Overrides URL-based resolution. _/
    wasmSource?: BufferSource;
  - **wasmModule** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit compiled WASM module. Highest-precedence bootstrap input. _/
    wasmModule?: WebAssembly.Module;

#### `jazz-tools/testing` — `testing/index.ts`

- **createPolicyTestApp** (function) — packages/jazz-tools/src/testing/policy-test-app.ts
  (app: PolicyTestAppSchema, permissions: CompiledPermissions, expectFn: ExpectLike): Promise<PolicyTestApp>
  Preferred factory: starts the local server, deploys the schema catalogue,
  creates the backend context, and returns the ready test app. The
  `PolicyTestAppSchema` and `ExpectLike` aliases are internal to this source
  module; pass the app's `{ wasmSchema }`, compiled permissions, and your test
  framework's `expect` function.
- **deploy** (function) — packages/jazz-tools/src/dev/catalogue.ts
  (options: DeployOptions): Promise<DeployResult>
  Purpose: Publishes a schema and optional permissions.
- **DeployOptions** (interface) — packages/jazz-tools/src/dev/catalogue.ts
  export interface DeployOptions extends CatalogueServerOptions
  Public members:
  - **schema** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Current schema. Will only be published if not already stored on the server.
      \*/
      schema: SchemaSourceInput;
  - **permissions** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Permissions to publish. Omitting this param restricts \`deploy\` to only publish the schema.
      \*/
      permissions?: CompiledPermissionsMap;
  - **migration** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Migration between the current server schema and the new schema.
    - Only published if there's no existing migration between these schemas.
    - In order to publish migrations, provide {@link permissions} as well.
      \*/
      migration?: DefinedMigration;
  - **noVerify** — packages/jazz-tools/src/dev/catalogue.ts
    /\*\*
    - Set to \`true\` to publish permissions even if a migration is missing between
    - the current server schema and the new schema.
      \*/
      noVerify?: boolean;
- **LocalJazzServerHandle** (interface) — packages/jazz-tools/src/dev/dev-server.ts
  export interface LocalJazzServerHandle
  Public members:
  - **appId** — packages/jazz-tools/src/dev/dev-server.ts
    appId: string;
  - **port** — packages/jazz-tools/src/dev/dev-server.ts
    port: number;
  - **url** — packages/jazz-tools/src/dev/dev-server.ts
    url: string;
  - **dataDir** — packages/jazz-tools/src/dev/dev-server.ts
    dataDir: string;
  - **adminSecret** — packages/jazz-tools/src/dev/dev-server.ts
    adminSecret: string;
  - **backendSecret** — packages/jazz-tools/src/dev/dev-server.ts
    backendSecret: string;
  - **stop** — packages/jazz-tools/src/dev/dev-server.ts
    stop: () => Promise<void>;
- **mergePermissionsIntoWasmSchema** (function) — packages/jazz-tools/src/schema-permissions.ts
  (schema: WasmSchema, compiledPermissions: CompiledPermissionsMap): WasmSchema
- **PolicyTestApp** (class) — packages/jazz-tools/src/testing/policy-test-app.ts
  export class PolicyTestApp
  Purpose: A test app for permissions tests. Simplifies setting up a test app and provides methods
  Public members:
  - **constructor** — packages/jazz-tools/src/testing/policy-test-app.ts
    (expect: ExpectLike, app: any, jazzContext: JazzContext, server: LocalJazzServerHandle): PolicyTestApp
    Advanced constructor that accepts internal lifecycle handles. For normal
    policy tests, use the `createPolicyTestApp` function above.
  - **seed** — packages/jazz-tools/src/testing/policy-test-app.ts
    <T>(callback: (db: Db) => SeedWrite<T>): Promise<T>
  - **as** — packages/jazz-tools/src/testing/policy-test-app.ts
    (session: Session): TestDb
  - **shutdown** — packages/jazz-tools/src/testing/policy-test-app.ts
    (): Promise<void>
- **startLocalJazzServer** (function) — packages/jazz-tools/src/dev/dev-server.ts
  (options?: StartLocalJazzServerOptions): Promise<LocalJazzServerHandle>
  Purpose: idempotent `stop()` method that shuts the server down and releases owned
- **StartLocalJazzServerOptions** (interface) — packages/jazz-tools/src/dev/dev-server.ts
  export interface StartLocalJazzServerOptions
  Public members:
  - **appId** — packages/jazz-tools/src/dev/dev-server.ts
    appId?: string;
  - **port** — packages/jazz-tools/src/dev/dev-server.ts
    port?: number;
  - **dataDir** — packages/jazz-tools/src/dev/dev-server.ts
    dataDir?: string;
  - **inMemory** — packages/jazz-tools/src/dev/dev-server.ts
    inMemory?: boolean;
  - **jwksUrl** — packages/jazz-tools/src/dev/dev-server.ts
    jwksUrl?: string;
  - **backendSecret** — packages/jazz-tools/src/dev/dev-server.ts
    backendSecret?: string;
  - **adminSecret** — packages/jazz-tools/src/dev/dev-server.ts
    adminSecret?: string;
  - **upstreamUrl** — packages/jazz-tools/src/dev/dev-server.ts
    upstreamUrl?: string;
  - **allowLocalFirstAuth** — packages/jazz-tools/src/dev/dev-server.ts
    allowLocalFirstAuth?: boolean;
  - **telemetryCollectorUrl** — packages/jazz-tools/src/dev/dev-server.ts
    telemetryCollectorUrl?: string;
  - **enableLogs** — packages/jazz-tools/src/dev/dev-server.ts
    enableLogs?: boolean;
  - **schema** — packages/jazz-tools/src/dev/dev-server.ts
    schema?: Uint8Array;
- **startTestJwtIssuer** (function) — packages/jazz-tools/src/testing/test-jwt-issuer.ts
  (): Promise<TestJwtIssuerHandle>
  Purpose: Start a local JWKS endpoint for tests and mint JWTs signed by its key.
- **TestDb** (type) — packages/jazz-tools/src/testing/policy-test-app.ts
  /\*\*
  - Db used for testing permissions.
  - Supports all {@link Db} operations plus helpers for client-local write
  - staging and serving-authority rejection. A rejected write briefly exists as
  - an optimistic local batch, but is not persisted by the server.
    _/
    export type TestDb = Db & {
    /\*\*
    _ Assert that the callback does not throw while staging its write locally.
    _ Write operations performed inside the callback are not persisted.
    _/
    expectAllowed(callback: TestDbMethodCallback): void;
    /\*\*
    _ Assert that a write is rejected by the serving authority.
    _
    _ Client writes are admitted optimistically, so this checks the write's edge
    _ receipt rather than expecting synchronous local permission enforcement.
    \*/
    expectDenied(callback: (db: Db) => PendingWrite): Promise<void>;
    };
    Purpose: Db used for testing permissions.
- **TestJwtIssuerHandle** (interface) — packages/jazz-tools/src/testing/test-jwt-issuer.ts
  export interface TestJwtIssuerHandle
  Public members:
  - **jwksUrl** — packages/jazz-tools/src/testing/test-jwt-issuer.ts
    jwksUrl: string;
  - **jwtForUser** — packages/jazz-tools/src/testing/test-jwt-issuer.ts
    jwtForUser: (userId: string, claims?: Record<string, unknown>, options?: {
    expiresInSeconds?: number;
    issuer?: string;
    }) => string;
  - **stop** — packages/jazz-tools/src/testing/test-jwt-issuer.ts
    stop: () => Promise<void>;

#### `jazz-tools/vue` — `vue/index.ts`

- **createJazzClient** (function) — packages/jazz-tools/src/web/create-jazz-client.ts
  (config: DbConfig): Promise<JazzClient>
- **DurabilityTier** (type) — packages/jazz-tools/src/runtime/client.ts
  /\*\*
  - Persistence tier for durability guarantees.
  -
  - - \`local\`: Persisted in local durable storage
  - - \`edge\`: Persisted at edge server
  - - \`global\`: Persisted at global server
      \*/
      export type DurabilityTier = "local" | "edge" | "global";
      Purpose: Persistence tier for durability guarantees.
- **JazzClient** (interface) — packages/jazz-tools/src/web/create-jazz-client.ts
  export interface JazzClient
  Public members:
  - **db** — packages/jazz-tools/src/web/create-jazz-client.ts
    db: Db;
  - **session** — packages/jazz-tools/src/web/create-jazz-client.ts
    session: Session | null;
  - **shutdown** — packages/jazz-tools/src/web/create-jazz-client.ts
    (): Promise<void>
- **JazzClientContextValue** (type) — packages/jazz-tools/src/vue/provider.ts
  export type JazzClientContextValue = CreatedJazzClient;
- **JazzProvider** (const) — packages/jazz-tools/src/vue/provider.ts
  const JazzProvider: any
- **JazzProviderProps** (interface) — packages/jazz-tools/src/vue/provider.ts
  export interface JazzProviderProps
  Public members:
  - **client** — packages/jazz-tools/src/vue/provider.ts
    client: CreatedJazzClient | Promise<CreatedJazzClient>;
  - **autoAttachDevTools** — packages/jazz-tools/src/vue/provider.ts
    autoAttachDevTools?: boolean;
- **QueryOptions** (type) — packages/jazz-tools/src/runtime/db.ts
  export type QueryOptions = QueryExecutionOptions;
- **RuntimeSourcesConfig** (interface) — packages/jazz-tools/src/runtime/context.ts
  export interface RuntimeSourcesConfig
  Purpose: Runtime source overrides for Jazz WASM and worker startup.
  Public members:
  - **baseUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*\*
    - Base URL for Jazz runtime files.
    -
    - When set, Jazz derives \`jazz_wasm_bg.wasm\` and the browser broker worker.
      \*/
      baseUrl?: string;
  - **wasmUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the WASM binary. Overrides \`baseUrl\`. _/
    wasmUrl?: string;
  - **brokerWorkerUrl** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit URL for the browser broker SharedWorker entry script. Overrides \`baseUrl\`. _/
    brokerWorkerUrl?: string;
  - **wasmSource** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit in-memory WASM source bytes. Overrides URL-based resolution. _/
    wasmSource?: BufferSource;
  - **wasmModule** — packages/jazz-tools/src/runtime/context.ts
    /\*_ Explicit compiled WASM module. Highest-precedence bootstrap input. _/
    wasmModule?: WebAssembly.Module;
- **useAll** (function) — packages/jazz-tools/src/vue/use-all.ts
  <T extends { id: string; }>(query: MaybeRefOrGetter<QueryBuilder<T> | undefined>, options?: MaybeRefOrGetter<QueryOptions | undefined>): UseAllResult<T>
  Purpose: Reads all matching rows and subscribes to query changes. `data` is
  undefined until the query resolves; `error` contains subscription failures.
- **useOne** (function) — packages/jazz-tools/src/vue/use-one.ts
  <T extends { id: string; }>(query: MaybeRefOrGetter<QueryBuilder<T> | undefined>, options?: MaybeRefOrGetter<QueryOptions | undefined>): UseOneResult<T>
  Purpose: Subscribes to the first matching row. `data.value` is `undefined`
  while loading, the row when found, or `null` after an empty result.
- **useOneSuspense** (function) — packages/jazz-tools/src/vue/use-one.ts
  <T extends { id: string; }>(query: QueryBuilder<T>, options?: QueryOptions): Promise<UseOneSuspenseResult<T>>
  Purpose: Resolves when the first-row query is ready for Vue Suspense.
- **useDb** (function) — packages/jazz-tools/src/vue/provider.ts
  (): Db
  Purpose: Returns the Jazz `Db` used to read and write data.
- **useJazzClient** (function) — packages/jazz-tools/src/vue/provider.ts
  (): JazzClientContextValue
  Purpose: Returns the current Jazz client, including its `Db`, session
  snapshot and shutdown helper.
- **useLocalFirstAuth** (function) — packages/jazz-tools/src/vue/use-local-first-auth.ts
  (store?: AuthSecretStore): UseLocalFirstAuth
  Purpose: Manages a local-first authentication secret. It uses the shared
  browser store by default; pass a custom store to isolate storage.
- **UseLocalFirstAuth** (interface) — packages/jazz-tools/src/vue/use-local-first-auth.ts
  export interface UseLocalFirstAuth
  Public members:
  - **secret** — packages/jazz-tools/src/vue/use-local-first-auth.ts
    secret: Ref<string | null>;
  - **isLoading** — packages/jazz-tools/src/vue/use-local-first-auth.ts
    isLoading: Ref<boolean>;
  - **login** — packages/jazz-tools/src/vue/use-local-first-auth.ts
    login: (secret: string) => Promise<void>;
  - **signOut** — packages/jazz-tools/src/vue/use-local-first-auth.ts
    signOut: () => Promise<void>;
- **useSession** (function) — packages/jazz-tools/src/vue/provider.ts
  (): ComputedRef<Session | null>
  Purpose: Subscribes to the current Jazz `Session`.

### `schema` namespace

The root `schema` value combines `col` with the schema helpers. It provides:

- column builders: `string()`, `boolean()`, `int()`, `bigint()`,
  `timestamp()`, `float()`, `bytes()`, `json(...)`, `enum(...)`,
  `ref(targetTable)` and `array(element)`
- migration helpers: `add`, `drop`, `rename(oldName)` and
  `renameFrom(oldName)`
- schema helpers: `table`, `defineSchema`, `defineApp`, `defineSliceableApp`,
  `defineMigration`, `renameTableFrom` and `definePermissions`

The root `col` export and the function entries above show the exact inferred
types.

The namespace also exports these types (source `src/index.ts–215`):

- `schema.TableDefinition` and `schema.SchemaDefinition`
- `schema.Schema<TSchema>`, `schema.App<TSchema>` and
  `schema.SliceableApp<TSchema>`
- `schema.RowOf<TTable>`, `schema.InsertOf<TTable>` and
  `schema.WhereOf<TQuery>`

### CoJSON is not in this snapshot

This checkout has no `cojson` package, package manifest, TypeScript source or
public entry file at the target commit. It contains only historical references
in the `jazz-rn` changelog. This guide therefore cannot list a supported CoJSON
TypeScript API. Check the separately published package or its repository if
that is the API you need.

### How the TypeScript API was checked

`pnpm --filter jazz-tools build` generated and checked the current TypeScript,
React Native, Svelte and Solid declarations. The catalogue was compared with
every emitted package entry point. The only emitted names not listed as
standalone entries are the deliberately omitted generic inference/support
types described at the start of this guide and package-private test-reset
helpers exported from development-only entry points.

The generated Svelte declaration confirms these provider props:

- `client: JazzClient | Promise<JazzClient>`
- `children: Snippet<[{ db: Db }]>`
- optional `fallback`
- optional `autoAttachDevTools`

See `packages/jazz-tools/dist/svelte/JazzSvelteProvider.svelte.d.ts`.

## Rust and native API catalogue

Use the application sections for database, schema, query and session work.
The internals, engine, transport and binding sections are for maintainers and
specialist integrations.

### Supported Rust API

For Rust application code, use the thread-affine `jazz::db::Db<S>` type and the
schema, query and session types exported by `jazz::tools::public_schema` and
`jazz::tools::*`. The API specification covers opening a database, writing,
querying and subscribing. It assigns transport and tick methods to bindings
(`crates/jazz/SPEC/13_db_api.md`, sections 13.1–13.5).

The relevant public entry files are:

- `crates/jazz/src/lib.rs` makes many modules public, including engine,
  protocol and server internals. Treating a module as `pub` does not make it
  supported application API.
- `crates/jazz/src/tools/mod.rs` exports the main convenience types,
  including IDs, schema, query, policy and session types. Committed
  transactions use `TransactionId`; open transactions use
  `OpenTransactionId`.
- `crates/jazz/src/tools/public_schema.rs` exports the stable schema and
  query vocabulary.
- `crates/jazz/src/tools/public_api/mod.rs` is `pub(crate)`. Import its
  exported items through `jazz::tools`, not through this module.

### `jazz` application API

The `Db` implementation is split across
`crates/jazz/src/db/{lifecycle,reads,subscriptions,mutations,transactions}.rs`
and `crates/jazz/src/db.rs`.

Opening/lifecycle (`lifecycle.rs`):

```rust
pub async fn Db::<S>::open(config: DbConfig<S>) -> Result<Self, Error>
pub async fn Db::<S>::open_history_complete(config: DbConfig<S>) -> Result<Self, Error>
pub fn Db::<S>::register_schema_view(&self, schema: JazzSchema) -> Result<Self, Error>
pub fn Db::<S>::schema_view(&self, schema_view_id: SchemaViewId) -> Result<Self, Error>
pub fn Db::<S>::schema_view_id(&self) -> SchemaViewId
pub fn Db::<S>::close(&self) -> Result<(), Error>
pub fn Db::<S>::set_non_durable_client(&self)
pub fn Db::<S>::set_initial_sync_flush_cadence(&self, cadence: InitialSyncFlushCadence)
pub fn Db::<S>::create_branch(&self) -> Result<BranchId, Error>
pub fn Db::<S>::create_branch_with_id(&self, branch: BranchId) -> Result<(), Error>
pub fn Db::<S>::write_state(&self, tx_id: TxId) -> Result<WriteState, Error>
pub async fn Db::<S>::wait_for_transaction(&self, tx_id: TxId, tier: DurabilityTier) -> Result<TxId, Error>
pub fn Db::<S>::on_mutation_error(&self, callback: MutationErrorCallback)
pub fn Db::<S>::clear_mutation_error_callback(&self)
```

`DbConfig<S>`, `DbIdentity`, `ProductionRowIdSource`, `SeededRowIdSource`, and `RowIdSource` are re-exported from `crates/jazz/src/db.rs`. `Db::open` is the ordinary non-history-complete client; `open_history_complete` is a serving/core path and should be labelled accordingly.

Queries/reads (`reads.rs`):

```rust
pub fn Db::<S>::table(&self, table: impl Into<String>) -> Query
pub fn Db::<S>::prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error>
pub fn Db::<S>::prepare_query_bound(&self, query: &Query, params: BTreeMap<String, Value>) -> Result<PreparedQuery, Error>
pub fn Db::<S>::read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error>
pub fn Db::<S>::read_profiled(&self, prepared: &PreparedQuery) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error>
pub fn Db::<S>::one(&self, prepared: &PreparedQuery) -> Result<Option<CurrentRow>, Error>
pub async fn Db::<S>::all(&self, prepared: &PreparedQuery, opts: ReadOpts) -> Result<Vec<CurrentRow>, Error>
pub async fn Db::<S>::all_for_identity(&self, prepared: &PreparedQuery, opts: ReadOpts, author: AuthorId) -> Result<Vec<CurrentRow>, Error>
pub async fn Db::<S>::all_relation_snapshot(&self, prepared: &PreparedQuery, opts: ReadOpts) -> Result<RelationSnapshot, Error>
pub async fn Db::<S>::all_relation_snapshot_for_identity(&self, prepared: &PreparedQuery, opts: ReadOpts, author: AuthorId) -> Result<RelationSnapshot, Error>
pub async fn Db::<S>::all_result_tree(&self, prepared: &PreparedQuery, opts: ReadOpts) -> Result<ResultTree, Error>
pub async fn Db::<S>::all_relation_query(&self, query: &RelationQuery, opts: ReadOpts) -> Result<RelationSnapshot, Error>
pub async fn Db::<S>::all_relation_query_for_identity(&self, query: &RelationQuery, opts: ReadOpts, author: AuthorId) -> Result<RelationSnapshot, Error>
```

Subscriptions/coverage (`subscriptions.rs`):

```rust
pub async fn Db::<S>::subscribe(&self, prepared: &PreparedQuery, opts: ReadOpts) -> Result<SubscriptionStream, Error>
pub async fn Db::<S>::subscribe_for_identity(&self, prepared: &PreparedQuery, opts: ReadOpts, author: AuthorId) -> Result<SubscriptionStream, Error>
pub async fn Db::<S>::subscribe_relation_query(&self, query: &RelationQuery, opts: ReadOpts) -> Result<SubscriptionStream, Error>
pub async fn Db::<S>::subscribe_relation_query_for_identity(&self, query: &RelationQuery, opts: ReadOpts, author: AuthorId) -> Result<SubscriptionStream, Error>
pub fn Db::<S>::attach_query_with_opts(&self, prepared: &PreparedQuery, opts: ReadOpts) -> Result<QueryAttachment, Error>
pub fn Db::<S>::attach_query_with_opts_for_identity(&self, prepared: &PreparedQuery, opts: ReadOpts, author: AuthorId) -> Result<QueryAttachment, Error>
pub fn Db::<S>::attach_query(&self, prepared: &PreparedQuery) -> Result<QueryAttachment, Error>
pub fn Db::<S>::query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool
pub fn Db::<S>::detach_query(&self, attachment: QueryAttachment)
pub async fn SubscriptionStream::next_event(&mut self) -> Option<SubscriptionEvent>
pub fn SubscriptionStream::try_next_event(&mut self) -> Option<SubscriptionEvent>
```

`SubscriptionEvent` is a public enum at `crates/jazz/src/db.rs` with `Delta { reset, publishable, added, updated, removed, terminal_operations, terminal_layout, settled, tier }`, `Rejected { reason }`, and `Closed`. `PreparedQuery::shape(&self) -> &ValidatedQuery` and `PreparedQuery::binding(&self) -> &Binding` are at `db.rs`.

Mutations (`mutations.rs`):

```rust
pub fn Db::<S>::insert(&self, table: &str, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::upsert(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::delete(&self, table: &str, row: RowUuid) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::restore(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::insert_attributed(&self, made_by: AuthorId, table: &str, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::insert_with_id_attributed(&self, made_by: AuthorId, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::insert_for_identity(&self, identity: AuthorId, table: &str, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::insert_with_id_for_identity(&self, identity: AuthorId, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::update_attributed(&self, made_by: AuthorId, table: &str, row: RowUuid, patch: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::update_for_identity(&self, identity: AuthorId, table: &str, row: RowUuid, patch: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::upsert_for_identity(&self, identity: AuthorId, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::delete_attributed(&self, made_by: AuthorId, table: &str, row: RowUuid) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::delete_for_identity(&self, identity: AuthorId, table: &str, row: RowUuid) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::restore_for_identity(&self, identity: AuthorId, table: &str, row: RowUuid, cells: RowCells) -> Result<WriteHandle<S>, Error>
pub fn Db::<S>::can_insert(&self, table: &str, cells: RowCells) -> Result<PermissionAdvice, Error>
pub fn Db::<S>::can_read(&self, table: &str, row: RowUuid) -> Result<PermissionAdvice, Error>
pub fn Db::<S>::can_update(&self, table: &str, row: RowUuid) -> Result<PermissionAdvice, Error>
pub fn Db::<S>::can_delete(&self, table: &str, row: RowUuid) -> Result<PermissionAdvice, Error>
pub fn Db::<S>::set_identity_claims(&self, identity: AuthorId, claims: BTreeMap<String, Value>)
pub fn Db::<S>::local_current_row(&self, table: &str, row: RowUuid) -> Result<Option<CurrentRow>, Error>
```

The `_at_ms` and `_for_identity_at_ms` variants are also public (`mutations.rs`) and must be either documented as provenance/testing/serving paths or deliberately omitted from an application guide. `insert_attributed` and identity variants are trusted serving paths, not ordinary client capabilities. `can_*` on a client returns `PermissionAdvice::Unknown` by design (`mutations.rs`).

Transactions (`transactions.rs`):

```rust
pub fn Db::<S>::mergeable_tx(&self) -> Result<MergeableTx<'_, S>, Error>
pub fn Db::<S>::transaction<T>(&self, callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>) -> Result<(T, TxId), Error>
pub fn Db::<S>::mergeable_tx_for_identity(&self, author: AuthorId) -> Result<MergeableTx<'_, S>, Error>
pub fn Db::<S>::transaction_for_identity<T>(&self, author: AuthorId, callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>) -> Result<(T, TxId), Error>
pub fn Db::<S>::begin_mergeable(&self, id: OpenTransactionId) -> Result<(), Error>
pub fn Db::<S>::begin_mergeable_for_identity(&self, id: OpenTransactionId, author: AuthorId) -> Result<(), Error>
pub fn Db::<S>::mergeable_tx_ref(&self, tx_id: OpenTransactionId) -> MergeableTxRef<'_, S>
pub fn Db::<S>::commit_mergeable_handle(&self, open_tx_id: OpenTransactionId) -> Result<TxId, Error>
pub fn Db::<S>::abandon_transaction_handle(&self, open_tx_id: OpenTransactionId) -> Result<(), Error>
pub fn Db::<S>::exclusive_tx(&self) -> Result<ExclusiveTx<'_, S>, Error>
pub fn Db::<S>::begin_exclusive(&self, id: OpenTransactionId) -> Result<(), Error>
pub fn Db::<S>::exclusive_tx_ref(&self, tx_id: OpenTransactionId) -> ExclusiveTxRef<'_, S>
pub fn Db::<S>::commit_exclusive_handle(&self, open_tx_id: OpenTransactionId) -> Result<TxId, Error>
pub fn Db::<S>::abandon_exclusive_handle(&self, open_tx_id: OpenTransactionId) -> Result<(), Error>
pub fn MergeableTx::commit(self) -> Result<TxId, Error>
pub fn ExclusiveTx::commit(self) -> Result<TxId, Error>
```

`MergeableTxOps<S>` and `ExclusiveTxOps<S>` are public traits defining read/insert/update/upsert/delete/restore operations for both owning and non-owning transaction handles (`db.rs`). `WriteHandle<S>` exposes `row_uuid() -> RowUuid`, `mergeable_tx_id() -> TxId`, `wait(tier: DurabilityTier) -> impl Future<Output=Result<TxId, Error>>`, and `write_state() -> Result<WriteState, Error>` (`db.rs`).

Sync/binding-facing methods (`lifecycle.rs`):

```rust
pub fn Db::<S>::connect_upstream(&self, transport: Box<dyn Transport>) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>)
pub fn Db::<S>::set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>)
pub fn Db::<S>::schedule_tick(&self, urgency: TickUrgency)
pub fn Db::<S>::request_permission_advice(&self, action: PermissionAdviceAction) -> PermissionAdviceFuture
pub fn Db::<S>::cancel_permission_advice_request(&self, request_id: PermissionAdviceRequestId)
pub fn Db::<S>::accept_subscriber(&self, transport: Box<dyn Transport>, identity: AuthorId) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::accept_subscriber_with_claims(&self, transport: Box<dyn Transport>, identity: AuthorId, claims: BTreeMap<String, Value>) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::accept_subscriber_with_claims_and_trust(&self, transport: Box<dyn Transport>, identity: AuthorId, claims: BTreeMap<String, Value>, trust: CommitUnitTrust) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::accept_edge_subscriber_with_claims(&self, transport: Box<dyn Transport>, identity: AuthorId, claims: BTreeMap<String, Value>) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::accept_edge_authority_subscriber_with_claims(&self, transport: Box<dyn Transport>, identity: AuthorId, claims: BTreeMap<String, Value>) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::accept_subscriber_with_resume(&self, transport: Box<dyn Transport>, identity: AuthorId, cursor: ResumeCursor) -> Rc<RefCell<PeerConnection<S>>>
pub fn Db::<S>::detach_connection(&self, connection: &Rc<RefCell<PeerConnection<S>>>) -> bool
pub fn Db::<S>::tick(&self) -> Result<(), Error>
pub fn Db::<S>::tick_stats(&self) -> Result<DbTickStats, Error>
```

`Transport`, `WireTransportAdapter`, `PeerConnection`, `ResumeCursor`, `TickScheduler`, `TickUrgency`, `ReadOpts`, `LocalUpdates`, `Propagation`, `Error`, `ErrorCode`, and callback/event types are declared/re-exported in `crates/jazz/src/db.rs`. `Transport` and `Db::tick` are binding contracts; normal app code should not need wire framing.

### Public Rust internals

The root modules expose broad implementation vocabulary. Representative externally visible items, with source paths:

- `ids.rs`: `NodeUuid`, `NodeAlias`, `SchemaVersionId`, `SchemaVersionAlias`, `PhysicalTableId`, `PhysicalColumnId`, `MigrationLensId`, `SchemaLineagePublicationId`, `BranchId`, `RowUuid`, `AuthorId` (new/from-bytes/to-bytes/as-bytes helpers).
- `schema.rs`: `JazzSchema`, `ColumnSchema`, `TableSchema`, `WritePolicies`, `MergeStrategy`, `Policy`, schema lowering/storage helpers. `JazzSchema::new`, `with_branch_read_policy`, `with_branch_write_policy`, `column_families`, `canonical_bytes`, `version_id` are at `schema.rs`; `TableSchema` construction and policy/index helpers begin at `schema.rs`.
- `node/mod.rs` and child modules: `NodeState`, `Node`, `CurrentRow`, commit/ingest/history/branch/query-engine internals. Public because low-level Rust/server code and bindings use them; not the small application SDK.
- `protocol.rs`: `SyncMessage`, `PermissionAdvice*`, catalogue/schema/lens/shape/read-view/version-carrier types, `SubscriptionKey`, `ShapeAst`, `Subscribe`, and many internal fact/result entries. `wire.rs`: `WireFrame`, `WireHello`, `WireTransport`, encode/decode/negotiate/compression functions and protocol feature constants.
- `tx.rs`: `Transaction`, `TxId`, `TxKind`, `Fate`, `RejectionReason`, `DurabilityTier`, `HistoryEntry`, `RejectedTransaction`, snapshots and row-read vocabulary.
- `result_tree.rs`: `ResultTree`, `ResultNode`, `ResultRelation` and
  `ResultTreeReplacement`. The `Db` subscription contracts are in
  `db.rs`.
- `serving/mod.rs` (feature `server`): public in-memory/server shells, `ServerBuilder`-adjacent config and lifecycle types, health/metrics/drain/report structures, and config validation. This is operational/server API, not app CRUD.
- `binding_codec.rs`: public row/relation/subscription byte codecs used by NAPI/WASM (`encode_rows`, `encode_relation_snapshot`, `encode_subscription_delta`, `row_batches`), but codec bytes are binding contracts, not a general app data model.

### Advanced Rust API: `groove`

`crates/groove/src/lib.rs` publishes `db`, `ivm`, `queries`, `records`, `schema`, `storage`, and re-exports `Intern`. Jazz also re-exports the crate as `jazz::groove` (`crates/jazz/src/lib.rs`). The crate README calls `groove::db::Database` the external entry point, but all submodules are public.

Main lower-level API (`crates/groove/src/db/facade.rs`, `db/mod.rs`,
`db/query.rs`, `db/batch.rs`):

```rust
pub fn Database::<S>::new(schema: DatabaseSchema, storage: S) -> Result<Self, Error>
pub fn Database::<S>::new_with_storage_layout(schema: DatabaseSchema, storage: S, layout: StorageLayout) -> Result<Self, Error>
pub fn Database::<S>::begin_durable_publication_scope(&mut self) -> Result<DurablePublicationScope, Error>
pub fn Database::<S>::ensure_usable(&self) -> Result<(), Error>
pub fn Database::<S>::approximate_class_bytes(&self, cf: &str) -> Result<Option<u64>, Error>
pub fn Database::<S>::into_storage(self) -> S
pub fn Database::<S>::close(&self) -> Result<(), Error>
pub fn Database::<S>::set_write_flush_cadence(&self, every: usize) -> Result<(), Error>
pub fn Database::<S>::flush_write_boundary(&self) -> Result<(), Error>
pub fn Database::<S>::set_auto_direct_family_enabled(&mut self, enabled: bool)
pub fn Database::<S>::set_tick_runtime_stats_enabled(&mut self, enabled: bool)
pub fn Database::<S>::runtime_stats(&self) -> RuntimeStats
pub fn Database::<S>::open_batch(&self) -> DatabaseBatch
pub fn Database::<S>::open_staged_batch(&mut self) -> StagedDatabaseBatch<'_, S>
pub fn Database::<S>::direct_record_store(&self, name: &str) -> Result<DirectRecordStore<'_, S>, Error>
pub fn Database::<S>::subscribe_query(&mut self, query: Query) -> Result<Subscription, Error>
pub fn Database::<S>::prepare_query(&mut self, query: Query) -> Result<PreparedShape, Error>
pub fn Database::<S>::query(&mut self, query: Query) -> Result<RecordDeltas, Error>
pub fn Database::<S>::unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool
pub fn DatabaseBatch::insert/update/delete(...)
pub fn StagedDatabaseBatch::insert/update/delete(...)
```

The exact query/batch and schema-admission signatures are in `crates/groove/src/db/{batch,query,schema_admission,storage_helpers}.rs`; records/storage are separately public. Representative exports include `RecordDescriptor`, `BorrowedRecord`, `Record`, `OwnedRecord`, `VariantRecord`, `Value`, `ValueType` (`records/mod.rs`, `records/values.rs`), `MemoryStorage`, platform-specific `OpfsStorage` or `NativeBtreeStorage`, `OrderedKvStorage`, `BoxedStorage`, `StorageFactory`, `RecordStore`, `StorageTransaction`, `StorageLayout`, and storage delta/write-operation types (`storage/mod.rs`). `RocksDbStorage` now lives in the separate `jazz-storage-rocksdb` crate. These are lower-level engine contracts and should not be presented as the Jazz app API without an explicit “Groove/advanced Rust” section.

### Node binding: `jazz-napi`

Package entrypoint: `crates/jazz-napi/package.json` (`main: index.js`, `types: index.d.ts`; NAPI build name `@garden-co/jazz-napi`). The checked-in generated declaration file is the most precise externally callable contract: `crates/jazz-napi/index.d.ts`.

Externally callable classes/signatures in that declaration:

- `JazzServer.start(options: { appId: string; backendSecret: string; adminSecret: string; port?: number; dataDir?: string; inMemory?: boolean; jwksUrl?: string; allowLocalFirstAuth?: boolean; upstreamUrl?: string; telemetryCollectorUrl?: string; schema?: Buffer | Uint8Array | number[] }): Promise<JazzServer>`; getters `appId/url/port/dataDir/backendSecret/adminSecret`; `stop(): Promise<void>`.
- `NapiDb.openMemory(schema: Uint8Array, config: Uint8Array): NapiDb`; `openPersistent(dataPath: string, schema: Uint8Array, config: Uint8Array): NapiDb`; schema/transaction methods `registerSchema`, `attachMergeableTx`, `attachExclusiveTx`, `beginTransaction`, `commitTransaction`, `rollbackTransaction`; scheduler/error callbacks; `prepareQuery`.
- Napi reads: `all`, `allForIdentity`, `allRelationSnapshot`, `allRelationSnapshotForIdentity`, `allRelationQuery`, `allRelationQueryForIdentity`, `localCurrentRow`, each returning `Uint8Array` and taking the declaration’s `{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }` options where shown.
- Napi subscriptions/coverage: `attachQuery`, `attachQueryForIdentity`, `queryAttachmentIsCovered`, `detachQuery`, `subscribe`, `subscribeForIdentity`, `subscribeRelationQuery`, `subscribeRelationQueryForIdentity`.
- Napi encoded writes: `insertWithIdEncoded`, `insertWithIdEncodedForIdentity`, `updateEncoded`, `updateEncodedForIdentity`, `upsertEncoded`, `upsertEncodedForIdentity`, `delete`, `deleteForIdentity`, `restoreEncoded`, `restoreEncodedForIdentity`; each returns `Write`.
- Napi runtime/transport: `tick`, `setNonDurableClient`, `connectUpstream`, `connectUpstreamWithSession(protocolVersion: number, features: number, remoteNode: Buffer, remoteEpoch: bigint, localNode: Buffer, localEpoch: bigint): Transport`, `mergeableTx`, `mergeableTxForIdentity`, `close`.
- `PreparedQuery` and `QueryAttachment` are opaque classes. `Subscription.readAll()/drain(): Array<SubscriptionEvent>`, `close(): boolean`. `Transport.sendWireFrame`, `sendWireFrames`, `recvWireFrames`, `tick`, `close`. `Tx` has encoded CRUD plus `commit(): Write` and `rollback(): void`. `Write` has `batchId`, `payload`, `writeState(): any`, `wait(tier: string): Promise<undefined>`, `close(): boolean`.
- Test/support and identity exports are also published: `TestJwtIssuer.start(): Promise<TestJwtIssuer>`, `jwksUrl`, `jwtForUser`, `stop`; `mintLocalFirstToken(seedB64: string, audience: string, ttlSeconds: number): string`; `verifyLocalFirstIdentityProof(token: string | undefined | null, expectedAudience: string): VerifyTokenResult`.
- Event/type unions in the declaration include `SubscriptionDeltaEvent`, `SubscriptionRejectedEvent`, `SubscriptionClosedEvent`, rejection reasons/codes, terminal operation/layout/edit/path structures, and `VerifyTokenResult` (`ok`, `id`, optional `error`).

The Rust-oriented list above is non-exhaustive. The complete consumer-facing JavaScript/TypeScript contract is the auto-generated declaration at `crates/jazz-napi/index.d.ts`; the exact public declaration is reproduced here for lookup:

```ts
/* auto-generated by NAPI-RS */
/* eslint-disable */
export declare class JazzServer {
  static start(options: {
    appId: string;
    backendSecret: string;
    adminSecret: string;
    port?: number;
    dataDir?: string;
    inMemory?: boolean;
    jwksUrl?: string;
    allowLocalFirstAuth?: boolean;
    upstreamUrl?: string;
    telemetryCollectorUrl?: string;
    schema?: Buffer | Uint8Array | number[];
  }): Promise<JazzServer>;
  get appId(): string;
  get url(): string;
  get port(): number;
  get dataDir(): string;
  get backendSecret(): string;
  get adminSecret(): string;
  stop(): Promise<void>;
}

export declare class NapiDb {
  static openMemory(schema: Uint8Array, config: Uint8Array): NapiDb;
  static openPersistent(dataPath: string, schema: Uint8Array, config: Uint8Array): NapiDb;
  /** Register and return a typed view backed by this same runtime owner. */
  registerSchema(schema: Uint8Array): NapiDb;
  /**
   * Attach a schema view to an owner-wide mergeable batch without opening,
   * committing, or abandoning that batch.
   */
  attachMergeableTx(openBatchId: string): Tx;
  /** Attach a schema view to an existing owner-wide exclusive batch. */
  attachExclusiveTx(openBatchId: string): Tx;
  /** Begin one owner-wide batch without creating an owning per-schema Tx. */
  beginTransaction(openBatchId: string, kind: string, author?: Uint8Array | undefined | null): void;
  /** Commit an owner-wide batch by id and optional kind. */
  commitTransaction(openBatchId: string, kind?: string | undefined | null): Write;
  /** Roll back an owner-wide open batch by id. */
  rollbackTransaction(openBatchId: string): void;
  setTickScheduler(callback: (err: Error | null, arg: string) => void): void;
  onMutationError(callback: (event: any) => void): void;
  prepareQuery(query: Uint8Array): PreparedQuery;
  all(
    query: PreparedQuery,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Uint8Array;
  setIdentityClaims(author: Uint8Array, claims?: Record<string, unknown> | undefined | null): void;
  allForIdentity(
    query: PreparedQuery,
    author: Uint8Array,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Uint8Array;
  allRelationSnapshot(
    query: PreparedQuery,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Uint8Array;
  allRelationSnapshotForIdentity(
    query: PreparedQuery,
    author: Uint8Array,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Uint8Array;
  allRelationQuery(
    queryJson: string,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Uint8Array;
  allRelationQueryForIdentity(
    queryJson: string,
    author: Uint8Array,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Uint8Array;
  localCurrentRow(table: string, rowId: Uint8Array): Uint8Array;
  attachQuery(query: PreparedQuery, opts?: any | undefined | null): QueryAttachment;
  attachQueryForIdentity(
    query: PreparedQuery,
    author: Uint8Array,
    opts?: any | undefined | null,
  ): QueryAttachment;
  queryAttachmentIsCovered(attachment: QueryAttachment): boolean;
  detachQuery(attachment: QueryAttachment): void;
  subscribe(
    query: PreparedQuery,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Subscription;
  subscribeForIdentity(
    query: PreparedQuery,
    author: Uint8Array,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Subscription;
  subscribeRelationQuery(
    queryJson: string,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Subscription;
  subscribeRelationQueryForIdentity(
    queryJson: string,
    author: Uint8Array,
    opts?:
      | { tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean }
      | undefined
      | null,
  ): Subscription;
  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  insertWithIdEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  updateEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  upsertEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | undefined | null): Write;
  deleteForIdentity(
    table: string,
    rowId: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  restoreEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): Write;
  tick(): void;
  setNonDurableClient(): void;
  connectUpstream(): Transport;
  connectUpstreamWithSession(
    protocolVersion: number,
    features: number,
    remoteNode: Buffer,
    remoteEpoch: bigint,
    localNode: Buffer,
    localEpoch: bigint,
  ): Transport;
  mergeableTx(openBatchId: string): Tx;
  mergeableTxForIdentity(openBatchId: string, author: Uint8Array): Tx;
  close(): void;
}

export declare class PreparedQuery {}

export declare class QueryAttachment {}

export declare class Subscription {
  readAll(): Array<SubscriptionEvent>;
  drain(): Array<SubscriptionEvent>;
  close(): boolean;
}

export declare class TestJwtIssuer {
  static start(): Promise<TestJwtIssuer>;
  get jwksUrl(): string;
  jwtForUser(
    userId: string,
    claims?: Record<string, unknown> | undefined,
    options?: { expiresInSeconds?: number; issuer?: string } | undefined,
  ): string;
  stop(): Promise<void>;
}

export declare class Transport {
  sendWireFrame(frame: Uint8Array): void;
  sendWireFrames(frames: Array<Uint8Array>): void;
  recvWireFrames(): Array<Uint8Array>;
  tick(): number;
  close(): boolean;
}

export declare class Tx {
  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): void;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): void;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): void;
  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | undefined | null): void;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | undefined | null,
  ): void;
  commit(): Write;
  rollback(): void;
}

export declare class Write {
  get batchId(): string;
  get payload(): Uint8Array;
  writeState(): any;
  wait(tier: string): Promise<undefined>;
  close(): boolean;
}

export declare function mintLocalFirstToken(
  seedB64: string,
  audience: string,
  ttlSeconds: number,
): string;

export interface SubscriptionClosedEvent {
  type: "closed";
}

export interface SubscriptionDeltaEvent {
  type: "delta";
  reset: boolean;
  delta: Uint8Array;
  terminalOperations: Array<SubscriptionTerminalOperation>;
  terminalLayouts: Array<SubscriptionTerminalLayout>;
  settled: boolean;
  tier: "None" | "Local" | "Edge" | "Global";
}

export type SubscriptionEvent =
  | SubscriptionDeltaEvent
  | SubscriptionRejectedEvent
  | SubscriptionClosedEvent;

export interface SubscriptionRejectedEvent {
  type: "rejected";
  reason: SubscriptionRejectionReason;
}

export type SubscriptionRejectionReason =
  | SubscriptionUnsupportedShapeCapabilityReason
  | SubscriptionShapeRegistrationPendingReason
  | SubscriptionServerFailureReason;

export interface SubscriptionServerFailureReason {
  type: "ServerFailure";
  code:
    | "TableNotFound"
    | "SchemaResolution"
    | "QueryValidation"
    | "QueryLowering"
    | "PolicyEvaluation"
    | "Internal";
}

export interface SubscriptionShapeRegistrationPendingReason {
  type: "ShapeRegistrationPendingCatalogueAdmission";
}

export interface SubscriptionTerminalCollectionPathSegment {
  Collection: string;
}

export type SubscriptionTerminalEdit =
  | SubscriptionTerminalInsertEdit
  | SubscriptionTerminalUpdateEdit
  | SubscriptionTerminalRemoveEdit
  | SubscriptionTerminalMoveEdit;

export interface SubscriptionTerminalInsert {
  index: number;
  key: Array<number>;
  value: Array<number>;
}

export interface SubscriptionTerminalInsertEdit {
  Insert: SubscriptionTerminalInsert;
}

export interface SubscriptionTerminalKeyPathSegment {
  Key: Array<number>;
}

/**
 * Immutable producer-owned root record contract.  The descriptor and public
 * slots are published once per NAPI subscription, before an operation may
 * reference `id`; TypeScript never has to infer a CurrentRow/layout family.
 */
export interface SubscriptionTerminalLayout {
  id: string;
  rootDescriptor: Array<number>;
  rootKeySlot: number;
  rootKeyFieldName: string;
  publicFields: Array<SubscriptionTerminalPublicField>;
  carrier: string;
}

export interface SubscriptionTerminalMove {
  key: Array<number>;
  index: number;
}

export interface SubscriptionTerminalMoveEdit {
  Move: SubscriptionTerminalMove;
}

export interface SubscriptionTerminalOperation {
  rootLayoutId: string;
  root_key: Array<number>;
  path: Array<SubscriptionTerminalPathSegment>;
  edit: SubscriptionTerminalEdit;
}

export type SubscriptionTerminalPathSegment =
  | SubscriptionTerminalCollectionPathSegment
  | SubscriptionTerminalKeyPathSegment;

export interface SubscriptionTerminalPublicField {
  name: string;
  descriptorFieldName: string;
  slot: number;
  carrier: string;
}

export interface SubscriptionTerminalRemove {
  key: Array<number>;
}

export interface SubscriptionTerminalRemoveEdit {
  Remove: SubscriptionTerminalRemove;
}

export interface SubscriptionTerminalUpdate {
  key: Array<number>;
  value: Array<number>;
}

export interface SubscriptionTerminalUpdateEdit {
  Update: SubscriptionTerminalUpdate;
}

export interface SubscriptionUnsupportedShapeCapabilityReason {
  type: "UnsupportedShapeCapability";
  detail: string;
}

export declare function verifyLocalFirstIdentityProof(
  token: string | undefined | null,
  expectedAudience: string,
): VerifyTokenResult;

export interface VerifyTokenResult {
  ok: boolean;
  id: string;
  error?: string;
}
```

Source attributes are in `crates/jazz-napi/src/lib.rs`. Treat `index.d.ts` as the binding contract and Rust `pub` helpers as implementation unless annotated `#[napi]`.

### WebAssembly binding: `jazz-wasm`

Package entrypoint is generated: `crates/jazz-wasm/package.json` publishes `pkg/jazz_wasm.js`, `pkg/jazz_wasm.d.ts`, and `pkg/jazz_wasm_bg.wasm`; `pkg/` is not checked into this revision. Rust exports are in `crates/jazz-wasm/src/lib.rs`.

Module-level `wasm_bindgen` functions:

```rust
pub fn init()
pub fn generate_id() -> String                 // JS generateId
pub fn current_timestamp() -> u64              // JS currentTimestamp
pub fn mint_local_first_token(seed_b64: String, audience: String, ttl_seconds: u32, now_seconds: u64) -> Result<String, JsValue>
pub fn derive_user_id(seed_b64: String) -> Result<String, JsValue>
pub fn mint_anonymous_token(seed_b64: String, audience: String, ttl_seconds: u32, now_seconds: u64) -> Result<String, JsValue>
```

Feature `bench-probes` adds five benchmark-only exports (`benchProbeArithmeticHash`, `benchProbeDynDispatch`, `benchProbeRefCellBorrow`, `benchProbeAllocChurn`, `benchProbeRandomAccessMemory`); do not document these as SDK.

Externally exported classes are `WasmDb`, `WasmTransport`, `WasmTx`, `WasmWrite`, `WasmPreparedQuery`, `QueryAttachment` (Rust `WasmQueryAttachment`), and `WasmPermissionAdviceRequest`. Exact method names/signatures are directly visible at `lib.rs`; the essential list is:

- `WasmDb.openMemory(schema: Vec<u8>, config: Vec<u8>): Result<WasmDb, JsValue>`; on `wasm32`, async `openBrowser(namespace: String, schema: Vec<u8>, config: Vec<u8>)`; `registerSchema`, `attachMergeableTx`, `attachExclusiveTx`, `beginTransaction(open_batch_id: String, kind: String, author: Option<Vec<u8>>)`; `commitTransaction`, `rollbackTransaction`; on `wasm32`, `destroyBrowserStorage`.
- Query/read methods: `prepareQuery`, `all`, `one`, `allInTransaction`, `allInTransactionForIdentity`, `oneInTransaction`, `oneInTransactionForIdentity`, `setIdentityClaims`, `allForIdentity`, `allRelationQuery`, `allRelationQueryForIdentity`, `allRelationSnapshot`, `allRelationSnapshotForIdentity`.
- Subscription/coverage methods: `subscribe`, `subscribeForIdentity`, `subscribeRelationQuery`, `subscribeRelationQueryForIdentity`, `attachQuery`, `attachQueryForIdentity`, `queryAttachmentIsCovered`, `detachQuery`, `setTickScheduler`, `onMutationError`.
- Encoded mutation/advice methods: `insertEncoded`, `canInsertEncoded`, `requestInsertPermissionAdviceEncoded`, `requestReadPermissionAdvice`, `insertWithIdEncoded`, `insertWithIdEncodedForIdentity`, `updateEncoded`, `requestUpdatePermissionAdviceEncoded`, `updateEncodedForIdentity`, `upsertEncoded`, `upsertEncodedForIdentity`, `delete`, `requestDeletePermissionAdvice`, `deleteForIdentity`, `restoreEncoded`, `restoreEncodedForIdentity`.
- Sync/transactions: `tick`, `setNonDurableClient`, `connectUpstream`, `connectUpstreamWithSession(protocol_version: u16, features: u32, remote_node: Vec<u8>, remote_epoch: u64, local_node: Vec<u8>, local_epoch: u64)`, `acceptSubscriber(identity: Vec<u8>, claims: JsValue)`, `mergeableTx`, `mergeableTxForIdentity`, `exclusiveTx`, `close`.
- `WasmTransport`: `updateAuthenticatedClaims`, `sendWireFrame`, `sendWireFrames`, `recvWireFrames`, `tick`, `close`. `WasmTx`: encoded `insertWithIdEncoded`, `updateEncoded`, `upsertEncoded`, `delete`, `restoreEncoded`, `commit`, `rollback`. `WasmWrite`: getters `batchId`, `payload`; `writeState`, `wait`, `close`. `WasmPermissionAdviceRequest`: getter `promise`, `cancel`.

The Rust list above is not complete. It describes low-level byte and JSON
methods used by `jazz-tools`. For the TypeScript API in this snapshot, use
`packages/jazz-tools/src/types/jazz-wasm.d.ts`, reproduced below.
The generated wasm-pack declaration shipped with a release takes precedence.

```ts
declare module "jazz-wasm" {
  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;
  export function generateId(): string;
  export function currentTimestamp(): bigint;
  export function deriveUserId(seedB64: string): string;
  export function mintLocalFirstToken(
    seedB64: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
  export function mintAnonymousToken(
    seedB64: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;

  export class WasmPreparedQuery {}
  export class QueryAttachment {}
  export class WasmPermissionAdviceRequest {
    readonly promise: Promise<"allowed" | "denied" | "unknown">;
    cancel(): void;
  }

  export class WasmWrite {
    readonly batchId: string;
    readonly payload: Uint8Array;
    writeState(): unknown;
    wait(tier: string): Promise<void>;
    close(): boolean;
  }

  export class WasmTransport {
    sendWireFrame(frame: Uint8Array): void;
    recvWireFrames(): Uint8Array[];
    tick(): number;
    updateAuthenticatedClaims(claims: Record<string, unknown>): void;
    close(): boolean;
  }

  export class WasmTx {
    insertWithIdEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    updateEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    upsertEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): void;
    restoreEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    commit(): WasmWrite;
    rollback(): void;
  }

  export class WasmDb {
    static openMemory(schema: Uint8Array, config: Uint8Array): WasmDb;
    static openBrowser(namespace: string, schema: Uint8Array, config: Uint8Array): Promise<WasmDb>;
    static destroyBrowserStorage(namespace: string): Promise<void>;

    registerSchema(schema: Uint8Array): WasmDb;
    beginTransaction(openBatchId: string, kind: string, author?: Uint8Array | null): void;
    commitTransaction(openBatchId: string, kind?: string | null): WasmWrite;
    rollbackTransaction(openBatchId: string): void;
    attachMergeableTx(openBatchId: string): WasmTx;
    attachExclusiveTx(openBatchId: string): WasmTx;

    prepareQuery(query: Uint8Array): WasmPreparedQuery;
    all(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    one(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    allForIdentity(query: WasmPreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
    allRelationQuery(queryJson: string, opts: unknown): Uint8Array;
    allRelationQueryForIdentity(queryJson: string, author: Uint8Array, opts: unknown): Uint8Array;
    attachQuery(query: WasmPreparedQuery, opts: unknown): QueryAttachment;
    attachQueryForIdentity(
      query: WasmPreparedQuery,
      author: Uint8Array,
      opts: unknown,
    ): QueryAttachment;
    queryAttachmentIsCovered(attachment: QueryAttachment): boolean;
    detachQuery(attachment: QueryAttachment): void;
    subscribe(query: WasmPreparedQuery, opts: unknown): ReadableStream<unknown>;
    subscribeRelationQuery(queryJson: string, opts: unknown): ReadableStream<unknown>;
    subscribeRelationQueryForIdentity(
      queryJson: string,
      author: Uint8Array,
      opts: unknown,
    ): ReadableStream<unknown>;

    insertEncoded(table: string, cells: Uint8Array): WasmWrite;
    canInsertEncoded(table: string, cells: Uint8Array): "allowed" | "denied" | "unknown";
    requestInsertPermissionAdviceEncoded(
      table: string,
      cells: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestReadPermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    insertWithIdEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): WasmWrite;
    updateEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    requestUpdatePermissionAdviceEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestDeletePermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    upsertEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): WasmWrite;
    deleteForIdentity(
      table: string,
      rowId: Uint8Array,
      author: Uint8Array,
      updatedAtMs?: number | null,
    ): WasmWrite;
    restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    restoreEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void;
    onMutationError(callback: (event: any) => void): void;
    tick(): void;
    setNonDurableClient(): void;
    close(): boolean;
    connectUpstream(): WasmTransport;
    connectUpstreamWithSession(
      protocolVersion: number,
      features: number,
      remoteNode: Uint8Array,
      remoteEpoch: bigint,
      localNode: Uint8Array,
      localEpoch: bigint,
    ): WasmTransport;
    acceptSubscriber(identity: Uint8Array, claims: Record<string, unknown>): WasmTransport;
    mergeableTx(openBatchId: string): WasmTx;
    mergeableTxForIdentity(openBatchId: string, author: Uint8Array): WasmTx;
    exclusiveTx(openBatchId: string): WasmTx;
  }
}
```

### React Native and UniFFI binding: `jazz-rn`

React Native is an active npm package (`crates/jazz-rn/package.json`). Its
Rust crate is excluded from the Cargo workspace (`Cargo.toml`) and has a
separate manifest at `crates/jazz-rn/rust/Cargo.toml`. Cargo workspace metadata
therefore does not include it, so this guide checks it separately.

Rust UniFFI export source: `crates/jazz-rn/rust/src/lib.rs`.

- Errors: `JazzRnError::{InvalidJson{message}, InvalidUuid{message}, InvalidTier{message}, Schema{message}, Runtime{message}, Internal{message}}`.
- Callback interfaces: `BatchedTickCallback::request_batched_tick(&self)`, `SubscriptionCallback::on_update(&self, delta_json: String)`, `AuthFailureCallback::on_failure(&self, reason: String)`, `MutationErrorCallback::on_error(&self, event_json: String)`.
- Object constructor: `RnRuntime::new(schema_json: String, app_id: String, jazz_env: String, user_branch: String, tier: Option<String>, data_path: Option<String>) -> Result<Arc<Self>, JazzRnError>`.
- Runtime methods: `on_batched_tick_needed(Option<Box<dyn BatchedTickCallback>>)`, `batched_tick()`, `insert(table: String, values_json: String, write_context_json: Option<String>, object_id: Option<String>) -> Result<String, JazzRnError>`, `restore(table: String, object_id: String, values_json: String, write_context_json: Option<String>) -> Result<String, JazzRnError>`, `update(_table: String, object_id: String, values_json: String, write_context_json: Option<String>) -> Result<String, JazzRnError>`, `upsert(table: String, object_id: String, values_json: String, write_context_json: Option<String>) -> Result<String, JazzRnError>`, `begin_transaction(transaction_kind: String) -> Result<String, JazzRnError>`, `rollback_transaction(transaction_id: String) -> Result<bool, JazzRnError>`, `delete_row(_table: String, object_id: String, write_context_json: Option<String>) -> Result<String, JazzRnError>`, async `wait_for_transaction(transaction_id: String, tier: String) -> Result<(), JazzRnError>`, async `query(query_json: String, session_json: Option<String>, tier: Option<String>, options_json: Option<String>) -> Result<String, JazzRnError>`, `unsubscribe(handle: u64)`, `create_subscription(query_json: String, session_json: Option<String>, tier: Option<String>) -> Result<u64, JazzRnError>`, `execute_subscription(handle: u64, callback: Box<dyn SubscriptionCallback>)`, `get_schema_hash() -> Result<String, JazzRnError>`, `on_mutation_error(Box<dyn MutationErrorCallback>)`, `commit_transaction(transaction_id: String)`, `close()`, `connect(url: String, auth_json: String)`, `disconnect()`, `update_auth(auth_json: String)`, and `on_auth_failure(Box<dyn AuthFailureCallback>)`.
- Module functions: `mint_local_first_token(seed_b64: String, audience: String, ttl_seconds: i64) -> Result<String, JazzRnError>` and `mint_anonymous_token(seed_b64: String, audience: String, ttl_seconds: i64) -> Result<String, JazzRnError>`.
- The generated TypeScript API is
  `crates/jazz-rn/src/generated/jazz_rn.ts` and is re-exported by
  `crates/jazz-rn/src/index.tsx`. `uniffiInitAsync(): Promise<void>` does
  nothing. Treat the generated `ubrn_uniffi_*` names as internal ABI details.
- The TypeScript `update` and `delete_` methods omit Rust's `_table` argument
  (`jazz_rn.ts`). The Android `installRustCrate()` and
  `cleanupRustCrate()` methods manage the native loader; they are not database
  operations (`JazzRnModule.kt:19-34`).

The Rust list above is not complete. For the consumer API, use the generated
TypeScript file at
`crates/jazz-rn/src/generated/jazz_rn.ts`. Its public
declarations are:

```ts
export function mintAnonymousToken(seedB64: string, audience: string, ttlSeconds: bigint): string;
export function mintLocalFirstToken(seedB64: string, audience: string, ttlSeconds: bigint): string;

export interface AuthFailureCallback {
  onFailure(reason: string): void;
}
export interface BatchedTickCallback {
  requestBatchedTick(): void;
}
export interface MutationErrorCallback {
  onError(eventJson: string): void;
}
export interface SubscriptionCallback {
  onUpdate(deltaJson: string): void;
}

export interface RnRuntimeInterface {
  batchedTick(): void;
  beginTransaction(transactionKind: string): string;
  close(): void;
  commitTransaction(transactionId: string): void;
  connect(url: string, authJson: string): void;
  createSubscription(
    queryJson: string,
    sessionJson: string | undefined,
    tier: string | undefined,
  ): bigint;
  delete_(objectId: string, writeContextJson: string | undefined): string;
  disconnect(): void;
  executeSubscription(handle: bigint, callback: SubscriptionCallback): void;
  getSchemaHash(): string;
  insert(
    table: string,
    valuesJson: string,
    writeContextJson: string | undefined,
    objectId: string | undefined,
  ): string;
  onAuthFailure(callback: AuthFailureCallback): void;
  onBatchedTickNeeded(callback: BatchedTickCallback | undefined): void;
  onMutationError(callback: MutationErrorCallback): void;
  query(
    queryJson: string,
    sessionJson: string | undefined,
    tier: string | undefined,
    optionsJson: string | undefined,
    asyncOpts_?: { signal: AbortSignal },
  ): Promise<string>;
  restore(
    table: string,
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  rollbackTransaction(transactionId: string): boolean;
  unsubscribe(handle: bigint): void;
  update(objectId: string, valuesJson: string, writeContextJson: string | undefined): string;
  updateAuth(authJson: string): void;
  upsert(
    table: string,
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  waitForTransaction(
    transactionId: string,
    tier: string,
    asyncOpts_?: { signal: AbortSignal },
  ): Promise<void>;
}

export class RnRuntime implements RnRuntimeInterface {
  constructor(
    schemaJson: string,
    appId: string,
    jazzEnv: string,
    userBranch: string,
    tier: string | undefined,
    dataPath: string | undefined,
  );
}
```

The generated class implements the interface methods above; `/*throws*/`
markers in the generated file mean UniFFI errors are raised as exceptions. The
`bigint` values are the JS view of Rust `i64`/`u64`; do not substitute
JavaScript `number` for token TTLs or subscription handles.

### Native transport and server crates

- `jazz-native-transport` provides WebSocket transport for bindings and
  servers. It exports `WebSocketClientError`, `WebSocketTransport`,
  `NativeWebSocketConnector`, the three connection methods and
  `negotiated_transport_metadata()`
  (`crates/jazz-native-transport/src/lib.rs`). It implements
  `NativeTransportConnector` (`:101-143`).
- `jazz-server` exports one process-level function
  (`crates/jazz-server/src/lib.rs`):

  ```rust
  pub async fn run(
      app_id_str: &str,
      port: u16,
      data_dir: &str,
      in_memory: bool,
      auth_config: AuthConfig,
      upstream_url: Option<String>,
      edge_cache_budget: Option<EdgeCacheBudget>,
      bound_port_file: Option<String>,
      shutdown_timeout: Duration,
  ) -> Result<(), Box<dyn std::error::Error>>
  ```

- `jazz-compression` exports feature-gated LZ4 and Zstandard compression
  functions (`crates/jazz-compression/src/lib.rs`). These are codec
  implementation APIs.
- `jazz-otel` exports builders and hooks for tracing, logs and metrics
  (`crates/jazz-otel/src/lib.rs`).
- `opfs-btree` exports its B-tree options, state, errors and platform file
  types (`crates/opfs-btree/src/lib.rs`). These are storage APIs.
- `jazz-testkit`, `jazz-sim`, `jazz-wasm-tracing` and
  `jazz-benchmark-guard` export test, simulation, observability and benchmark
  tools. Do not treat them as application API.

### Cargo package entry points

`cargo metadata --no-deps` reports the packages below. In this table,
“publishable” means the manifest does not set `publish = false`; it does not
confirm that the package has been released or is intended for application use.
See `Cargo.toml` for workspace membership.

| Cargo package           | publish status                                  | library/entrypoint                                                                                                     |
| ----------------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `groove`                | publishable                                     | `crates/groove/src/lib.rs`                                                                                             |
| `opfs-btree`            | publishable                                     | `crates/opfs-btree/src/lib.rs` (`cdylib`, `rlib`)                                                                      |
| `jazz`                  | publishable                                     | `crates/jazz/src/lib.rs`                                                                                               |
| `jazz-compression`      | publishable                                     | `crates/jazz-compression/src/lib.rs`                                                                                   |
| `jazz-testkit`          | publishable by manifest, test-only role         | `crates/jazz-testkit/src/lib.rs`                                                                                       |
| `jazz-native-transport` | publishable by manifest, binding/server role    | `crates/jazz-native-transport/src/lib.rs`                                                                              |
| `jazz-storage-rocksdb`  | publishable by manifest, native storage adapter | `crates/jazz-storage-rocksdb/src/lib.rs`                                                                               |
| `jazz-cli`              | publishable by manifest; process package        | `crates/jazz-cli/src/lib.rs`; bins `jazz-tools` (`src/bin/jazz-tools.rs`) and `jazz-server` (`src/bin/jazz-server.rs`) |
| `jazz-otel`             | publishable by manifest, telemetry role         | `crates/jazz-otel/src/lib.rs`                                                                                          |
| `jazz-server`           | publishable by manifest, process shell role     | `crates/jazz-server/src/lib.rs`                                                                                        |
| `jazz-sim`              | publishable by manifest, simulation role        | `crates/jazz-sim/src/lib.rs`                                                                                           |
| `jazz-wasm`             | publishable by manifest                         | `crates/jazz-wasm/src/lib.rs` (`cdylib`, `rlib`), generated wasm package                                               |
| `jazz-napi`             | publishable by manifest                         | `crates/jazz-napi/src/lib.rs` (`cdylib`), generated NAPI package                                                       |
| `jazz-wasm-tracing`     | publishable by manifest                         | `crates/wasm-tracing/src/lib.rs`                                                                                       |

Explicit `publish = false`: `jazz-benchmark-guard` (`crates/benchmark-guard/src/lib.rs`) and `jazz-storage-bench-core` (`dev/benchmarks/storage/bench-core/src/lib.rs`). `jazz-rn` is not a workspace member but its separate Rust manifest has a `cdylib`, `staticlib`, and `lib` target and is shipped through the npm package.

## Package entry points and guide limits

### Public npm package entry points

The pnpm workspace includes `packages/*`, `crates/jazz-rn`,
`crates/jazz-wasm`, `crates/jazz-napi` and `crates`
(`pnpm-workspace.yaml`). These manifests are not marked private:

| npm package                                             | manifest/entrypoint                                                                                                             | public exports                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `jazz-tools`                                            | `packages/jazz-tools/package.json`; `main=dist/index.js`, `types=dist/index.d.ts`, bin `jazz-tools`                             | `.`, `backend`, `better-auth-adapter`, `client`, `react`, `react-native`, `solid`, `vue`, `react-core`, `shared`, `permissions`, `testing`, `dev`, `_dev/schema-hash`, `dev/next`, `dev/vite`, `dev/expo`, `dev/sveltekit`, `svelte`, `expo`, `expo/polyfills`, `passphrase`, `passkey-backup`, `package.json` |
| `create-jazz`                                           | `packages/create-jazz/package.json`; `main=dist/index.js`, bin `create-jazz`, export `.`                                        | CLI/scaffolder                                                                                                                                                                                                                                                                                                 |
| `jazz-napi` (build config names `@garden-co/jazz-napi`) | `crates/jazz-napi/package.json`; `main=index.js`, `types=index.d.ts`                                                            | generated NAPI declarations in `crates/jazz-napi/index.d.ts`                                                                                                                                                                                                                                                   |
| `jazz-wasm`                                             | `crates/jazz-wasm/package.json`; `main=pkg/jazz_wasm.js`, `types=pkg/jazz_wasm.d.ts`                                            | generated wasm-bindgen package; generated files absent from this revision                                                                                                                                                                                                                                      |
| `jazz-rn`                                               | `crates/jazz-rn/package.json`; `main=lib/module/index.js`, `types=lib/typescript/src/index.d.ts`, export `.` and `package.json` | generated UniFFI React Native API                                                                                                                                                                                                                                                                              |

Do not include private workspace packages in the public API. These include
`@jazz/rust` (`crates/package.json`), `@jazz/opfs-btree`, `inspector`,
`create-jazz-e2e`, the docs, starters, examples, stress-test apps and the local
telemetry viewer.

### Limits of this guide

- This guide records source and package manifests at the pinned commit.
  `jazz-tools` declarations were built and checked. Rustdoc, wasm-pack, NAPI
  and UniFFI release artefacts were not regenerated, so conditional Rust
  features and generated release files can still change those declarations.
- The TokenSave index was stale and did not track this branch. We used it only
  to find likely files, then checked every source reference against the pinned
  worktree.
- The repository has no machine-readable distinction between supported Rust
  API and implementation-visible `pub` items. The support labels in this guide
  come from the `Db` API specification, public entry files, binding annotations,
  source comments and package roles.
- Cargo workspace metadata excludes `jazz-rn`, although its npm package is
  active. This guide checks its Rust UniFFI source and generated TypeScript,
  Android and iOS files separately.
- Cargo publish settings do not prove that a crate is intended for application
  imports.
- The native SQLite runtime and the high-level `jazz-tools/react-native` API
  are separate layers. This guide labels both.
- This guide covers only the pinned new-core snapshot. It does not assess or
  propose changes to the documentation site.

### Which generated declarations to use

- For `jazz-napi`, use the checked-in generated declaration at
  `crates/jazz-napi/index.d.ts`. The package manifest points to `index.js` and
  `index.d.ts`.
- For `jazz-wasm`, use the declarations generated in `pkg` for the release you
  ship. This snapshot has no `pkg` directory, so the guide uses the
  `#[wasm_bindgen]` source and
  `packages/jazz-tools/src/types/jazz-wasm.d.ts`.
- For `jazz-rn`, use
  `crates/jazz-rn/src/generated/jazz_rn.ts`. Treat names in
  `jazz_rn-ffi.ts` as internal ABI details.
- For `jazz-tools`, this guide uses the public source entry files and package
  export map because `dist` was not built.
- The Cargo package list comes from `cargo metadata --no-deps`. The guide adds
  `jazz-rn` from its separate Cargo manifest.
