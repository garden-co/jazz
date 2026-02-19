Released Jazz 0.20.10:
- Added optional restricted deletion mode for CoLists via schema permissions (`co.list().withPermission({ writer: "appendOnly" })`), so only manager/admin roles can delete when append-only mode is enabled.
- Added periodic full storage reconciliation to keep locally stored CoValues in sync with the server, with interruption handling and resume support.
- Improved sync load handling by prioritizing pending loads, fixing in-flight load tracking, and ensuring peers always reply to load requests even when there is no content delta.
- Added richer observability for sync and transport internals, including queue/load metrics and WebSocket ping-delay metadata/logging.
- Improved reconciliation and sync performance across storage adapters by reducing unnecessary content loading, optimizing SQLite reconciliation queries, and refining batch/ack flow behavior.
- Introduced runtime validation modes for write operations  (`strict` and `loose`) and app-level defaults via `setDefaultValidationMode()`. The current default remains `loose` (invalid writes still apply but emit a warning), and moving to `strict` is encouraged ahead of a future default change.
- Added contextual hints when CoValues fail to load due to sync configuration (`when: "never"` or `when: "signedUp"` restrictions).
- Added an optional `navigation` prop to `JazzSvelteProvider` to wait for pending CoValue syncs before SvelteKit navigations, reducing stale data on SSR pages.
- Throw immediately when calling `.create()` in groups where the current user does not have write permissions.
- Reused Expo and OP-SQLite DB clients across multiple Jazz providers to avoid duplicate clients.
- Replaced `@manuscripts/prosemirror-recreate-steps` with a local implementation to remove transitive vulnerabilities.
- **BREAKING:** Removed legacy `coField` and `Encoders` exports and completed migration to runtime schema descriptors. Apps using old schema APIs should migrate to current `co`/Zod-based schemas.
- **BREAKING:** On CoMap instances, the `in` operator now returns `true` for schema-defined keys even when the value is unset/deleted. Use `coMap.$jazz.has("key")` to check whether a value is actually set. Also fixed Hermes V1 proxy invariant issues by making internal CoValue property definitions configurable.
- Bugfix: fixed support for React Native 0.84
- Bugfix: fixed CoValues getting stuck in loading state with persistent peers by marking closed peers unavailable after a grace timeout and not treating `KNOWN`+`header: true` as completion without content.

Released Jazz 0.20.9:
- Bugfix: revert the Expo db adapter to use withTransactionAsync instead of withExclusiveTransactionAsync

Released Jazz 0.20.8:
- Improved FileStream base64 encoding performance. Up to **20x faster** in `asBase64` conversion on React Native and around **5x faster** blob conversions on all the platforms.
- Delayed CoValue content parsing in subscriptions until the value is fully downloaded, avoiding unnecessary intermediate parsing for streaming values
- Added `getOrCreateUnique` method to CoMap, CoList, and CoFeed. This provides a "get or create only" semantic — it returns an existing value as-is, and only uses the provided value when creating a new CoValue. Unlike `upsertUnique`, it does NOT update existing values. Also deprecates `loadUnique` and `upsertUnique` in favor of `getOrCreateUnique`.
- Introduced key revelations based on a group owned asymmetric key. This makes extending groups without having access to the encryption key zero-cost for the parent group.
- Added optional `name` metadata to Groups. Groups can now be created with a display name (e.g. `Group.create({ owner: account, name: "Billing" })`)
- Improved performance of writeKey revelations permission checks in groups with many writeOnlyKeys
- Bugfix: fixed `createdAt` getter to use CoValue's header
- Bugfix: fixed issue with CoRecord serialisation
- Bugfix: prevent conflicts between concurrent async SQLite transactions

Released Jazz 0.20.7:
- Bugfix: fixed a memory leak in the WebSocket outgoing queue introduced in 0.20.1 and improved queue close management

Released Jazz 0.20.6:
- Improved performance of read key lookups in groups by using cached indices instead of iterating through all keys

Released Jazz 0.20.5:
- Bugfix: fixed "TypeError: crypto.randomUUID is not a function (it is undefined)" on React Native
- Bugfix: fixed "can't access property useContext, dispatcher is null" error when using the inspector in Svelte

Released Jazz 0.20.4:
- Bugfix: infinite re-render loop when accessing nested CoValues in React hooks calls with `resolve: {}`

Released Jazz 0.20.3:
- Added caching for groups when accessing a readKey

Released Jazz 0.20.2:
- Added a Performance tab in the Jazz tools inspector
- Optimized peer reconciliation to prevent unnecessary data transfer on reconnect.

Released Jazz 0.20.1:
- Added client-side load request throttling to improve the loading experience when loading a lot of data concurrently. When a client requests more than 1k CoValues concurrently, load requests are now queued locally and sent as capacity becomes available.
- Bugfix: `setDefaultSchemaPermissions` now modifies existing CoValue schemas
- Bugfix: fixed `CoList` to return the correct length when calling `getOwnPropertyDescriptor` with `length`. Previously it was always returning 0.

Released Jazz 0.20.0:

With this release, we introduce a new, simple to use API for [permanently deleting CoValues](https://jazz.tools/docs/react/core-concepts/deleting). 

For auditing and data recovery purposes, we still recommend using soft-deletes wherever possible. However, we appreciate that for various reasons (data privacy, storage space), it may be preferable to delete data permanently. From Jazz 0.20.0 onwards, you'll be able to do this easily, and build experiences where your users can manage their own data.

Additionally, with this release we complete the migration to a pure native Rust toolchain and remove the JavaScript crypto compatibility layer. The native Rust core now runs everywhere: React Native, Edge runtimes, all server-side environments, and the web.

The JavaScript crypto implementation is much slower than native Rust crypto. Although workarounds like RNQuickCrypto for React Native improved performance, they still only wrapped certain native libraries, rather than running Jazz's full Rust crypto.

With native Rust crypto now running everywhere, Jazz delivers good performance on every platform. This also helps us speed up the migration of Jazz Core to Rust which will improve Jazz overall performance.

Changes:
- **Removed `PureJSCrypto`** from `cojson` (including the `cojson/crypto/PureJSCrypto` export).
- **Removed `RNQuickCrypto`** from `jazz-tools`.
- **No more fallback to JavaScript crypto**: if crypto fails to initialize, Jazz now throws an error instead of falling back silently.
- **React Native + Expo**: **`RNCrypto` (via `cojson-core-rn`) is now the default**.
- Optimized the JS-to-Rust communication by implementing native data type exchange, eliminating serialization overhead.
- Added permanent [CoValue deletion](https://jazz.tools/docs/react/core-concepts/deleting) with a new `deleted` loading state.
- Restricted `unique` parameters to strings or string records for deterministic serialization.
- `removeMember` now throws when the caller is unauthorized.
- React context changes: `useJazzContextValue` replaces value access, `useJazzContext` returns the manager, and nested `JazzProvider` now throws.

Full migration guide: [here](https://jazz.tools/docs/upgrade/0-20-0)

Released Jazz 0.19.22:
- Added a 512 variant for progressive image loading
- Bugfix: fixed an issue when generating image placeholders from clients using Expo Image Manipulator
- Bugfix: wait for CoValues to be synced before garbage-collecting them
- Bugfix: wait for CoValues' dependencies to be garbage-collected before collecting them. This makes accounts and groups safe to be collected

Released Jazz 0.19.21:
  - Added `useCoStates` & `useSuspenseCoStates` React hooks to load multiple CoValues at the same time
  - Added Clerk authentication support for Svelte with `useClerkAuth` hook and `JazzSvelteProviderWithClerk` component
  - Optimized initial CoValue sync, now if there is no content to be synced the sync-server won't load the CoValue in memory only their known state

Released Jazz 0.19.20:
- Added React Native passkey (WebAuthn) authentication support with new exports from `jazz-tools/react-native-core`:
  - `ReactNativePasskeyAuth`: Core auth class for passkey authentication
  - `usePasskeyAuth`: React hook for passkey auth state management
  - `PasskeyAuthBasicUI`: Ready-to-use auth UI component with dark/light mode support
  - `isPasskeySupported`: Helper to check device passkey support
  - Uses `react-native-passkey` as an optional peer dependency. Requires domain configuration (AASA for iOS, assetlinks.json for Android) for passkey verification.
- `createAs` now accepts `waitForSync`'s timeout option, and returns credentials in `onCreate` callback
- Improved storage content streaming by introducing a priority-based streaming queue, reducing main-thread blocking and prioritizing important CoValues during heavy streaming
- Bugfix: fixed serialisation of secret seed in Better Auth

Released Jazz 0.19.19:
- Added Svelte Better Auth support and upgraded Better Auth compatibility to version 1.4.7
- Added `getJazzErrorType` helper function to identify the type of Jazz error from an Error object thrown by suspense hooks. This enables error boundaries to display appropriate UI based on whether the error is "unauthorized", "unavailable", or "unknown"
- Resume interrupted CoValue sync on app restart (without requiring CoValues to be manually reloaded)
- Bugfix: Context.authenticate now doesn't replace the context if the same AccountID is already logged in

Released Jazz 0.19.18:
- Bugfix: fixed Clerk metadata schema to correctly parse the Jazz credentials. Bug introduced in 0.19.17

Released Jazz 0.19.17:
- Bugfix: fixed an issue where calling logOut multiple times concurrently could trigger duplicate logout operations

Released Jazz 0.19.16:
- Improved sync timeout error messages to include known state, peer state, and any error information when waiting for sync times out
- Bugfix: fixed a race condition in Clerk auth where the signup flow could trigger a duplicate login attempt

Released Jazz 0.19.15:
- Added a locking system for session IDs in React Native to make mounting multiple JazzProviders safer (still not advised as duplicate the data loading effort)
- Added a value.$jazz.createdBy getter to CoValues
- Bugfix: fixed coMap.getEdits() to also return deleted keys
- Bugfix: fixed an issue where spreading the uniqueness object when creating CoValues could introduce unexpected properties into the header

Released Jazz 0.19.14:
- Introduced support for 16KB page sizes to Android builds. This update ensures our Native Core remains compatible with upcoming Android hardware and Google Play standards.
- Upgraded Node-API Rust crate to 3.7.1 to mitigate potential memory leaks.

Released Jazz 0.19.13:
- Introduced a new API to define the permissions at Schema level. Docs [here](https://jazz.tools/docs/react/permissions-and-sharing/overview#defining-permissions-at-the-schema-level)!
- Bugfix: improved the session lock system for web apps. Before the first session of an account wasn't locked, and there was some race conditions in the lock algorithm that would cause a slow initialization or load failures when opening multiple tabs

Released Jazz  0.19.12:
- Bugfix: fixed the transactions detection on the inspector to not mark CoMap transactions as Group transactions
- Bugfix: fixed React warning when using `useCoState` about the promise not being cached
- Bugfix: we now close server peers before triggering the onAnonymousAccountDiscarded, to avoid having two websocket connections active at the same time
- Bugfix: on React onAnonymousAccountDiscarded is now triggered only if the related prop is provided (thanks @wizzel for the bug report)
- Updated better-sqlite3 on jazz-run to v12.5.0, to make it work with versions of Node.js higher than v22 (thanks to [antoncuranz](https://github.com/antoncuranz) for the contribution)

Released Jazz  0.19.11:
- Bugfix(breaking): changed the return type of `Account.createAs` to return also the new account credentials

Released Jazz 0.19.10:
- Added useSuspenseCoState and useSuspenseAccount hooks, to use Jazz with Suspense :saxophone: 
- Implemented a Subscription de-duplication system, now if two components request the same query we give them the same subscription
  - individual covalues subscriptions were already de-duplicated, this logic applies to the resolve queries
- Released our [native crypto adapter for React Native](https://github.com/garden-co/jazz/tree/main/crates/cojson-core-rn#readme)
  - With this one React Native apps go full native, becoming blazing fast!
  - Instructions on how to do the switch [here](https://jazz.tools/docs/react-native-expo/project-setup/providers#rncrypto)
  - After validating that the installation process works for everyone this is going to become the default

Released Jazz 0.19.8:
- improved the transaction revalidation system, now group updates should have a way smaller impact on performance
- improved error logging in subscriptions and added stacktraces on errors coming from React hooks (thanks @booorad for the contribution!)
- added `jazzConfig.setCustomErrorReporter` API to intercept subscription errors and send them to an error tracker (thanks @gabrola for the help!)
- narrowed down .load return type to not include the loading state
- Added polyfills helper to React Native and Expo exports (see https://jazz.tools/docs/react-native-expo/project-setup#add-polyfills)

Released Jazz 0.19.7:
- Bugfix: avoid migrating unauthorized CoValues

Released Jazz 0.19.6:
- Added `value.$jazz.export()` API and preloaded option in React hooks
  - This makes possible to pass CoValues from a server component to a client component, example here ([live](https://jazz-jazz-nextjs.vercel.app/) - [source](https://github.com/garden-co/jazz/tree/main/examples/jazz-nextjs/src/app)), docs are coming
- Added blake3 to the native APIs in RNQuickCrypto  (thanks @booorad for adding blake3 to eact-native-quick-crypto!)
  - This should improve performance a bit, but the result may vary depending on the app
  -  **Breaking:** Requires react-native-quick-crypto to be updated to ^1.0.0-beta.21 :exclamation:
- Changed the implementation of Account.createAs and added an onCreate hook to make it easier to setup the account root. 
  - This API should make it easier to create accounts via worker, docs are coming but until then [this test](https://github.com/garden-co/jazz/blob/0d3d4d9f4abaea3a52ac0ebfdc6943b4584a7d72/packages/jazz-tools/src/tools/tests/account.test.ts#L447) can be used as reference
  - **Breaking:** Now the API returns a loaded account instead of a controlled one to avoid memory leaks:exclamation:

Released Jazz 0.19.5:
- Improved the permission checks performance by incrementally building the parent groups info (gain will vary depending on the permissions structure)

Released Jazz 0.19.4:
- Improved the performance of CoValue creation by caching schema->coField transformations (around 7% speedup on a small schema, probably more with more complex schemas)
- Improved readability for CoPlainText's history in inspector
- Added edit support for CoPlainText in the inspector
- Bugfix: fixed "unable to add key to index 'uniqueSessions'" when using a Jazz app in multiple tabs (thanks @tobiaslins for the bug report)
- Bugfix: now ensureLoaded properly handles $onError in resolve queries (thanks @wrangelvid for the bug report)
- Bugfix: In the inspector, accounts are now identified by header's meta type

Released Jazz 0.19.3:
- Bugfix: fixed co.discriminatedUnion load for React Native
- Bugfix: Show invalid transactions in the inspector even if they are not decryptable

Released Jazz 0.19.2:
- Added editing and history rollback for co maps in our inspector :sparkles: 
- Added inline creation for CoVector
- Bugfix: prevent CoValues adding themselves as dependencies

Released Jazz 0.19.1:
- co.discriminatedUnion schemas now support resolve queries! (thanks @gabrola for this amazing contribution :rocket:)

**Jazz 0.19.0 released - Explicit CoValue loading states**

This release introduces explicit loading states when loading CoValues, as well as a new way to define how CoValues are loaded.

Changes:
- Added a new  $isLoaded field to discriminate between loaded and unloaded CoValues
- Added $jazz.loadingState to provide additional info about the loading state
- All methods and functions that load CoValues now return a MaybeLoaded<CoValue> instead of CoValue | null | undefined
- Resolve queries can now be defined at the schema level. Those queries will be used when loading CoValues, if no other resolve query is provided.
- Renamed $onError: null to $onError: "catch"
- Split the useAccount hook into three separate hooks:
- useAccount: now only returns an Account
- useLogOut: returns a function for logging out of the current account
- useAgent: returns the current agent
- Added a select option (and an optional equalityFn) to useAccount and useCoState, and removed useAccountWithSelector and useCoStateWithSelector.

You can learn more and see some usage examples of the new APIs in our [upgrade guide](https://jazz.tools/docs/react/upgrade/0-19-0)

For older release notes take a look at the #releases channel on [our Discord server](https://discord.com/invite/utDMjHYg42)
