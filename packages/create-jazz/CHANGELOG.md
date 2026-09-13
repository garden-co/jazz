# create-jazz

## 2.0.0-alpha.55

### Patch Changes

- d545098: Enforce an eight-character minimum password in the SvelteKit Better Auth starter.

## 2.0.0-alpha.54

### Patch Changes

- bc70549: Jazz now runs on Groove, a new low-level database engine built around incremental view maintenance. It shares work between similar query subscriptions and provides a foundation for improving correctness and performance. This release remains an alpha: please report anything that breaks or feels slow.

  ### Breaking storage change

  Alpha.54 changes the storage format without automatic migration from alpha.53. If you have existing production data, contact us for migration help before upgrading. For a fresh start, create a new Jazz Cloud app, or use the alpha.54 CLI with fresh server storage when self-hosting. Future storage-format changes will include automatic migration.

  ### Four major changes

  **Simpler read tiers.** Choose `local-first` for immediate local results with background sync; `remote-if-possible` for remotely confirmed state when online, local fallback when offline, and immediate local writes; or `remote` for remotely confirmed state only, including confirmation of local writes before they appear.

  **Clearer account lifecycle.** Configure your account and client once with `createJazzSession`. The session manages auth transitions, graceful shutdown and client replacement. Framework adapters expose that state without app-owned lifecycle plumbing. External authentication can use `loginOrRegisterJWT`; explicit linking remains available for attaching a fresh external identity to an existing account. Backends use the same session API with `initial: { backendSecret }` or `becomeBackend({ backendSecret })`.

  **Build-your-own branching.** Define branched table views with `branchBy` columns and explicit head/base view options. Branch identifiers can be strings or references to your own branch table, letting your app define branch metadata, workflows and row-level permissions.

  **Large values are ordinary columns.** Store files and streams in `s.bytes()`, large JSON documents in `s.json()`, and text in `s.string()`. Read complete values or stream supported ranges and JSON subpaths; update them with byte patches, appends, JSON edits or string splices. Content-based chunking in a Prolly Tree makes these operations efficient while retaining ordinary column permissions and query semantics. JSON and rich-text merge strategies are planned for a later release.

  ### API migration checklist
  - Replace `Db.subscribeAll` with `Db.subscribe` for complete current results.
  - React and React Native `useAll` now return `{ data, isLoading, error }`; `useAllSuspense` still returns rows.
  - Replace `localUpdates` and `propagation` options with read-tier selection.
  - Move account/client lifecycle handling to `createJazzSession` and the framework adapters. Linking happens outside contexts; the session gracefully shuts down the old client and syncs pending writes before replacing it.
  - `$createdBy` and `$updatedBy` are non-null `{ account, identity: { issuer, subject } }` values. Use `.account` for ownership comparisons. SYSTEM authorship uses a reserved account and issuer, with the originating node as subject.
  - Direct `jazz-wasm` consumers must await `acceptSubscriber` and `acceptSubscriberWithSelfSignedProof`.

  ### Reliability improvements

  This release also improves browser and React Native persistence, offline recovery and reconnect behavior; query correctness across relations, aggregates, branches and permissions; and authentication and framework lifecycles. It includes fixes for large JSON relation values, native runtime packaging and generated starter apps.

## 2.0.0-alpha.53

## 2.0.0-alpha.52

## 2.0.0-alpha.51

## 2.0.0-alpha.50

### Patch Changes

- f463ae9: Drop Node.js 20 support. Minimum is now Node.js 22.12 (Jod LTS). `engines.node` is set to `>=22.12` on `jazz-tools` and `create-jazz`; consumers on Node 20 will see an `EBADENGINE` warning (npm/pnpm) or a hard install failure (Yarn).

## 2.0.0-alpha.49

## 2.0.0-alpha.48

## 2.0.0-alpha.47

### Patch Changes

- bc68b95: Add three TypeScript (no framework) starters: `ts-localfirst`, `ts-hybrid`, and `ts-betterauth`. Each mirrors its `react-*` counterpart but uses direct DOM manipulation inside the Jazz subscription callback, so users can see the underlying Jazz API without a UI framework in the way. The Hono + BetterAuth server in `ts-hybrid` and `ts-betterauth` is byte-identical to the corresponding `react-*` server (enforced by the parity script).

  Also expose the existing `react-localfirst`, `react-hybrid`, and `react-betterauth` starters in the interactive picker as the "React (Vite)" framework option, and accept them via `--starter` (they were previously rejected as unknown).

## 2.0.0-alpha.46

## 2.0.0-alpha.45

### Patch Changes

- 2ee98be: Add sync protocol version checks to the WebSocket handshake so incompatible clients and servers fail with an explicit update prompt.

## 2.0.0-alpha.44

## 2.0.0-alpha.43

### Patch Changes

- 5dec68f: Advance the `create-jazz` spinner to "Provisioning Jazz Cloud app" during the dashboard call, and stop credential/banner output from concatenating onto the active spinner line.

## 2.0.0-alpha.42

## 2.0.0-alpha.41

## 2.0.0-alpha.40

### Patch Changes

- 206f0a9: The "Resolving dependencies" spinner now updates as each package resolves (e.g. `Resolving dependencies (2/5)`), so `npm create jazz` no longer appears frozen during that step.
- b988375: chore: expand scaffold test coverage to all six self-hosted starters

## 2.0.0

### Patch Changes

- c5534e1: Initial release of `create-jazz` — an interactive CLI scaffolder (`npm create jazz`) with six starter templates spanning Next.js and SvelteKit across three auth modes (local-first, hybrid, BetterAuth). Resolves `workspace:*` and `catalog:` dependency references to published versions at scaffold time.
