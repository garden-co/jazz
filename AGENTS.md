# AGENTS.md

Guidelines for AI agents working on the Jazz codebase.

## Project Overview

Jazz is a distributed database framework for building local-first applications. It provides real-time collaboration, offline support, and end-to-end encryption through CRDTs (Conflict-free Replicated Data Types). Data is stored locally and synced peer-to-peer with automatic conflict resolution.

- **Monorepo**: pnpm workspaces with Turbo build orchestration
- **Languages**: TypeScript (primary), Rust (performance-critical CRDT code in `crates/`)
- **Node.js**: 22.18.0+ required
- **Package Manager**: pnpm 10.16.1

### Key Domain Concepts

- **CoValue**: Base collaborative value type — the core data abstraction. Variants: `CoMap` (key-value), `CoList` (ordered list), `CoStream` (append-only stream), `CoPlainText` (collaborative text), `BinaryCoStream` (files/images/audio)
- **Group**: Permission and access control entity that manages read/write access to CoValues. Supports invitations via `InviteSecret`
- **Account**: User identity entity extending Group, with authentication and agent secrets
- **LocalNode**: Entry point for creating and accessing CoValues from a specific account perspective. Manages sync with peers
- **Peer**: A sync connection to another node (server or client)
- **SyncMessage**: Protocol messages for sync: `Load`, `KnownState`, `NewContent`, `Done`
- **CryptoProvider**: Abstract interface for crypto operations (encrypt, sign, derive keys). Implementations: NAPI (Node.js), WASM (browser), RN (mobile)

### Architecture Layers

1. **Core layer** (`cojson`): CRDT operations, sync protocol, storage and crypto abstractions
2. **Native layer** (`crates/`): Rust implementations compiled to NAPI, WASM, and React Native
3. **Tools layer** (`jazz-tools`): Framework bindings (React, Svelte, Vue, RN), schema definitions, high-level APIs
4. **Storage adapters**: Pluggable backends — SQLite, IndexedDB, Durable Objects SQLite
5. **Transport layer**: WebSocket transport (`cojson-transport-ws`) for peer-to-peer sync

## Quick Reference

```bash
pnpm install              # Install dependencies
pnpm build:packages       # Build TypeScript packages
pnpm build:core           # Build native packages (NAPI, WASM, RN)
pnpm build:all-packages   # Build everything (native + TypeScript)
pnpm test                 # Run tests (Vitest, watch mode)
pnpm test --watch=false   # Run tests without watch
pnpm test fileName        # Run tests on files matching fileName
pnpm format-and-lint:fix  # Format and lint (Biome)
pnpm bench                # Run benchmarks
```

### Framework Support

- **React** 19.1.0 — hooks (`useCoState`, `useAccount`, `useJazz`)
- **Svelte** 5.46.4 — stores and components
- **Vue** — community bindings (composables)
- **React Native** 0.81.5 / **Expo** 54.0.23 — mobile support
- **Next.js** / **SvelteKit** 2.49.5 — SSR integration

## Repository Structure

```
packages/       # Main npm packages (jazz-tools, cojson, etc.)
crates/         # Rust code (cojson-core NAPI, WASM, React Native)
examples/       # Example applications
starters/       # Project starter templates
tests/          # Integration and e2e tests
homepage/       # Documentation site (Next.js)
```

## Code Conventions on packages

### Imports

- **Always use `.js` extensions** in imports, even for TypeScript files (enforced by Biome `useImportExtensions` rule at error level)
- Use **relative imports** only — no path aliases are configured
- Use **barrel exports** via `index.ts` files for public APIs

### Naming

- `camelCase` for functions and variables
- `PascalCase` for types, interfaces, classes, and components
- `Raw` prefix for low-level cojson types (e.g., `RawCoMap`, `RawGroup`, `RawAccount`)
- `unstable_` or `experimental_` prefix for unstable APIs

### TypeScript

- **Strict mode** enabled across all packages
- **`noUncheckedIndexedAccess`** enabled
- Avoid `any` types in production code
- **Module resolution**: `bundler`
- **Targets**: ES2020 for `cojson`, ES2021 for `jazz-tools` (React Native/Hermes compatibility)

### Documentation

- Document public APIs with JSDoc using `@param`, `@returns`, `@example` tags
- Update JSDoc comments for any modified public APIs

### Error Handling

- Use custom error classes extending `Error` with a descriptive `name` property
- Use discriminated union types for error variants where appropriate
- `JazzError` for CoValue loading states (`UNAVAILABLE`, `DELETED`, `UNAUTHORIZED`)

### Formatting

- **Biome** formatter with **space indentation** (not tabs)
- Import organization is disabled (assist actions only)
- Pre-commit hook runs Biome check on staged files via Lefthook

## Testing

### Unit & Integration Tests (Vitest)

- **File naming**: `*.test.ts`, `*.test.tsx`
- **Location**: `src/tests/**/*.test.ts` within each package
- **Crypto**: Use `WasmCrypto` for test crypto implementations
- **Utilities**: `setupTestNode`, `createTestNode`, `waitFor` from test utilities
- **First run**: Execute `pnpm exec playwright install` for browser-based Vitest tests
- **Run**: `pnpm test` (watch mode) or `pnpm test --watch=false` (CI mode)

### E2E Tests (Playwright)

- **File naming**: `*.spec.ts`
- **Location**: `examples/*/tests/*.spec.ts` or `tests/*/`
- **Pattern**: Page Object Model for page interactions
- **WebAuthn**: Mocks credentials API for passkey tests
- **First run**: `pnpm exec playwright install`
- **Run**: `npx playwright test` from the specific example/test directory

## CI/CD Pipelines

| Workflow | Trigger | Purpose |
|---|---|---|
| `code-quality.yml` | PRs, pushes | Biome CI checks |
| `unit-test.yml` | PRs, pushes | Vitest unit tests (builds packages + NAPI first) |
| `playwright.yml` | PRs, pushes | E2E tests, sharded across 2 runners |
| `release.yml` | Main branch | Changeset-based release to npm |
| `pre-release.yml` | `pre-release` label | Pre-release builds via pkg-pr-new |
| `napi.yml` | Changes to `crates/` | NAPI binary builds |
| `e2e-rn-test-cloud.yml` | — | React Native E2E tests |

## Versioning & Releases

- All core packages are in a **fixed version group** via Changesets (cojson, jazz-tools, jazz-run, jazz-webhook, all storage/transport packages, and NAPI binary packages)
- Internal dependency updates use **minor version bumps**
- Release process: `pnpm changeset` → `pnpm changeset-version` → `pnpm release`
- Backport releases: `pnpm release:backports` (publishes with `backport` tag)

## Commit Messages

Format: `type(scope): description`

Types: `fix`, `feat`, `chore`, `refactor`, `test`, `docs`, `perf`

Examples:
- `fix(cojson): prevent message queuing after connection closure`
- `feat(jazz-tools): add useCoState hook`
- `test(browser-integration): add sync conflict tests`

## Before Submitting Changes

1. Run `pnpm format-and-lint:fix` to format and lint code
2. Run `pnpm test --watch=false` to verify tests pass
3. Create a changeset using the related skill if the change affects package versions
4. Update JSDoc comments for any modified public APIs

## Key External Dependencies

| Library | Purpose |
|---|---|
| `@noble/hashes` | TypeScript crypto hash functions |
| `@scure/base` | Base encoding/decoding |
| `@opentelemetry/api` | Observability and tracing |
| `better-sqlite3` | SQLite bindings for Node.js |
| `ws` | WebSocket client/server |
| `napi-rs` | Rust-to-Node.js native bindings |
| `wasm-bindgen` | Rust-to-WebAssembly bindings |
| `uniffi-bindgen-react-native` | Rust-to-React-Native bindings |
| `ts-morph` | TypeScript AST manipulation (codemods) |

## Documentation

- Official docs: [jazz.tools](https://jazz.tools)
- Documentation site source: `homepage/` (Next.js)
- Markdown docs are available after the docs site is built: `homepage/homepage/public/docs`. An index is available below.

<!--DOCS_INDEX_START-->

[Jazz Docs Index]|root:homepage/homepage/public/docs
|LEGEND: b=base (no prefix), r=react, rn=react-native, rne=react-native-expo, s=svelte, v=vanilla, ss=server-side
|RULES: All files are .md. Resolve `[fw]/[dir]/[file].md`. If variant is 'b', path is `[dir]/[file].md`.

|.:{api-reference:b|r|rn|rne|s|v{#Defining Schemas#Creating CoValues#Loading and Reading CoValues},index:b|r|rn|rne|s|v{#Quickstart#How it works#A Minimal Jazz App},project-setup:b|r|rn|rne|s|v{#Install dependencies#Write your schema#Give your app context},quickstart:b|r|rn|rne|s|v{#Create your App#Install Jazz#Get your free API key},troubleshooting:b|r|rn|rne|s|v{#Node.js version requirements#npx jazz-run: command not found},react-native-expo:b{#Quickstart#How it works#A Minimal Jazz App},react-native:b{#Quickstart#How it works#A Minimal Jazz App},react:b{#Quickstart#How it works#A Minimal Jazz App},svelte:b{#Quickstart#How it works#A Minimal Jazz App},vanilla:b{#Quickstart#How it works#A Minimal Jazz App}}
|core-concepts:{deleting:b|r|rn|rne|s|v{#Basic Usage#Deleting Nested CoValues#Handling Inaccessible Data},subscription-and-loading:b|r|rn|rne|s|v{#Subscription Hooks#Deep Loading#Loading Errors},sync-and-storage:b|r|rn|rne|s|v{#Using Jazz Cloud#Self-hosting your sync server}}
|core-concepts/covalues:{cofeeds:b|r|rn|rne|s|v{#Creating CoFeeds#Reading from CoFeeds#Writing to CoFeeds},colists:b|r|rn|rne|s|v{#Creating CoLists#Reading from CoLists#Updating CoLists},comaps:b|r|rn|rne|s|v{#Creating CoMaps#Reading from CoMaps#Updating CoMaps},cotexts:b|r|rn|rne|s|v{#`co.plainText()` vs `z.string()`#Creating CoText Values#Reading Text},covectors:b|r|rn|rne|s|v{#Creating CoVectors#Semantic Search#Embedding Models},filestreams:b|r|rn|rne|s|v{#Creating FileStreams#Reading from FileStreams#Writing to FileStreams},imagedef:b|r|rn|rne|s|v{#Installation \[!framework=react-native\]#Installation \[!framework=react-native-exp\]#Creating Images#Displaying Images#Custom image manipulation implementations},overview:b|r|rn|rne|s|v{#Start your app with a schema#Types of CoValues#CoValue field/item types}}
|core-concepts/schemas:{accounts-and-migrations:b|r|rn|rne|s|v{#CoValues as a graph of data rooted in accounts#Resolving CoValues starting at `profile` or `root`#Populating and evolving `root` and `profile` schemas with migrations},codecs:b|r|rn|rne|s|v{#Using Zod codecs},connecting-covalues:b|r|rn|rne|s|v,schemaunions:b|r|rn|rne|s|v{#Creating schema unions#Narrowing unions#Loading schema unions}}
|key-features:{history:b|r|rn|rne|s|v{#The $jazz.getEdits() method#Edit Structure#Accessing History},version-control:b|r|rn|rne|s|v{#Working with branches#Conflict Resolution#Private branches}}
|key-features/authentication:{authentication-states:b|r|rn|rne|s|v{#Anonymous Authentication#Authenticated Account#Guest Mode},better-auth-database-adapter:b|r|rn|rne|s|v{#Getting started#How it works#How to access the database},better-auth:b|r|rn|rne|s|v{#How it works#Authentication methods and plugins#Getting started},clerk:b|r|rn|rne|s|v{#How it works#Key benefits#Implementation},overview:b|r|rn|rne|s|v{#Authentication Flow#Available Authentication Methods},passkey:b|r|rn|rne|s|v{#How it works#Key benefits#Implementation},passphrase:b|r|rn|rne|s|v{#How it works#Key benefits#Implementation},quickstart:b|r|rn|rne|s|v{#Add passkey authentication#Give it a go!#Add a recovery method}}
|permissions-and-sharing:{cascading-permissions:b|r|rn|rne|s|v{#Basic usage#Levels of inheritance#Roles},overview:b|r|rn|rne|s|v{#Role Matrix#Creating a Group#Adding group members by ID},quickstart:b|r|rn|rne|s|v{#Understanding Groups#Create an invite link#Accept an invite},sharing:b|r|rn|rne|s|v{#Public sharing#Invites}}
|project-setup:{providers:b|r|rn|rne|s|v{#Setting up the Provider#Provider Options#Authentication}}
|reference:{data-modelling:b|r|rn|rne|s|v{#Jazz as a Collaborative Graph#Permissions are part of the data model#Choosing your building blocks},encryption:b|r|rn|rne|s|v{#How encryption works#Key rotation and security#Streaming encryption},faq:b|r|rn|rne|s|v{#How established is Jazz?#Will Jazz be around long-term?#How secure is my data?},performance:b|r|rn|rne|s|v{#Use the best crypto implementation for your platform#Initialize WASM asynchronously#Minimise group extensions},testing:b|r|rn|rne|s|v{#Core test helpers#Managing active Accounts#Managing Context},workflow-world:b|r|rn|rne|s|v{#Getting started#Get your free API key#Install the Jazz Workflow World}}
|reference/design-patterns:{form:b|r|rn|rne|s|v{#Updating a CoValue#Creating a CoValue#Writing the components in React},history-patterns:b|r|rn|rne|s|v{#Audit Logs#Activity Feeds#Change Indicators},organization:b|r|rn|rne|s|v{#Defining the schema for an Organization#Adding a list of Organizations to the user's Account#Adding members to an Organization}}
|server-side:{jazz-rpc:b|r|rn|rne|s|v{#Setting up JazzRPC#Handling JazzRPC requests on the server#Making requests from the client},quickstart:b|r|rn|rne|s|v{#Create Your App#Install Jazz#Set your API key},setup:b|r|rn|rne|s|v{#Generating credentials#Running a server worker#Storing & providing credentials},ssr:b|r|rn|rne|s|v{#Creating an agent#Telling Jazz to use the SSR agent#Making your data public}}
|server-side/communicating-with-workers:{http-requests:b|r|rn|rne|s|v{#Creating a Request#Authenticating requests#Multi-account environments},inbox:b|r|rn|rne|s|v{#Setting up the Inbox API#Sending messages from the client#Deployment considerations},overview:b|r|rn|rne|s|v{#Overview#JazzRPC (Recommended)#HTTP Requests}}
|tooling-and-resources:{ai-tools:b|r|rn|rne|s|v{#Setting up AI tools#llms.txt convention#Limitations and considerations},create-jazz-app:b|r|rn|rne|s|v{#Quick Start with Starter Templates#Command Line Options#Start From an Example App},inspector:b|r|rn|rne|s|v{#Exporting current account to Inspector from your app#Embedding the Inspector widget into your app \[!framework=react,svelte,vue,vanilla\]}}

<!--DOCS_INDEX_END-->