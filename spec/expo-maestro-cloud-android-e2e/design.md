# Design: Expo Android E2E on Maestro Cloud

## Overview

This design adds a CI-only Android E2E suite for [`examples/todo-client-localfirst-expo`](../../../examples/todo-client-localfirst-expo) and runs it on Maestro Cloud.

The suite validates the real app behavior end-to-end:

1. launch app
2. add multiple todos
3. mark one todo as done
4. remove one or more todos
5. prove the app communicated with a live `jazz-tools server` for the full run

Key constraint: Maestro Cloud devices run outside GitHub Actions networking, so the CI-started server must be reachable from the public internet during the run (ephemeral tunnel).

## Architecture / Components

### 1) GitHub Actions workflow and trigger policy

Create a dedicated workflow (for isolation and easier reruns):

- `.github/workflows/expo-android-maestro-e2e.yml`

Trigger behavior:

- `push` on `main`: always run.
- `pull_request` targeting `main`: run only when relevant files change.

PR path filters should include:

- `crates/**` (native/runtime changes)
- `packages/jazz-tools/**`
- `examples/todo-client-localfirst-expo/**`
- lock/build plumbing (`Cargo.toml`, `Cargo.lock`, `pnpm-lock.yaml`, `pnpm-workspace.yaml`, `turbo.json`)
- the workflow file itself

This matches your requirement to ignore unrelated example changes while still running when the Expo example itself changes.

### 2) Build and start `jazz-tools server` in CI

Use your exact runtime shape in CI:

```bash
cargo build -p jazz-tools --bin jazz-tools --features cli
target/debug/jazz-tools server 6316f08d-d5d1-41df-82b8-8c16aa26db84 \
  --admin-secret d0a2f110-36a8-45b9-8632-ecbc09128e2a \
  --port 1625
```

Run it in the background and capture logs to `server.log`.

Health gate before running tests:

```bash
curl --fail --retry 20 --retry-delay 1 http://127.0.0.1:1625/health
```

### 3) Expose the server to Maestro Cloud devices

Because cloud devices cannot resolve local CI hostnames like `server-ns`, publish an ephemeral HTTPS URL via tunnel (for example `cloudflared` quick tunnel), then inject that URL as `EXPO_PUBLIC_JAZZ_SERVER_URL` at build time.

Example:

```bash
cloudflared tunnel --url http://127.0.0.1:1625 --no-autoupdate > cloudflared.log 2>&1 &
SERVER_PUBLIC_URL="$(grep -Eo 'https://[-a-z0-9]+\.trycloudflare.com' cloudflared.log | head -n1)"
```

### 4) Build an Android artifact suitable for Maestro Cloud

Build a release APK from the Expo example with E2E env vars embedded:

```bash
EXPO_PUBLIC_JAZZ_APP_ID=6316f08d-d5d1-41df-82b8-8c16aa26db84 \
EXPO_PUBLIC_JAZZ_ADMIN_SECRET=d0a2f110-36a8-45b9-8632-ecbc09128e2a \
EXPO_PUBLIC_JAZZ_SERVER_URL="$SERVER_PUBLIC_URL" \
  ./gradlew :app:assembleRelease
```

Artifact path:

- `examples/todo-client-localfirst-expo/android/app/build/outputs/apk/release/app-release.apk`

### 5) Maestro Cloud execution

Use Maestro’s official GitHub Action with repository secrets:

- `MAESTRO_KEY` -> `api-key`
- `MAESTRO_PROJECT_ID` -> `project-id`

Representative workflow snippet:

```yaml
- name: Run Maestro Cloud flows
  uses: mobile-dev-inc/action-maestro-cloud@v2
  with:
    api-key: ${{ secrets.MAESTRO_KEY }}
    project-id: ${{ secrets.MAESTRO_PROJECT_ID }}
    app-file: examples/todo-client-localfirst-expo/android/app/build/outputs/apk/release/app-release.apk
    workspace: .maestro
```

### 6) Server communication proof from logs

The server already emits useful transport logs in `crates/jazz-tools/src/routes.rs`:

- `sync request` (POST `/sync`)
- `events stream connecting` (GET `/events`)

After Maestro run, validate both occurred:

```bash
grep -q "sync request" server.log
grep -q "events stream connecting" server.log
```

If either is missing, fail the workflow. This guarantees the app did not only render locally; it actually exchanged sync traffic with the CI server.

### 7) App testability hooks (required for reliable selectors)

Add deterministic `testID` props in [`examples/todo-client-localfirst-expo/src/TodoList.tsx`](../../../examples/todo-client-localfirst-expo/src/TodoList.tsx):

```tsx
function toTestIdSegment(value: string): string {
  return value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

<TextInput testID="todo-input" ... />
<Pressable testID="todo-add" ... />
<Switch testID={`todo-toggle-${toTestIdSegment(item.title)}`} ... />
<Pressable testID={`todo-delete-${toTestIdSegment(item.title)}`} ... />
```

Without stable IDs, the flow is brittle because Android switch/delete controls are repeated per row.

## Data Models

This feature introduces operational data models for CI orchestration rather than new product schema.

### `E2ERunConfig`

| Field              | Type                 | Source                       |
| ------------------ | -------------------- | ---------------------------- |
| `appId`            | `string (uuid)`      | hardcoded E2E constant       |
| `adminSecret`      | `string (uuid)`      | hardcoded E2E constant       |
| `serverPort`       | `number`             | workflow env (`1625`)        |
| `serverPublicUrl`  | `string (https url)` | parsed from tunnel logs      |
| `maestroProjectId` | `string`             | `secrets.MAESTRO_PROJECT_ID` |

### `TodoScenario`

| Field           | Type       | Example                                 |
| --------------- | ---------- | --------------------------------------- |
| `createdTitles` | `string[]` | `["buy milk", "pay rent", "book trip"]` |
| `toggleTitle`   | `string`   | `"pay rent"`                            |
| `deleteTitles`  | `string[]` | `["book trip"]`                         |

### `ServerEvidence`

| Field              | Type      | Meaning                                       |
| ------------------ | --------- | --------------------------------------------- |
| `syncRequests`     | `number`  | count of `sync request` log lines             |
| `eventConnections` | `number`  | count of `events stream connecting` log lines |
| `healthPassed`     | `boolean` | `/health` pre-check result                    |

## Testing Strategy

### Maestro flow coverage

Add flow files under `.maestro/`:

- `.maestro/flows/todo_crud.yaml`
- `.maestro/config.yaml` (optional shared setup)

Representative flow:

```yaml
appId: dev.jazz.todo.localfirstexpo
---
- launchApp
- assertVisible: "Todos"

- tapOn:
    id: "todo-input"
- inputText: "buy milk"
- tapOn:
    id: "todo-add"

- tapOn:
    id: "todo-input"
- inputText: "pay rent"
- tapOn:
    id: "todo-add"

- tapOn:
    id: "todo-input"
- inputText: "book trip"
- tapOn:
    id: "todo-add"

- tapOn:
    id: "todo-toggle-pay-rent"
- tapOn:
    id: "todo-delete-book-trip"
- assertVisible: "buy milk"
- assertVisible: "pay rent"
- assertNotVisible: "book trip"
```

### CI assertions

1. server health reachable before upload
2. Maestro run must succeed
3. server evidence assertions must pass (`sync request` and `events stream connecting` present)
4. always upload `server.log` and `cloudflared.log` as artifacts for debugging

### Representative integration verification snippet

```bash
SYNC_COUNT="$(grep -c 'sync request' server.log || true)"
EVENT_COUNT="$(grep -c 'events stream connecting' server.log || true)"

test "${SYNC_COUNT}" -gt 0
test "${EVENT_COUNT}" -gt 0
```

## Open Questions

### Trigger scope

1. On PRs, should Expo-example-only changes (`examples/todo-client-localfirst-expo/**`) trigger this job?  
   Impact: you asked both “only crates/jazz-tools/native” and “except Expo example”; this decides final `paths` filter.

### Network/security

2. Is an ephemeral public tunnel from CI acceptable for this test backend?  
   Impact: without a public endpoint, Maestro Cloud cannot reach the GitHub-runner server.

### Build variant

3. Do you want `release` APK only, or `debug` APK on cloud?  
   Impact: `release` is usually more deterministic in cloud runs (bundled JS, no Metro dependency), but slower to build.
