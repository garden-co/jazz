# Design: Expo Android E2E on Maestro Cloud

## Overview

This design adds a CI-only Android E2E suite for [`examples/todo-client-localfirst-expo`](../../../examples/todo-client-localfirst-expo) and runs it on Maestro Cloud.

The suite validates the real app behavior end-to-end:

1. launch app
2. add multiple todos
3. mark one todo as done
4. remove one or more todos
5. prove the app communicated with a live `jazz-tools server` for the full run

Key constraint: Maestro Cloud devices run outside GitHub Actions networking, so the CI-started server must be reachable from the public internet during the run.

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
- the workflow helper script (`scripts/expo-android-maestro-e2e.sh`)

This matches your requirement to ignore unrelated example changes while still running when the Expo example itself changes.

### 2) Build `jazz-tools` for sandbox runtime and start it in Vercel Sandbox

Because Vercel Sandboxes expose ports for processes running inside the sandbox, the server process must run in the sandbox (not on the GitHub runner). Detect sandbox architecture at runtime (`uname -m`), build the matching Linux binary target, and copy that binary into the sandbox.

Required repository secrets for sandbox bootstrap:

- `VERCEL_SANDBOXES_TOKEN`
- `VERCEL_SANDBOXES_PROJECT_ID`
- `VERCEL_TEAM_ID` (optional, only for team-scoped projects)

Representative sequence:

```bash
SANDBOX_CREATE_OUTPUT="$(sandbox create --project "$VERCEL_SANDBOXES_PROJECT_ID" --runtime node22 --timeout 60m --publish-port 1625)"
SANDBOX_ID="$(printf '%s\n' "$SANDBOX_CREATE_OUTPUT" | grep -Eo 'sb_[a-zA-Z0-9]+' | head -n1)"
SANDBOX_ARCH="$(sandbox exec "$SANDBOX_ID" sh -lc 'uname -m' | tail -n1)"
# map x86_64|amd64 -> x86_64-unknown-linux-gnu
# map aarch64|arm64 -> aarch64-unknown-linux-gnu
cargo build --release -p jazz-tools --bin jazz-tools --features cli --target "$MATCHING_TARGET"
sandbox copy "$MATCHING_BINARY" "${SANDBOX_ID}:/tmp/jazz-tools"
sandbox exec "$SANDBOX_ID" sh -lc "chmod +x /tmp/jazz-tools && nohup /tmp/jazz-tools server 6316f08d-d5d1-41df-82b8-8c16aa26db84 --admin-secret d0a2f110-36a8-45b9-8632-ecbc09128e2a --port 1625 >/tmp/server.log 2>&1 &"
```

### 3) Resolve sandbox public URL and gate on `/health`

Extract or resolve the sandbox public URL, export it as `SERVER_PUBLIC_URL`, and inject it into APK build as `EXPO_PUBLIC_JAZZ_SERVER_URL`.

Health gate before running Maestro:

```bash
curl --fail --retry 20 --retry-delay 1 "${SERVER_PUBLIC_URL}/health"
```

Persist sandbox bootstrap output to `vercel-sandbox.log` for post-failure diagnostics.

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

| Field              | Type                 | Source                                     |
| ------------------ | -------------------- | ------------------------------------------ |
| `appId`            | `string (uuid)`      | hardcoded E2E constant                     |
| `adminSecret`      | `string (uuid)`      | hardcoded E2E constant                     |
| `serverPort`       | `number`             | workflow env (`1625`)                      |
| `serverPublicUrl`  | `string (https url)` | parsed/resolved from sandbox create output |
| `maestroProjectId` | `string`             | `secrets.MAESTRO_PROJECT_ID`               |

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
4. always upload `server.log` and `vercel-sandbox.log` as artifacts for debugging

### Representative integration verification snippet

```bash
SYNC_COUNT="$(grep -c 'sync request' server.log || true)"
EVENT_COUNT="$(grep -c 'events stream connecting' server.log || true)"

test "${SYNC_COUNT}" -gt 0
test "${EVENT_COUNT}" -gt 0
```

## Open Questions

### Security policy

1. Confirm this CI-only policy is acceptable: sandbox URL is public but scoped to test runtime, and server uses app id/admin secret dedicated to E2E scope only.  
   Impact: this is the main security boundary for exposing the CI test server to cloud devices.
