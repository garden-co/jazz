# Designer AI Harness Schema - MVP

This narrows [designer_workspace_chat_canvas_schema.md](./designer_workspace_chat_canvas_schema.md)
to the AI harness contract needed for deep integration with
`~/code/prom/ide/designer`.

Primary references:

- `~/docs/plan/18/designer-initial-rendering.md`
- `~/code/prom/ide/designer/src/agent/runtime/agent-runtime.ts`
- `~/code/prom/ide/designer/src/agent/types.ts`
- `~/code/prom/ide/designer/src/agent/session-tree.ts`
- `~/code/prom/agents/ai-proxy/src/sdk/client.ts`
- `examples/codex-sessions-backend/`

## Goal

Design the Jazz2-side model for a Designer workspace where:

- a workspace is the durable collaboration root
- a workspace exposes one or more AI agents
- users create sessions inside that workspace
- each session has chat plus panes
- one pane kind is canvas
- the AI harness can execute through `ai-proxy`, Codex, Claude Code, or future runtimes without changing the durable session identity

The target is not "chat storage" in isolation. The target is a durable harness
model that can replace Designer's current local JSONL session files while still
supporting the existing branchable transcript, tool execution loop, and
chat-to-canvas linkage.

## Design Constraints From Current Designer

Current Designer already tells us the right shape:

- transcript entries are a tree via `parentId`, not a flat message list
- entries contain typed blocks: text, thinking, tool calls, file refs, selections
- tool results are their own entries
- harness identity is separate from provider/model selection
- `previousResponseId` and `sessionId` belong to provider/runtime continuity, not to the durable product session identity
- `prepareInput()` composes live editor context at request time; that context is not the same thing as persisted shared state

That means the Jazz model must separate four domains:

1. workspace collaboration state
2. durable user-visible session/transcript state
3. durable AI execution ledger state
4. local or ephemeral runtime state

## What Belongs In Jazz

Jazz should own:

- workspace membership and agent presets
- session identity and branchable transcript
- pane roots and canvas objects
- durable run history for prompt execution
- provider/native binding metadata that lets a session reattach to external runtimes
- generated artifacts and links between transcript and canvas

Jazz should not own:

- live React projection state
- token-by-token SSE deltas
- renderer truncation logic
- local process handles, abort controllers, listeners, or open file handles
- full code-defined harness implementations, tool registries, or prompt builder functions
- machine-local absolute workspace paths as shared collaboration state

## Core Decision

Treat the harness as a layered system:

- `workspace_agents` are persisted user-visible presets
- harness code remains local application code, addressed by `harnessKey`
- `sessions` are durable user-visible threads
- `session_runtime_bindings` attach a session to a provider-native thread or session
- `session_runs` record each user prompt cycle through the harness
- `session_entries` and `session_entry_blocks` store the visible transcript
- `session_artifacts` and `canvas_items` connect execution output to the canvas

This keeps Jazz authoritative for product state without trying to serialize the
entire runtime implementation into the database.

## Proposed Shared Schema

```ts
import { schema as s } from "jazz-tools";

const schema = {
  profiles: s.table({
    userId: s.string(),
    displayName: s.string(),
    avatarUrl: s.string().optional(),
  }),

  workspaces: s.table({
    slug: s.string(),
    title: s.string(),
    repoIdentityJson: s.json().optional(),
    archived: s.boolean().default(false),
  }),

  workspace_members: s.table({
    workspaceId: s.ref("workspaces"),
    userId: s.string(),
    role: s.enum("owner", "editor", "viewer"),
    joinedAt: s.timestamp(),
  }),

  workspace_agents: s.table({
    workspaceId: s.ref("workspaces"),
    handle: s.string(),
    label: s.string(),
    harnessKey: s.string(),
    appearance: s.enum("agent", "ask", "designer").optional(),
    providerKind: s.enum("ai_proxy", "openai", "codex", "claude_code", "custom").optional(),
    modelName: s.string().optional(),
    reasoningEffort: s.enum("none", "low", "medium", "high", "xhigh").optional(),
    instructionsOverride: s.string().optional(),
    configJson: s.json().optional(),
    archived: s.boolean().default(false),
  }),

  sessions: s.table({
    workspaceId: s.ref("workspaces"),
    createdByUserId: s.string(),
    primaryAgentId: s.ref("workspace_agents").optional(),
    title: s.string().optional(),
    summary: s.string().optional(),
    status: s.enum("active", "archived"),
    forkedFromSessionId: s.ref("sessions").optional(),
    latestActivityAt: s.timestamp(),
    archivedAt: s.timestamp().optional(),
  }),

  session_runtime_bindings: s.table({
    sessionId: s.ref("sessions"),
    agentId: s.ref("workspace_agents").optional(),
    providerKind: s.enum("ai_proxy_responses", "openai_responses", "codex", "claude_code", "custom"),
    nativeSessionId: s.string().optional(),
    nativeThreadId: s.string().optional(),
    nativeRunId: s.string().optional(),
    cursorResponseId: s.string().optional(),
    status: s.enum("idle", "starting", "running", "ready", "interrupted", "error", "closed"),
    lastError: s.string().optional(),
    lastHeartbeatAt: s.timestamp().optional(),
    metadataJson: s.json().optional(),
    updatedAt: s.timestamp(),
  }),

  session_runs: s.table({
    sessionId: s.ref("sessions"),
    runtimeBindingId: s.ref("session_runtime_bindings").optional(),
    agentId: s.ref("workspace_agents").optional(),
    requestedByUserId: s.string(),
    triggerEntryId: s.ref("session_entries").optional(),
    baseLeafEntryId: s.ref("session_entries").optional(),
    deliveryMode: s.enum("steer", "follow_up"),
    status: s.enum("queued", "starting", "streaming", "tool_running", "completed", "aborted", "error"),
    modelProvider: s.string().optional(),
    modelName: s.string().optional(),
    reasoningEffort: s.string().optional(),
    previousResponseId: s.string().optional(),
    completedResponseId: s.string().optional(),
    errorText: s.string().optional(),
    requestJson: s.json().optional(),
    resultJson: s.json().optional(),
    startedAt: s.timestamp(),
    updatedAt: s.timestamp(),
    completedAt: s.timestamp().optional(),
  }),

  session_entries: s.table({
    sessionId: s.ref("sessions"),
    runId: s.ref("session_runs").optional(),
    parentEntryId: s.ref("session_entries").optional(),
    authorKind: s.enum("user", "assistant", "system"),
    authorUserId: s.string().optional(),
    authorAgentId: s.ref("workspace_agents").optional(),
    entryKind: s.enum(
      "message",
      "tool_result",
      "branch_summary",
      "compaction",
      "model_change",
      "harness_change",
      "session_info",
      "label",
    ),
    status: s.enum("pending", "streaming", "completed", "aborted", "error").optional(),
    responseId: s.string().optional(),
    sourceToolCallId: s.string().optional(),
    summaryText: s.string().optional(),
    metadataJson: s.json().optional(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
  }),

  session_entry_blocks: s.table({
    entryId: s.ref("session_entries"),
    blockId: s.string(),
    blockOrder: s.int(),
    blockKind: s.enum(
      "text",
      "image",
      "file_ref",
      "selection",
      "thinking",
      "tool_call",
      "tool_result",
      "plan",
      "canvas_ref",
      "artifact_ref",
    ),
    text: s.string().optional(),
    mimeType: s.string().optional(),
    fileId: s.ref("files").optional(),
    path: s.string().optional(),
    selectionStartLine: s.int().optional(),
    selectionEndLine: s.int().optional(),
    toolCallId: s.string().optional(),
    toolName: s.string().optional(),
    dataJson: s.json().optional(),
  }),

  session_artifacts: s.table({
    sessionId: s.ref("sessions"),
    runId: s.ref("session_runs").optional(),
    sourceEntryId: s.ref("session_entries").optional(),
    artifactKind: s.enum("file", "image", "plan", "patch", "selection", "report", "snapshot"),
    title: s.string().optional(),
    fileId: s.ref("files").optional(),
    metadataJson: s.json().optional(),
    createdAt: s.timestamp(),
  }),

  session_panes: s.table({
    sessionId: s.ref("sessions"),
    paneKey: s.string(),
    paneKind: s.enum("chat", "canvas", "terminal", "diff", "inspector"),
    title: s.string(),
    slot: s.enum("left", "center", "right", "bottom", "overlay"),
    paneOrder: s.int(),
    status: s.enum("open", "closed"),
    metadataJson: s.json().optional(),
  }),

  canvases: s.table({
    paneId: s.ref("session_panes"),
    canvasKind: s.enum("board", "flow", "document", "preview", "image", "code"),
    title: s.string(),
    coordinateMode: s.enum("freeform", "flow", "stack"),
    background: s.string().optional(),
    metadataJson: s.json().optional(),
  }),

  canvas_items: s.table({
    canvasId: s.ref("canvases"),
    parentItemId: s.ref("canvas_items").optional(),
    sourceEntryId: s.ref("session_entries").optional(),
    sourceArtifactId: s.ref("session_artifacts").optional(),
    itemKind: s.enum(
      "note",
      "text",
      "frame",
      "group",
      "image",
      "file",
      "selection",
      "prompt",
      "artifact",
      "embed",
    ),
    title: s.string().optional(),
    text: s.string().optional(),
    fileId: s.ref("files").optional(),
    x: s.float(),
    y: s.float(),
    width: s.float(),
    height: s.float(),
    rotation: s.float().default(0),
    zIndex: s.int(),
    hidden: s.boolean().default(false),
    locked: s.boolean().default(false),
    styleJson: s.json().optional(),
    dataJson: s.json().optional(),
  }),

  canvas_edges: s.table({
    canvasId: s.ref("canvases"),
    fromItemId: s.ref("canvas_items"),
    toItemId: s.ref("canvas_items"),
    edgeType: s.enum("link", "flow", "dependency", "reference"),
    label: s.string().optional(),
    pointsJson: s.json().optional(),
    styleJson: s.json().optional(),
  }),

  file_parts: s.table({
    data: s.bytes(),
  }),

  files: s.table({
    name: s.string().optional(),
    mimeType: s.string(),
    partIds: s.array(s.ref("file_parts")),
    partSizes: s.array(s.int()),
  }),
};
```

## Why These Tables Exist

### `workspace_agents`

This is the persisted "AI agent preset" layer, not the full harness
implementation.

Store here:

- label and handle shown in the workspace UI
- which local harness implementation to use via `harnessKey`
- default provider/model selection
- optional workspace-specific prompt overrides

Do not store here:

- serialized tool implementations
- full system prompt assembly output
- live runtime process state

### `session_runtime_bindings`

This is the bridge from a durable Designer session to an external runtime.

Examples:

- `ai_proxy_responses` binding with `nativeSessionId` and `cursorResponseId`
- Codex binding with native session/thread ids
- Claude Code binding with SDK session/thread ids

This lets the same Designer session survive provider restarts, runtime restarts,
or even switching providers.

### `session_runs`

One row per prompt cycle.

This is the durable execution ledger for:

- queued prompt
- started run
- active streaming run
- tool-running loop
- completion or failure

This is where `previousResponseId` belongs. It is execution continuity state,
not the session identity.

### `session_entries` + `session_entry_blocks`

This is the user-visible transcript.

It directly matches Designer's current runtime model:

- tree structure by `parentEntryId`
- metadata entries such as model/harness changes
- assistant messages with typed blocks
- separate tool-result entries
- branch summaries and compactions

The important rule is:

- rows model structure and authorship
- blocks model content parts

Do not collapse this back into one giant `contentJson`.

### `session_artifacts`

Generated files and structured outputs should not only exist as opaque tool
result text.

Examples:

- a written file
- generated image
- plan markdown
- diff/patch payload
- selected code fragment

Canvas items can point at artifacts directly, which avoids reparsing transcript
text just to populate the canvas.

### `canvas_items`

Canvas items stay generic, but they are linked back to the harness:

- `sourceEntryId` for "this came from that chat turn"
- `sourceArtifactId` for "this is a pinned output"

This is the main bridge between the middle chat pane and the right canvas pane.

## What Stays Out Of Shared Jazz State

The following currently exist in Designer runtime code and should stay local or
derived:

- `streamDraft`
- `toolResultDraftsByCallId`
- `apiRequestSnapshotsByEntryId`
- UI truncation in `ui-entry-projection.ts`
- `prepareInput()` expansions such as current open editors or technical drawing context
- workspace absolute paths and session JSONL file paths

These are either:

- process-local runtime details
- transient UI projections
- machine-local state that should not sync across collaborators

If needed later, they can be exposed through a local-only cache or a device-
scoped projection, but not as the shared collaboration root.

## Permission Shape

Workspace membership should be the only real access root.

Sketch:

```ts
import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, allowedTo, anyOf }) => {
  const isWorkspaceMember = (workspaceId: unknown) =>
    policy.workspace_members.exists.where({
      workspaceId,
      userId: session.user_id,
    });

  const canEditWorkspace = (workspaceId: unknown) =>
    policy.workspace_members.exists.where({
      workspaceId,
      userId: session.user_id,
      role: { in: ["owner", "editor"] },
    });

  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate.where({ userId: session.user_id });

  policy.workspaces.allowRead.where((workspace) => isWorkspaceMember(workspace.id));
  policy.workspaces.allowInsert.where({});
  policy.workspaces.allowUpdate.where((workspace) => canEditWorkspace(workspace.id));

  policy.workspace_members.allowRead.where((member) => isWorkspaceMember(member.workspaceId));
  policy.workspace_members.allowInsert.where((member) => canEditWorkspace(member.workspaceId));
  policy.workspace_members.allowDelete.where((member) => canEditWorkspace(member.workspaceId));

  policy.workspace_agents.allowRead.where(allowedTo.read("workspaceId"));
  policy.workspace_agents.allowInsert.where((agent) => canEditWorkspace(agent.workspaceId));
  policy.workspace_agents.allowUpdate.where((agent) => canEditWorkspace(agent.workspaceId));
  policy.workspace_agents.allowDelete.where((agent) => canEditWorkspace(agent.workspaceId));

  policy.sessions.allowRead.where(allowedTo.read("workspaceId"));
  policy.sessions.allowInsert.where((sessionRow) =>
    anyOf([
      { createdByUserId: session.user_id },
      canEditWorkspace(sessionRow.workspaceId),
    ]),
  );
  policy.sessions.allowUpdate.where((sessionRow) => canEditWorkspace(sessionRow.workspaceId));

  policy.session_runtime_bindings.allowRead.where(allowedTo.read("sessionId"));
  policy.session_runs.allowRead.where(allowedTo.read("sessionId"));
  policy.session_entries.allowRead.where(allowedTo.read("sessionId"));
  policy.session_entry_blocks.allowRead.where(allowedTo.read("entryId"));
  policy.session_artifacts.allowRead.where(allowedTo.read("sessionId"));
  policy.session_panes.allowRead.where(allowedTo.read("sessionId"));
  policy.canvases.allowRead.where(allowedTo.read("paneId"));
  policy.canvas_items.allowRead.where(allowedTo.read("canvasId"));
  policy.canvas_edges.allowRead.where(allowedTo.read("canvasId"));

  policy.files.allowRead.where(
    anyOf([
      allowedTo.readReferencing(policy.session_entry_blocks, "fileId"),
      allowedTo.readReferencing(policy.session_artifacts, "fileId"),
      allowedTo.readReferencing(policy.canvas_items, "fileId"),
    ]),
  );
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));

  // Assistant/system writes should come from a trusted runtime principal.
  // End-user clients should not be allowed to impersonate assistant authorship.
});
```

This design relies on the newer relation-hop permission support working
correctly across:

- workspace -> sessions
- sessions -> entries / runs / panes
- panes -> canvases
- canvases -> items / edges

That is exactly the shape this app needs.

## Mapping From Current Designer Runtime

| Current Designer concept | Jazz table(s) | Notes |
|---|---|---|
| `SessionSummary` | `sessions` + latest related rows | title/preview become projections over transcript |
| `SessionEntry` union | `session_entries` + `session_entry_blocks` | keep tree shape via `parentEntryId` |
| `harness` metadata entry | `workspace_agents.harnessKey` plus optional `harness_change` entry | current value is first-class, changes remain visible |
| `modelChange` entry | `session_entries` | keep historical model changes |
| `previousResponseId` | `session_runs.previousResponseId` / `completedResponseId` | execution continuity only |
| `sessionId` passed to OpenAI / ai-proxy | `session_runtime_bindings.nativeSessionId` | external provider identity |
| `queuedMessages` | `session_runs` with `status = queued` | durable queue, no separate blob |
| `streamDraft` | not shared Jazz state | local/projection only |
| `toolResultDraftsByCallId` | not shared Jazz state | local/projection only |
| `apiRequestSnapshotsByEntryId` | optional debug projection, not core schema | keep out of shared contract |

## Implementation Slices For Designer

### Slice 1: Replace JSONL transcript storage

Wire these first:

- `sessions`
- `session_entries`
- `session_entry_blocks`

Goal:

- current Designer chat sidebar can load from Jazz instead of local `.jsonl`
- branch replay logic still works with no semantic change

### Slice 2: Add durable run ledger

Wire these next:

- `session_runtime_bindings`
- `session_runs`
- `workspace_agents`

Goal:

- `ai-proxy` or Codex/Claude turns become queryable durable runs
- queued prompts survive reloads
- provider continuity no longer depends on local in-memory state

### Slice 3: Link output to canvas

Wire these after the transcript is stable:

- `session_artifacts`
- `session_panes`
- `canvases`
- `canvas_items`
- `canvas_edges`

Goal:

- assistant output can be pinned into canvas without reparsing message text
- canvas edits merge at row/column granularity

### Slice 4: Optional per-user synced view state

Only if cross-device UI continuity matters:

- `workspace_user_views`
- `session_user_views`

Keep local-only unless there is a real product need.

## Important Modeling Rules

1. Do not put local absolute paths on shared `workspaces` or `sessions`.
2. Do not serialize harness implementations or tool registries into Jazz rows.
3. Do not store transcript content as one giant JSON blob.
4. Do not make provider/native session ids the product session identity.
5. Do not sync token-by-token stream deltas as the shared source of truth.

## Why This Fits Current Jazz2

This model specifically benefits from the recent engine work:

- MRCA-based per-column LWW makes per-item canvas edits and row-wise session metadata updates much safer
- qualified hopped permission rules make workspace-rooted access control viable without duplicating ACL columns everywhere
- improved large-key page splitting reduces one failure mode, but this schema still avoids giant indexed JSON blobs by design

## Recommended Next Step In This Repo

Build a concrete example app under `examples/` with:

- `schema.ts`
- `permissions.ts`
- one end-to-end test for transcript branching
- one end-to-end test for queued runs and runtime binding continuity
- one end-to-end test for chat-to-canvas artifact pinning

That example should be the contract Designer and future Go bindings target.
