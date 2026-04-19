# Designer Workspace / Session / Canvas Schema - TODO (MVP)

Design the Jazz2-side data model for a new Designer-style app where:

- a workspace is the durable collaboration root
- a workspace contains agents
- users create sessions inside a workspace
- each session renders as chat plus panes
- one pane kind is `canvas`, and canvases can host multiple item types

This is the first-pass schema shape, not the final runtime protocol.

## Why This Exists

The current Designer code and nearby tools all split this product a little differently:

- current Designer already has a 3-pane target shell: Agents | Chat | Canvas
- current Designer stores chat as a branchable session-entry tree, not as a flat message list
- T3 Code separates project/workspace, thread, and live provider session
- OpenCode separates project, session, message, and part

Jazz2 should keep those separations, but express them in a relational, local-first form that fits:

- explicit references instead of nested app-state blobs
- permission inheritance through relations
- MRCA-based per-column LWW on concurrent writes

The latest Jazz2 patches make this especially relevant:

- `66dc47a` makes row-wise concurrent editing a much better fit for visible merge previews
- `213288a` makes qualified relation-hop permission rules viable for workspace/member/session access
- `0585935` removes one large-JSON index sharp edge, but it is still better design not to make chat or canvas state one giant indexed JSON blob

## Goals

- Make `workspace` the permission and collaboration root.
- Separate durable product state from ephemeral provider runtime state.
- Model chat as a branchable session log so tool calls, compaction, summaries, and assistant output all fit naturally.
- Model panes as first-class children of a session rather than embedding layout JSON on the session row.
- Model canvas as a relational document with per-item rows so concurrent edits merge at field granularity.
- Keep per-user view state separate from shared workspace/session state.

## Non-goals (MVP)

- Final runtime event model for every provider.
- Presence, cursors, or transient streaming deltas.
- Full multi-agent orchestration history.
- Rich canvas subtype tables for every future canvas item kind.
- Locking down the final prompt/tool protocol.

## Evidence From Existing Surfaces

### Current Designer

The current app already implies three distinct domains:

- workspace state and persistent workbench/view state
- session/thread state for the agent conversation
- canvas/editor state as a separate surface

Relevant files:

- `~/code/prom/ide/designer/src/components/shell/Shell.tsx`
- `~/code/prom/ide/designer/src/store/workspace-view.ts`
- `~/code/prom/ide/designer/src/agent/types.ts`
- `~/code/prom/ide/designer/src/agent/session-tree.ts`

Important observation:

- chat is not just `messages`; it is a typed entry tree with `parentId`
- view/layout state is per workspace, not part of the chat transcript

### T3 Code

T3 Code separates:

- `project`
- `thread`
- live `session`

and keeps thread data distinct from provider-runtime session state.

Relevant files:

- `~/repos/pingdotgg/t3code/packages/contracts/src/orchestration.ts`
- `~/repos/pingdotgg/t3code/apps/web/src/types.ts`

Important observation:

- a durable thread should not be the same row as the live provider session

### OpenCode

OpenCode stores:

- `project`
- `session`
- `message`
- `part`

Relevant files:

- `~/repos/anomalyco/opencode/packages/opencode/src/session/session.sql.ts`

Important observation:

- normalized message-part storage is a better fit than one giant content blob once tools, media, and compaction exist

## Core Decisions

### 1. Workspace is the root domain object

Everything durable hangs off `workspace`:

- membership
- available agents
- sessions
- files/artifacts

This makes permissions simple and composable. The main question becomes:

- "can this session user read or edit this workspace?"

and child rows inherit from there.

### 2. Session is a durable conversation/thread, not a provider runtime session

We need two different concepts:

- a product session the user sees in the UI
- a provider/runtime binding used to execute turns

Do not overload one row with both responsibilities.

The durable session should survive:

- provider restarts
- provider switching
- reattachment to a new remote/native session id
- future multi-runtime execution

### 3. Chat should be a branchable entry log, not just `messages`

Current Designer already has:

- user entries
- assistant entries
- tool results
- compaction markers
- branch summaries
- model changes
- session metadata

That is closer to `session_entries` plus `session_entry_blocks` than to a flat `messages` table.

This keeps us from painting ourselves into a corner once:

- assistant tool calls become visible
- plans need to be persisted
- branches/forks matter
- canvas objects need to point back to source chat entries

### 4. Panes are session children

The session owns durable surfaces:

- chat pane
- canvas pane
- later terminal / diff / inspector panes

Do not store pane layout as one JSON blob on `sessions`.

Instead:

- each pane is a row
- pane-local content roots are references
- per-user widths, active tabs, and collapsed state live elsewhere

### 5. Canvas is a relational document

Canvas should not be one `canvas_state_json` column.

Use:

- one row for the canvas itself
- one row per canvas item
- one row per edge/connection

That matches Jazz2's strengths:

- per-column LWW
- selective queries
- relation-based permissions
- source linkage from chat entries to canvas objects

### 6. Per-user view state must be separate from shared collaborative state

There are three layers of state here:

1. shared collaborative state
2. per-user synced view state
3. purely local ephemeral UI/runtime state

Examples:

- shared: session rows, entries, panes, canvas items
- per-user synced: active session, selected branch leaf, pane widths if you want them on every device
- local-only: draft text, scroll position, streaming partial buffers

Do not mix these in the same table.

## Proposed Shared Schema

```ts
import { schema as s } from "jazz-tools";

const schema = {
  users: s.table({
    userId: s.string(),
    displayName: s.string(),
    avatarUrl: s.string().optional(),
  }),

  workspaces: s.table({
    slug: s.string(),
    title: s.string(),
    rootPath: s.string().optional(),
    repositoryKey: s.string().optional(),
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
    kind: s.enum("assistant", "human", "system"),
    provider: s.enum("codex", "claude", "cursor", "opencode", "custom").optional(),
    model: s.string().optional(),
    instructions: s.string().optional(),
    configJson: s.json().optional(),
    archived: s.boolean().default(false),
  }),

  sessions: s.table({
    workspaceId: s.ref("workspaces"),
    createdByUserId: s.string(),
    primaryAgentId: s.ref("workspace_agents").optional(),
    title: s.string(),
    summary: s.string().optional(),
    status: s.enum("draft", "active", "archived"),
    branch: s.string().optional(),
    worktreePath: s.string().optional(),
    forkedFromSessionId: s.ref("sessions").optional(),
    latestActivityAt: s.timestamp(),
    archivedAt: s.timestamp().optional(),
  }),

  session_runtime_bindings: s.table({
    sessionId: s.ref("sessions"),
    agentId: s.ref("workspace_agents").optional(),
    provider: s.enum("codex", "claude", "cursor", "opencode", "custom"),
    providerSessionId: s.string(),
    providerThreadId: s.string().optional(),
    providerRunId: s.string().optional(),
    status: s.enum("starting", "running", "ready", "interrupted", "error", "closed"),
    lastError: s.string().optional(),
    metadataJson: s.json().optional(),
    lastHeartbeatAt: s.timestamp().optional(),
    updatedAt: s.timestamp(),
  }),

  session_entries: s.table({
    sessionId: s.ref("sessions"),
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
      "session_meta",
      "label",
    ),
    status: s.enum("pending", "streaming", "completed", "aborted", "error").optional(),
    providerMessageId: s.string().optional(),
    providerTurnId: s.string().optional(),
    responseId: s.string().optional(),
    targetEntryId: s.ref("session_entries").optional(),
    summaryText: s.string().optional(),
    metadataJson: s.json().optional(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
  }),

  session_entry_blocks: s.table({
    entryId: s.ref("session_entries"),
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
    itemType: s.enum(
      "text",
      "note",
      "shape",
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

## What Each Layer Is For

### `workspaces`

Stable collaborative container for:

- repo/project identity
- membership
- agent catalog
- sessions

This is the nearest equivalent to Designer's workspace root and T3 Code's project root.

### `workspace_agents`

Persistent agent definitions available in a workspace.

Use this for:

- labels and handles shown in the left pane
- provider/model defaults
- reusable instructions/personas

Do not use it for live runtime state.

### `sessions`

User-visible chat threads inside a workspace.

Keep on the session row:

- title
- assigned/default agent
- branch/worktree metadata
- durable lifecycle

Do not put:

- streaming partial assistant output
- provider websocket status
- pane widths

### `session_runtime_bindings`

Live or historical bindings from a durable session to a provider-native session.

Why separate:

- a single Designer session may be resumed by a different runtime instance later
- the external provider ids are integration details, not the product's root identity

### `session_entries` and `session_entry_blocks`

This is the chat transcript model.

Why two tables:

- `session_entries` stores branch structure and entry metadata
- `session_entry_blocks` stores typed body parts

This matches the shape current Designer already wants:

- entries are tree nodes
- each entry can carry one or more typed blocks

That lets us represent:

- user text
- assistant text
- tool calls and results
- image/file blocks
- compaction summaries
- future plan blocks

without stuffing everything into one `contentJson`.

### `session_panes`

Durable session-owned surfaces.

MVP pane kinds:

- `chat`
- `canvas`

Future pane kinds:

- `terminal`
- `diff`
- `inspector`

This keeps the screen model flexible without making layout one monolithic object.

### `canvases`, `canvas_items`, `canvas_edges`

Canvas is a session child, but it should behave like its own document.

Key rule:

- put geometry and common collaborative fields in scalar columns
- keep type-specific payload in `dataJson`

This gives us better merge behavior than one blob:

- moving an item edits `x/y/width/height`
- renaming an item edits `title/text`
- style tweaks can stay isolated in `styleJson`

If a particular canvas subtype becomes heavily collaborative later, split it into subtype tables instead of expanding `dataJson` forever.

## Optional Per-User Synced View State

If the product wants cross-device persistence for layout and last-open state, add separate user-scoped tables like:

```ts
const viewSchema = {
  workspace_user_views: s.table({
    workspaceId: s.ref("workspaces"),
    userId: s.string(),
    activeSessionId: s.ref("sessions").optional(),
    layoutMode: s.enum("chat-canvas", "chat-only", "canvas-only"),
    leftPaneWidth: s.int().optional(),
    centerPaneWidth: s.int().optional(),
    rightPaneWidth: s.int().optional(),
    metadataJson: s.json().optional(),
  }),

  session_user_views: s.table({
    sessionId: s.ref("sessions"),
    userId: s.string(),
    activePaneId: s.ref("session_panes").optional(),
    activeLeafEntryId: s.ref("session_entries").optional(),
    lastReadEntryId: s.ref("session_entries").optional(),
    metadataJson: s.json().optional(),
  }),
};
```

Important:

- this is user state, not shared workspace state
- drafts and scroll position are often better left entirely local

## Permission Shape

The clean permission root is workspace membership.

Sketch:

```ts
import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, allowedTo, anyOf, allOf }) => {
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

  policy.workspaces.allowRead.where((workspace) => isWorkspaceMember(workspace.id));
  policy.workspaces.allowInsert.always();
  policy.workspaces.allowUpdate.where((workspace) => canEditWorkspace(workspace.id));

  policy.workspace_members.allowRead.where((member) => isWorkspaceMember(member.workspaceId));
  policy.workspace_members.allowInsert.where((member) => canEditWorkspace(member.workspaceId));
  policy.workspace_members.allowDelete.where((member) => canEditWorkspace(member.workspaceId));

  policy.workspace_agents.allowRead.where(allowedTo.read("workspaceId"));
  policy.workspace_agents.allowInsert.where((agent) => canEditWorkspace(agent.workspaceId));
  policy.workspace_agents.allowUpdate.where((agent) => canEditWorkspace(agent.workspaceId));
  policy.workspace_agents.allowDelete.where((agent) => canEditWorkspace(agent.workspaceId));

  policy.sessions.allowRead.where(allowedTo.read("workspaceId"));
  policy.sessions.allowInsert.where(
    allOf([allowedTo.read("workspaceId"), { createdByUserId: session.user_id }]),
  );
  policy.sessions.allowUpdate.where((sessionRow) => canEditWorkspace(sessionRow.workspaceId));

  policy.session_entries.allowRead.where(allowedTo.read("sessionId"));
  policy.session_entry_blocks.allowRead.where(allowedTo.read("entryId"));
  policy.session_panes.allowRead.where(allowedTo.read("sessionId"));
  policy.canvases.allowRead.where(allowedTo.read("paneId"));
  policy.canvas_items.allowRead.where(allowedTo.read("canvasId"));
  policy.canvas_edges.allowRead.where(allowedTo.read("canvasId"));

  // Assistant-authored rows should come from a trusted backend/runtime principal,
  // not from arbitrary clients pretending to be the assistant.
});
```

Two important notes:

1. `213288a` matters here because permission rules over hopped relations are exactly what this model wants.
2. Assistant/system writes should generally be inserted server-side or by a trusted runtime identity, not by end-user clients.

## Query Shapes This Model Must Serve

### Left pane: agents + sessions

Queries:

- all `workspace_agents` for a workspace
- all `sessions` for a workspace ordered by `latestActivityAt`

### Center pane: chat

Queries:

- all `session_entries` for a session
- all `session_entry_blocks` for the active branch
- optional replay/branch traversal by `parentEntryId`

### Right pane: canvas

Queries:

- `session_panes` for the session
- active canvas row for the chosen pane
- all `canvas_items` and `canvas_edges` for that canvas

### Cross-surface linkage

Queries:

- canvas items created from a chat entry via `sourceEntryId`
- chat blocks that refer to a canvas object via `canvas_ref`

## Why Not Simpler Alternatives

### Not one `workspace_state_json`

That would lose:

- row-wise permissions
- field-granular merges
- selective queries
- clean links between chat and canvas

### Not one `messages` table only

That would make these awkward immediately:

- tool-call visibility
- branch summaries
- compaction markers
- model changes
- future plan objects

### Not one shared layout blob on `sessions`

That would mix:

- collaborative pane structure
- user-specific widths and selections

Those do not belong to the same consistency domain.

## Recommended MVP Scope

Start with these tables first:

- `workspaces`
- `workspace_members`
- `workspace_agents`
- `sessions`
- `session_runtime_bindings`
- `session_entries`
- `session_entry_blocks`
- `session_panes`
- `canvases`
- `canvas_items`
- `canvas_edges`
- `files`
- `file_parts`

Keep `workspace_user_views` / `session_user_views` optional until you decide whether cross-device UI state is worth syncing.

## Next Steps

1. Turn this spec into a concrete `schema.ts` and `permissions.ts` example app inside `examples/`.
2. Prove the three core queries end to end:
   - workspace session list
   - active session transcript with blocks
   - active canvas with items and edges
3. Add one permission integration test where:
   - Alice and Bob share a workspace
   - Alice can read/write her session and canvas
   - a non-member cannot read the workspace graph
4. Add one merge test where two peers concurrently edit different canvas item columns and both changes survive.
