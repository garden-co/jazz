import type { Schema } from "../drivers/types.js";

export const DESIGNER_TRACE_BRANCH_CONVENTION = {
  catalogue_branch: "main",
  default_user_branch: "main",
  workspace_branch_prefix: "workspace/",
  writer_branch_prefix: "writer/",
} as const;

export const DESIGNER_TRACE_SYNC_TARGET = {
  base_url: "https://nikitavoloboev-jazz2-sync-ingress.tailbf2c6c.ts.net",
  app_id: "313aa802-8598-5165-bb91-dab72dcb9d46",
  sync_path: "/sync",
  events_path: "/events",
  server_path_prefix: "",
  branch_convention: DESIGNER_TRACE_BRANCH_CONVENTION,
} as const;

export const DESIGNER_TRACE_RUNTIME_LAYOUT = {
  root: "app.getPath('userData')/designer/indexer-v1",
  workspace_manifest: "workspaces/<workspace_id>/embeddable_files.txt",
  folder_description: "workspaces/<workspace_id>/high_level_folder_description.json",
  control_sqlite: "control.sqlite",
  blobs_root: "blobs/",
  checkpoints_root: "checkpoints/",
  overlays_root: "overlays/",
  semantic_helix_root: "semantic/helix/",
  code_host: "x/nikiv/indexer",
} as const;

const DESIGNER_TRACE_WASM_SCHEMA_LITERAL = {
  trace_sessions: {
    columns: [
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      { name: "schema_version", column_type: { type: "Text" }, nullable: false },
      { name: "codebase_id", column_type: { type: "Text" }, nullable: false },
      { name: "workspace_id", column_type: { type: "Text" }, nullable: false },
      { name: "writer_id", column_type: { type: "Text" }, nullable: false },
      { name: "install_id", column_type: { type: "Text" }, nullable: false },
      { name: "writer_surface", column_type: { type: "Text" }, nullable: false },
      { name: "started_at", column_type: { type: "Timestamp" }, nullable: false },
      { name: "replication_scope", column_type: { type: "Text" }, nullable: false },
      { name: "privacy_mode", column_type: { type: "Text" }, nullable: false },
      { name: "hosted_sync_json", column_type: { type: "Json" }, nullable: false },
      { name: "runtime_layout_json", column_type: { type: "Json" }, nullable: false },
      { name: "ignore_contract_json", column_type: { type: "Json" }, nullable: false },
      { name: "indexing_contract_json", column_type: { type: "Json" }, nullable: false },
      { name: "benchmark_contract_json", column_type: { type: "Json" }, nullable: false },
    ],
  },
  trace_events: {
    columns: [
      { name: "event_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "schema_version", column_type: { type: "Text" }, nullable: false },
      { name: "kind", column_type: { type: "Text" }, nullable: false },
      { name: "occurred_at", column_type: { type: "Timestamp" }, nullable: false },
      { name: "writer_id", column_type: { type: "Text" }, nullable: false },
      { name: "replication_scope", column_type: { type: "Text" }, nullable: false },
      { name: "privacy_mode", column_type: { type: "Text" }, nullable: false },
      { name: "canonical_hash", column_type: { type: "Text" }, nullable: false },
      { name: "code_state_id", column_type: { type: "Text" }, nullable: true },
      { name: "buffer_state_id", column_type: { type: "Text" }, nullable: true },
      { name: "checkpoint_id", column_type: { type: "Text" }, nullable: true },
      { name: "chunk_hash", column_type: { type: "Text" }, nullable: true },
      { name: "projection_id", column_type: { type: "Text" }, nullable: true },
      { name: "git_snapshot_id", column_type: { type: "Text" }, nullable: true },
      { name: "payload_json", column_type: { type: "Json" }, nullable: false },
      { name: "refs_json", column_type: { type: "Json" }, nullable: false },
    ],
  },
  state_heads: {
    columns: [
      { name: "head_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "entity_kind", column_type: { type: "Text" }, nullable: false },
      { name: "entity_key", column_type: { type: "Text" }, nullable: false },
      { name: "current_state_id", column_type: { type: "Text" }, nullable: true },
      { name: "last_mutation_id", column_type: { type: "Text" }, nullable: true },
      { name: "tombstone", column_type: { type: "Boolean" }, nullable: false },
      { name: "tombstoned_at", column_type: { type: "Timestamp" }, nullable: true },
      { name: "conflict_keys_json", column_type: { type: "Json" }, nullable: false },
      { name: "updated_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
  state_mutations: {
    columns: [
      { name: "mutation_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "head_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "head_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "state_heads",
      },
      { name: "entity_kind", column_type: { type: "Text" }, nullable: false },
      { name: "entity_key", column_type: { type: "Text" }, nullable: false },
      { name: "mutation_kind", column_type: { type: "Text" }, nullable: false },
      { name: "before_state_id", column_type: { type: "Text" }, nullable: true },
      { name: "after_state_id", column_type: { type: "Text" }, nullable: true },
      { name: "event_id", column_type: { type: "Text" }, nullable: true },
      { name: "checkpoint_id", column_type: { type: "Text" }, nullable: true },
      { name: "payload_json", column_type: { type: "Json" }, nullable: false },
      { name: "occurred_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
  trace_snapshots: {
    columns: [
      { name: "snapshot_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "snapshot_kind", column_type: { type: "Text" }, nullable: false },
      { name: "checkpoint_id", column_type: { type: "Text" }, nullable: true },
      { name: "git_snapshot_id", column_type: { type: "Text" }, nullable: true },
      { name: "metadata_json", column_type: { type: "Json" }, nullable: true },
      { name: "occurred_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
} as const;

function cloneTraceSchema(): Schema {
  return Object.fromEntries(
    Object.entries(DESIGNER_TRACE_WASM_SCHEMA_LITERAL).map(([tableName, tableSchema]) => [
      tableName,
      {
        ...tableSchema,
        columns: tableSchema.columns.map((column) => ({ ...column })),
      },
    ]),
  ) as Schema;
}

export const DESIGNER_TRACE_WASM_SCHEMA: Schema = cloneTraceSchema();

export const app = {
  wasmSchema: DESIGNER_TRACE_WASM_SCHEMA,
};

export default app;
