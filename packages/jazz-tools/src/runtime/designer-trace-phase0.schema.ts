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
  object_refs: {
    columns: [
      { name: "object_ref_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "workspace_id", column_type: { type: "Text" }, nullable: false },
      { name: "role", column_type: { type: "Text" }, nullable: false },
      { name: "schema_version", column_type: { type: "Text" }, nullable: false },
      { name: "provider", column_type: { type: "Text" }, nullable: false },
      { name: "bucket", column_type: { type: "Text" }, nullable: true },
      { name: "key", column_type: { type: "Text" }, nullable: false },
      { name: "uri", column_type: { type: "Text" }, nullable: false },
      { name: "content_hash", column_type: { type: "Text" }, nullable: true },
      { name: "content_encoding", column_type: { type: "Text" }, nullable: true },
      { name: "content_type", column_type: { type: "Text" }, nullable: true },
      { name: "size_bytes", column_type: { type: "Integer" }, nullable: true },
      { name: "replication_scope", column_type: { type: "Text" }, nullable: false },
      { name: "privacy_mode", column_type: { type: "Text" }, nullable: false },
      { name: "access_policy_json", column_type: { type: "Json" }, nullable: false },
      { name: "metadata_json", column_type: { type: "Json" }, nullable: false },
      { name: "created_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
  upload_jobs: {
    columns: [
      { name: "upload_job_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "workspace_id", column_type: { type: "Text" }, nullable: false },
      { name: "target_kind", column_type: { type: "Text" }, nullable: false },
      { name: "target_id", column_type: { type: "Text" }, nullable: false },
      { name: "status", column_type: { type: "Text" }, nullable: false },
      { name: "backend", column_type: { type: "Text" }, nullable: false },
      { name: "object_ref_id", column_type: { type: "Text" }, nullable: true },
      {
        name: "object_ref_row_id",
        column_type: { type: "Uuid" },
        nullable: true,
        references: "object_refs",
      },
      { name: "attempt_count", column_type: { type: "Integer" }, nullable: false },
      { name: "last_error", column_type: { type: "Text" }, nullable: true },
      { name: "next_retry_at", column_type: { type: "Timestamp" }, nullable: true },
      { name: "claimed_by", column_type: { type: "Text" }, nullable: true },
      { name: "lease_expires_at", column_type: { type: "Timestamp" }, nullable: true },
      { name: "last_heartbeat_at", column_type: { type: "Timestamp" }, nullable: true },
      { name: "completed_at", column_type: { type: "Timestamp" }, nullable: true },
      { name: "failed_at", column_type: { type: "Timestamp" }, nullable: true },
      { name: "access_policy_json", column_type: { type: "Json" }, nullable: false },
      { name: "request_json", column_type: { type: "Json" }, nullable: false },
      { name: "created_at", column_type: { type: "Timestamp" }, nullable: false },
      { name: "updated_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
  upload_receipts: {
    columns: [
      { name: "receipt_id", column_type: { type: "Text" }, nullable: false },
      { name: "upload_job_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "upload_job_row_id",
        column_type: { type: "Uuid" },
        nullable: true,
        references: "upload_jobs",
      },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "object_ref_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "object_ref_row_id",
        column_type: { type: "Uuid" },
        nullable: true,
        references: "object_refs",
      },
      { name: "backend", column_type: { type: "Text" }, nullable: false },
      { name: "storage_backend", column_type: { type: "Text" }, nullable: true },
      { name: "bucket", column_type: { type: "Text" }, nullable: true },
      { name: "region", column_type: { type: "Text" }, nullable: true },
      { name: "key", column_type: { type: "Text" }, nullable: false },
      { name: "uri", column_type: { type: "Text" }, nullable: false },
      { name: "received_at", column_type: { type: "Timestamp" }, nullable: false },
      { name: "metadata_json", column_type: { type: "Json" }, nullable: false },
    ],
  },
  codebase_index_snapshots: {
    columns: [
      { name: "snapshot_id", column_type: { type: "Text" }, nullable: false },
      { name: "session_id", column_type: { type: "Text" }, nullable: false },
      {
        name: "session_row_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "trace_sessions",
      },
      { name: "workspace_id", column_type: { type: "Text" }, nullable: false },
      { name: "checkpoint_id", column_type: { type: "Text" }, nullable: true },
      { name: "phase", column_type: { type: "Text" }, nullable: false },
      { name: "root_path", column_type: { type: "Text" }, nullable: false },
      { name: "project_hash", column_type: { type: "Text" }, nullable: true },
      { name: "file_count", column_type: { type: "Integer" }, nullable: false },
      { name: "changed_path_count", column_type: { type: "Integer" }, nullable: false },
      { name: "manifest_object_ref_id", column_type: { type: "Text" }, nullable: true },
      {
        name: "manifest_object_ref_row_id",
        column_type: { type: "Uuid" },
        nullable: true,
        references: "object_refs",
      },
      { name: "delta_object_ref_id", column_type: { type: "Text" }, nullable: true },
      {
        name: "delta_object_ref_row_id",
        column_type: { type: "Uuid" },
        nullable: true,
        references: "object_refs",
      },
      { name: "latest_object_ref_id", column_type: { type: "Text" }, nullable: true },
      {
        name: "latest_object_ref_row_id",
        column_type: { type: "Uuid" },
        nullable: true,
        references: "object_refs",
      },
      { name: "access_policy_json", column_type: { type: "Json" }, nullable: false },
      { name: "metadata_json", column_type: { type: "Json" }, nullable: false },
      { name: "captured_at", column_type: { type: "Timestamp" }, nullable: false },
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
