// AUTO-GENERATED FILE - DO NOT EDIT

// Regenerate via: node scripts/generate-app.mjs

// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean;
  $canEdit: boolean;
  $canDelete: boolean;
}

export interface Agent {
  id: string;
  agent_id: string;
  lane?: string;
  spec_path?: string;
  prompt_surface?: string;
  status?: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface AgentRun {
  id: string;
  run_id: string;
  agent_id: string;
  agent_row_id: string;
  thread_id?: string;
  turn_id?: string;
  cwd?: string;
  repo_root?: string;
  request_summary?: string;
  status: string;
  started_at: Date;
  ended_at?: Date;
  context_json?: JsonValue;
  source_trace_path?: string;
}

export interface RunItem {
  id: string;
  item_id: string;
  run_id: string;
  run_row_id: string;
  item_kind: string;
  phase?: string;
  sequence: number;
  status: string;
  summary_json?: JsonValue;
  started_at: Date;
  completed_at?: Date;
}

export interface SemanticEvent {
  id: string;
  event_id: string;
  run_id: string;
  run_row_id: string;
  item_id?: string;
  item_row_id?: string;
  event_type: string;
  summary_text?: string;
  payload_json?: JsonValue;
  occurred_at: Date;
}

export interface WireEvent {
  id: string;
  event_id: string;
  run_id?: string;
  run_row_id?: string;
  connection_id?: number;
  session_id?: number;
  direction: string;
  method?: string;
  request_id?: string;
  payload_json?: JsonValue;
  occurred_at: Date;
}

export interface Artifact {
  id: string;
  artifact_id: string;
  run_id: string;
  run_row_id: string;
  artifact_kind: string;
  title?: string;
  absolute_path: string;
  checksum?: string;
  created_at: Date;
}

export interface AgentStateSnapshot {
  id: string;
  snapshot_id: string;
  agent_id: string;
  agent_row_id: string;
  state_version?: number;
  status?: string;
  state_json: JsonValue;
  captured_at: Date;
}

export interface WorkspaceSnapshot {
  id: string;
  snapshot_id: string;
  run_id: string;
  run_row_id: string;
  repo_root: string;
  branch?: string;
  head_commit?: string;
  dirty_path_count?: number;
  snapshot_json?: JsonValue;
  captured_at: Date;
}

export interface MemoryLink {
  id: string;
  link_id: string;
  run_id?: string;
  run_row_id?: string;
  item_id?: string;
  item_row_id?: string;
  memory_scope: string;
  memory_ref?: string;
  query_text?: string;
  link_json?: JsonValue;
  created_at: Date;
}

export interface SourceFile {
  id: string;
  source_file_id: string;
  run_id?: string;
  run_row_id?: string;
  file_kind: string;
  absolute_path: string;
  checksum?: string;
  created_at: Date;
}

export interface DaemonLogSource {
  id: string;
  source_id: string;
  manager: string;
  daemon_name: string;
  stream: string;
  host_id?: string;
  log_path: string;
  config_path?: string;
  repo_root?: string;
  workspace_root?: string;
  owner_agent?: string;
  flow_daemon_name?: string;
  launchd_label?: string;
  retention_class: string;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export interface DaemonLogChunk {
  id: string;
  chunk_id: string;
  source_id: string;
  source_row_id: string;
  daemon_name: string;
  stream: string;
  host_id?: string;
  log_path: string;
  file_fingerprint: string;
  start_offset: number;
  end_offset: number;
  first_line_no: number;
  last_line_no: number;
  line_count: number;
  byte_count: number;
  first_at?: Date;
  last_at?: Date;
  sha256: string;
  body_ref?: string;
  body_preview?: string;
  compression: string;
  ingested_at: Date;
}

export interface DaemonLogEvent {
  id: string;
  event_id: string;
  source_id: string;
  source_row_id: string;
  chunk_id: string;
  chunk_row_id: string;
  daemon_name: string;
  stream: string;
  seq: number;
  line_no: number;
  at?: Date;
  level: string;
  message: string;
  fields_json?: JsonValue;
  repo_root?: string;
  workspace_root?: string;
  conversation?: string;
  conversation_hash?: string;
  run_id?: string;
  job_id?: string;
  trace_id?: string;
  span_id?: string;
  error_kind?: string;
  created_at: Date;
}

export interface DaemonLogCheckpoint {
  id: string;
  checkpoint_id: string;
  source_id: string;
  source_row_id: string;
  host_id?: string;
  log_path: string;
  file_fingerprint: string;
  inode?: string;
  device?: string;
  offset: number;
  line_no: number;
  last_chunk_id?: string;
  last_event_id?: string;
  last_seen_at?: Date;
  updated_at: Date;
}

export interface DaemonLogSummary {
  id: string;
  summary_id: string;
  source_id: string;
  source_row_id: string;
  daemon_name: string;
  window_start: Date;
  window_end: Date;
  level_counts_json: JsonValue;
  error_count: number;
  warning_count: number;
  first_error_event_id?: string;
  last_error_event_id?: string;
  top_error_kinds_json?: JsonValue;
  summary_text?: string;
  created_at: Date;
}

export interface TaskRecord {
  id: string;
  task_id: string;
  context: string;
  title: string;
  status: string;
  priority: string;
  placement: string;
  focus_rank?: number;
  project: string;
  issue?: string;
  branch?: string;
  workspace?: string;
  plan?: string;
  pr?: string;
  tags_json?: JsonValue;
  next_text?: string;
  context_text?: string;
  notes_text?: string;
  annotations_json?: JsonValue;
  source_kind?: string;
  source_path?: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerObjectRef {
  id: string;
  object_ref_id: string;
  provider: string;
  uri: string;
  bucket?: string;
  key?: string;
  region?: string;
  digest_sha256?: string;
  byte_size?: number;
  content_type?: string;
  object_kind: string;
  status: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerAgent {
  id: string;
  agent_id: string;
  agent_kind: string;
  provider: string;
  display_name: string;
  model?: string;
  default_context_json?: JsonValue;
  tool_contract_json?: JsonValue;
  status: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerAgentTool {
  id: string;
  tool_id: string;
  agent_id: string;
  agent_row_id: string;
  tool_name: string;
  tool_kind: string;
  input_schema_json?: JsonValue;
  output_schema_json?: JsonValue;
  scope_json?: JsonValue;
  status: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerAgentContext {
  id: string;
  context_id: string;
  agent_id: string;
  agent_row_id: string;
  context_kind: string;
  source_kind: string;
  object_ref_id?: string;
  object_ref_row_id?: string;
  inline_context_json?: JsonValue;
  priority: number;
  status: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCodexConversation {
  id: string;
  conversation_id: string;
  provider: string;
  provider_session_id: string;
  thread_id?: string;
  workspace_id?: string;
  workspace_key?: string;
  repo_root?: string;
  workspace_root?: string;
  branch?: string;
  model?: string;
  status: string;
  transcript_object_ref_id: string;
  transcript_object_row_id: string;
  latest_event_sequence?: number;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
  ended_at?: Date;
}

export interface DesignerCodexTurn {
  id: string;
  turn_id: string;
  conversation_id: string;
  conversation_row_id: string;
  sequence: number;
  turn_kind: string;
  role: string;
  actor_kind: string;
  actor_id?: string;
  summary_text?: string;
  payload_object_ref_id: string;
  payload_object_row_id: string;
  prompt_object_ref_id?: string;
  prompt_object_row_id?: string;
  response_object_ref_id?: string;
  response_object_row_id?: string;
  token_counts_json?: JsonValue;
  status: string;
  started_at: Date;
  completed_at?: Date;
}

export interface DesignerTelemetryEvent {
  id: string;
  telemetry_event_id: string;
  session_id?: string;
  workspace_id?: string;
  conversation_id?: string;
  conversation_row_id?: string;
  event_type: string;
  pane?: string;
  sequence?: number;
  summary_text?: string;
  payload_object_ref_id: string;
  payload_object_row_id: string;
  properties_json?: JsonValue;
  occurred_at: Date;
  ingested_at: Date;
}

export interface DesignerLiveCommit {
  id: string;
  commit_id: string;
  repo_root: string;
  workspace_root?: string;
  branch: string;
  bookmark?: string;
  live_ref?: string;
  tree_id?: string;
  parent_commit_ids_json?: JsonValue;
  subject: string;
  body?: string;
  author_name?: string;
  author_email?: string;
  committer_name?: string;
  committer_email?: string;
  trace_ref?: string;
  source_session_id?: string;
  source_turn_ordinal?: number;
  source_conversation_id?: string;
  source_conversation_row_id?: string;
  source_turn_id?: string;
  source_turn_row_id?: string;
  agent_id?: string;
  agent_row_id?: string;
  courier_run_id?: string;
  live_snapshot_ref?: string;
  changed_paths_json?: JsonValue;
  patch_object_ref_id?: string;
  patch_object_row_id?: string;
  manifest_object_ref_id?: string;
  manifest_object_row_id?: string;
  status: string;
  committed_at?: Date;
  reflected_at?: Date;
  ingested_at: Date;
}

export interface DesignerCadWorkspace {
  id: string;
  workspace_id: string;
  workspace_key: string;
  title?: string;
  repo_root?: string;
  workspace_root?: string;
  status: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCadDocument {
  id: string;
  document_id: string;
  workspace_id: string;
  workspace_row_id: string;
  file_path: string;
  language: string;
  source_kind: string;
  source_hash?: string;
  status: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCadSession {
  id: string;
  cad_session_id: string;
  workspace_id: string;
  workspace_row_id: string;
  document_id: string;
  document_row_id: string;
  codex_session_id?: string;
  agent_run_id?: string;
  status: string;
  active_tool_session_id?: string;
  latest_projection_id?: string;
  opened_by?: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
  closed_at?: Date;
}

export interface DesignerCadEvent {
  id: string;
  event_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  sequence: number;
  event_kind: string;
  actor_kind: string;
  actor_id?: string;
  tool_session_id?: string;
  operation_id?: string;
  preview_id?: string;
  source_event_id?: string;
  payload_json?: JsonValue;
  occurred_at: Date;
  observed_at: Date;
}

export interface DesignerCadSceneNode {
  id: string;
  node_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  document_id: string;
  document_row_id: string;
  projection_id: string;
  kind: string;
  label?: string;
  path?: string;
  parent_node_id?: string;
  stable_ref?: string;
  visibility?: string;
  source_span_json?: JsonValue;
  geometry_ref_json?: JsonValue;
  metadata_json?: JsonValue;
  updated_at: Date;
}

export interface DesignerCadSelection {
  id: string;
  selection_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  actor_kind: string;
  actor_id?: string;
  target_kind: string;
  target_id: string;
  node_id?: string;
  selection_json?: JsonValue;
  status: string;
  updated_at: Date;
}

export interface DesignerCadToolSession {
  id: string;
  tool_session_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  tool_kind: string;
  actor_kind: string;
  actor_id?: string;
  status: string;
  input_json?: JsonValue;
  state_json?: JsonValue;
  started_at: Date;
  updated_at: Date;
  completed_at?: Date;
}

export interface DesignerCadOperation {
  id: string;
  operation_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  tool_session_id?: string;
  tool_session_row_id?: string;
  actor_kind: string;
  actor_id?: string;
  operation_kind: string;
  status: string;
  operation_json: JsonValue;
  validation_json?: JsonValue;
  result_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
  applied_at?: Date;
}

export interface DesignerCadSourceEdit {
  id: string;
  edit_id: string;
  operation_id: string;
  operation_row_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  sequence: number;
  file_path: string;
  range_json: JsonValue;
  text_preview?: string;
  text_sha256?: string;
  status: string;
  created_at: Date;
}

export interface DesignerCadPreviewHandle {
  id: string;
  preview_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  tool_session_id?: string;
  tool_session_row_id?: string;
  operation_id?: string;
  operation_row_id?: string;
  preview_kind: string;
  target_json?: JsonValue;
  status: string;
  handle_ref?: string;
  created_at: Date;
  updated_at: Date;
  disposed_at?: Date;
}

export interface DesignerCadPreviewUpdate {
  id: string;
  update_id: string;
  preview_id: string;
  preview_row_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  sequence: number;
  params_json?: JsonValue;
  mesh_ref_json?: JsonValue;
  status: string;
  error_text?: string;
  requested_at: Date;
  completed_at?: Date;
}

export interface DesignerCadWidget {
  id: string;
  widget_id: string;
  workspace_id: string;
  workspace_row_id: string;
  widget_key: string;
  title?: string;
  source_kind: string;
  source_path?: string;
  version?: string;
  status: string;
  manifest_json?: JsonValue;
  state_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCadSteer {
  id: string;
  steer_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  actor_kind: string;
  actor_id?: string;
  target_agent_id?: string;
  target_run_id?: string;
  message_text: string;
  context_json?: JsonValue;
  status: string;
  created_at: Date;
}

export interface AgentInit {
  agent_id: string;
  lane?: string | null;
  spec_path?: string | null;
  prompt_surface?: string | null;
  status?: string | null;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface AgentRunInit {
  run_id: string;
  agent_id: string;
  agent_row_id: string;
  thread_id?: string | null;
  turn_id?: string | null;
  cwd?: string | null;
  repo_root?: string | null;
  request_summary?: string | null;
  status: string;
  started_at: Date;
  ended_at?: Date | null;
  context_json?: JsonValue | null;
  source_trace_path?: string | null;
}

export interface RunItemInit {
  item_id: string;
  run_id: string;
  run_row_id: string;
  item_kind: string;
  phase?: string | null;
  sequence: number;
  status: string;
  summary_json?: JsonValue | null;
  started_at: Date;
  completed_at?: Date | null;
}

export interface SemanticEventInit {
  event_id: string;
  run_id: string;
  run_row_id: string;
  item_id?: string | null;
  item_row_id?: string | null;
  event_type: string;
  summary_text?: string | null;
  payload_json?: JsonValue | null;
  occurred_at: Date;
}

export interface WireEventInit {
  event_id: string;
  run_id?: string | null;
  run_row_id?: string | null;
  connection_id?: number | null;
  session_id?: number | null;
  direction: string;
  method?: string | null;
  request_id?: string | null;
  payload_json?: JsonValue | null;
  occurred_at: Date;
}

export interface ArtifactInit {
  artifact_id: string;
  run_id: string;
  run_row_id: string;
  artifact_kind: string;
  title?: string | null;
  absolute_path: string;
  checksum?: string | null;
  created_at: Date;
}

export interface AgentStateSnapshotInit {
  snapshot_id: string;
  agent_id: string;
  agent_row_id: string;
  state_version?: number | null;
  status?: string | null;
  state_json: JsonValue;
  captured_at: Date;
}

export interface WorkspaceSnapshotInit {
  snapshot_id: string;
  run_id: string;
  run_row_id: string;
  repo_root: string;
  branch?: string | null;
  head_commit?: string | null;
  dirty_path_count?: number | null;
  snapshot_json?: JsonValue | null;
  captured_at: Date;
}

export interface MemoryLinkInit {
  link_id: string;
  run_id?: string | null;
  run_row_id?: string | null;
  item_id?: string | null;
  item_row_id?: string | null;
  memory_scope: string;
  memory_ref?: string | null;
  query_text?: string | null;
  link_json?: JsonValue | null;
  created_at: Date;
}

export interface SourceFileInit {
  source_file_id: string;
  run_id?: string | null;
  run_row_id?: string | null;
  file_kind: string;
  absolute_path: string;
  checksum?: string | null;
  created_at: Date;
}

export interface DaemonLogSourceInit {
  source_id: string;
  manager: string;
  daemon_name: string;
  stream: string;
  host_id?: string | null;
  log_path: string;
  config_path?: string | null;
  repo_root?: string | null;
  workspace_root?: string | null;
  owner_agent?: string | null;
  flow_daemon_name?: string | null;
  launchd_label?: string | null;
  retention_class: string;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export interface DaemonLogChunkInit {
  chunk_id: string;
  source_id: string;
  source_row_id: string;
  daemon_name: string;
  stream: string;
  host_id?: string | null;
  log_path: string;
  file_fingerprint: string;
  start_offset: number;
  end_offset: number;
  first_line_no: number;
  last_line_no: number;
  line_count: number;
  byte_count: number;
  first_at?: Date | null;
  last_at?: Date | null;
  sha256: string;
  body_ref?: string | null;
  body_preview?: string | null;
  compression: string;
  ingested_at: Date;
}

export interface DaemonLogEventInit {
  event_id: string;
  source_id: string;
  source_row_id: string;
  chunk_id: string;
  chunk_row_id: string;
  daemon_name: string;
  stream: string;
  seq: number;
  line_no: number;
  at?: Date | null;
  level: string;
  message: string;
  fields_json?: JsonValue | null;
  repo_root?: string | null;
  workspace_root?: string | null;
  conversation?: string | null;
  conversation_hash?: string | null;
  run_id?: string | null;
  job_id?: string | null;
  trace_id?: string | null;
  span_id?: string | null;
  error_kind?: string | null;
  created_at: Date;
}

export interface DaemonLogCheckpointInit {
  checkpoint_id: string;
  source_id: string;
  source_row_id: string;
  host_id?: string | null;
  log_path: string;
  file_fingerprint: string;
  inode?: string | null;
  device?: string | null;
  offset: number;
  line_no: number;
  last_chunk_id?: string | null;
  last_event_id?: string | null;
  last_seen_at?: Date | null;
  updated_at: Date;
}

export interface DaemonLogSummaryInit {
  summary_id: string;
  source_id: string;
  source_row_id: string;
  daemon_name: string;
  window_start: Date;
  window_end: Date;
  level_counts_json: JsonValue;
  error_count: number;
  warning_count: number;
  first_error_event_id?: string | null;
  last_error_event_id?: string | null;
  top_error_kinds_json?: JsonValue | null;
  summary_text?: string | null;
  created_at: Date;
}

export interface TaskRecordInit {
  task_id: string;
  context: string;
  title: string;
  status: string;
  priority: string;
  placement: string;
  focus_rank?: number | null;
  project: string;
  issue?: string | null;
  branch?: string | null;
  workspace?: string | null;
  plan?: string | null;
  pr?: string | null;
  tags_json?: JsonValue | null;
  next_text?: string | null;
  context_text?: string | null;
  notes_text?: string | null;
  annotations_json?: JsonValue | null;
  source_kind?: string | null;
  source_path?: string | null;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerObjectRefInit {
  object_ref_id: string;
  provider: string;
  uri: string;
  bucket?: string | null;
  key?: string | null;
  region?: string | null;
  digest_sha256?: string | null;
  byte_size?: number | null;
  content_type?: string | null;
  object_kind: string;
  status: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerAgentInit {
  agent_id: string;
  agent_kind: string;
  provider: string;
  display_name: string;
  model?: string | null;
  default_context_json?: JsonValue | null;
  tool_contract_json?: JsonValue | null;
  status: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerAgentToolInit {
  tool_id: string;
  agent_id: string;
  agent_row_id: string;
  tool_name: string;
  tool_kind: string;
  input_schema_json?: JsonValue | null;
  output_schema_json?: JsonValue | null;
  scope_json?: JsonValue | null;
  status: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerAgentContextInit {
  context_id: string;
  agent_id: string;
  agent_row_id: string;
  context_kind: string;
  source_kind: string;
  object_ref_id?: string | null;
  object_ref_row_id?: string | null;
  inline_context_json?: JsonValue | null;
  priority: number;
  status: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCodexConversationInit {
  conversation_id: string;
  provider: string;
  provider_session_id: string;
  thread_id?: string | null;
  workspace_id?: string | null;
  workspace_key?: string | null;
  repo_root?: string | null;
  workspace_root?: string | null;
  branch?: string | null;
  model?: string | null;
  status: string;
  transcript_object_ref_id: string;
  transcript_object_row_id: string;
  latest_event_sequence?: number | null;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
  ended_at?: Date | null;
}

export interface DesignerCodexTurnInit {
  turn_id: string;
  conversation_id: string;
  conversation_row_id: string;
  sequence: number;
  turn_kind: string;
  role: string;
  actor_kind: string;
  actor_id?: string | null;
  summary_text?: string | null;
  payload_object_ref_id: string;
  payload_object_row_id: string;
  prompt_object_ref_id?: string | null;
  prompt_object_row_id?: string | null;
  response_object_ref_id?: string | null;
  response_object_row_id?: string | null;
  token_counts_json?: JsonValue | null;
  status: string;
  started_at: Date;
  completed_at?: Date | null;
}

export interface DesignerTelemetryEventInit {
  telemetry_event_id: string;
  session_id?: string | null;
  workspace_id?: string | null;
  conversation_id?: string | null;
  conversation_row_id?: string | null;
  event_type: string;
  pane?: string | null;
  sequence?: number | null;
  summary_text?: string | null;
  payload_object_ref_id: string;
  payload_object_row_id: string;
  properties_json?: JsonValue | null;
  occurred_at: Date;
  ingested_at: Date;
}

export interface DesignerLiveCommitInit {
  commit_id: string;
  repo_root: string;
  workspace_root?: string | null;
  branch: string;
  bookmark?: string | null;
  live_ref?: string | null;
  tree_id?: string | null;
  parent_commit_ids_json?: JsonValue | null;
  subject: string;
  body?: string | null;
  author_name?: string | null;
  author_email?: string | null;
  committer_name?: string | null;
  committer_email?: string | null;
  trace_ref?: string | null;
  source_session_id?: string | null;
  source_turn_ordinal?: number | null;
  source_conversation_id?: string | null;
  source_conversation_row_id?: string | null;
  source_turn_id?: string | null;
  source_turn_row_id?: string | null;
  agent_id?: string | null;
  agent_row_id?: string | null;
  courier_run_id?: string | null;
  live_snapshot_ref?: string | null;
  changed_paths_json?: JsonValue | null;
  patch_object_ref_id?: string | null;
  patch_object_row_id?: string | null;
  manifest_object_ref_id?: string | null;
  manifest_object_row_id?: string | null;
  status: string;
  committed_at?: Date | null;
  reflected_at?: Date | null;
  ingested_at: Date;
}

export interface DesignerCadWorkspaceInit {
  workspace_id: string;
  workspace_key: string;
  title?: string | null;
  repo_root?: string | null;
  workspace_root?: string | null;
  status: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCadDocumentInit {
  document_id: string;
  workspace_id: string;
  workspace_row_id: string;
  file_path: string;
  language: string;
  source_kind: string;
  source_hash?: string | null;
  status: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCadSessionInit {
  cad_session_id: string;
  workspace_id: string;
  workspace_row_id: string;
  document_id: string;
  document_row_id: string;
  codex_session_id?: string | null;
  agent_run_id?: string | null;
  status: string;
  active_tool_session_id?: string | null;
  latest_projection_id?: string | null;
  opened_by?: string | null;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
  closed_at?: Date | null;
}

export interface DesignerCadEventInit {
  event_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  sequence: number;
  event_kind: string;
  actor_kind: string;
  actor_id?: string | null;
  tool_session_id?: string | null;
  operation_id?: string | null;
  preview_id?: string | null;
  source_event_id?: string | null;
  payload_json?: JsonValue | null;
  occurred_at: Date;
  observed_at: Date;
}

export interface DesignerCadSceneNodeInit {
  node_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  document_id: string;
  document_row_id: string;
  projection_id: string;
  kind: string;
  label?: string | null;
  path?: string | null;
  parent_node_id?: string | null;
  stable_ref?: string | null;
  visibility?: string | null;
  source_span_json?: JsonValue | null;
  geometry_ref_json?: JsonValue | null;
  metadata_json?: JsonValue | null;
  updated_at: Date;
}

export interface DesignerCadSelectionInit {
  selection_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  actor_kind: string;
  actor_id?: string | null;
  target_kind: string;
  target_id: string;
  node_id?: string | null;
  selection_json?: JsonValue | null;
  status: string;
  updated_at: Date;
}

export interface DesignerCadToolSessionInit {
  tool_session_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  tool_kind: string;
  actor_kind: string;
  actor_id?: string | null;
  status: string;
  input_json?: JsonValue | null;
  state_json?: JsonValue | null;
  started_at: Date;
  updated_at: Date;
  completed_at?: Date | null;
}

export interface DesignerCadOperationInit {
  operation_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  tool_session_id?: string | null;
  tool_session_row_id?: string | null;
  actor_kind: string;
  actor_id?: string | null;
  operation_kind: string;
  status: string;
  operation_json: JsonValue;
  validation_json?: JsonValue | null;
  result_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
  applied_at?: Date | null;
}

export interface DesignerCadSourceEditInit {
  edit_id: string;
  operation_id: string;
  operation_row_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  sequence: number;
  file_path: string;
  range_json: JsonValue;
  text_preview?: string | null;
  text_sha256?: string | null;
  status: string;
  created_at: Date;
}

export interface DesignerCadPreviewHandleInit {
  preview_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  tool_session_id?: string | null;
  tool_session_row_id?: string | null;
  operation_id?: string | null;
  operation_row_id?: string | null;
  preview_kind: string;
  target_json?: JsonValue | null;
  status: string;
  handle_ref?: string | null;
  created_at: Date;
  updated_at: Date;
  disposed_at?: Date | null;
}

export interface DesignerCadPreviewUpdateInit {
  update_id: string;
  preview_id: string;
  preview_row_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  sequence: number;
  params_json?: JsonValue | null;
  mesh_ref_json?: JsonValue | null;
  status: string;
  error_text?: string | null;
  requested_at: Date;
  completed_at?: Date | null;
}

export interface DesignerCadWidgetInit {
  widget_id: string;
  workspace_id: string;
  workspace_row_id: string;
  widget_key: string;
  title?: string | null;
  source_kind: string;
  source_path?: string | null;
  version?: string | null;
  status: string;
  manifest_json?: JsonValue | null;
  state_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface DesignerCadSteerInit {
  steer_id: string;
  cad_session_id: string;
  cad_session_row_id: string;
  actor_kind: string;
  actor_id?: string | null;
  target_agent_id?: string | null;
  target_run_id?: string | null;
  message_text: string;
  context_json?: JsonValue | null;
  status: string;
  created_at: Date;
}

export interface AgentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  lane?: string | { eq?: string; ne?: string; contains?: string };
  spec_path?: string | { eq?: string; ne?: string; contains?: string };
  prompt_surface?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface AgentRunWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_row_id?: string | { eq?: string; ne?: string };
  thread_id?: string | { eq?: string; ne?: string; contains?: string };
  turn_id?: string | { eq?: string; ne?: string; contains?: string };
  cwd?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  request_summary?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  ended_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  context_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  source_trace_path?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface RunItemWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  item_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  item_kind?: string | { eq?: string; ne?: string; contains?: string };
  phase?: string | { eq?: string; ne?: string; contains?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  status?: string | { eq?: string; ne?: string; contains?: string };
  summary_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface SemanticEventWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  event_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  item_id?: string | { eq?: string; ne?: string; contains?: string };
  item_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  event_type?: string | { eq?: string; ne?: string; contains?: string };
  summary_text?: string | { eq?: string; ne?: string; contains?: string };
  payload_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  occurred_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface WireEventWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  event_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  connection_id?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  session_id?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  direction?: string | { eq?: string; ne?: string; contains?: string };
  method?: string | { eq?: string; ne?: string; contains?: string };
  request_id?: string | { eq?: string; ne?: string; contains?: string };
  payload_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  occurred_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ArtifactWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  artifact_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  artifact_kind?: string | { eq?: string; ne?: string; contains?: string };
  title?: string | { eq?: string; ne?: string; contains?: string };
  absolute_path?: string | { eq?: string; ne?: string; contains?: string };
  checksum?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface AgentStateSnapshotWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  snapshot_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_row_id?: string | { eq?: string; ne?: string };
  state_version?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  status?: string | { eq?: string; ne?: string; contains?: string };
  state_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  captured_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface WorkspaceSnapshotWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  snapshot_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  branch?: string | { eq?: string; ne?: string; contains?: string };
  head_commit?: string | { eq?: string; ne?: string; contains?: string };
  dirty_path_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  snapshot_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  captured_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface MemoryLinkWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  link_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  item_id?: string | { eq?: string; ne?: string; contains?: string };
  item_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  memory_scope?: string | { eq?: string; ne?: string; contains?: string };
  memory_ref?: string | { eq?: string; ne?: string; contains?: string };
  query_text?: string | { eq?: string; ne?: string; contains?: string };
  link_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface SourceFileWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  source_file_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  file_kind?: string | { eq?: string; ne?: string; contains?: string };
  absolute_path?: string | { eq?: string; ne?: string; contains?: string };
  checksum?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DaemonLogSourceWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  source_id?: string | { eq?: string; ne?: string; contains?: string };
  manager?: string | { eq?: string; ne?: string; contains?: string };
  daemon_name?: string | { eq?: string; ne?: string; contains?: string };
  stream?: string | { eq?: string; ne?: string; contains?: string };
  host_id?: string | { eq?: string; ne?: string; contains?: string };
  log_path?: string | { eq?: string; ne?: string; contains?: string };
  config_path?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  workspace_root?: string | { eq?: string; ne?: string; contains?: string };
  owner_agent?: string | { eq?: string; ne?: string; contains?: string };
  flow_daemon_name?: string | { eq?: string; ne?: string; contains?: string };
  launchd_label?: string | { eq?: string; ne?: string; contains?: string };
  retention_class?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DaemonLogChunkWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chunk_id?: string | { eq?: string; ne?: string; contains?: string };
  source_id?: string | { eq?: string; ne?: string; contains?: string };
  source_row_id?: string | { eq?: string; ne?: string };
  daemon_name?: string | { eq?: string; ne?: string; contains?: string };
  stream?: string | { eq?: string; ne?: string; contains?: string };
  host_id?: string | { eq?: string; ne?: string; contains?: string };
  log_path?: string | { eq?: string; ne?: string; contains?: string };
  file_fingerprint?: string | { eq?: string; ne?: string; contains?: string };
  start_offset?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  end_offset?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  first_line_no?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  last_line_no?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  line_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  byte_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  first_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  sha256?: string | { eq?: string; ne?: string; contains?: string };
  body_ref?: string | { eq?: string; ne?: string; contains?: string };
  body_preview?: string | { eq?: string; ne?: string; contains?: string };
  compression?: string | { eq?: string; ne?: string; contains?: string };
  ingested_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DaemonLogEventWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  event_id?: string | { eq?: string; ne?: string; contains?: string };
  source_id?: string | { eq?: string; ne?: string; contains?: string };
  source_row_id?: string | { eq?: string; ne?: string };
  chunk_id?: string | { eq?: string; ne?: string; contains?: string };
  chunk_row_id?: string | { eq?: string; ne?: string };
  daemon_name?: string | { eq?: string; ne?: string; contains?: string };
  stream?: string | { eq?: string; ne?: string; contains?: string };
  seq?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  line_no?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  level?: string | { eq?: string; ne?: string; contains?: string };
  message?: string | { eq?: string; ne?: string; contains?: string };
  fields_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  workspace_root?: string | { eq?: string; ne?: string; contains?: string };
  conversation?: string | { eq?: string; ne?: string; contains?: string };
  conversation_hash?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  job_id?: string | { eq?: string; ne?: string; contains?: string };
  trace_id?: string | { eq?: string; ne?: string; contains?: string };
  span_id?: string | { eq?: string; ne?: string; contains?: string };
  error_kind?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DaemonLogCheckpointWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  checkpoint_id?: string | { eq?: string; ne?: string; contains?: string };
  source_id?: string | { eq?: string; ne?: string; contains?: string };
  source_row_id?: string | { eq?: string; ne?: string };
  host_id?: string | { eq?: string; ne?: string; contains?: string };
  log_path?: string | { eq?: string; ne?: string; contains?: string };
  file_fingerprint?: string | { eq?: string; ne?: string; contains?: string };
  inode?: string | { eq?: string; ne?: string; contains?: string };
  device?: string | { eq?: string; ne?: string; contains?: string };
  offset?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  line_no?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  last_chunk_id?: string | { eq?: string; ne?: string; contains?: string };
  last_event_id?: string | { eq?: string; ne?: string; contains?: string };
  last_seen_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DaemonLogSummaryWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  summary_id?: string | { eq?: string; ne?: string; contains?: string };
  source_id?: string | { eq?: string; ne?: string; contains?: string };
  source_row_id?: string | { eq?: string; ne?: string };
  daemon_name?: string | { eq?: string; ne?: string; contains?: string };
  window_start?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  window_end?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  level_counts_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  error_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  warning_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  first_error_event_id?: string | { eq?: string; ne?: string; contains?: string };
  last_error_event_id?: string | { eq?: string; ne?: string; contains?: string };
  top_error_kinds_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  summary_text?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface TaskRecordWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  task_id?: string | { eq?: string; ne?: string; contains?: string };
  context?: string | { eq?: string; ne?: string; contains?: string };
  title?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  priority?: string | { eq?: string; ne?: string; contains?: string };
  placement?: string | { eq?: string; ne?: string; contains?: string };
  focus_rank?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  project?: string | { eq?: string; ne?: string; contains?: string };
  issue?: string | { eq?: string; ne?: string; contains?: string };
  branch?: string | { eq?: string; ne?: string; contains?: string };
  workspace?: string | { eq?: string; ne?: string; contains?: string };
  plan?: string | { eq?: string; ne?: string; contains?: string };
  pr?: string | { eq?: string; ne?: string; contains?: string };
  tags_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  next_text?: string | { eq?: string; ne?: string; contains?: string };
  context_text?: string | { eq?: string; ne?: string; contains?: string };
  notes_text?: string | { eq?: string; ne?: string; contains?: string };
  annotations_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  source_kind?: string | { eq?: string; ne?: string; contains?: string };
  source_path?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerObjectRefWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  provider?: string | { eq?: string; ne?: string; contains?: string };
  uri?: string | { eq?: string; ne?: string; contains?: string };
  bucket?: string | { eq?: string; ne?: string; contains?: string };
  key?: string | { eq?: string; ne?: string; contains?: string };
  region?: string | { eq?: string; ne?: string; contains?: string };
  digest_sha256?: string | { eq?: string; ne?: string; contains?: string };
  byte_size?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  content_type?: string | { eq?: string; ne?: string; contains?: string };
  object_kind?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerAgentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_kind?: string | { eq?: string; ne?: string; contains?: string };
  provider?: string | { eq?: string; ne?: string; contains?: string };
  display_name?: string | { eq?: string; ne?: string; contains?: string };
  model?: string | { eq?: string; ne?: string; contains?: string };
  default_context_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  tool_contract_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerAgentToolWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  tool_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_row_id?: string | { eq?: string; ne?: string };
  tool_name?: string | { eq?: string; ne?: string; contains?: string };
  tool_kind?: string | { eq?: string; ne?: string; contains?: string };
  input_schema_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  output_schema_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  scope_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerAgentContextWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  context_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_row_id?: string | { eq?: string; ne?: string };
  context_kind?: string | { eq?: string; ne?: string; contains?: string };
  source_kind?: string | { eq?: string; ne?: string; contains?: string };
  object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  object_ref_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  inline_context_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  priority?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCodexConversationWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  conversation_id?: string | { eq?: string; ne?: string; contains?: string };
  provider?: string | { eq?: string; ne?: string; contains?: string };
  provider_session_id?: string | { eq?: string; ne?: string; contains?: string };
  thread_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_key?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  workspace_root?: string | { eq?: string; ne?: string; contains?: string };
  branch?: string | { eq?: string; ne?: string; contains?: string };
  model?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  transcript_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  transcript_object_row_id?: string | { eq?: string; ne?: string };
  latest_event_sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  ended_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCodexTurnWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  turn_id?: string | { eq?: string; ne?: string; contains?: string };
  conversation_id?: string | { eq?: string; ne?: string; contains?: string };
  conversation_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  turn_kind?: string | { eq?: string; ne?: string; contains?: string };
  role?: string | { eq?: string; ne?: string; contains?: string };
  actor_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_id?: string | { eq?: string; ne?: string; contains?: string };
  summary_text?: string | { eq?: string; ne?: string; contains?: string };
  payload_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  payload_object_row_id?: string | { eq?: string; ne?: string };
  prompt_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  prompt_object_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  response_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  response_object_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  token_counts_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerTelemetryEventWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  telemetry_event_id?: string | { eq?: string; ne?: string; contains?: string };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_id?: string | { eq?: string; ne?: string; contains?: string };
  conversation_id?: string | { eq?: string; ne?: string; contains?: string };
  conversation_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  event_type?: string | { eq?: string; ne?: string; contains?: string };
  pane?: string | { eq?: string; ne?: string; contains?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  summary_text?: string | { eq?: string; ne?: string; contains?: string };
  payload_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  payload_object_row_id?: string | { eq?: string; ne?: string };
  properties_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  occurred_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  ingested_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerLiveCommitWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  commit_id?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  workspace_root?: string | { eq?: string; ne?: string; contains?: string };
  branch?: string | { eq?: string; ne?: string; contains?: string };
  bookmark?: string | { eq?: string; ne?: string; contains?: string };
  live_ref?: string | { eq?: string; ne?: string; contains?: string };
  tree_id?: string | { eq?: string; ne?: string; contains?: string };
  parent_commit_ids_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  subject?: string | { eq?: string; ne?: string; contains?: string };
  body?: string | { eq?: string; ne?: string; contains?: string };
  author_name?: string | { eq?: string; ne?: string; contains?: string };
  author_email?: string | { eq?: string; ne?: string; contains?: string };
  committer_name?: string | { eq?: string; ne?: string; contains?: string };
  committer_email?: string | { eq?: string; ne?: string; contains?: string };
  trace_ref?: string | { eq?: string; ne?: string; contains?: string };
  source_session_id?: string | { eq?: string; ne?: string; contains?: string };
  source_turn_ordinal?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  source_conversation_id?: string | { eq?: string; ne?: string; contains?: string };
  source_conversation_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  source_turn_id?: string | { eq?: string; ne?: string; contains?: string };
  source_turn_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  agent_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  courier_run_id?: string | { eq?: string; ne?: string; contains?: string };
  live_snapshot_ref?: string | { eq?: string; ne?: string; contains?: string };
  changed_paths_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  patch_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  patch_object_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  manifest_object_ref_id?: string | { eq?: string; ne?: string; contains?: string };
  manifest_object_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  status?: string | { eq?: string; ne?: string; contains?: string };
  committed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  reflected_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  ingested_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadWorkspaceWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  workspace_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_key?: string | { eq?: string; ne?: string; contains?: string };
  title?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  workspace_root?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadDocumentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  document_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_row_id?: string | { eq?: string; ne?: string };
  file_path?: string | { eq?: string; ne?: string; contains?: string };
  language?: string | { eq?: string; ne?: string; contains?: string };
  source_kind?: string | { eq?: string; ne?: string; contains?: string };
  source_hash?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadSessionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_row_id?: string | { eq?: string; ne?: string };
  document_id?: string | { eq?: string; ne?: string; contains?: string };
  document_row_id?: string | { eq?: string; ne?: string };
  codex_session_id?: string | { eq?: string; ne?: string; contains?: string };
  agent_run_id?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  active_tool_session_id?: string | { eq?: string; ne?: string; contains?: string };
  latest_projection_id?: string | { eq?: string; ne?: string; contains?: string };
  opened_by?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  closed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadEventWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  event_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  event_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_id?: string | { eq?: string; ne?: string; contains?: string };
  tool_session_id?: string | { eq?: string; ne?: string; contains?: string };
  operation_id?: string | { eq?: string; ne?: string; contains?: string };
  preview_id?: string | { eq?: string; ne?: string; contains?: string };
  source_event_id?: string | { eq?: string; ne?: string; contains?: string };
  payload_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  occurred_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  observed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadSceneNodeWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  node_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  document_id?: string | { eq?: string; ne?: string; contains?: string };
  document_row_id?: string | { eq?: string; ne?: string };
  projection_id?: string | { eq?: string; ne?: string; contains?: string };
  kind?: string | { eq?: string; ne?: string; contains?: string };
  label?: string | { eq?: string; ne?: string; contains?: string };
  path?: string | { eq?: string; ne?: string; contains?: string };
  parent_node_id?: string | { eq?: string; ne?: string; contains?: string };
  stable_ref?: string | { eq?: string; ne?: string; contains?: string };
  visibility?: string | { eq?: string; ne?: string; contains?: string };
  source_span_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  geometry_ref_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadSelectionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  selection_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  actor_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_id?: string | { eq?: string; ne?: string; contains?: string };
  target_kind?: string | { eq?: string; ne?: string; contains?: string };
  target_id?: string | { eq?: string; ne?: string; contains?: string };
  node_id?: string | { eq?: string; ne?: string; contains?: string };
  selection_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadToolSessionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  tool_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  tool_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_id?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  input_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  state_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadOperationWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  operation_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  tool_session_id?: string | { eq?: string; ne?: string; contains?: string };
  tool_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  actor_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_id?: string | { eq?: string; ne?: string; contains?: string };
  operation_kind?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  operation_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  validation_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  result_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  applied_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadSourceEditWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  edit_id?: string | { eq?: string; ne?: string; contains?: string };
  operation_id?: string | { eq?: string; ne?: string; contains?: string };
  operation_row_id?: string | { eq?: string; ne?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  file_path?: string | { eq?: string; ne?: string; contains?: string };
  range_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  text_preview?: string | { eq?: string; ne?: string; contains?: string };
  text_sha256?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadPreviewHandleWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  preview_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  tool_session_id?: string | { eq?: string; ne?: string; contains?: string };
  tool_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  operation_id?: string | { eq?: string; ne?: string; contains?: string };
  operation_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  preview_kind?: string | { eq?: string; ne?: string; contains?: string };
  target_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  handle_ref?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  disposed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadPreviewUpdateWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  update_id?: string | { eq?: string; ne?: string; contains?: string };
  preview_id?: string | { eq?: string; ne?: string; contains?: string };
  preview_row_id?: string | { eq?: string; ne?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  params_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  mesh_ref_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  error_text?: string | { eq?: string; ne?: string; contains?: string };
  requested_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadWidgetWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  widget_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_id?: string | { eq?: string; ne?: string; contains?: string };
  workspace_row_id?: string | { eq?: string; ne?: string };
  widget_key?: string | { eq?: string; ne?: string; contains?: string };
  title?: string | { eq?: string; ne?: string; contains?: string };
  source_kind?: string | { eq?: string; ne?: string; contains?: string };
  source_path?: string | { eq?: string; ne?: string; contains?: string };
  version?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  manifest_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  state_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface DesignerCadSteerWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  steer_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_id?: string | { eq?: string; ne?: string; contains?: string };
  cad_session_row_id?: string | { eq?: string; ne?: string };
  actor_kind?: string | { eq?: string; ne?: string; contains?: string };
  actor_id?: string | { eq?: string; ne?: string; contains?: string };
  target_agent_id?: string | { eq?: string; ne?: string; contains?: string };
  target_run_id?: string | { eq?: string; ne?: string; contains?: string };
  message_text?: string | { eq?: string; ne?: string; contains?: string };
  context_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyAgentQueryBuilder<T = any> = { readonly _table: "agents" } & QueryBuilder<T>;
type AnyAgentRunQueryBuilder<T = any> = { readonly _table: "agent_runs" } & QueryBuilder<T>;
type AnyRunItemQueryBuilder<T = any> = { readonly _table: "run_items" } & QueryBuilder<T>;
type AnySemanticEventQueryBuilder<T = any> = { readonly _table: "semantic_events" } & QueryBuilder<T>;
type AnyWireEventQueryBuilder<T = any> = { readonly _table: "wire_events" } & QueryBuilder<T>;
type AnyArtifactQueryBuilder<T = any> = { readonly _table: "artifacts" } & QueryBuilder<T>;
type AnyAgentStateSnapshotQueryBuilder<T = any> = { readonly _table: "agent_state_snapshots" } & QueryBuilder<T>;
type AnyWorkspaceSnapshotQueryBuilder<T = any> = { readonly _table: "workspace_snapshots" } & QueryBuilder<T>;
type AnyMemoryLinkQueryBuilder<T = any> = { readonly _table: "memory_links" } & QueryBuilder<T>;
type AnySourceFileQueryBuilder<T = any> = { readonly _table: "source_files" } & QueryBuilder<T>;
type AnyDaemonLogSourceQueryBuilder<T = any> = { readonly _table: "daemon_log_sources" } & QueryBuilder<T>;
type AnyDaemonLogChunkQueryBuilder<T = any> = { readonly _table: "daemon_log_chunks" } & QueryBuilder<T>;
type AnyDaemonLogEventQueryBuilder<T = any> = { readonly _table: "daemon_log_events" } & QueryBuilder<T>;
type AnyDaemonLogCheckpointQueryBuilder<T = any> = { readonly _table: "daemon_log_checkpoints" } & QueryBuilder<T>;
type AnyDaemonLogSummaryQueryBuilder<T = any> = { readonly _table: "daemon_log_summaries" } & QueryBuilder<T>;
type AnyTaskRecordQueryBuilder<T = any> = { readonly _table: "task_records" } & QueryBuilder<T>;
type AnyDesignerObjectRefQueryBuilder<T = any> = { readonly _table: "designer_object_refs" } & QueryBuilder<T>;
type AnyDesignerAgentQueryBuilder<T = any> = { readonly _table: "designer_agents" } & QueryBuilder<T>;
type AnyDesignerAgentToolQueryBuilder<T = any> = { readonly _table: "designer_agent_tools" } & QueryBuilder<T>;
type AnyDesignerAgentContextQueryBuilder<T = any> = { readonly _table: "designer_agent_contexts" } & QueryBuilder<T>;
type AnyDesignerCodexConversationQueryBuilder<T = any> = { readonly _table: "designer_codex_conversations" } & QueryBuilder<T>;
type AnyDesignerCodexTurnQueryBuilder<T = any> = { readonly _table: "designer_codex_turns" } & QueryBuilder<T>;
type AnyDesignerTelemetryEventQueryBuilder<T = any> = { readonly _table: "designer_telemetry_events" } & QueryBuilder<T>;
type AnyDesignerLiveCommitQueryBuilder<T = any> = { readonly _table: "designer_live_commits" } & QueryBuilder<T>;
type AnyDesignerCadWorkspaceQueryBuilder<T = any> = { readonly _table: "designer_cad_workspaces" } & QueryBuilder<T>;
type AnyDesignerCadDocumentQueryBuilder<T = any> = { readonly _table: "designer_cad_documents" } & QueryBuilder<T>;
type AnyDesignerCadSessionQueryBuilder<T = any> = { readonly _table: "designer_cad_sessions" } & QueryBuilder<T>;
type AnyDesignerCadEventQueryBuilder<T = any> = { readonly _table: "designer_cad_events" } & QueryBuilder<T>;
type AnyDesignerCadSceneNodeQueryBuilder<T = any> = { readonly _table: "designer_cad_scene_nodes" } & QueryBuilder<T>;
type AnyDesignerCadSelectionQueryBuilder<T = any> = { readonly _table: "designer_cad_selections" } & QueryBuilder<T>;
type AnyDesignerCadToolSessionQueryBuilder<T = any> = { readonly _table: "designer_cad_tool_sessions" } & QueryBuilder<T>;
type AnyDesignerCadOperationQueryBuilder<T = any> = { readonly _table: "designer_cad_operations" } & QueryBuilder<T>;
type AnyDesignerCadSourceEditQueryBuilder<T = any> = { readonly _table: "designer_cad_source_edits" } & QueryBuilder<T>;
type AnyDesignerCadPreviewHandleQueryBuilder<T = any> = { readonly _table: "designer_cad_preview_handles" } & QueryBuilder<T>;
type AnyDesignerCadPreviewUpdateQueryBuilder<T = any> = { readonly _table: "designer_cad_preview_updates" } & QueryBuilder<T>;
type AnyDesignerCadWidgetQueryBuilder<T = any> = { readonly _table: "designer_cad_widgets" } & QueryBuilder<T>;
type AnyDesignerCadSteerQueryBuilder<T = any> = { readonly _table: "designer_cad_steers" } & QueryBuilder<T>;

export interface AgentInclude {
  agent_runsViaAgent_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
  agent_state_snapshotsViaAgent_row?: true | AgentStateSnapshotInclude | AnyAgentStateSnapshotQueryBuilder<any>;
}

export interface AgentRunInclude {
  agent_row?: true | AgentInclude | AnyAgentQueryBuilder<any>;
  run_itemsViaRun_row?: true | RunItemInclude | AnyRunItemQueryBuilder<any>;
  semantic_eventsViaRun_row?: true | SemanticEventInclude | AnySemanticEventQueryBuilder<any>;
  wire_eventsViaRun_row?: true | WireEventInclude | AnyWireEventQueryBuilder<any>;
  artifactsViaRun_row?: true | ArtifactInclude | AnyArtifactQueryBuilder<any>;
  workspace_snapshotsViaRun_row?: true | WorkspaceSnapshotInclude | AnyWorkspaceSnapshotQueryBuilder<any>;
  memory_linksViaRun_row?: true | MemoryLinkInclude | AnyMemoryLinkQueryBuilder<any>;
  source_filesViaRun_row?: true | SourceFileInclude | AnySourceFileQueryBuilder<any>;
}

export interface RunItemInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
  semantic_eventsViaItem_row?: true | SemanticEventInclude | AnySemanticEventQueryBuilder<any>;
  memory_linksViaItem_row?: true | MemoryLinkInclude | AnyMemoryLinkQueryBuilder<any>;
}

export interface SemanticEventInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
  item_row?: true | RunItemInclude | AnyRunItemQueryBuilder<any>;
}

export interface WireEventInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
}

export interface ArtifactInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
}

export interface AgentStateSnapshotInclude {
  agent_row?: true | AgentInclude | AnyAgentQueryBuilder<any>;
}

export interface WorkspaceSnapshotInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
}

export interface MemoryLinkInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
  item_row?: true | RunItemInclude | AnyRunItemQueryBuilder<any>;
}

export interface SourceFileInclude {
  run_row?: true | AgentRunInclude | AnyAgentRunQueryBuilder<any>;
}

export interface DaemonLogSourceInclude {
  daemon_log_chunksViaSource_row?: true | DaemonLogChunkInclude | AnyDaemonLogChunkQueryBuilder<any>;
  daemon_log_eventsViaSource_row?: true | DaemonLogEventInclude | AnyDaemonLogEventQueryBuilder<any>;
  daemon_log_checkpointsViaSource_row?: true | DaemonLogCheckpointInclude | AnyDaemonLogCheckpointQueryBuilder<any>;
  daemon_log_summariesViaSource_row?: true | DaemonLogSummaryInclude | AnyDaemonLogSummaryQueryBuilder<any>;
}

export interface DaemonLogChunkInclude {
  source_row?: true | DaemonLogSourceInclude | AnyDaemonLogSourceQueryBuilder<any>;
  daemon_log_eventsViaChunk_row?: true | DaemonLogEventInclude | AnyDaemonLogEventQueryBuilder<any>;
}

export interface DaemonLogEventInclude {
  source_row?: true | DaemonLogSourceInclude | AnyDaemonLogSourceQueryBuilder<any>;
  chunk_row?: true | DaemonLogChunkInclude | AnyDaemonLogChunkQueryBuilder<any>;
}

export interface DaemonLogCheckpointInclude {
  source_row?: true | DaemonLogSourceInclude | AnyDaemonLogSourceQueryBuilder<any>;
}

export interface DaemonLogSummaryInclude {
  source_row?: true | DaemonLogSourceInclude | AnyDaemonLogSourceQueryBuilder<any>;
}

export interface DesignerObjectRefInclude {
  designer_agent_contextsViaObject_ref_row?: true | DesignerAgentContextInclude | AnyDesignerAgentContextQueryBuilder<any>;
  designer_codex_conversationsViaTranscript_object_row?: true | DesignerCodexConversationInclude | AnyDesignerCodexConversationQueryBuilder<any>;
  designer_codex_turnsViaPayload_object_row?: true | DesignerCodexTurnInclude | AnyDesignerCodexTurnQueryBuilder<any>;
  designer_codex_turnsViaPrompt_object_row?: true | DesignerCodexTurnInclude | AnyDesignerCodexTurnQueryBuilder<any>;
  designer_codex_turnsViaResponse_object_row?: true | DesignerCodexTurnInclude | AnyDesignerCodexTurnQueryBuilder<any>;
  designer_telemetry_eventsViaPayload_object_row?: true | DesignerTelemetryEventInclude | AnyDesignerTelemetryEventQueryBuilder<any>;
  designer_live_commitsViaPatch_object_row?: true | DesignerLiveCommitInclude | AnyDesignerLiveCommitQueryBuilder<any>;
  designer_live_commitsViaManifest_object_row?: true | DesignerLiveCommitInclude | AnyDesignerLiveCommitQueryBuilder<any>;
}

export interface DesignerAgentInclude {
  designer_agent_toolsViaAgent_row?: true | DesignerAgentToolInclude | AnyDesignerAgentToolQueryBuilder<any>;
  designer_agent_contextsViaAgent_row?: true | DesignerAgentContextInclude | AnyDesignerAgentContextQueryBuilder<any>;
  designer_live_commitsViaAgent_row?: true | DesignerLiveCommitInclude | AnyDesignerLiveCommitQueryBuilder<any>;
}

export interface DesignerAgentToolInclude {
  agent_row?: true | DesignerAgentInclude | AnyDesignerAgentQueryBuilder<any>;
}

export interface DesignerAgentContextInclude {
  agent_row?: true | DesignerAgentInclude | AnyDesignerAgentQueryBuilder<any>;
  object_ref_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
}

export interface DesignerCodexConversationInclude {
  transcript_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
  designer_codex_turnsViaConversation_row?: true | DesignerCodexTurnInclude | AnyDesignerCodexTurnQueryBuilder<any>;
  designer_telemetry_eventsViaConversation_row?: true | DesignerTelemetryEventInclude | AnyDesignerTelemetryEventQueryBuilder<any>;
  designer_live_commitsViaSource_conversation_row?: true | DesignerLiveCommitInclude | AnyDesignerLiveCommitQueryBuilder<any>;
}

export interface DesignerCodexTurnInclude {
  conversation_row?: true | DesignerCodexConversationInclude | AnyDesignerCodexConversationQueryBuilder<any>;
  payload_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
  prompt_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
  response_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
  designer_live_commitsViaSource_turn_row?: true | DesignerLiveCommitInclude | AnyDesignerLiveCommitQueryBuilder<any>;
}

export interface DesignerTelemetryEventInclude {
  conversation_row?: true | DesignerCodexConversationInclude | AnyDesignerCodexConversationQueryBuilder<any>;
  payload_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
}

export interface DesignerLiveCommitInclude {
  source_conversation_row?: true | DesignerCodexConversationInclude | AnyDesignerCodexConversationQueryBuilder<any>;
  source_turn_row?: true | DesignerCodexTurnInclude | AnyDesignerCodexTurnQueryBuilder<any>;
  agent_row?: true | DesignerAgentInclude | AnyDesignerAgentQueryBuilder<any>;
  patch_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
  manifest_object_row?: true | DesignerObjectRefInclude | AnyDesignerObjectRefQueryBuilder<any>;
}

export interface DesignerCadWorkspaceInclude {
  designer_cad_documentsViaWorkspace_row?: true | DesignerCadDocumentInclude | AnyDesignerCadDocumentQueryBuilder<any>;
  designer_cad_sessionsViaWorkspace_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
  designer_cad_widgetsViaWorkspace_row?: true | DesignerCadWidgetInclude | AnyDesignerCadWidgetQueryBuilder<any>;
}

export interface DesignerCadDocumentInclude {
  workspace_row?: true | DesignerCadWorkspaceInclude | AnyDesignerCadWorkspaceQueryBuilder<any>;
  designer_cad_sessionsViaDocument_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
  designer_cad_scene_nodesViaDocument_row?: true | DesignerCadSceneNodeInclude | AnyDesignerCadSceneNodeQueryBuilder<any>;
}

export interface DesignerCadSessionInclude {
  workspace_row?: true | DesignerCadWorkspaceInclude | AnyDesignerCadWorkspaceQueryBuilder<any>;
  document_row?: true | DesignerCadDocumentInclude | AnyDesignerCadDocumentQueryBuilder<any>;
  designer_cad_eventsViaCad_session_row?: true | DesignerCadEventInclude | AnyDesignerCadEventQueryBuilder<any>;
  designer_cad_scene_nodesViaCad_session_row?: true | DesignerCadSceneNodeInclude | AnyDesignerCadSceneNodeQueryBuilder<any>;
  designer_cad_selectionsViaCad_session_row?: true | DesignerCadSelectionInclude | AnyDesignerCadSelectionQueryBuilder<any>;
  designer_cad_tool_sessionsViaCad_session_row?: true | DesignerCadToolSessionInclude | AnyDesignerCadToolSessionQueryBuilder<any>;
  designer_cad_operationsViaCad_session_row?: true | DesignerCadOperationInclude | AnyDesignerCadOperationQueryBuilder<any>;
  designer_cad_source_editsViaCad_session_row?: true | DesignerCadSourceEditInclude | AnyDesignerCadSourceEditQueryBuilder<any>;
  designer_cad_preview_handlesViaCad_session_row?: true | DesignerCadPreviewHandleInclude | AnyDesignerCadPreviewHandleQueryBuilder<any>;
  designer_cad_preview_updatesViaCad_session_row?: true | DesignerCadPreviewUpdateInclude | AnyDesignerCadPreviewUpdateQueryBuilder<any>;
  designer_cad_steersViaCad_session_row?: true | DesignerCadSteerInclude | AnyDesignerCadSteerQueryBuilder<any>;
}

export interface DesignerCadEventInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
}

export interface DesignerCadSceneNodeInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
  document_row?: true | DesignerCadDocumentInclude | AnyDesignerCadDocumentQueryBuilder<any>;
}

export interface DesignerCadSelectionInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
}

export interface DesignerCadToolSessionInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
  designer_cad_operationsViaTool_session_row?: true | DesignerCadOperationInclude | AnyDesignerCadOperationQueryBuilder<any>;
  designer_cad_preview_handlesViaTool_session_row?: true | DesignerCadPreviewHandleInclude | AnyDesignerCadPreviewHandleQueryBuilder<any>;
}

export interface DesignerCadOperationInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
  tool_session_row?: true | DesignerCadToolSessionInclude | AnyDesignerCadToolSessionQueryBuilder<any>;
  designer_cad_source_editsViaOperation_row?: true | DesignerCadSourceEditInclude | AnyDesignerCadSourceEditQueryBuilder<any>;
  designer_cad_preview_handlesViaOperation_row?: true | DesignerCadPreviewHandleInclude | AnyDesignerCadPreviewHandleQueryBuilder<any>;
}

export interface DesignerCadSourceEditInclude {
  operation_row?: true | DesignerCadOperationInclude | AnyDesignerCadOperationQueryBuilder<any>;
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
}

export interface DesignerCadPreviewHandleInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
  tool_session_row?: true | DesignerCadToolSessionInclude | AnyDesignerCadToolSessionQueryBuilder<any>;
  operation_row?: true | DesignerCadOperationInclude | AnyDesignerCadOperationQueryBuilder<any>;
  designer_cad_preview_updatesViaPreview_row?: true | DesignerCadPreviewUpdateInclude | AnyDesignerCadPreviewUpdateQueryBuilder<any>;
}

export interface DesignerCadPreviewUpdateInclude {
  preview_row?: true | DesignerCadPreviewHandleInclude | AnyDesignerCadPreviewHandleQueryBuilder<any>;
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
}

export interface DesignerCadWidgetInclude {
  workspace_row?: true | DesignerCadWorkspaceInclude | AnyDesignerCadWorkspaceQueryBuilder<any>;
}

export interface DesignerCadSteerInclude {
  cad_session_row?: true | DesignerCadSessionInclude | AnyDesignerCadSessionQueryBuilder<any>;
}

export type AgentIncludedRelations<I extends AgentInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "agent_runsViaAgent_row"
      ? NonNullable<I["agent_runsViaAgent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? AgentRun[]
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends AgentRunInclude
              ? AgentRunWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "agent_state_snapshotsViaAgent_row"
      ? NonNullable<I["agent_state_snapshotsViaAgent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? AgentStateSnapshot[]
          : RelationInclude extends AnyAgentStateSnapshotQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends AgentStateSnapshotInclude
              ? AgentStateSnapshotWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type AgentRunIncludedRelations<I extends AgentRunInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "agent_row"
      ? NonNullable<I["agent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Agent : Agent | undefined
          : RelationInclude extends AnyAgentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends AgentInclude
              ? R extends true ? AgentWithIncludes<RelationInclude, false> : AgentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "run_itemsViaRun_row"
      ? NonNullable<I["run_itemsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? RunItem[]
          : RelationInclude extends AnyRunItemQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends RunItemInclude
              ? RunItemWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "semantic_eventsViaRun_row"
      ? NonNullable<I["semantic_eventsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? SemanticEvent[]
          : RelationInclude extends AnySemanticEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends SemanticEventInclude
              ? SemanticEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "wire_eventsViaRun_row"
      ? NonNullable<I["wire_eventsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? WireEvent[]
          : RelationInclude extends AnyWireEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends WireEventInclude
              ? WireEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "artifactsViaRun_row"
      ? NonNullable<I["artifactsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Artifact[]
          : RelationInclude extends AnyArtifactQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ArtifactInclude
              ? ArtifactWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "workspace_snapshotsViaRun_row"
      ? NonNullable<I["workspace_snapshotsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? WorkspaceSnapshot[]
          : RelationInclude extends AnyWorkspaceSnapshotQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends WorkspaceSnapshotInclude
              ? WorkspaceSnapshotWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "memory_linksViaRun_row"
      ? NonNullable<I["memory_linksViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? MemoryLink[]
          : RelationInclude extends AnyMemoryLinkQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MemoryLinkInclude
              ? MemoryLinkWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "source_filesViaRun_row"
      ? NonNullable<I["source_filesViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? SourceFile[]
          : RelationInclude extends AnySourceFileQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends SourceFileInclude
              ? SourceFileWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type RunItemIncludedRelations<I extends RunItemInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? AgentRun : AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? R extends true ? AgentRunWithIncludes<RelationInclude, false> : AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "semantic_eventsViaItem_row"
      ? NonNullable<I["semantic_eventsViaItem_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? SemanticEvent[]
          : RelationInclude extends AnySemanticEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends SemanticEventInclude
              ? SemanticEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "memory_linksViaItem_row"
      ? NonNullable<I["memory_linksViaItem_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? MemoryLink[]
          : RelationInclude extends AnyMemoryLinkQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MemoryLinkInclude
              ? MemoryLinkWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type SemanticEventIncludedRelations<I extends SemanticEventInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? AgentRun : AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? R extends true ? AgentRunWithIncludes<RelationInclude, false> : AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "item_row"
      ? NonNullable<I["item_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? RunItem | undefined
          : RelationInclude extends AnyRunItemQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends RunItemInclude
              ? RunItemWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type WireEventIncludedRelations<I extends WireEventInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type ArtifactIncludedRelations<I extends ArtifactInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? AgentRun : AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? R extends true ? AgentRunWithIncludes<RelationInclude, false> : AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type AgentStateSnapshotIncludedRelations<I extends AgentStateSnapshotInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "agent_row"
      ? NonNullable<I["agent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Agent : Agent | undefined
          : RelationInclude extends AnyAgentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends AgentInclude
              ? R extends true ? AgentWithIncludes<RelationInclude, false> : AgentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type WorkspaceSnapshotIncludedRelations<I extends WorkspaceSnapshotInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? AgentRun : AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? R extends true ? AgentRunWithIncludes<RelationInclude, false> : AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type MemoryLinkIncludedRelations<I extends MemoryLinkInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "item_row"
      ? NonNullable<I["item_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? RunItem | undefined
          : RelationInclude extends AnyRunItemQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends RunItemInclude
              ? RunItemWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type SourceFileIncludedRelations<I extends SourceFileInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? AgentRun | undefined
          : RelationInclude extends AnyAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends AgentRunInclude
              ? AgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DaemonLogSourceIncludedRelations<I extends DaemonLogSourceInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "daemon_log_chunksViaSource_row"
      ? NonNullable<I["daemon_log_chunksViaSource_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DaemonLogChunk[]
          : RelationInclude extends AnyDaemonLogChunkQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DaemonLogChunkInclude
              ? DaemonLogChunkWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "daemon_log_eventsViaSource_row"
      ? NonNullable<I["daemon_log_eventsViaSource_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DaemonLogEvent[]
          : RelationInclude extends AnyDaemonLogEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DaemonLogEventInclude
              ? DaemonLogEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "daemon_log_checkpointsViaSource_row"
      ? NonNullable<I["daemon_log_checkpointsViaSource_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DaemonLogCheckpoint[]
          : RelationInclude extends AnyDaemonLogCheckpointQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DaemonLogCheckpointInclude
              ? DaemonLogCheckpointWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "daemon_log_summariesViaSource_row"
      ? NonNullable<I["daemon_log_summariesViaSource_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DaemonLogSummary[]
          : RelationInclude extends AnyDaemonLogSummaryQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DaemonLogSummaryInclude
              ? DaemonLogSummaryWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DaemonLogChunkIncludedRelations<I extends DaemonLogChunkInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "source_row"
      ? NonNullable<I["source_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DaemonLogSource : DaemonLogSource | undefined
          : RelationInclude extends AnyDaemonLogSourceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DaemonLogSourceInclude
              ? R extends true ? DaemonLogSourceWithIncludes<RelationInclude, false> : DaemonLogSourceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "daemon_log_eventsViaChunk_row"
      ? NonNullable<I["daemon_log_eventsViaChunk_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DaemonLogEvent[]
          : RelationInclude extends AnyDaemonLogEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DaemonLogEventInclude
              ? DaemonLogEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DaemonLogEventIncludedRelations<I extends DaemonLogEventInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "source_row"
      ? NonNullable<I["source_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DaemonLogSource : DaemonLogSource | undefined
          : RelationInclude extends AnyDaemonLogSourceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DaemonLogSourceInclude
              ? R extends true ? DaemonLogSourceWithIncludes<RelationInclude, false> : DaemonLogSourceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "chunk_row"
      ? NonNullable<I["chunk_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DaemonLogChunk : DaemonLogChunk | undefined
          : RelationInclude extends AnyDaemonLogChunkQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DaemonLogChunkInclude
              ? R extends true ? DaemonLogChunkWithIncludes<RelationInclude, false> : DaemonLogChunkWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DaemonLogCheckpointIncludedRelations<I extends DaemonLogCheckpointInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "source_row"
      ? NonNullable<I["source_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DaemonLogSource : DaemonLogSource | undefined
          : RelationInclude extends AnyDaemonLogSourceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DaemonLogSourceInclude
              ? R extends true ? DaemonLogSourceWithIncludes<RelationInclude, false> : DaemonLogSourceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DaemonLogSummaryIncludedRelations<I extends DaemonLogSummaryInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "source_row"
      ? NonNullable<I["source_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DaemonLogSource : DaemonLogSource | undefined
          : RelationInclude extends AnyDaemonLogSourceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DaemonLogSourceInclude
              ? R extends true ? DaemonLogSourceWithIncludes<RelationInclude, false> : DaemonLogSourceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerObjectRefIncludedRelations<I extends DesignerObjectRefInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "designer_agent_contextsViaObject_ref_row"
      ? NonNullable<I["designer_agent_contextsViaObject_ref_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerAgentContext[]
          : RelationInclude extends AnyDesignerAgentContextQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerAgentContextInclude
              ? DesignerAgentContextWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_codex_conversationsViaTranscript_object_row"
      ? NonNullable<I["designer_codex_conversationsViaTranscript_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexConversation[]
          : RelationInclude extends AnyDesignerCodexConversationQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCodexConversationInclude
              ? DesignerCodexConversationWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_codex_turnsViaPayload_object_row"
      ? NonNullable<I["designer_codex_turnsViaPayload_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexTurn[]
          : RelationInclude extends AnyDesignerCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCodexTurnInclude
              ? DesignerCodexTurnWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_codex_turnsViaPrompt_object_row"
      ? NonNullable<I["designer_codex_turnsViaPrompt_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexTurn[]
          : RelationInclude extends AnyDesignerCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCodexTurnInclude
              ? DesignerCodexTurnWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_codex_turnsViaResponse_object_row"
      ? NonNullable<I["designer_codex_turnsViaResponse_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexTurn[]
          : RelationInclude extends AnyDesignerCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCodexTurnInclude
              ? DesignerCodexTurnWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_telemetry_eventsViaPayload_object_row"
      ? NonNullable<I["designer_telemetry_eventsViaPayload_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerTelemetryEvent[]
          : RelationInclude extends AnyDesignerTelemetryEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerTelemetryEventInclude
              ? DesignerTelemetryEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_live_commitsViaPatch_object_row"
      ? NonNullable<I["designer_live_commitsViaPatch_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerLiveCommit[]
          : RelationInclude extends AnyDesignerLiveCommitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerLiveCommitInclude
              ? DesignerLiveCommitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_live_commitsViaManifest_object_row"
      ? NonNullable<I["designer_live_commitsViaManifest_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerLiveCommit[]
          : RelationInclude extends AnyDesignerLiveCommitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerLiveCommitInclude
              ? DesignerLiveCommitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerAgentIncludedRelations<I extends DesignerAgentInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "designer_agent_toolsViaAgent_row"
      ? NonNullable<I["designer_agent_toolsViaAgent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerAgentTool[]
          : RelationInclude extends AnyDesignerAgentToolQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerAgentToolInclude
              ? DesignerAgentToolWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_agent_contextsViaAgent_row"
      ? NonNullable<I["designer_agent_contextsViaAgent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerAgentContext[]
          : RelationInclude extends AnyDesignerAgentContextQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerAgentContextInclude
              ? DesignerAgentContextWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_live_commitsViaAgent_row"
      ? NonNullable<I["designer_live_commitsViaAgent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerLiveCommit[]
          : RelationInclude extends AnyDesignerLiveCommitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerLiveCommitInclude
              ? DesignerLiveCommitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerAgentToolIncludedRelations<I extends DesignerAgentToolInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "agent_row"
      ? NonNullable<I["agent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerAgent : DesignerAgent | undefined
          : RelationInclude extends AnyDesignerAgentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerAgentInclude
              ? R extends true ? DesignerAgentWithIncludes<RelationInclude, false> : DesignerAgentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerAgentContextIncludedRelations<I extends DesignerAgentContextInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "agent_row"
      ? NonNullable<I["agent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerAgent : DesignerAgent | undefined
          : RelationInclude extends AnyDesignerAgentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerAgentInclude
              ? R extends true ? DesignerAgentWithIncludes<RelationInclude, false> : DesignerAgentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "object_ref_row"
      ? NonNullable<I["object_ref_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCodexConversationIncludedRelations<I extends DesignerCodexConversationInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "transcript_object_row"
      ? NonNullable<I["transcript_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerObjectRef : DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? R extends true ? DesignerObjectRefWithIncludes<RelationInclude, false> : DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_codex_turnsViaConversation_row"
      ? NonNullable<I["designer_codex_turnsViaConversation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexTurn[]
          : RelationInclude extends AnyDesignerCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCodexTurnInclude
              ? DesignerCodexTurnWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_telemetry_eventsViaConversation_row"
      ? NonNullable<I["designer_telemetry_eventsViaConversation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerTelemetryEvent[]
          : RelationInclude extends AnyDesignerTelemetryEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerTelemetryEventInclude
              ? DesignerTelemetryEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_live_commitsViaSource_conversation_row"
      ? NonNullable<I["designer_live_commitsViaSource_conversation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerLiveCommit[]
          : RelationInclude extends AnyDesignerLiveCommitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerLiveCommitInclude
              ? DesignerLiveCommitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCodexTurnIncludedRelations<I extends DesignerCodexTurnInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "conversation_row"
      ? NonNullable<I["conversation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCodexConversation : DesignerCodexConversation | undefined
          : RelationInclude extends AnyDesignerCodexConversationQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCodexConversationInclude
              ? R extends true ? DesignerCodexConversationWithIncludes<RelationInclude, false> : DesignerCodexConversationWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "payload_object_row"
      ? NonNullable<I["payload_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerObjectRef : DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? R extends true ? DesignerObjectRefWithIncludes<RelationInclude, false> : DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "prompt_object_row"
      ? NonNullable<I["prompt_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "response_object_row"
      ? NonNullable<I["response_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_live_commitsViaSource_turn_row"
      ? NonNullable<I["designer_live_commitsViaSource_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerLiveCommit[]
          : RelationInclude extends AnyDesignerLiveCommitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerLiveCommitInclude
              ? DesignerLiveCommitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerTelemetryEventIncludedRelations<I extends DesignerTelemetryEventInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "conversation_row"
      ? NonNullable<I["conversation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexConversation | undefined
          : RelationInclude extends AnyDesignerCodexConversationQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerCodexConversationInclude
              ? DesignerCodexConversationWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "payload_object_row"
      ? NonNullable<I["payload_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerObjectRef : DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? R extends true ? DesignerObjectRefWithIncludes<RelationInclude, false> : DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerLiveCommitIncludedRelations<I extends DesignerLiveCommitInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "source_conversation_row"
      ? NonNullable<I["source_conversation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexConversation | undefined
          : RelationInclude extends AnyDesignerCodexConversationQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerCodexConversationInclude
              ? DesignerCodexConversationWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "source_turn_row"
      ? NonNullable<I["source_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCodexTurn | undefined
          : RelationInclude extends AnyDesignerCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerCodexTurnInclude
              ? DesignerCodexTurnWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "agent_row"
      ? NonNullable<I["agent_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerAgent | undefined
          : RelationInclude extends AnyDesignerAgentQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerAgentInclude
              ? DesignerAgentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "patch_object_row"
      ? NonNullable<I["patch_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "manifest_object_row"
      ? NonNullable<I["manifest_object_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerObjectRef | undefined
          : RelationInclude extends AnyDesignerObjectRefQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerObjectRefInclude
              ? DesignerObjectRefWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadWorkspaceIncludedRelations<I extends DesignerCadWorkspaceInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "designer_cad_documentsViaWorkspace_row"
      ? NonNullable<I["designer_cad_documentsViaWorkspace_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadDocument[]
          : RelationInclude extends AnyDesignerCadDocumentQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadDocumentInclude
              ? DesignerCadDocumentWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_sessionsViaWorkspace_row"
      ? NonNullable<I["designer_cad_sessionsViaWorkspace_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSession[]
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSessionInclude
              ? DesignerCadSessionWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_widgetsViaWorkspace_row"
      ? NonNullable<I["designer_cad_widgetsViaWorkspace_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadWidget[]
          : RelationInclude extends AnyDesignerCadWidgetQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadWidgetInclude
              ? DesignerCadWidgetWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCadDocumentIncludedRelations<I extends DesignerCadDocumentInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "workspace_row"
      ? NonNullable<I["workspace_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadWorkspace : DesignerCadWorkspace | undefined
          : RelationInclude extends AnyDesignerCadWorkspaceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadWorkspaceInclude
              ? R extends true ? DesignerCadWorkspaceWithIncludes<RelationInclude, false> : DesignerCadWorkspaceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_cad_sessionsViaDocument_row"
      ? NonNullable<I["designer_cad_sessionsViaDocument_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSession[]
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSessionInclude
              ? DesignerCadSessionWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_scene_nodesViaDocument_row"
      ? NonNullable<I["designer_cad_scene_nodesViaDocument_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSceneNode[]
          : RelationInclude extends AnyDesignerCadSceneNodeQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSceneNodeInclude
              ? DesignerCadSceneNodeWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCadSessionIncludedRelations<I extends DesignerCadSessionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "workspace_row"
      ? NonNullable<I["workspace_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadWorkspace : DesignerCadWorkspace | undefined
          : RelationInclude extends AnyDesignerCadWorkspaceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadWorkspaceInclude
              ? R extends true ? DesignerCadWorkspaceWithIncludes<RelationInclude, false> : DesignerCadWorkspaceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "document_row"
      ? NonNullable<I["document_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadDocument : DesignerCadDocument | undefined
          : RelationInclude extends AnyDesignerCadDocumentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadDocumentInclude
              ? R extends true ? DesignerCadDocumentWithIncludes<RelationInclude, false> : DesignerCadDocumentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_cad_eventsViaCad_session_row"
      ? NonNullable<I["designer_cad_eventsViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadEvent[]
          : RelationInclude extends AnyDesignerCadEventQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadEventInclude
              ? DesignerCadEventWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_scene_nodesViaCad_session_row"
      ? NonNullable<I["designer_cad_scene_nodesViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSceneNode[]
          : RelationInclude extends AnyDesignerCadSceneNodeQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSceneNodeInclude
              ? DesignerCadSceneNodeWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_selectionsViaCad_session_row"
      ? NonNullable<I["designer_cad_selectionsViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSelection[]
          : RelationInclude extends AnyDesignerCadSelectionQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSelectionInclude
              ? DesignerCadSelectionWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_tool_sessionsViaCad_session_row"
      ? NonNullable<I["designer_cad_tool_sessionsViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadToolSession[]
          : RelationInclude extends AnyDesignerCadToolSessionQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadToolSessionInclude
              ? DesignerCadToolSessionWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_operationsViaCad_session_row"
      ? NonNullable<I["designer_cad_operationsViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadOperation[]
          : RelationInclude extends AnyDesignerCadOperationQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadOperationInclude
              ? DesignerCadOperationWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_source_editsViaCad_session_row"
      ? NonNullable<I["designer_cad_source_editsViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSourceEdit[]
          : RelationInclude extends AnyDesignerCadSourceEditQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSourceEditInclude
              ? DesignerCadSourceEditWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_preview_handlesViaCad_session_row"
      ? NonNullable<I["designer_cad_preview_handlesViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadPreviewHandle[]
          : RelationInclude extends AnyDesignerCadPreviewHandleQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadPreviewHandleInclude
              ? DesignerCadPreviewHandleWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_preview_updatesViaCad_session_row"
      ? NonNullable<I["designer_cad_preview_updatesViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadPreviewUpdate[]
          : RelationInclude extends AnyDesignerCadPreviewUpdateQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadPreviewUpdateInclude
              ? DesignerCadPreviewUpdateWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_steersViaCad_session_row"
      ? NonNullable<I["designer_cad_steersViaCad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSteer[]
          : RelationInclude extends AnyDesignerCadSteerQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSteerInclude
              ? DesignerCadSteerWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCadEventIncludedRelations<I extends DesignerCadEventInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadSceneNodeIncludedRelations<I extends DesignerCadSceneNodeInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "document_row"
      ? NonNullable<I["document_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadDocument : DesignerCadDocument | undefined
          : RelationInclude extends AnyDesignerCadDocumentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadDocumentInclude
              ? R extends true ? DesignerCadDocumentWithIncludes<RelationInclude, false> : DesignerCadDocumentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadSelectionIncludedRelations<I extends DesignerCadSelectionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadToolSessionIncludedRelations<I extends DesignerCadToolSessionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_cad_operationsViaTool_session_row"
      ? NonNullable<I["designer_cad_operationsViaTool_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadOperation[]
          : RelationInclude extends AnyDesignerCadOperationQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadOperationInclude
              ? DesignerCadOperationWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_preview_handlesViaTool_session_row"
      ? NonNullable<I["designer_cad_preview_handlesViaTool_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadPreviewHandle[]
          : RelationInclude extends AnyDesignerCadPreviewHandleQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadPreviewHandleInclude
              ? DesignerCadPreviewHandleWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCadOperationIncludedRelations<I extends DesignerCadOperationInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "tool_session_row"
      ? NonNullable<I["tool_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadToolSession | undefined
          : RelationInclude extends AnyDesignerCadToolSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerCadToolSessionInclude
              ? DesignerCadToolSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_cad_source_editsViaOperation_row"
      ? NonNullable<I["designer_cad_source_editsViaOperation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadSourceEdit[]
          : RelationInclude extends AnyDesignerCadSourceEditQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadSourceEditInclude
              ? DesignerCadSourceEditWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "designer_cad_preview_handlesViaOperation_row"
      ? NonNullable<I["designer_cad_preview_handlesViaOperation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadPreviewHandle[]
          : RelationInclude extends AnyDesignerCadPreviewHandleQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadPreviewHandleInclude
              ? DesignerCadPreviewHandleWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCadSourceEditIncludedRelations<I extends DesignerCadSourceEditInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "operation_row"
      ? NonNullable<I["operation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadOperation : DesignerCadOperation | undefined
          : RelationInclude extends AnyDesignerCadOperationQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadOperationInclude
              ? R extends true ? DesignerCadOperationWithIncludes<RelationInclude, false> : DesignerCadOperationWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadPreviewHandleIncludedRelations<I extends DesignerCadPreviewHandleInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "tool_session_row"
      ? NonNullable<I["tool_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadToolSession | undefined
          : RelationInclude extends AnyDesignerCadToolSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerCadToolSessionInclude
              ? DesignerCadToolSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "operation_row"
      ? NonNullable<I["operation_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadOperation | undefined
          : RelationInclude extends AnyDesignerCadOperationQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends DesignerCadOperationInclude
              ? DesignerCadOperationWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "designer_cad_preview_updatesViaPreview_row"
      ? NonNullable<I["designer_cad_preview_updatesViaPreview_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? DesignerCadPreviewUpdate[]
          : RelationInclude extends AnyDesignerCadPreviewUpdateQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends DesignerCadPreviewUpdateInclude
              ? DesignerCadPreviewUpdateWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type DesignerCadPreviewUpdateIncludedRelations<I extends DesignerCadPreviewUpdateInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "preview_row"
      ? NonNullable<I["preview_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadPreviewHandle : DesignerCadPreviewHandle | undefined
          : RelationInclude extends AnyDesignerCadPreviewHandleQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadPreviewHandleInclude
              ? R extends true ? DesignerCadPreviewHandleWithIncludes<RelationInclude, false> : DesignerCadPreviewHandleWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadWidgetIncludedRelations<I extends DesignerCadWidgetInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "workspace_row"
      ? NonNullable<I["workspace_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadWorkspace : DesignerCadWorkspace | undefined
          : RelationInclude extends AnyDesignerCadWorkspaceQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadWorkspaceInclude
              ? R extends true ? DesignerCadWorkspaceWithIncludes<RelationInclude, false> : DesignerCadWorkspaceWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type DesignerCadSteerIncludedRelations<I extends DesignerCadSteerInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "cad_session_row"
      ? NonNullable<I["cad_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? DesignerCadSession : DesignerCadSession | undefined
          : RelationInclude extends AnyDesignerCadSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends DesignerCadSessionInclude
              ? R extends true ? DesignerCadSessionWithIncludes<RelationInclude, false> : DesignerCadSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface AgentRelations {
  agent_runsViaAgent_row: AgentRun[];
  agent_state_snapshotsViaAgent_row: AgentStateSnapshot[];
}

export interface AgentRunRelations {
  agent_row: Agent | undefined;
  run_itemsViaRun_row: RunItem[];
  semantic_eventsViaRun_row: SemanticEvent[];
  wire_eventsViaRun_row: WireEvent[];
  artifactsViaRun_row: Artifact[];
  workspace_snapshotsViaRun_row: WorkspaceSnapshot[];
  memory_linksViaRun_row: MemoryLink[];
  source_filesViaRun_row: SourceFile[];
}

export interface RunItemRelations {
  run_row: AgentRun | undefined;
  semantic_eventsViaItem_row: SemanticEvent[];
  memory_linksViaItem_row: MemoryLink[];
}

export interface SemanticEventRelations {
  run_row: AgentRun | undefined;
  item_row: RunItem | undefined;
}

export interface WireEventRelations {
  run_row: AgentRun | undefined;
}

export interface ArtifactRelations {
  run_row: AgentRun | undefined;
}

export interface AgentStateSnapshotRelations {
  agent_row: Agent | undefined;
}

export interface WorkspaceSnapshotRelations {
  run_row: AgentRun | undefined;
}

export interface MemoryLinkRelations {
  run_row: AgentRun | undefined;
  item_row: RunItem | undefined;
}

export interface SourceFileRelations {
  run_row: AgentRun | undefined;
}

export interface DaemonLogSourceRelations {
  daemon_log_chunksViaSource_row: DaemonLogChunk[];
  daemon_log_eventsViaSource_row: DaemonLogEvent[];
  daemon_log_checkpointsViaSource_row: DaemonLogCheckpoint[];
  daemon_log_summariesViaSource_row: DaemonLogSummary[];
}

export interface DaemonLogChunkRelations {
  source_row: DaemonLogSource | undefined;
  daemon_log_eventsViaChunk_row: DaemonLogEvent[];
}

export interface DaemonLogEventRelations {
  source_row: DaemonLogSource | undefined;
  chunk_row: DaemonLogChunk | undefined;
}

export interface DaemonLogCheckpointRelations {
  source_row: DaemonLogSource | undefined;
}

export interface DaemonLogSummaryRelations {
  source_row: DaemonLogSource | undefined;
}

export interface DesignerObjectRefRelations {
  designer_agent_contextsViaObject_ref_row: DesignerAgentContext[];
  designer_codex_conversationsViaTranscript_object_row: DesignerCodexConversation[];
  designer_codex_turnsViaPayload_object_row: DesignerCodexTurn[];
  designer_codex_turnsViaPrompt_object_row: DesignerCodexTurn[];
  designer_codex_turnsViaResponse_object_row: DesignerCodexTurn[];
  designer_telemetry_eventsViaPayload_object_row: DesignerTelemetryEvent[];
  designer_live_commitsViaPatch_object_row: DesignerLiveCommit[];
  designer_live_commitsViaManifest_object_row: DesignerLiveCommit[];
}

export interface DesignerAgentRelations {
  designer_agent_toolsViaAgent_row: DesignerAgentTool[];
  designer_agent_contextsViaAgent_row: DesignerAgentContext[];
  designer_live_commitsViaAgent_row: DesignerLiveCommit[];
}

export interface DesignerAgentToolRelations {
  agent_row: DesignerAgent | undefined;
}

export interface DesignerAgentContextRelations {
  agent_row: DesignerAgent | undefined;
  object_ref_row: DesignerObjectRef | undefined;
}

export interface DesignerCodexConversationRelations {
  transcript_object_row: DesignerObjectRef | undefined;
  designer_codex_turnsViaConversation_row: DesignerCodexTurn[];
  designer_telemetry_eventsViaConversation_row: DesignerTelemetryEvent[];
  designer_live_commitsViaSource_conversation_row: DesignerLiveCommit[];
}

export interface DesignerCodexTurnRelations {
  conversation_row: DesignerCodexConversation | undefined;
  payload_object_row: DesignerObjectRef | undefined;
  prompt_object_row: DesignerObjectRef | undefined;
  response_object_row: DesignerObjectRef | undefined;
  designer_live_commitsViaSource_turn_row: DesignerLiveCommit[];
}

export interface DesignerTelemetryEventRelations {
  conversation_row: DesignerCodexConversation | undefined;
  payload_object_row: DesignerObjectRef | undefined;
}

export interface DesignerLiveCommitRelations {
  source_conversation_row: DesignerCodexConversation | undefined;
  source_turn_row: DesignerCodexTurn | undefined;
  agent_row: DesignerAgent | undefined;
  patch_object_row: DesignerObjectRef | undefined;
  manifest_object_row: DesignerObjectRef | undefined;
}

export interface DesignerCadWorkspaceRelations {
  designer_cad_documentsViaWorkspace_row: DesignerCadDocument[];
  designer_cad_sessionsViaWorkspace_row: DesignerCadSession[];
  designer_cad_widgetsViaWorkspace_row: DesignerCadWidget[];
}

export interface DesignerCadDocumentRelations {
  workspace_row: DesignerCadWorkspace | undefined;
  designer_cad_sessionsViaDocument_row: DesignerCadSession[];
  designer_cad_scene_nodesViaDocument_row: DesignerCadSceneNode[];
}

export interface DesignerCadSessionRelations {
  workspace_row: DesignerCadWorkspace | undefined;
  document_row: DesignerCadDocument | undefined;
  designer_cad_eventsViaCad_session_row: DesignerCadEvent[];
  designer_cad_scene_nodesViaCad_session_row: DesignerCadSceneNode[];
  designer_cad_selectionsViaCad_session_row: DesignerCadSelection[];
  designer_cad_tool_sessionsViaCad_session_row: DesignerCadToolSession[];
  designer_cad_operationsViaCad_session_row: DesignerCadOperation[];
  designer_cad_source_editsViaCad_session_row: DesignerCadSourceEdit[];
  designer_cad_preview_handlesViaCad_session_row: DesignerCadPreviewHandle[];
  designer_cad_preview_updatesViaCad_session_row: DesignerCadPreviewUpdate[];
  designer_cad_steersViaCad_session_row: DesignerCadSteer[];
}

export interface DesignerCadEventRelations {
  cad_session_row: DesignerCadSession | undefined;
}

export interface DesignerCadSceneNodeRelations {
  cad_session_row: DesignerCadSession | undefined;
  document_row: DesignerCadDocument | undefined;
}

export interface DesignerCadSelectionRelations {
  cad_session_row: DesignerCadSession | undefined;
}

export interface DesignerCadToolSessionRelations {
  cad_session_row: DesignerCadSession | undefined;
  designer_cad_operationsViaTool_session_row: DesignerCadOperation[];
  designer_cad_preview_handlesViaTool_session_row: DesignerCadPreviewHandle[];
}

export interface DesignerCadOperationRelations {
  cad_session_row: DesignerCadSession | undefined;
  tool_session_row: DesignerCadToolSession | undefined;
  designer_cad_source_editsViaOperation_row: DesignerCadSourceEdit[];
  designer_cad_preview_handlesViaOperation_row: DesignerCadPreviewHandle[];
}

export interface DesignerCadSourceEditRelations {
  operation_row: DesignerCadOperation | undefined;
  cad_session_row: DesignerCadSession | undefined;
}

export interface DesignerCadPreviewHandleRelations {
  cad_session_row: DesignerCadSession | undefined;
  tool_session_row: DesignerCadToolSession | undefined;
  operation_row: DesignerCadOperation | undefined;
  designer_cad_preview_updatesViaPreview_row: DesignerCadPreviewUpdate[];
}

export interface DesignerCadPreviewUpdateRelations {
  preview_row: DesignerCadPreviewHandle | undefined;
  cad_session_row: DesignerCadSession | undefined;
}

export interface DesignerCadWidgetRelations {
  workspace_row: DesignerCadWorkspace | undefined;
}

export interface DesignerCadSteerRelations {
  cad_session_row: DesignerCadSession | undefined;
}

export type AgentWithIncludes<I extends AgentInclude = {}, R extends boolean = false> = Agent & AgentIncludedRelations<I, R>;

export type AgentRunWithIncludes<I extends AgentRunInclude = {}, R extends boolean = false> = AgentRun & AgentRunIncludedRelations<I, R>;

export type RunItemWithIncludes<I extends RunItemInclude = {}, R extends boolean = false> = RunItem & RunItemIncludedRelations<I, R>;

export type SemanticEventWithIncludes<I extends SemanticEventInclude = {}, R extends boolean = false> = SemanticEvent & SemanticEventIncludedRelations<I, R>;

export type WireEventWithIncludes<I extends WireEventInclude = {}, R extends boolean = false> = WireEvent & WireEventIncludedRelations<I, R>;

export type ArtifactWithIncludes<I extends ArtifactInclude = {}, R extends boolean = false> = Artifact & ArtifactIncludedRelations<I, R>;

export type AgentStateSnapshotWithIncludes<I extends AgentStateSnapshotInclude = {}, R extends boolean = false> = AgentStateSnapshot & AgentStateSnapshotIncludedRelations<I, R>;

export type WorkspaceSnapshotWithIncludes<I extends WorkspaceSnapshotInclude = {}, R extends boolean = false> = WorkspaceSnapshot & WorkspaceSnapshotIncludedRelations<I, R>;

export type MemoryLinkWithIncludes<I extends MemoryLinkInclude = {}, R extends boolean = false> = MemoryLink & MemoryLinkIncludedRelations<I, R>;

export type SourceFileWithIncludes<I extends SourceFileInclude = {}, R extends boolean = false> = SourceFile & SourceFileIncludedRelations<I, R>;

export type DaemonLogSourceWithIncludes<I extends DaemonLogSourceInclude = {}, R extends boolean = false> = DaemonLogSource & DaemonLogSourceIncludedRelations<I, R>;

export type DaemonLogChunkWithIncludes<I extends DaemonLogChunkInclude = {}, R extends boolean = false> = DaemonLogChunk & DaemonLogChunkIncludedRelations<I, R>;

export type DaemonLogEventWithIncludes<I extends DaemonLogEventInclude = {}, R extends boolean = false> = DaemonLogEvent & DaemonLogEventIncludedRelations<I, R>;

export type DaemonLogCheckpointWithIncludes<I extends DaemonLogCheckpointInclude = {}, R extends boolean = false> = DaemonLogCheckpoint & DaemonLogCheckpointIncludedRelations<I, R>;

export type DaemonLogSummaryWithIncludes<I extends DaemonLogSummaryInclude = {}, R extends boolean = false> = DaemonLogSummary & DaemonLogSummaryIncludedRelations<I, R>;

export type DesignerObjectRefWithIncludes<I extends DesignerObjectRefInclude = {}, R extends boolean = false> = DesignerObjectRef & DesignerObjectRefIncludedRelations<I, R>;

export type DesignerAgentWithIncludes<I extends DesignerAgentInclude = {}, R extends boolean = false> = DesignerAgent & DesignerAgentIncludedRelations<I, R>;

export type DesignerAgentToolWithIncludes<I extends DesignerAgentToolInclude = {}, R extends boolean = false> = DesignerAgentTool & DesignerAgentToolIncludedRelations<I, R>;

export type DesignerAgentContextWithIncludes<I extends DesignerAgentContextInclude = {}, R extends boolean = false> = DesignerAgentContext & DesignerAgentContextIncludedRelations<I, R>;

export type DesignerCodexConversationWithIncludes<I extends DesignerCodexConversationInclude = {}, R extends boolean = false> = DesignerCodexConversation & DesignerCodexConversationIncludedRelations<I, R>;

export type DesignerCodexTurnWithIncludes<I extends DesignerCodexTurnInclude = {}, R extends boolean = false> = DesignerCodexTurn & DesignerCodexTurnIncludedRelations<I, R>;

export type DesignerTelemetryEventWithIncludes<I extends DesignerTelemetryEventInclude = {}, R extends boolean = false> = DesignerTelemetryEvent & DesignerTelemetryEventIncludedRelations<I, R>;

export type DesignerLiveCommitWithIncludes<I extends DesignerLiveCommitInclude = {}, R extends boolean = false> = DesignerLiveCommit & DesignerLiveCommitIncludedRelations<I, R>;

export type DesignerCadWorkspaceWithIncludes<I extends DesignerCadWorkspaceInclude = {}, R extends boolean = false> = DesignerCadWorkspace & DesignerCadWorkspaceIncludedRelations<I, R>;

export type DesignerCadDocumentWithIncludes<I extends DesignerCadDocumentInclude = {}, R extends boolean = false> = DesignerCadDocument & DesignerCadDocumentIncludedRelations<I, R>;

export type DesignerCadSessionWithIncludes<I extends DesignerCadSessionInclude = {}, R extends boolean = false> = DesignerCadSession & DesignerCadSessionIncludedRelations<I, R>;

export type DesignerCadEventWithIncludes<I extends DesignerCadEventInclude = {}, R extends boolean = false> = DesignerCadEvent & DesignerCadEventIncludedRelations<I, R>;

export type DesignerCadSceneNodeWithIncludes<I extends DesignerCadSceneNodeInclude = {}, R extends boolean = false> = DesignerCadSceneNode & DesignerCadSceneNodeIncludedRelations<I, R>;

export type DesignerCadSelectionWithIncludes<I extends DesignerCadSelectionInclude = {}, R extends boolean = false> = DesignerCadSelection & DesignerCadSelectionIncludedRelations<I, R>;

export type DesignerCadToolSessionWithIncludes<I extends DesignerCadToolSessionInclude = {}, R extends boolean = false> = DesignerCadToolSession & DesignerCadToolSessionIncludedRelations<I, R>;

export type DesignerCadOperationWithIncludes<I extends DesignerCadOperationInclude = {}, R extends boolean = false> = DesignerCadOperation & DesignerCadOperationIncludedRelations<I, R>;

export type DesignerCadSourceEditWithIncludes<I extends DesignerCadSourceEditInclude = {}, R extends boolean = false> = DesignerCadSourceEdit & DesignerCadSourceEditIncludedRelations<I, R>;

export type DesignerCadPreviewHandleWithIncludes<I extends DesignerCadPreviewHandleInclude = {}, R extends boolean = false> = DesignerCadPreviewHandle & DesignerCadPreviewHandleIncludedRelations<I, R>;

export type DesignerCadPreviewUpdateWithIncludes<I extends DesignerCadPreviewUpdateInclude = {}, R extends boolean = false> = DesignerCadPreviewUpdate & DesignerCadPreviewUpdateIncludedRelations<I, R>;

export type DesignerCadWidgetWithIncludes<I extends DesignerCadWidgetInclude = {}, R extends boolean = false> = DesignerCadWidget & DesignerCadWidgetIncludedRelations<I, R>;

export type DesignerCadSteerWithIncludes<I extends DesignerCadSteerInclude = {}, R extends boolean = false> = DesignerCadSteer & DesignerCadSteerIncludedRelations<I, R>;

export type AgentSelectableColumn = keyof Agent | PermissionIntrospectionColumn | "*";
export type AgentOrderableColumn = keyof Agent | PermissionIntrospectionColumn;

export type AgentSelected<S extends AgentSelectableColumn = keyof Agent> = ("*" extends S ? Agent : Pick<Agent, Extract<S | "id", keyof Agent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type AgentSelectedWithIncludes<I extends AgentInclude = {}, S extends AgentSelectableColumn = keyof Agent, R extends boolean = false> = AgentSelected<S> & AgentIncludedRelations<I, R>;

export type AgentRunSelectableColumn = keyof AgentRun | PermissionIntrospectionColumn | "*";
export type AgentRunOrderableColumn = keyof AgentRun | PermissionIntrospectionColumn;

export type AgentRunSelected<S extends AgentRunSelectableColumn = keyof AgentRun> = ("*" extends S ? AgentRun : Pick<AgentRun, Extract<S | "id", keyof AgentRun>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type AgentRunSelectedWithIncludes<I extends AgentRunInclude = {}, S extends AgentRunSelectableColumn = keyof AgentRun, R extends boolean = false> = AgentRunSelected<S> & AgentRunIncludedRelations<I, R>;

export type RunItemSelectableColumn = keyof RunItem | PermissionIntrospectionColumn | "*";
export type RunItemOrderableColumn = keyof RunItem | PermissionIntrospectionColumn;

export type RunItemSelected<S extends RunItemSelectableColumn = keyof RunItem> = ("*" extends S ? RunItem : Pick<RunItem, Extract<S | "id", keyof RunItem>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type RunItemSelectedWithIncludes<I extends RunItemInclude = {}, S extends RunItemSelectableColumn = keyof RunItem, R extends boolean = false> = RunItemSelected<S> & RunItemIncludedRelations<I, R>;

export type SemanticEventSelectableColumn = keyof SemanticEvent | PermissionIntrospectionColumn | "*";
export type SemanticEventOrderableColumn = keyof SemanticEvent | PermissionIntrospectionColumn;

export type SemanticEventSelected<S extends SemanticEventSelectableColumn = keyof SemanticEvent> = ("*" extends S ? SemanticEvent : Pick<SemanticEvent, Extract<S | "id", keyof SemanticEvent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type SemanticEventSelectedWithIncludes<I extends SemanticEventInclude = {}, S extends SemanticEventSelectableColumn = keyof SemanticEvent, R extends boolean = false> = SemanticEventSelected<S> & SemanticEventIncludedRelations<I, R>;

export type WireEventSelectableColumn = keyof WireEvent | PermissionIntrospectionColumn | "*";
export type WireEventOrderableColumn = keyof WireEvent | PermissionIntrospectionColumn;

export type WireEventSelected<S extends WireEventSelectableColumn = keyof WireEvent> = ("*" extends S ? WireEvent : Pick<WireEvent, Extract<S | "id", keyof WireEvent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type WireEventSelectedWithIncludes<I extends WireEventInclude = {}, S extends WireEventSelectableColumn = keyof WireEvent, R extends boolean = false> = WireEventSelected<S> & WireEventIncludedRelations<I, R>;

export type ArtifactSelectableColumn = keyof Artifact | PermissionIntrospectionColumn | "*";
export type ArtifactOrderableColumn = keyof Artifact | PermissionIntrospectionColumn;

export type ArtifactSelected<S extends ArtifactSelectableColumn = keyof Artifact> = ("*" extends S ? Artifact : Pick<Artifact, Extract<S | "id", keyof Artifact>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ArtifactSelectedWithIncludes<I extends ArtifactInclude = {}, S extends ArtifactSelectableColumn = keyof Artifact, R extends boolean = false> = ArtifactSelected<S> & ArtifactIncludedRelations<I, R>;

export type AgentStateSnapshotSelectableColumn = keyof AgentStateSnapshot | PermissionIntrospectionColumn | "*";
export type AgentStateSnapshotOrderableColumn = keyof AgentStateSnapshot | PermissionIntrospectionColumn;

export type AgentStateSnapshotSelected<S extends AgentStateSnapshotSelectableColumn = keyof AgentStateSnapshot> = ("*" extends S ? AgentStateSnapshot : Pick<AgentStateSnapshot, Extract<S | "id", keyof AgentStateSnapshot>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type AgentStateSnapshotSelectedWithIncludes<I extends AgentStateSnapshotInclude = {}, S extends AgentStateSnapshotSelectableColumn = keyof AgentStateSnapshot, R extends boolean = false> = AgentStateSnapshotSelected<S> & AgentStateSnapshotIncludedRelations<I, R>;

export type WorkspaceSnapshotSelectableColumn = keyof WorkspaceSnapshot | PermissionIntrospectionColumn | "*";
export type WorkspaceSnapshotOrderableColumn = keyof WorkspaceSnapshot | PermissionIntrospectionColumn;

export type WorkspaceSnapshotSelected<S extends WorkspaceSnapshotSelectableColumn = keyof WorkspaceSnapshot> = ("*" extends S ? WorkspaceSnapshot : Pick<WorkspaceSnapshot, Extract<S | "id", keyof WorkspaceSnapshot>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type WorkspaceSnapshotSelectedWithIncludes<I extends WorkspaceSnapshotInclude = {}, S extends WorkspaceSnapshotSelectableColumn = keyof WorkspaceSnapshot, R extends boolean = false> = WorkspaceSnapshotSelected<S> & WorkspaceSnapshotIncludedRelations<I, R>;

export type MemoryLinkSelectableColumn = keyof MemoryLink | PermissionIntrospectionColumn | "*";
export type MemoryLinkOrderableColumn = keyof MemoryLink | PermissionIntrospectionColumn;

export type MemoryLinkSelected<S extends MemoryLinkSelectableColumn = keyof MemoryLink> = ("*" extends S ? MemoryLink : Pick<MemoryLink, Extract<S | "id", keyof MemoryLink>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type MemoryLinkSelectedWithIncludes<I extends MemoryLinkInclude = {}, S extends MemoryLinkSelectableColumn = keyof MemoryLink, R extends boolean = false> = MemoryLinkSelected<S> & MemoryLinkIncludedRelations<I, R>;

export type SourceFileSelectableColumn = keyof SourceFile | PermissionIntrospectionColumn | "*";
export type SourceFileOrderableColumn = keyof SourceFile | PermissionIntrospectionColumn;

export type SourceFileSelected<S extends SourceFileSelectableColumn = keyof SourceFile> = ("*" extends S ? SourceFile : Pick<SourceFile, Extract<S | "id", keyof SourceFile>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type SourceFileSelectedWithIncludes<I extends SourceFileInclude = {}, S extends SourceFileSelectableColumn = keyof SourceFile, R extends boolean = false> = SourceFileSelected<S> & SourceFileIncludedRelations<I, R>;

export type DaemonLogSourceSelectableColumn = keyof DaemonLogSource | PermissionIntrospectionColumn | "*";
export type DaemonLogSourceOrderableColumn = keyof DaemonLogSource | PermissionIntrospectionColumn;

export type DaemonLogSourceSelected<S extends DaemonLogSourceSelectableColumn = keyof DaemonLogSource> = ("*" extends S ? DaemonLogSource : Pick<DaemonLogSource, Extract<S | "id", keyof DaemonLogSource>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DaemonLogSourceSelectedWithIncludes<I extends DaemonLogSourceInclude = {}, S extends DaemonLogSourceSelectableColumn = keyof DaemonLogSource, R extends boolean = false> = DaemonLogSourceSelected<S> & DaemonLogSourceIncludedRelations<I, R>;

export type DaemonLogChunkSelectableColumn = keyof DaemonLogChunk | PermissionIntrospectionColumn | "*";
export type DaemonLogChunkOrderableColumn = keyof DaemonLogChunk | PermissionIntrospectionColumn;

export type DaemonLogChunkSelected<S extends DaemonLogChunkSelectableColumn = keyof DaemonLogChunk> = ("*" extends S ? DaemonLogChunk : Pick<DaemonLogChunk, Extract<S | "id", keyof DaemonLogChunk>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DaemonLogChunkSelectedWithIncludes<I extends DaemonLogChunkInclude = {}, S extends DaemonLogChunkSelectableColumn = keyof DaemonLogChunk, R extends boolean = false> = DaemonLogChunkSelected<S> & DaemonLogChunkIncludedRelations<I, R>;

export type DaemonLogEventSelectableColumn = keyof DaemonLogEvent | PermissionIntrospectionColumn | "*";
export type DaemonLogEventOrderableColumn = keyof DaemonLogEvent | PermissionIntrospectionColumn;

export type DaemonLogEventSelected<S extends DaemonLogEventSelectableColumn = keyof DaemonLogEvent> = ("*" extends S ? DaemonLogEvent : Pick<DaemonLogEvent, Extract<S | "id", keyof DaemonLogEvent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DaemonLogEventSelectedWithIncludes<I extends DaemonLogEventInclude = {}, S extends DaemonLogEventSelectableColumn = keyof DaemonLogEvent, R extends boolean = false> = DaemonLogEventSelected<S> & DaemonLogEventIncludedRelations<I, R>;

export type DaemonLogCheckpointSelectableColumn = keyof DaemonLogCheckpoint | PermissionIntrospectionColumn | "*";
export type DaemonLogCheckpointOrderableColumn = keyof DaemonLogCheckpoint | PermissionIntrospectionColumn;

export type DaemonLogCheckpointSelected<S extends DaemonLogCheckpointSelectableColumn = keyof DaemonLogCheckpoint> = ("*" extends S ? DaemonLogCheckpoint : Pick<DaemonLogCheckpoint, Extract<S | "id", keyof DaemonLogCheckpoint>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DaemonLogCheckpointSelectedWithIncludes<I extends DaemonLogCheckpointInclude = {}, S extends DaemonLogCheckpointSelectableColumn = keyof DaemonLogCheckpoint, R extends boolean = false> = DaemonLogCheckpointSelected<S> & DaemonLogCheckpointIncludedRelations<I, R>;

export type DaemonLogSummarySelectableColumn = keyof DaemonLogSummary | PermissionIntrospectionColumn | "*";
export type DaemonLogSummaryOrderableColumn = keyof DaemonLogSummary | PermissionIntrospectionColumn;

export type DaemonLogSummarySelected<S extends DaemonLogSummarySelectableColumn = keyof DaemonLogSummary> = ("*" extends S ? DaemonLogSummary : Pick<DaemonLogSummary, Extract<S | "id", keyof DaemonLogSummary>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DaemonLogSummarySelectedWithIncludes<I extends DaemonLogSummaryInclude = {}, S extends DaemonLogSummarySelectableColumn = keyof DaemonLogSummary, R extends boolean = false> = DaemonLogSummarySelected<S> & DaemonLogSummaryIncludedRelations<I, R>;

export type TaskRecordSelectableColumn = keyof TaskRecord | PermissionIntrospectionColumn | "*";
export type TaskRecordOrderableColumn = keyof TaskRecord | PermissionIntrospectionColumn;

export type TaskRecordSelected<S extends TaskRecordSelectableColumn = keyof TaskRecord> = ("*" extends S ? TaskRecord : Pick<TaskRecord, Extract<S | "id", keyof TaskRecord>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerObjectRefSelectableColumn = keyof DesignerObjectRef | PermissionIntrospectionColumn | "*";
export type DesignerObjectRefOrderableColumn = keyof DesignerObjectRef | PermissionIntrospectionColumn;

export type DesignerObjectRefSelected<S extends DesignerObjectRefSelectableColumn = keyof DesignerObjectRef> = ("*" extends S ? DesignerObjectRef : Pick<DesignerObjectRef, Extract<S | "id", keyof DesignerObjectRef>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerObjectRefSelectedWithIncludes<I extends DesignerObjectRefInclude = {}, S extends DesignerObjectRefSelectableColumn = keyof DesignerObjectRef, R extends boolean = false> = DesignerObjectRefSelected<S> & DesignerObjectRefIncludedRelations<I, R>;

export type DesignerAgentSelectableColumn = keyof DesignerAgent | PermissionIntrospectionColumn | "*";
export type DesignerAgentOrderableColumn = keyof DesignerAgent | PermissionIntrospectionColumn;

export type DesignerAgentSelected<S extends DesignerAgentSelectableColumn = keyof DesignerAgent> = ("*" extends S ? DesignerAgent : Pick<DesignerAgent, Extract<S | "id", keyof DesignerAgent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerAgentSelectedWithIncludes<I extends DesignerAgentInclude = {}, S extends DesignerAgentSelectableColumn = keyof DesignerAgent, R extends boolean = false> = DesignerAgentSelected<S> & DesignerAgentIncludedRelations<I, R>;

export type DesignerAgentToolSelectableColumn = keyof DesignerAgentTool | PermissionIntrospectionColumn | "*";
export type DesignerAgentToolOrderableColumn = keyof DesignerAgentTool | PermissionIntrospectionColumn;

export type DesignerAgentToolSelected<S extends DesignerAgentToolSelectableColumn = keyof DesignerAgentTool> = ("*" extends S ? DesignerAgentTool : Pick<DesignerAgentTool, Extract<S | "id", keyof DesignerAgentTool>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerAgentToolSelectedWithIncludes<I extends DesignerAgentToolInclude = {}, S extends DesignerAgentToolSelectableColumn = keyof DesignerAgentTool, R extends boolean = false> = DesignerAgentToolSelected<S> & DesignerAgentToolIncludedRelations<I, R>;

export type DesignerAgentContextSelectableColumn = keyof DesignerAgentContext | PermissionIntrospectionColumn | "*";
export type DesignerAgentContextOrderableColumn = keyof DesignerAgentContext | PermissionIntrospectionColumn;

export type DesignerAgentContextSelected<S extends DesignerAgentContextSelectableColumn = keyof DesignerAgentContext> = ("*" extends S ? DesignerAgentContext : Pick<DesignerAgentContext, Extract<S | "id", keyof DesignerAgentContext>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerAgentContextSelectedWithIncludes<I extends DesignerAgentContextInclude = {}, S extends DesignerAgentContextSelectableColumn = keyof DesignerAgentContext, R extends boolean = false> = DesignerAgentContextSelected<S> & DesignerAgentContextIncludedRelations<I, R>;

export type DesignerCodexConversationSelectableColumn = keyof DesignerCodexConversation | PermissionIntrospectionColumn | "*";
export type DesignerCodexConversationOrderableColumn = keyof DesignerCodexConversation | PermissionIntrospectionColumn;

export type DesignerCodexConversationSelected<S extends DesignerCodexConversationSelectableColumn = keyof DesignerCodexConversation> = ("*" extends S ? DesignerCodexConversation : Pick<DesignerCodexConversation, Extract<S | "id", keyof DesignerCodexConversation>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCodexConversationSelectedWithIncludes<I extends DesignerCodexConversationInclude = {}, S extends DesignerCodexConversationSelectableColumn = keyof DesignerCodexConversation, R extends boolean = false> = DesignerCodexConversationSelected<S> & DesignerCodexConversationIncludedRelations<I, R>;

export type DesignerCodexTurnSelectableColumn = keyof DesignerCodexTurn | PermissionIntrospectionColumn | "*";
export type DesignerCodexTurnOrderableColumn = keyof DesignerCodexTurn | PermissionIntrospectionColumn;

export type DesignerCodexTurnSelected<S extends DesignerCodexTurnSelectableColumn = keyof DesignerCodexTurn> = ("*" extends S ? DesignerCodexTurn : Pick<DesignerCodexTurn, Extract<S | "id", keyof DesignerCodexTurn>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCodexTurnSelectedWithIncludes<I extends DesignerCodexTurnInclude = {}, S extends DesignerCodexTurnSelectableColumn = keyof DesignerCodexTurn, R extends boolean = false> = DesignerCodexTurnSelected<S> & DesignerCodexTurnIncludedRelations<I, R>;

export type DesignerTelemetryEventSelectableColumn = keyof DesignerTelemetryEvent | PermissionIntrospectionColumn | "*";
export type DesignerTelemetryEventOrderableColumn = keyof DesignerTelemetryEvent | PermissionIntrospectionColumn;

export type DesignerTelemetryEventSelected<S extends DesignerTelemetryEventSelectableColumn = keyof DesignerTelemetryEvent> = ("*" extends S ? DesignerTelemetryEvent : Pick<DesignerTelemetryEvent, Extract<S | "id", keyof DesignerTelemetryEvent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerTelemetryEventSelectedWithIncludes<I extends DesignerTelemetryEventInclude = {}, S extends DesignerTelemetryEventSelectableColumn = keyof DesignerTelemetryEvent, R extends boolean = false> = DesignerTelemetryEventSelected<S> & DesignerTelemetryEventIncludedRelations<I, R>;

export type DesignerLiveCommitSelectableColumn = keyof DesignerLiveCommit | PermissionIntrospectionColumn | "*";
export type DesignerLiveCommitOrderableColumn = keyof DesignerLiveCommit | PermissionIntrospectionColumn;

export type DesignerLiveCommitSelected<S extends DesignerLiveCommitSelectableColumn = keyof DesignerLiveCommit> = ("*" extends S ? DesignerLiveCommit : Pick<DesignerLiveCommit, Extract<S | "id", keyof DesignerLiveCommit>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerLiveCommitSelectedWithIncludes<I extends DesignerLiveCommitInclude = {}, S extends DesignerLiveCommitSelectableColumn = keyof DesignerLiveCommit, R extends boolean = false> = DesignerLiveCommitSelected<S> & DesignerLiveCommitIncludedRelations<I, R>;

export type DesignerCadWorkspaceSelectableColumn = keyof DesignerCadWorkspace | PermissionIntrospectionColumn | "*";
export type DesignerCadWorkspaceOrderableColumn = keyof DesignerCadWorkspace | PermissionIntrospectionColumn;

export type DesignerCadWorkspaceSelected<S extends DesignerCadWorkspaceSelectableColumn = keyof DesignerCadWorkspace> = ("*" extends S ? DesignerCadWorkspace : Pick<DesignerCadWorkspace, Extract<S | "id", keyof DesignerCadWorkspace>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadWorkspaceSelectedWithIncludes<I extends DesignerCadWorkspaceInclude = {}, S extends DesignerCadWorkspaceSelectableColumn = keyof DesignerCadWorkspace, R extends boolean = false> = DesignerCadWorkspaceSelected<S> & DesignerCadWorkspaceIncludedRelations<I, R>;

export type DesignerCadDocumentSelectableColumn = keyof DesignerCadDocument | PermissionIntrospectionColumn | "*";
export type DesignerCadDocumentOrderableColumn = keyof DesignerCadDocument | PermissionIntrospectionColumn;

export type DesignerCadDocumentSelected<S extends DesignerCadDocumentSelectableColumn = keyof DesignerCadDocument> = ("*" extends S ? DesignerCadDocument : Pick<DesignerCadDocument, Extract<S | "id", keyof DesignerCadDocument>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadDocumentSelectedWithIncludes<I extends DesignerCadDocumentInclude = {}, S extends DesignerCadDocumentSelectableColumn = keyof DesignerCadDocument, R extends boolean = false> = DesignerCadDocumentSelected<S> & DesignerCadDocumentIncludedRelations<I, R>;

export type DesignerCadSessionSelectableColumn = keyof DesignerCadSession | PermissionIntrospectionColumn | "*";
export type DesignerCadSessionOrderableColumn = keyof DesignerCadSession | PermissionIntrospectionColumn;

export type DesignerCadSessionSelected<S extends DesignerCadSessionSelectableColumn = keyof DesignerCadSession> = ("*" extends S ? DesignerCadSession : Pick<DesignerCadSession, Extract<S | "id", keyof DesignerCadSession>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadSessionSelectedWithIncludes<I extends DesignerCadSessionInclude = {}, S extends DesignerCadSessionSelectableColumn = keyof DesignerCadSession, R extends boolean = false> = DesignerCadSessionSelected<S> & DesignerCadSessionIncludedRelations<I, R>;

export type DesignerCadEventSelectableColumn = keyof DesignerCadEvent | PermissionIntrospectionColumn | "*";
export type DesignerCadEventOrderableColumn = keyof DesignerCadEvent | PermissionIntrospectionColumn;

export type DesignerCadEventSelected<S extends DesignerCadEventSelectableColumn = keyof DesignerCadEvent> = ("*" extends S ? DesignerCadEvent : Pick<DesignerCadEvent, Extract<S | "id", keyof DesignerCadEvent>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadEventSelectedWithIncludes<I extends DesignerCadEventInclude = {}, S extends DesignerCadEventSelectableColumn = keyof DesignerCadEvent, R extends boolean = false> = DesignerCadEventSelected<S> & DesignerCadEventIncludedRelations<I, R>;

export type DesignerCadSceneNodeSelectableColumn = keyof DesignerCadSceneNode | PermissionIntrospectionColumn | "*";
export type DesignerCadSceneNodeOrderableColumn = keyof DesignerCadSceneNode | PermissionIntrospectionColumn;

export type DesignerCadSceneNodeSelected<S extends DesignerCadSceneNodeSelectableColumn = keyof DesignerCadSceneNode> = ("*" extends S ? DesignerCadSceneNode : Pick<DesignerCadSceneNode, Extract<S | "id", keyof DesignerCadSceneNode>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadSceneNodeSelectedWithIncludes<I extends DesignerCadSceneNodeInclude = {}, S extends DesignerCadSceneNodeSelectableColumn = keyof DesignerCadSceneNode, R extends boolean = false> = DesignerCadSceneNodeSelected<S> & DesignerCadSceneNodeIncludedRelations<I, R>;

export type DesignerCadSelectionSelectableColumn = keyof DesignerCadSelection | PermissionIntrospectionColumn | "*";
export type DesignerCadSelectionOrderableColumn = keyof DesignerCadSelection | PermissionIntrospectionColumn;

export type DesignerCadSelectionSelected<S extends DesignerCadSelectionSelectableColumn = keyof DesignerCadSelection> = ("*" extends S ? DesignerCadSelection : Pick<DesignerCadSelection, Extract<S | "id", keyof DesignerCadSelection>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadSelectionSelectedWithIncludes<I extends DesignerCadSelectionInclude = {}, S extends DesignerCadSelectionSelectableColumn = keyof DesignerCadSelection, R extends boolean = false> = DesignerCadSelectionSelected<S> & DesignerCadSelectionIncludedRelations<I, R>;

export type DesignerCadToolSessionSelectableColumn = keyof DesignerCadToolSession | PermissionIntrospectionColumn | "*";
export type DesignerCadToolSessionOrderableColumn = keyof DesignerCadToolSession | PermissionIntrospectionColumn;

export type DesignerCadToolSessionSelected<S extends DesignerCadToolSessionSelectableColumn = keyof DesignerCadToolSession> = ("*" extends S ? DesignerCadToolSession : Pick<DesignerCadToolSession, Extract<S | "id", keyof DesignerCadToolSession>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadToolSessionSelectedWithIncludes<I extends DesignerCadToolSessionInclude = {}, S extends DesignerCadToolSessionSelectableColumn = keyof DesignerCadToolSession, R extends boolean = false> = DesignerCadToolSessionSelected<S> & DesignerCadToolSessionIncludedRelations<I, R>;

export type DesignerCadOperationSelectableColumn = keyof DesignerCadOperation | PermissionIntrospectionColumn | "*";
export type DesignerCadOperationOrderableColumn = keyof DesignerCadOperation | PermissionIntrospectionColumn;

export type DesignerCadOperationSelected<S extends DesignerCadOperationSelectableColumn = keyof DesignerCadOperation> = ("*" extends S ? DesignerCadOperation : Pick<DesignerCadOperation, Extract<S | "id", keyof DesignerCadOperation>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadOperationSelectedWithIncludes<I extends DesignerCadOperationInclude = {}, S extends DesignerCadOperationSelectableColumn = keyof DesignerCadOperation, R extends boolean = false> = DesignerCadOperationSelected<S> & DesignerCadOperationIncludedRelations<I, R>;

export type DesignerCadSourceEditSelectableColumn = keyof DesignerCadSourceEdit | PermissionIntrospectionColumn | "*";
export type DesignerCadSourceEditOrderableColumn = keyof DesignerCadSourceEdit | PermissionIntrospectionColumn;

export type DesignerCadSourceEditSelected<S extends DesignerCadSourceEditSelectableColumn = keyof DesignerCadSourceEdit> = ("*" extends S ? DesignerCadSourceEdit : Pick<DesignerCadSourceEdit, Extract<S | "id", keyof DesignerCadSourceEdit>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadSourceEditSelectedWithIncludes<I extends DesignerCadSourceEditInclude = {}, S extends DesignerCadSourceEditSelectableColumn = keyof DesignerCadSourceEdit, R extends boolean = false> = DesignerCadSourceEditSelected<S> & DesignerCadSourceEditIncludedRelations<I, R>;

export type DesignerCadPreviewHandleSelectableColumn = keyof DesignerCadPreviewHandle | PermissionIntrospectionColumn | "*";
export type DesignerCadPreviewHandleOrderableColumn = keyof DesignerCadPreviewHandle | PermissionIntrospectionColumn;

export type DesignerCadPreviewHandleSelected<S extends DesignerCadPreviewHandleSelectableColumn = keyof DesignerCadPreviewHandle> = ("*" extends S ? DesignerCadPreviewHandle : Pick<DesignerCadPreviewHandle, Extract<S | "id", keyof DesignerCadPreviewHandle>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadPreviewHandleSelectedWithIncludes<I extends DesignerCadPreviewHandleInclude = {}, S extends DesignerCadPreviewHandleSelectableColumn = keyof DesignerCadPreviewHandle, R extends boolean = false> = DesignerCadPreviewHandleSelected<S> & DesignerCadPreviewHandleIncludedRelations<I, R>;

export type DesignerCadPreviewUpdateSelectableColumn = keyof DesignerCadPreviewUpdate | PermissionIntrospectionColumn | "*";
export type DesignerCadPreviewUpdateOrderableColumn = keyof DesignerCadPreviewUpdate | PermissionIntrospectionColumn;

export type DesignerCadPreviewUpdateSelected<S extends DesignerCadPreviewUpdateSelectableColumn = keyof DesignerCadPreviewUpdate> = ("*" extends S ? DesignerCadPreviewUpdate : Pick<DesignerCadPreviewUpdate, Extract<S | "id", keyof DesignerCadPreviewUpdate>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadPreviewUpdateSelectedWithIncludes<I extends DesignerCadPreviewUpdateInclude = {}, S extends DesignerCadPreviewUpdateSelectableColumn = keyof DesignerCadPreviewUpdate, R extends boolean = false> = DesignerCadPreviewUpdateSelected<S> & DesignerCadPreviewUpdateIncludedRelations<I, R>;

export type DesignerCadWidgetSelectableColumn = keyof DesignerCadWidget | PermissionIntrospectionColumn | "*";
export type DesignerCadWidgetOrderableColumn = keyof DesignerCadWidget | PermissionIntrospectionColumn;

export type DesignerCadWidgetSelected<S extends DesignerCadWidgetSelectableColumn = keyof DesignerCadWidget> = ("*" extends S ? DesignerCadWidget : Pick<DesignerCadWidget, Extract<S | "id", keyof DesignerCadWidget>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadWidgetSelectedWithIncludes<I extends DesignerCadWidgetInclude = {}, S extends DesignerCadWidgetSelectableColumn = keyof DesignerCadWidget, R extends boolean = false> = DesignerCadWidgetSelected<S> & DesignerCadWidgetIncludedRelations<I, R>;

export type DesignerCadSteerSelectableColumn = keyof DesignerCadSteer | PermissionIntrospectionColumn | "*";
export type DesignerCadSteerOrderableColumn = keyof DesignerCadSteer | PermissionIntrospectionColumn;

export type DesignerCadSteerSelected<S extends DesignerCadSteerSelectableColumn = keyof DesignerCadSteer> = ("*" extends S ? DesignerCadSteer : Pick<DesignerCadSteer, Extract<S | "id", keyof DesignerCadSteer>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type DesignerCadSteerSelectedWithIncludes<I extends DesignerCadSteerInclude = {}, S extends DesignerCadSteerSelectableColumn = keyof DesignerCadSteer, R extends boolean = false> = DesignerCadSteerSelected<S> & DesignerCadSteerIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "agents": {
    "columns": [
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "lane",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "spec_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "prompt_surface",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "agent_runs": {
    "columns": [
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "agents"
      },
      {
        "name": "thread_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "cwd",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "request_summary",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "started_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "ended_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "context_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "source_trace_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      }
    ]
  },
  "run_items": {
    "columns": [
      {
        "name": "item_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "agent_runs"
      },
      {
        "name": "item_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "phase",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "summary_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "started_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "completed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "semantic_events": {
    "columns": [
      {
        "name": "event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "agent_runs"
      },
      {
        "name": "item_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "item_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "run_items"
      },
      {
        "name": "event_type",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "summary_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "payload_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "occurred_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "wire_events": {
    "columns": [
      {
        "name": "event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "agent_runs"
      },
      {
        "name": "connection_id",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "session_id",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "direction",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "method",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "request_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "payload_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "occurred_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "artifacts": {
    "columns": [
      {
        "name": "artifact_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "agent_runs"
      },
      {
        "name": "artifact_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "title",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "absolute_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "checksum",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "agent_state_snapshots": {
    "columns": [
      {
        "name": "snapshot_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "agents"
      },
      {
        "name": "state_version",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "state_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": false
      },
      {
        "name": "captured_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "workspace_snapshots": {
    "columns": [
      {
        "name": "snapshot_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "agent_runs"
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "branch",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "head_commit",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "dirty_path_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "snapshot_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "captured_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "memory_links": {
    "columns": [
      {
        "name": "link_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "agent_runs"
      },
      {
        "name": "item_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "item_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "run_items"
      },
      {
        "name": "memory_scope",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "memory_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "query_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "link_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "source_files": {
    "columns": [
      {
        "name": "source_file_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "run_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "agent_runs"
      },
      {
        "name": "file_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "absolute_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "checksum",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "daemon_log_sources": {
    "columns": [
      {
        "name": "source_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "manager",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "daemon_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "stream",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "host_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "log_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "config_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "owner_agent",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "flow_daemon_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "launchd_label",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "retention_class",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "daemon_log_chunks": {
    "columns": [
      {
        "name": "chunk_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "daemon_log_sources"
      },
      {
        "name": "daemon_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "stream",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "host_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "log_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "file_fingerprint",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "start_offset",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "end_offset",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "first_line_no",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "last_line_no",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "line_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "byte_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "first_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "sha256",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "body_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "body_preview",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "compression",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "ingested_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "daemon_log_events": {
    "columns": [
      {
        "name": "event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "daemon_log_sources"
      },
      {
        "name": "chunk_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "chunk_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "daemon_log_chunks"
      },
      {
        "name": "daemon_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "stream",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "seq",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "line_no",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "level",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "message",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "fields_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "conversation",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "conversation_hash",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "job_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "trace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "span_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "error_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "daemon_log_checkpoints": {
    "columns": [
      {
        "name": "checkpoint_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "daemon_log_sources"
      },
      {
        "name": "host_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "log_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "file_fingerprint",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "inode",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "device",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "offset",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "line_no",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "last_chunk_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "last_event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "last_seen_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "daemon_log_summaries": {
    "columns": [
      {
        "name": "summary_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "daemon_log_sources"
      },
      {
        "name": "daemon_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "window_start",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "window_end",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "level_counts_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": false
      },
      {
        "name": "error_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "warning_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "first_error_event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "last_error_event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "top_error_kinds_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "summary_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "task_records": {
    "columns": [
      {
        "name": "task_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "context",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "title",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "priority",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "placement",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "focus_rank",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "project",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "issue",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "branch",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "plan",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "pr",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "tags_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "next_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "context_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "notes_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "annotations_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "source_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_object_refs": {
    "columns": [
      {
        "name": "object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "provider",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "uri",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "bucket",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "key",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "region",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "digest_sha256",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "byte_size",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "content_type",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "object_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_agents": {
    "columns": [
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "provider",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "display_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "model",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "default_context_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "tool_contract_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_agent_tools": {
    "columns": [
      {
        "name": "tool_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_agents"
      },
      {
        "name": "tool_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "tool_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "input_schema_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "output_schema_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "scope_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_agent_contexts": {
    "columns": [
      {
        "name": "context_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "agent_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_agents"
      },
      {
        "name": "context_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "object_ref_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_object_refs"
      },
      {
        "name": "inline_context_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "priority",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_codex_conversations": {
    "columns": [
      {
        "name": "conversation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "provider",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "provider_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "thread_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_key",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "branch",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "model",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "transcript_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "transcript_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_object_refs"
      },
      {
        "name": "latest_event_sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "ended_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_codex_turns": {
    "columns": [
      {
        "name": "turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "conversation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "conversation_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_codex_conversations"
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "turn_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "role",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "summary_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "payload_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "payload_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_object_refs"
      },
      {
        "name": "prompt_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "prompt_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_object_refs"
      },
      {
        "name": "response_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "response_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_object_refs"
      },
      {
        "name": "token_counts_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "started_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "completed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_telemetry_events": {
    "columns": [
      {
        "name": "telemetry_event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "conversation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "conversation_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_codex_conversations"
      },
      {
        "name": "event_type",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "pane",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "summary_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "payload_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "payload_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_object_refs"
      },
      {
        "name": "properties_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "occurred_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "ingested_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_live_commits": {
    "columns": [
      {
        "name": "commit_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "branch",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "bookmark",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "live_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "tree_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "parent_commit_ids_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "subject",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "body",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "author_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "author_email",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "committer_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "committer_email",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "trace_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_turn_ordinal",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "source_conversation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_conversation_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_codex_conversations"
      },
      {
        "name": "source_turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_turn_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_codex_turns"
      },
      {
        "name": "agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "agent_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_agents"
      },
      {
        "name": "courier_run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "live_snapshot_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "changed_paths_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "patch_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "patch_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_object_refs"
      },
      {
        "name": "manifest_object_ref_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "manifest_object_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_object_refs"
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "committed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "reflected_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "ingested_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_workspaces": {
    "columns": [
      {
        "name": "workspace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_key",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "title",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "workspace_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_documents": {
    "columns": [
      {
        "name": "document_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_workspaces"
      },
      {
        "name": "file_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "language",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_hash",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_sessions": {
    "columns": [
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_workspaces"
      },
      {
        "name": "document_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "document_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_documents"
      },
      {
        "name": "codex_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "agent_run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "active_tool_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_projection_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "opened_by",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "closed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_cad_events": {
    "columns": [
      {
        "name": "event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "event_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "tool_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "operation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "preview_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_event_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "payload_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "occurred_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "observed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_scene_nodes": {
    "columns": [
      {
        "name": "node_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "document_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "document_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_documents"
      },
      {
        "name": "projection_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "label",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "parent_node_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "stable_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "visibility",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_span_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "geometry_ref_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_selections": {
    "columns": [
      {
        "name": "selection_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "actor_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "target_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "target_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "node_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "selection_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_tool_sessions": {
    "columns": [
      {
        "name": "tool_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "tool_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "input_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "state_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "started_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "completed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_cad_operations": {
    "columns": [
      {
        "name": "operation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "tool_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "tool_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_cad_tool_sessions"
      },
      {
        "name": "actor_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "operation_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "operation_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": false
      },
      {
        "name": "validation_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "result_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "applied_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_cad_source_edits": {
    "columns": [
      {
        "name": "edit_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "operation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "operation_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_operations"
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "file_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "range_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": false
      },
      {
        "name": "text_preview",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "text_sha256",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_preview_handles": {
    "columns": [
      {
        "name": "preview_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "tool_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "tool_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_cad_tool_sessions"
      },
      {
        "name": "operation_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "operation_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "designer_cad_operations"
      },
      {
        "name": "preview_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "target_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "handle_ref",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "disposed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_cad_preview_updates": {
    "columns": [
      {
        "name": "update_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "preview_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "preview_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_preview_handles"
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "params_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "mesh_ref_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "error_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "requested_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "completed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "designer_cad_widgets": {
    "columns": [
      {
        "name": "widget_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "workspace_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_workspaces"
      },
      {
        "name": "widget_key",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "title",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "source_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "version",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "manifest_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "state_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "designer_cad_steers": {
    "columns": [
      {
        "name": "steer_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cad_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "designer_cad_sessions"
      },
      {
        "name": "actor_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "actor_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "target_agent_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "target_run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "message_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "context_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  }
};

export class AgentQueryBuilder<I extends AgentInclude = {}, S extends AgentSelectableColumn = keyof Agent, R extends boolean = false> implements QueryBuilder<AgentSelectedWithIncludes<I, S, R>> {
  readonly _table = "agents";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: AgentSelectedWithIncludes<I, S, R>;
  readonly _initType!: AgentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<AgentInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: AgentWhereInput): AgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends AgentSelectableColumn>(...columns: [NewS, ...NewS[]]): AgentQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends AgentInclude>(relations: NewI): AgentQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): AgentQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: AgentOrderableColumn, direction: "asc" | "desc" = "asc"): AgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): AgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): AgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "agent_runsViaAgent_row" | "agent_state_snapshotsViaAgent_row"): AgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: AgentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): AgentQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends AgentInclude = I, CloneS extends AgentSelectableColumn = S, CloneR extends boolean = R>(): AgentQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new AgentQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class AgentRunQueryBuilder<I extends AgentRunInclude = {}, S extends AgentRunSelectableColumn = keyof AgentRun, R extends boolean = false> implements QueryBuilder<AgentRunSelectedWithIncludes<I, S, R>> {
  readonly _table = "agent_runs";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: AgentRunSelectedWithIncludes<I, S, R>;
  readonly _initType!: AgentRunInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<AgentRunInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: AgentRunWhereInput): AgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends AgentRunSelectableColumn>(...columns: [NewS, ...NewS[]]): AgentRunQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends AgentRunInclude>(relations: NewI): AgentRunQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): AgentRunQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: AgentRunOrderableColumn, direction: "asc" | "desc" = "asc"): AgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): AgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): AgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "agent_row" | "run_itemsViaRun_row" | "semantic_eventsViaRun_row" | "wire_eventsViaRun_row" | "artifactsViaRun_row" | "workspace_snapshotsViaRun_row" | "memory_linksViaRun_row" | "source_filesViaRun_row"): AgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: AgentRunWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): AgentRunQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends AgentRunInclude = I, CloneS extends AgentRunSelectableColumn = S, CloneR extends boolean = R>(): AgentRunQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new AgentRunQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class RunItemQueryBuilder<I extends RunItemInclude = {}, S extends RunItemSelectableColumn = keyof RunItem, R extends boolean = false> implements QueryBuilder<RunItemSelectedWithIncludes<I, S, R>> {
  readonly _table = "run_items";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: RunItemSelectedWithIncludes<I, S, R>;
  readonly _initType!: RunItemInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<RunItemInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: RunItemWhereInput): RunItemQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends RunItemSelectableColumn>(...columns: [NewS, ...NewS[]]): RunItemQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends RunItemInclude>(relations: NewI): RunItemQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): RunItemQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: RunItemOrderableColumn, direction: "asc" | "desc" = "asc"): RunItemQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): RunItemQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): RunItemQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "semantic_eventsViaItem_row" | "memory_linksViaItem_row"): RunItemQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: RunItemWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): RunItemQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends RunItemInclude = I, CloneS extends RunItemSelectableColumn = S, CloneR extends boolean = R>(): RunItemQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new RunItemQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class SemanticEventQueryBuilder<I extends SemanticEventInclude = {}, S extends SemanticEventSelectableColumn = keyof SemanticEvent, R extends boolean = false> implements QueryBuilder<SemanticEventSelectedWithIncludes<I, S, R>> {
  readonly _table = "semantic_events";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: SemanticEventSelectedWithIncludes<I, S, R>;
  readonly _initType!: SemanticEventInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<SemanticEventInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: SemanticEventWhereInput): SemanticEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends SemanticEventSelectableColumn>(...columns: [NewS, ...NewS[]]): SemanticEventQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends SemanticEventInclude>(relations: NewI): SemanticEventQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): SemanticEventQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: SemanticEventOrderableColumn, direction: "asc" | "desc" = "asc"): SemanticEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): SemanticEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): SemanticEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "item_row"): SemanticEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: SemanticEventWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): SemanticEventQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends SemanticEventInclude = I, CloneS extends SemanticEventSelectableColumn = S, CloneR extends boolean = R>(): SemanticEventQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new SemanticEventQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class WireEventQueryBuilder<I extends WireEventInclude = {}, S extends WireEventSelectableColumn = keyof WireEvent, R extends boolean = false> implements QueryBuilder<WireEventSelectedWithIncludes<I, S, R>> {
  readonly _table = "wire_events";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: WireEventSelectedWithIncludes<I, S, R>;
  readonly _initType!: WireEventInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<WireEventInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: WireEventWhereInput): WireEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends WireEventSelectableColumn>(...columns: [NewS, ...NewS[]]): WireEventQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends WireEventInclude>(relations: NewI): WireEventQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): WireEventQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: WireEventOrderableColumn, direction: "asc" | "desc" = "asc"): WireEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): WireEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): WireEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row"): WireEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: WireEventWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): WireEventQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends WireEventInclude = I, CloneS extends WireEventSelectableColumn = S, CloneR extends boolean = R>(): WireEventQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new WireEventQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class ArtifactQueryBuilder<I extends ArtifactInclude = {}, S extends ArtifactSelectableColumn = keyof Artifact, R extends boolean = false> implements QueryBuilder<ArtifactSelectedWithIncludes<I, S, R>> {
  readonly _table = "artifacts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ArtifactSelectedWithIncludes<I, S, R>;
  readonly _initType!: ArtifactInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ArtifactInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ArtifactWhereInput): ArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends ArtifactSelectableColumn>(...columns: [NewS, ...NewS[]]): ArtifactQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ArtifactInclude>(relations: NewI): ArtifactQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ArtifactQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ArtifactOrderableColumn, direction: "asc" | "desc" = "asc"): ArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row"): ArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ArtifactWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ArtifactQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends ArtifactInclude = I, CloneS extends ArtifactSelectableColumn = S, CloneR extends boolean = R>(): ArtifactQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ArtifactQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class AgentStateSnapshotQueryBuilder<I extends AgentStateSnapshotInclude = {}, S extends AgentStateSnapshotSelectableColumn = keyof AgentStateSnapshot, R extends boolean = false> implements QueryBuilder<AgentStateSnapshotSelectedWithIncludes<I, S, R>> {
  readonly _table = "agent_state_snapshots";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: AgentStateSnapshotSelectedWithIncludes<I, S, R>;
  readonly _initType!: AgentStateSnapshotInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<AgentStateSnapshotInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: AgentStateSnapshotWhereInput): AgentStateSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends AgentStateSnapshotSelectableColumn>(...columns: [NewS, ...NewS[]]): AgentStateSnapshotQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends AgentStateSnapshotInclude>(relations: NewI): AgentStateSnapshotQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): AgentStateSnapshotQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: AgentStateSnapshotOrderableColumn, direction: "asc" | "desc" = "asc"): AgentStateSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): AgentStateSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): AgentStateSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "agent_row"): AgentStateSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: AgentStateSnapshotWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): AgentStateSnapshotQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends AgentStateSnapshotInclude = I, CloneS extends AgentStateSnapshotSelectableColumn = S, CloneR extends boolean = R>(): AgentStateSnapshotQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new AgentStateSnapshotQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class WorkspaceSnapshotQueryBuilder<I extends WorkspaceSnapshotInclude = {}, S extends WorkspaceSnapshotSelectableColumn = keyof WorkspaceSnapshot, R extends boolean = false> implements QueryBuilder<WorkspaceSnapshotSelectedWithIncludes<I, S, R>> {
  readonly _table = "workspace_snapshots";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: WorkspaceSnapshotSelectedWithIncludes<I, S, R>;
  readonly _initType!: WorkspaceSnapshotInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<WorkspaceSnapshotInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: WorkspaceSnapshotWhereInput): WorkspaceSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends WorkspaceSnapshotSelectableColumn>(...columns: [NewS, ...NewS[]]): WorkspaceSnapshotQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends WorkspaceSnapshotInclude>(relations: NewI): WorkspaceSnapshotQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): WorkspaceSnapshotQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: WorkspaceSnapshotOrderableColumn, direction: "asc" | "desc" = "asc"): WorkspaceSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): WorkspaceSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): WorkspaceSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row"): WorkspaceSnapshotQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: WorkspaceSnapshotWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): WorkspaceSnapshotQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends WorkspaceSnapshotInclude = I, CloneS extends WorkspaceSnapshotSelectableColumn = S, CloneR extends boolean = R>(): WorkspaceSnapshotQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new WorkspaceSnapshotQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class MemoryLinkQueryBuilder<I extends MemoryLinkInclude = {}, S extends MemoryLinkSelectableColumn = keyof MemoryLink, R extends boolean = false> implements QueryBuilder<MemoryLinkSelectedWithIncludes<I, S, R>> {
  readonly _table = "memory_links";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: MemoryLinkSelectedWithIncludes<I, S, R>;
  readonly _initType!: MemoryLinkInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<MemoryLinkInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: MemoryLinkWhereInput): MemoryLinkQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends MemoryLinkSelectableColumn>(...columns: [NewS, ...NewS[]]): MemoryLinkQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends MemoryLinkInclude>(relations: NewI): MemoryLinkQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): MemoryLinkQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: MemoryLinkOrderableColumn, direction: "asc" | "desc" = "asc"): MemoryLinkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MemoryLinkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MemoryLinkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "item_row"): MemoryLinkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: MemoryLinkWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MemoryLinkQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends MemoryLinkInclude = I, CloneS extends MemoryLinkSelectableColumn = S, CloneR extends boolean = R>(): MemoryLinkQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new MemoryLinkQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class SourceFileQueryBuilder<I extends SourceFileInclude = {}, S extends SourceFileSelectableColumn = keyof SourceFile, R extends boolean = false> implements QueryBuilder<SourceFileSelectedWithIncludes<I, S, R>> {
  readonly _table = "source_files";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: SourceFileSelectedWithIncludes<I, S, R>;
  readonly _initType!: SourceFileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<SourceFileInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: SourceFileWhereInput): SourceFileQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends SourceFileSelectableColumn>(...columns: [NewS, ...NewS[]]): SourceFileQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends SourceFileInclude>(relations: NewI): SourceFileQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): SourceFileQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: SourceFileOrderableColumn, direction: "asc" | "desc" = "asc"): SourceFileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): SourceFileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): SourceFileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row"): SourceFileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: SourceFileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): SourceFileQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends SourceFileInclude = I, CloneS extends SourceFileSelectableColumn = S, CloneR extends boolean = R>(): SourceFileQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new SourceFileQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DaemonLogSourceQueryBuilder<I extends DaemonLogSourceInclude = {}, S extends DaemonLogSourceSelectableColumn = keyof DaemonLogSource, R extends boolean = false> implements QueryBuilder<DaemonLogSourceSelectedWithIncludes<I, S, R>> {
  readonly _table = "daemon_log_sources";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DaemonLogSourceSelectedWithIncludes<I, S, R>;
  readonly _initType!: DaemonLogSourceInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DaemonLogSourceInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DaemonLogSourceWhereInput): DaemonLogSourceQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DaemonLogSourceSelectableColumn>(...columns: [NewS, ...NewS[]]): DaemonLogSourceQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DaemonLogSourceInclude>(relations: NewI): DaemonLogSourceQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DaemonLogSourceQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DaemonLogSourceOrderableColumn, direction: "asc" | "desc" = "asc"): DaemonLogSourceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DaemonLogSourceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DaemonLogSourceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "daemon_log_chunksViaSource_row" | "daemon_log_eventsViaSource_row" | "daemon_log_checkpointsViaSource_row" | "daemon_log_summariesViaSource_row"): DaemonLogSourceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DaemonLogSourceWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DaemonLogSourceQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DaemonLogSourceInclude = I, CloneS extends DaemonLogSourceSelectableColumn = S, CloneR extends boolean = R>(): DaemonLogSourceQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DaemonLogSourceQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DaemonLogChunkQueryBuilder<I extends DaemonLogChunkInclude = {}, S extends DaemonLogChunkSelectableColumn = keyof DaemonLogChunk, R extends boolean = false> implements QueryBuilder<DaemonLogChunkSelectedWithIncludes<I, S, R>> {
  readonly _table = "daemon_log_chunks";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DaemonLogChunkSelectedWithIncludes<I, S, R>;
  readonly _initType!: DaemonLogChunkInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DaemonLogChunkInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DaemonLogChunkWhereInput): DaemonLogChunkQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DaemonLogChunkSelectableColumn>(...columns: [NewS, ...NewS[]]): DaemonLogChunkQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DaemonLogChunkInclude>(relations: NewI): DaemonLogChunkQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DaemonLogChunkQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DaemonLogChunkOrderableColumn, direction: "asc" | "desc" = "asc"): DaemonLogChunkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DaemonLogChunkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DaemonLogChunkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "source_row" | "daemon_log_eventsViaChunk_row"): DaemonLogChunkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DaemonLogChunkWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DaemonLogChunkQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DaemonLogChunkInclude = I, CloneS extends DaemonLogChunkSelectableColumn = S, CloneR extends boolean = R>(): DaemonLogChunkQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DaemonLogChunkQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DaemonLogEventQueryBuilder<I extends DaemonLogEventInclude = {}, S extends DaemonLogEventSelectableColumn = keyof DaemonLogEvent, R extends boolean = false> implements QueryBuilder<DaemonLogEventSelectedWithIncludes<I, S, R>> {
  readonly _table = "daemon_log_events";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DaemonLogEventSelectedWithIncludes<I, S, R>;
  readonly _initType!: DaemonLogEventInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DaemonLogEventInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DaemonLogEventWhereInput): DaemonLogEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DaemonLogEventSelectableColumn>(...columns: [NewS, ...NewS[]]): DaemonLogEventQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DaemonLogEventInclude>(relations: NewI): DaemonLogEventQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DaemonLogEventQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DaemonLogEventOrderableColumn, direction: "asc" | "desc" = "asc"): DaemonLogEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DaemonLogEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DaemonLogEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "source_row" | "chunk_row"): DaemonLogEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DaemonLogEventWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DaemonLogEventQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DaemonLogEventInclude = I, CloneS extends DaemonLogEventSelectableColumn = S, CloneR extends boolean = R>(): DaemonLogEventQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DaemonLogEventQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DaemonLogCheckpointQueryBuilder<I extends DaemonLogCheckpointInclude = {}, S extends DaemonLogCheckpointSelectableColumn = keyof DaemonLogCheckpoint, R extends boolean = false> implements QueryBuilder<DaemonLogCheckpointSelectedWithIncludes<I, S, R>> {
  readonly _table = "daemon_log_checkpoints";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DaemonLogCheckpointSelectedWithIncludes<I, S, R>;
  readonly _initType!: DaemonLogCheckpointInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DaemonLogCheckpointInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DaemonLogCheckpointWhereInput): DaemonLogCheckpointQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DaemonLogCheckpointSelectableColumn>(...columns: [NewS, ...NewS[]]): DaemonLogCheckpointQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DaemonLogCheckpointInclude>(relations: NewI): DaemonLogCheckpointQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DaemonLogCheckpointQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DaemonLogCheckpointOrderableColumn, direction: "asc" | "desc" = "asc"): DaemonLogCheckpointQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DaemonLogCheckpointQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DaemonLogCheckpointQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "source_row"): DaemonLogCheckpointQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DaemonLogCheckpointWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DaemonLogCheckpointQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DaemonLogCheckpointInclude = I, CloneS extends DaemonLogCheckpointSelectableColumn = S, CloneR extends boolean = R>(): DaemonLogCheckpointQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DaemonLogCheckpointQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DaemonLogSummaryQueryBuilder<I extends DaemonLogSummaryInclude = {}, S extends DaemonLogSummarySelectableColumn = keyof DaemonLogSummary, R extends boolean = false> implements QueryBuilder<DaemonLogSummarySelectedWithIncludes<I, S, R>> {
  readonly _table = "daemon_log_summaries";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DaemonLogSummarySelectedWithIncludes<I, S, R>;
  readonly _initType!: DaemonLogSummaryInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DaemonLogSummaryInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DaemonLogSummaryWhereInput): DaemonLogSummaryQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DaemonLogSummarySelectableColumn>(...columns: [NewS, ...NewS[]]): DaemonLogSummaryQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DaemonLogSummaryInclude>(relations: NewI): DaemonLogSummaryQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DaemonLogSummaryQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DaemonLogSummaryOrderableColumn, direction: "asc" | "desc" = "asc"): DaemonLogSummaryQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DaemonLogSummaryQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DaemonLogSummaryQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "source_row"): DaemonLogSummaryQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DaemonLogSummaryWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DaemonLogSummaryQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DaemonLogSummaryInclude = I, CloneS extends DaemonLogSummarySelectableColumn = S, CloneR extends boolean = R>(): DaemonLogSummaryQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DaemonLogSummaryQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class TaskRecordQueryBuilder<I extends Record<string, never> = {}, S extends TaskRecordSelectableColumn = keyof TaskRecord, R extends boolean = false> implements QueryBuilder<TaskRecordSelected<S>> {
  readonly _table = "task_records";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: TaskRecordSelected<S>;
  readonly _initType!: TaskRecordInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: TaskRecordWhereInput): TaskRecordQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends TaskRecordSelectableColumn>(...columns: [NewS, ...NewS[]]): TaskRecordQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(column: TaskRecordOrderableColumn, direction: "asc" | "desc" = "asc"): TaskRecordQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): TaskRecordQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): TaskRecordQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: TaskRecordWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): TaskRecordQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends Record<string, never> = I, CloneS extends TaskRecordSelectableColumn = S, CloneR extends boolean = R>(): TaskRecordQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new TaskRecordQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerObjectRefQueryBuilder<I extends DesignerObjectRefInclude = {}, S extends DesignerObjectRefSelectableColumn = keyof DesignerObjectRef, R extends boolean = false> implements QueryBuilder<DesignerObjectRefSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_object_refs";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerObjectRefSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerObjectRefInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerObjectRefInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerObjectRefWhereInput): DesignerObjectRefQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerObjectRefSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerObjectRefQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerObjectRefInclude>(relations: NewI): DesignerObjectRefQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerObjectRefQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerObjectRefOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerObjectRefQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerObjectRefQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerObjectRefQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "designer_agent_contextsViaObject_ref_row" | "designer_codex_conversationsViaTranscript_object_row" | "designer_codex_turnsViaPayload_object_row" | "designer_codex_turnsViaPrompt_object_row" | "designer_codex_turnsViaResponse_object_row" | "designer_telemetry_eventsViaPayload_object_row" | "designer_live_commitsViaPatch_object_row" | "designer_live_commitsViaManifest_object_row"): DesignerObjectRefQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerObjectRefWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerObjectRefQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerObjectRefInclude = I, CloneS extends DesignerObjectRefSelectableColumn = S, CloneR extends boolean = R>(): DesignerObjectRefQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerObjectRefQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerAgentQueryBuilder<I extends DesignerAgentInclude = {}, S extends DesignerAgentSelectableColumn = keyof DesignerAgent, R extends boolean = false> implements QueryBuilder<DesignerAgentSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_agents";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerAgentSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerAgentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerAgentInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerAgentWhereInput): DesignerAgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerAgentSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerAgentQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerAgentInclude>(relations: NewI): DesignerAgentQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerAgentQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerAgentOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerAgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerAgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerAgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "designer_agent_toolsViaAgent_row" | "designer_agent_contextsViaAgent_row" | "designer_live_commitsViaAgent_row"): DesignerAgentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerAgentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerAgentQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerAgentInclude = I, CloneS extends DesignerAgentSelectableColumn = S, CloneR extends boolean = R>(): DesignerAgentQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerAgentQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerAgentToolQueryBuilder<I extends DesignerAgentToolInclude = {}, S extends DesignerAgentToolSelectableColumn = keyof DesignerAgentTool, R extends boolean = false> implements QueryBuilder<DesignerAgentToolSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_agent_tools";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerAgentToolSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerAgentToolInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerAgentToolInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerAgentToolWhereInput): DesignerAgentToolQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerAgentToolSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerAgentToolQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerAgentToolInclude>(relations: NewI): DesignerAgentToolQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerAgentToolQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerAgentToolOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerAgentToolQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerAgentToolQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerAgentToolQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "agent_row"): DesignerAgentToolQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerAgentToolWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerAgentToolQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerAgentToolInclude = I, CloneS extends DesignerAgentToolSelectableColumn = S, CloneR extends boolean = R>(): DesignerAgentToolQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerAgentToolQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerAgentContextQueryBuilder<I extends DesignerAgentContextInclude = {}, S extends DesignerAgentContextSelectableColumn = keyof DesignerAgentContext, R extends boolean = false> implements QueryBuilder<DesignerAgentContextSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_agent_contexts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerAgentContextSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerAgentContextInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerAgentContextInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerAgentContextWhereInput): DesignerAgentContextQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerAgentContextSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerAgentContextQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerAgentContextInclude>(relations: NewI): DesignerAgentContextQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerAgentContextQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerAgentContextOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerAgentContextQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerAgentContextQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerAgentContextQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "agent_row" | "object_ref_row"): DesignerAgentContextQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerAgentContextWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerAgentContextQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerAgentContextInclude = I, CloneS extends DesignerAgentContextSelectableColumn = S, CloneR extends boolean = R>(): DesignerAgentContextQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerAgentContextQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCodexConversationQueryBuilder<I extends DesignerCodexConversationInclude = {}, S extends DesignerCodexConversationSelectableColumn = keyof DesignerCodexConversation, R extends boolean = false> implements QueryBuilder<DesignerCodexConversationSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_codex_conversations";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCodexConversationSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCodexConversationInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCodexConversationInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCodexConversationWhereInput): DesignerCodexConversationQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCodexConversationSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCodexConversationQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCodexConversationInclude>(relations: NewI): DesignerCodexConversationQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCodexConversationQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCodexConversationOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCodexConversationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCodexConversationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCodexConversationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "transcript_object_row" | "designer_codex_turnsViaConversation_row" | "designer_telemetry_eventsViaConversation_row" | "designer_live_commitsViaSource_conversation_row"): DesignerCodexConversationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCodexConversationWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCodexConversationQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCodexConversationInclude = I, CloneS extends DesignerCodexConversationSelectableColumn = S, CloneR extends boolean = R>(): DesignerCodexConversationQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCodexConversationQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCodexTurnQueryBuilder<I extends DesignerCodexTurnInclude = {}, S extends DesignerCodexTurnSelectableColumn = keyof DesignerCodexTurn, R extends boolean = false> implements QueryBuilder<DesignerCodexTurnSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_codex_turns";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCodexTurnSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCodexTurnInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCodexTurnInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCodexTurnWhereInput): DesignerCodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCodexTurnSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCodexTurnQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCodexTurnInclude>(relations: NewI): DesignerCodexTurnQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCodexTurnQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCodexTurnOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "conversation_row" | "payload_object_row" | "prompt_object_row" | "response_object_row" | "designer_live_commitsViaSource_turn_row"): DesignerCodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCodexTurnWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCodexTurnQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCodexTurnInclude = I, CloneS extends DesignerCodexTurnSelectableColumn = S, CloneR extends boolean = R>(): DesignerCodexTurnQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCodexTurnQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerTelemetryEventQueryBuilder<I extends DesignerTelemetryEventInclude = {}, S extends DesignerTelemetryEventSelectableColumn = keyof DesignerTelemetryEvent, R extends boolean = false> implements QueryBuilder<DesignerTelemetryEventSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_telemetry_events";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerTelemetryEventSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerTelemetryEventInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerTelemetryEventInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerTelemetryEventWhereInput): DesignerTelemetryEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerTelemetryEventSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerTelemetryEventQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerTelemetryEventInclude>(relations: NewI): DesignerTelemetryEventQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerTelemetryEventQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerTelemetryEventOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerTelemetryEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerTelemetryEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerTelemetryEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "conversation_row" | "payload_object_row"): DesignerTelemetryEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerTelemetryEventWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerTelemetryEventQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerTelemetryEventInclude = I, CloneS extends DesignerTelemetryEventSelectableColumn = S, CloneR extends boolean = R>(): DesignerTelemetryEventQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerTelemetryEventQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerLiveCommitQueryBuilder<I extends DesignerLiveCommitInclude = {}, S extends DesignerLiveCommitSelectableColumn = keyof DesignerLiveCommit, R extends boolean = false> implements QueryBuilder<DesignerLiveCommitSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_live_commits";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerLiveCommitSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerLiveCommitInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerLiveCommitInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerLiveCommitWhereInput): DesignerLiveCommitQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerLiveCommitSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerLiveCommitQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerLiveCommitInclude>(relations: NewI): DesignerLiveCommitQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerLiveCommitQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerLiveCommitOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerLiveCommitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerLiveCommitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerLiveCommitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "source_conversation_row" | "source_turn_row" | "agent_row" | "patch_object_row" | "manifest_object_row"): DesignerLiveCommitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerLiveCommitWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerLiveCommitQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerLiveCommitInclude = I, CloneS extends DesignerLiveCommitSelectableColumn = S, CloneR extends boolean = R>(): DesignerLiveCommitQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerLiveCommitQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadWorkspaceQueryBuilder<I extends DesignerCadWorkspaceInclude = {}, S extends DesignerCadWorkspaceSelectableColumn = keyof DesignerCadWorkspace, R extends boolean = false> implements QueryBuilder<DesignerCadWorkspaceSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_workspaces";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadWorkspaceSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadWorkspaceInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadWorkspaceInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadWorkspaceWhereInput): DesignerCadWorkspaceQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadWorkspaceSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadWorkspaceQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadWorkspaceInclude>(relations: NewI): DesignerCadWorkspaceQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadWorkspaceQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadWorkspaceOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadWorkspaceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadWorkspaceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadWorkspaceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "designer_cad_documentsViaWorkspace_row" | "designer_cad_sessionsViaWorkspace_row" | "designer_cad_widgetsViaWorkspace_row"): DesignerCadWorkspaceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadWorkspaceWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadWorkspaceQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadWorkspaceInclude = I, CloneS extends DesignerCadWorkspaceSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadWorkspaceQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadWorkspaceQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadDocumentQueryBuilder<I extends DesignerCadDocumentInclude = {}, S extends DesignerCadDocumentSelectableColumn = keyof DesignerCadDocument, R extends boolean = false> implements QueryBuilder<DesignerCadDocumentSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_documents";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadDocumentSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadDocumentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadDocumentInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadDocumentWhereInput): DesignerCadDocumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadDocumentSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadDocumentQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadDocumentInclude>(relations: NewI): DesignerCadDocumentQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadDocumentQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadDocumentOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadDocumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadDocumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadDocumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "workspace_row" | "designer_cad_sessionsViaDocument_row" | "designer_cad_scene_nodesViaDocument_row"): DesignerCadDocumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadDocumentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadDocumentQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadDocumentInclude = I, CloneS extends DesignerCadDocumentSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadDocumentQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadDocumentQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadSessionQueryBuilder<I extends DesignerCadSessionInclude = {}, S extends DesignerCadSessionSelectableColumn = keyof DesignerCadSession, R extends boolean = false> implements QueryBuilder<DesignerCadSessionSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_sessions";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadSessionSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadSessionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadSessionInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadSessionWhereInput): DesignerCadSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadSessionSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadSessionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadSessionInclude>(relations: NewI): DesignerCadSessionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadSessionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadSessionOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "workspace_row" | "document_row" | "designer_cad_eventsViaCad_session_row" | "designer_cad_scene_nodesViaCad_session_row" | "designer_cad_selectionsViaCad_session_row" | "designer_cad_tool_sessionsViaCad_session_row" | "designer_cad_operationsViaCad_session_row" | "designer_cad_source_editsViaCad_session_row" | "designer_cad_preview_handlesViaCad_session_row" | "designer_cad_preview_updatesViaCad_session_row" | "designer_cad_steersViaCad_session_row"): DesignerCadSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadSessionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadSessionQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadSessionInclude = I, CloneS extends DesignerCadSessionSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadSessionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadSessionQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadEventQueryBuilder<I extends DesignerCadEventInclude = {}, S extends DesignerCadEventSelectableColumn = keyof DesignerCadEvent, R extends boolean = false> implements QueryBuilder<DesignerCadEventSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_events";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadEventSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadEventInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadEventInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadEventWhereInput): DesignerCadEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadEventSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadEventQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadEventInclude>(relations: NewI): DesignerCadEventQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadEventQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadEventOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row"): DesignerCadEventQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadEventWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadEventQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadEventInclude = I, CloneS extends DesignerCadEventSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadEventQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadEventQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadSceneNodeQueryBuilder<I extends DesignerCadSceneNodeInclude = {}, S extends DesignerCadSceneNodeSelectableColumn = keyof DesignerCadSceneNode, R extends boolean = false> implements QueryBuilder<DesignerCadSceneNodeSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_scene_nodes";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadSceneNodeSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadSceneNodeInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadSceneNodeInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadSceneNodeWhereInput): DesignerCadSceneNodeQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadSceneNodeSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadSceneNodeQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadSceneNodeInclude>(relations: NewI): DesignerCadSceneNodeQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadSceneNodeQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadSceneNodeOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadSceneNodeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadSceneNodeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadSceneNodeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row" | "document_row"): DesignerCadSceneNodeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadSceneNodeWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadSceneNodeQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadSceneNodeInclude = I, CloneS extends DesignerCadSceneNodeSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadSceneNodeQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadSceneNodeQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadSelectionQueryBuilder<I extends DesignerCadSelectionInclude = {}, S extends DesignerCadSelectionSelectableColumn = keyof DesignerCadSelection, R extends boolean = false> implements QueryBuilder<DesignerCadSelectionSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_selections";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadSelectionSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadSelectionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadSelectionInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadSelectionWhereInput): DesignerCadSelectionQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadSelectionSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadSelectionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadSelectionInclude>(relations: NewI): DesignerCadSelectionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadSelectionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadSelectionOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadSelectionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadSelectionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadSelectionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row"): DesignerCadSelectionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadSelectionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadSelectionQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadSelectionInclude = I, CloneS extends DesignerCadSelectionSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadSelectionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadSelectionQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadToolSessionQueryBuilder<I extends DesignerCadToolSessionInclude = {}, S extends DesignerCadToolSessionSelectableColumn = keyof DesignerCadToolSession, R extends boolean = false> implements QueryBuilder<DesignerCadToolSessionSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_tool_sessions";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadToolSessionSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadToolSessionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadToolSessionInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadToolSessionWhereInput): DesignerCadToolSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadToolSessionSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadToolSessionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadToolSessionInclude>(relations: NewI): DesignerCadToolSessionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadToolSessionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadToolSessionOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadToolSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadToolSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadToolSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row" | "designer_cad_operationsViaTool_session_row" | "designer_cad_preview_handlesViaTool_session_row"): DesignerCadToolSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadToolSessionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadToolSessionQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadToolSessionInclude = I, CloneS extends DesignerCadToolSessionSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadToolSessionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadToolSessionQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadOperationQueryBuilder<I extends DesignerCadOperationInclude = {}, S extends DesignerCadOperationSelectableColumn = keyof DesignerCadOperation, R extends boolean = false> implements QueryBuilder<DesignerCadOperationSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_operations";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadOperationSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadOperationInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadOperationInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadOperationWhereInput): DesignerCadOperationQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadOperationSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadOperationQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadOperationInclude>(relations: NewI): DesignerCadOperationQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadOperationQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadOperationOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadOperationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadOperationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadOperationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row" | "tool_session_row" | "designer_cad_source_editsViaOperation_row" | "designer_cad_preview_handlesViaOperation_row"): DesignerCadOperationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadOperationWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadOperationQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadOperationInclude = I, CloneS extends DesignerCadOperationSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadOperationQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadOperationQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadSourceEditQueryBuilder<I extends DesignerCadSourceEditInclude = {}, S extends DesignerCadSourceEditSelectableColumn = keyof DesignerCadSourceEdit, R extends boolean = false> implements QueryBuilder<DesignerCadSourceEditSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_source_edits";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadSourceEditSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadSourceEditInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadSourceEditInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadSourceEditWhereInput): DesignerCadSourceEditQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadSourceEditSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadSourceEditQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadSourceEditInclude>(relations: NewI): DesignerCadSourceEditQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadSourceEditQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadSourceEditOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadSourceEditQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadSourceEditQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadSourceEditQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "operation_row" | "cad_session_row"): DesignerCadSourceEditQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadSourceEditWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadSourceEditQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadSourceEditInclude = I, CloneS extends DesignerCadSourceEditSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadSourceEditQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadSourceEditQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadPreviewHandleQueryBuilder<I extends DesignerCadPreviewHandleInclude = {}, S extends DesignerCadPreviewHandleSelectableColumn = keyof DesignerCadPreviewHandle, R extends boolean = false> implements QueryBuilder<DesignerCadPreviewHandleSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_preview_handles";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadPreviewHandleSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadPreviewHandleInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadPreviewHandleInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadPreviewHandleWhereInput): DesignerCadPreviewHandleQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadPreviewHandleSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadPreviewHandleQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadPreviewHandleInclude>(relations: NewI): DesignerCadPreviewHandleQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadPreviewHandleQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadPreviewHandleOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadPreviewHandleQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadPreviewHandleQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadPreviewHandleQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row" | "tool_session_row" | "operation_row" | "designer_cad_preview_updatesViaPreview_row"): DesignerCadPreviewHandleQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadPreviewHandleWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadPreviewHandleQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadPreviewHandleInclude = I, CloneS extends DesignerCadPreviewHandleSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadPreviewHandleQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadPreviewHandleQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadPreviewUpdateQueryBuilder<I extends DesignerCadPreviewUpdateInclude = {}, S extends DesignerCadPreviewUpdateSelectableColumn = keyof DesignerCadPreviewUpdate, R extends boolean = false> implements QueryBuilder<DesignerCadPreviewUpdateSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_preview_updates";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadPreviewUpdateSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadPreviewUpdateInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadPreviewUpdateInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadPreviewUpdateWhereInput): DesignerCadPreviewUpdateQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadPreviewUpdateSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadPreviewUpdateQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadPreviewUpdateInclude>(relations: NewI): DesignerCadPreviewUpdateQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadPreviewUpdateQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadPreviewUpdateOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadPreviewUpdateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadPreviewUpdateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadPreviewUpdateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "preview_row" | "cad_session_row"): DesignerCadPreviewUpdateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadPreviewUpdateWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadPreviewUpdateQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadPreviewUpdateInclude = I, CloneS extends DesignerCadPreviewUpdateSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadPreviewUpdateQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadPreviewUpdateQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadWidgetQueryBuilder<I extends DesignerCadWidgetInclude = {}, S extends DesignerCadWidgetSelectableColumn = keyof DesignerCadWidget, R extends boolean = false> implements QueryBuilder<DesignerCadWidgetSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_widgets";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadWidgetSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadWidgetInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadWidgetInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadWidgetWhereInput): DesignerCadWidgetQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadWidgetSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadWidgetQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadWidgetInclude>(relations: NewI): DesignerCadWidgetQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadWidgetQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadWidgetOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadWidgetQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadWidgetQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadWidgetQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "workspace_row"): DesignerCadWidgetQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadWidgetWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadWidgetQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadWidgetInclude = I, CloneS extends DesignerCadWidgetSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadWidgetQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadWidgetQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class DesignerCadSteerQueryBuilder<I extends DesignerCadSteerInclude = {}, S extends DesignerCadSteerSelectableColumn = keyof DesignerCadSteer, R extends boolean = false> implements QueryBuilder<DesignerCadSteerSelectedWithIncludes<I, S, R>> {
  readonly _table = "designer_cad_steers";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: DesignerCadSteerSelectedWithIncludes<I, S, R>;
  readonly _initType!: DesignerCadSteerInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<DesignerCadSteerInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: DesignerCadSteerWhereInput): DesignerCadSteerQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends DesignerCadSteerSelectableColumn>(...columns: [NewS, ...NewS[]]): DesignerCadSteerQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends DesignerCadSteerInclude>(relations: NewI): DesignerCadSteerQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): DesignerCadSteerQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: DesignerCadSteerOrderableColumn, direction: "asc" | "desc" = "asc"): DesignerCadSteerQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): DesignerCadSteerQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): DesignerCadSteerQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cad_session_row"): DesignerCadSteerQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: DesignerCadSteerWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): DesignerCadSteerQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends DesignerCadSteerInclude = I, CloneS extends DesignerCadSteerSelectableColumn = S, CloneR extends boolean = R>(): DesignerCadSteerQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new DesignerCadSteerQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  agents: AgentQueryBuilder;
  agent_runs: AgentRunQueryBuilder;
  run_items: RunItemQueryBuilder;
  semantic_events: SemanticEventQueryBuilder;
  wire_events: WireEventQueryBuilder;
  artifacts: ArtifactQueryBuilder;
  agent_state_snapshots: AgentStateSnapshotQueryBuilder;
  workspace_snapshots: WorkspaceSnapshotQueryBuilder;
  memory_links: MemoryLinkQueryBuilder;
  source_files: SourceFileQueryBuilder;
  daemon_log_sources: DaemonLogSourceQueryBuilder;
  daemon_log_chunks: DaemonLogChunkQueryBuilder;
  daemon_log_events: DaemonLogEventQueryBuilder;
  daemon_log_checkpoints: DaemonLogCheckpointQueryBuilder;
  daemon_log_summaries: DaemonLogSummaryQueryBuilder;
  task_records: TaskRecordQueryBuilder;
  designer_object_refs: DesignerObjectRefQueryBuilder;
  designer_agents: DesignerAgentQueryBuilder;
  designer_agent_tools: DesignerAgentToolQueryBuilder;
  designer_agent_contexts: DesignerAgentContextQueryBuilder;
  designer_codex_conversations: DesignerCodexConversationQueryBuilder;
  designer_codex_turns: DesignerCodexTurnQueryBuilder;
  designer_telemetry_events: DesignerTelemetryEventQueryBuilder;
  designer_live_commits: DesignerLiveCommitQueryBuilder;
  designer_cad_workspaces: DesignerCadWorkspaceQueryBuilder;
  designer_cad_documents: DesignerCadDocumentQueryBuilder;
  designer_cad_sessions: DesignerCadSessionQueryBuilder;
  designer_cad_events: DesignerCadEventQueryBuilder;
  designer_cad_scene_nodes: DesignerCadSceneNodeQueryBuilder;
  designer_cad_selections: DesignerCadSelectionQueryBuilder;
  designer_cad_tool_sessions: DesignerCadToolSessionQueryBuilder;
  designer_cad_operations: DesignerCadOperationQueryBuilder;
  designer_cad_source_edits: DesignerCadSourceEditQueryBuilder;
  designer_cad_preview_handles: DesignerCadPreviewHandleQueryBuilder;
  designer_cad_preview_updates: DesignerCadPreviewUpdateQueryBuilder;
  designer_cad_widgets: DesignerCadWidgetQueryBuilder;
  designer_cad_steers: DesignerCadSteerQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  agents: new AgentQueryBuilder(),
  agent_runs: new AgentRunQueryBuilder(),
  run_items: new RunItemQueryBuilder(),
  semantic_events: new SemanticEventQueryBuilder(),
  wire_events: new WireEventQueryBuilder(),
  artifacts: new ArtifactQueryBuilder(),
  agent_state_snapshots: new AgentStateSnapshotQueryBuilder(),
  workspace_snapshots: new WorkspaceSnapshotQueryBuilder(),
  memory_links: new MemoryLinkQueryBuilder(),
  source_files: new SourceFileQueryBuilder(),
  daemon_log_sources: new DaemonLogSourceQueryBuilder(),
  daemon_log_chunks: new DaemonLogChunkQueryBuilder(),
  daemon_log_events: new DaemonLogEventQueryBuilder(),
  daemon_log_checkpoints: new DaemonLogCheckpointQueryBuilder(),
  daemon_log_summaries: new DaemonLogSummaryQueryBuilder(),
  task_records: new TaskRecordQueryBuilder(),
  designer_object_refs: new DesignerObjectRefQueryBuilder(),
  designer_agents: new DesignerAgentQueryBuilder(),
  designer_agent_tools: new DesignerAgentToolQueryBuilder(),
  designer_agent_contexts: new DesignerAgentContextQueryBuilder(),
  designer_codex_conversations: new DesignerCodexConversationQueryBuilder(),
  designer_codex_turns: new DesignerCodexTurnQueryBuilder(),
  designer_telemetry_events: new DesignerTelemetryEventQueryBuilder(),
  designer_live_commits: new DesignerLiveCommitQueryBuilder(),
  designer_cad_workspaces: new DesignerCadWorkspaceQueryBuilder(),
  designer_cad_documents: new DesignerCadDocumentQueryBuilder(),
  designer_cad_sessions: new DesignerCadSessionQueryBuilder(),
  designer_cad_events: new DesignerCadEventQueryBuilder(),
  designer_cad_scene_nodes: new DesignerCadSceneNodeQueryBuilder(),
  designer_cad_selections: new DesignerCadSelectionQueryBuilder(),
  designer_cad_tool_sessions: new DesignerCadToolSessionQueryBuilder(),
  designer_cad_operations: new DesignerCadOperationQueryBuilder(),
  designer_cad_source_edits: new DesignerCadSourceEditQueryBuilder(),
  designer_cad_preview_handles: new DesignerCadPreviewHandleQueryBuilder(),
  designer_cad_preview_updates: new DesignerCadPreviewUpdateQueryBuilder(),
  designer_cad_widgets: new DesignerCadWidgetQueryBuilder(),
  designer_cad_steers: new DesignerCadSteerQueryBuilder(),
  wasmSchema,
};

