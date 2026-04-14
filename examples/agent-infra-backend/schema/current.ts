import { table, col } from "jazz-tools";

table("agents", {
  agent_id: col.string(),
  lane: col.string().optional(),
  spec_path: col.string().optional(),
  prompt_surface: col.string().optional(),
  status: col.string().optional(),
  metadata_json: col.json().optional(),
  created_at: col.timestamp(),
  updated_at: col.timestamp(),
});

table("agent_runs", {
  run_id: col.string(),
  agent_id: col.string(),
  agent_row_id: col.ref("agents"),
  thread_id: col.string().optional(),
  turn_id: col.string().optional(),
  cwd: col.string().optional(),
  repo_root: col.string().optional(),
  request_summary: col.string().optional(),
  status: col.string(),
  started_at: col.timestamp(),
  ended_at: col.timestamp().optional(),
  context_json: col.json().optional(),
  source_trace_path: col.string().optional(),
});

table("run_items", {
  item_id: col.string(),
  run_id: col.string(),
  run_row_id: col.ref("agent_runs"),
  item_kind: col.string(),
  phase: col.string().optional(),
  sequence: col.int(),
  status: col.string(),
  summary_json: col.json().optional(),
  started_at: col.timestamp(),
  completed_at: col.timestamp().optional(),
});

table("semantic_events", {
  event_id: col.string(),
  run_id: col.string(),
  run_row_id: col.ref("agent_runs"),
  item_id: col.string().optional(),
  item_row_id: col.ref("run_items").optional(),
  event_type: col.string(),
  summary_text: col.string().optional(),
  payload_json: col.json().optional(),
  occurred_at: col.timestamp(),
});

table("wire_events", {
  event_id: col.string(),
  run_id: col.string().optional(),
  run_row_id: col.ref("agent_runs").optional(),
  connection_id: col.int().optional(),
  session_id: col.int().optional(),
  direction: col.string(),
  method: col.string().optional(),
  request_id: col.string().optional(),
  payload_json: col.json().optional(),
  occurred_at: col.timestamp(),
});

table("artifacts", {
  artifact_id: col.string(),
  run_id: col.string(),
  run_row_id: col.ref("agent_runs"),
  artifact_kind: col.string(),
  title: col.string().optional(),
  absolute_path: col.string(),
  checksum: col.string().optional(),
  created_at: col.timestamp(),
});

table("agent_state_snapshots", {
  snapshot_id: col.string(),
  agent_id: col.string(),
  agent_row_id: col.ref("agents"),
  state_version: col.int().optional(),
  status: col.string().optional(),
  state_json: col.json(),
  captured_at: col.timestamp(),
});

table("workspace_snapshots", {
  snapshot_id: col.string(),
  run_id: col.string(),
  run_row_id: col.ref("agent_runs"),
  repo_root: col.string(),
  branch: col.string().optional(),
  head_commit: col.string().optional(),
  dirty_path_count: col.int().optional(),
  snapshot_json: col.json().optional(),
  captured_at: col.timestamp(),
});

table("memory_links", {
  link_id: col.string(),
  run_id: col.string().optional(),
  run_row_id: col.ref("agent_runs").optional(),
  item_id: col.string().optional(),
  item_row_id: col.ref("run_items").optional(),
  memory_scope: col.string(),
  memory_ref: col.string().optional(),
  query_text: col.string().optional(),
  link_json: col.json().optional(),
  created_at: col.timestamp(),
});

table("source_files", {
  source_file_id: col.string(),
  run_id: col.string().optional(),
  run_row_id: col.ref("agent_runs").optional(),
  file_kind: col.string(),
  absolute_path: col.string(),
  checksum: col.string().optional(),
  created_at: col.timestamp(),
});

table("task_records", {
  task_id: col.string(),
  context: col.string(),
  title: col.string(),
  status: col.string(),
  priority: col.string(),
  placement: col.string(),
  focus_rank: col.int().optional(),
  project: col.string(),
  issue: col.string().optional(),
  branch: col.string().optional(),
  workspace: col.string().optional(),
  plan: col.string().optional(),
  pr: col.string().optional(),
  tags_json: col.json().optional(),
  next_text: col.string().optional(),
  context_text: col.string().optional(),
  notes_text: col.string().optional(),
  annotations_json: col.json().optional(),
  source_kind: col.string().optional(),
  source_path: col.string().optional(),
  metadata_json: col.json().optional(),
  created_at: col.timestamp(),
  updated_at: col.timestamp(),
});
