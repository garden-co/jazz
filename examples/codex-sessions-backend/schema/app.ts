// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface CodexSession {
  id: string;
  session_id: string;
  rollout_path: string;
  cwd: string;
  project_root: string;
  repo_root?: string;
  git_branch?: string;
  originator?: string;
  source?: string;
  cli_version?: string;
  model_provider?: string;
  model_name?: string;
  reasoning_effort?: string;
  agent_nickname?: string;
  agent_role?: string;
  agent_path?: string;
  first_user_message?: string;
  latest_user_message?: string;
  latest_assistant_message?: string;
  latest_assistant_partial?: string;
  latest_preview?: string;
  status: string;
  created_at: Date;
  updated_at: Date;
  latest_activity_at: Date;
  last_user_at?: Date;
  last_assistant_at?: Date;
  last_completion_at?: Date;
  metadata_json?: JsonValue;
}

export interface CodexTurn {
  id: string;
  turn_id: string;
  session_id: string;
  session_row_id: string;
  sequence: number;
  status: string;
  user_message?: string;
  assistant_message?: string;
  assistant_partial?: string;
  plan_text?: string;
  reasoning_summary?: string;
  started_at?: Date;
  completed_at?: Date;
  duration_ms?: number;
  updated_at: Date;
}

export interface CodexSessionPresence {
  id: string;
  session_id: string;
  session_row_id: string;
  project_root: string;
  repo_root?: string;
  cwd: string;
  state: string;
  current_turn_id?: string;
  current_turn_row_id?: string;
  current_turn_status?: string;
  started_at: Date;
  latest_activity_at: Date;
  last_event_at: Date;
  last_user_at?: Date;
  last_assistant_at?: Date;
  last_completion_at?: Date;
  last_synced_at: Date;
  runtime_pid?: number;
  runtime_tty?: string;
  runtime_host?: string;
  last_heartbeat_at?: Date;
  updated_at: Date;
}

export interface CodexSyncState {
  id: string;
  source_id: string;
  absolute_path: string;
  session_id?: string;
  session_row_id?: string;
  line_count: number;
  synced_at: Date;
}

export interface JAgentDefinition {
  id: string;
  definition_id: string;
  name: string;
  version: string;
  source_kind: string;
  entrypoint: string;
  metadata_json?: JsonValue;
  created_at: Date;
  updated_at: Date;
}

export interface JAgentRun {
  id: string;
  run_id: string;
  definition_id: string;
  definition_row_id: string;
  status: string;
  project_root: string;
  repo_root?: string;
  cwd?: string;
  trigger_source?: string;
  parent_session_id?: string;
  parent_session_row_id?: string;
  parent_turn_id?: string;
  initiator_session_id?: string;
  initiator_session_row_id?: string;
  requested_role?: string;
  requested_model?: string;
  requested_reasoning_effort?: string;
  fork_turns?: number;
  current_step_key?: string;
  input_json?: JsonValue;
  output_json?: JsonValue;
  error_text?: string;
  started_at: Date;
  updated_at: Date;
  completed_at?: Date;
}

export interface JAgentStep {
  id: string;
  step_id: string;
  run_id: string;
  run_row_id: string;
  sequence: number;
  step_key: string;
  step_kind: string;
  status: string;
  input_json?: JsonValue;
  output_json?: JsonValue;
  error_text?: string;
  started_at: Date;
  updated_at: Date;
  completed_at?: Date;
}

export interface JAgentAttempt {
  id: string;
  attempt_id: string;
  run_id: string;
  run_row_id: string;
  step_id: string;
  step_row_id: string;
  attempt: number;
  status: string;
  codex_session_id?: string;
  codex_session_row_id?: string;
  codex_turn_id?: string;
  codex_turn_row_id?: string;
  fork_turns?: number;
  model_name?: string;
  reasoning_effort?: string;
  started_at: Date;
  completed_at?: Date;
  error_text?: string;
}

export interface JAgentWait {
  id: string;
  wait_id: string;
  run_id: string;
  run_row_id: string;
  step_id: string;
  step_row_id: string;
  wait_kind: string;
  target_session_id?: string;
  target_session_row_id?: string;
  target_turn_id?: string;
  target_turn_row_id?: string;
  resume_condition_json?: JsonValue;
  status: string;
  started_at: Date;
  resumed_at?: Date;
}

export interface JAgentSessionBinding {
  id: string;
  binding_id: string;
  run_id: string;
  run_row_id: string;
  codex_session_id: string;
  codex_session_row_id: string;
  binding_role: string;
  parent_session_id?: string;
  parent_session_row_id?: string;
  created_at: Date;
}

export interface JAgentArtifact {
  id: string;
  artifact_id: string;
  run_id: string;
  run_row_id: string;
  step_id?: string;
  step_row_id?: string;
  kind: string;
  path: string;
  text_preview?: string;
  metadata_json?: JsonValue;
  created_at: Date;
}

export interface CodexSessionInit {
  session_id: string;
  rollout_path: string;
  cwd: string;
  project_root: string;
  repo_root?: string | null;
  git_branch?: string | null;
  originator?: string | null;
  source?: string | null;
  cli_version?: string | null;
  model_provider?: string | null;
  model_name?: string | null;
  reasoning_effort?: string | null;
  agent_nickname?: string | null;
  agent_role?: string | null;
  agent_path?: string | null;
  first_user_message?: string | null;
  latest_user_message?: string | null;
  latest_assistant_message?: string | null;
  latest_assistant_partial?: string | null;
  latest_preview?: string | null;
  status: string;
  created_at: Date;
  updated_at: Date;
  latest_activity_at: Date;
  last_user_at?: Date | null;
  last_assistant_at?: Date | null;
  last_completion_at?: Date | null;
  metadata_json?: JsonValue | null;
}

export interface CodexTurnInit {
  turn_id: string;
  session_id: string;
  session_row_id: string;
  sequence: number;
  status: string;
  user_message?: string | null;
  assistant_message?: string | null;
  assistant_partial?: string | null;
  plan_text?: string | null;
  reasoning_summary?: string | null;
  started_at?: Date | null;
  completed_at?: Date | null;
  duration_ms?: number | null;
  updated_at: Date;
}

export interface CodexSessionPresenceInit {
  session_id: string;
  session_row_id: string;
  project_root: string;
  repo_root?: string | null;
  cwd: string;
  state: string;
  current_turn_id?: string | null;
  current_turn_row_id?: string | null;
  current_turn_status?: string | null;
  started_at: Date;
  latest_activity_at: Date;
  last_event_at: Date;
  last_user_at?: Date | null;
  last_assistant_at?: Date | null;
  last_completion_at?: Date | null;
  last_synced_at: Date;
  runtime_pid?: number | null;
  runtime_tty?: string | null;
  runtime_host?: string | null;
  last_heartbeat_at?: Date | null;
  updated_at: Date;
}

export interface CodexSyncStateInit {
  source_id: string;
  absolute_path: string;
  session_id?: string | null;
  session_row_id?: string | null;
  line_count: number;
  synced_at: Date;
}

export interface JAgentDefinitionInit {
  definition_id: string;
  name: string;
  version: string;
  source_kind: string;
  entrypoint: string;
  metadata_json?: JsonValue | null;
  created_at: Date;
  updated_at: Date;
}

export interface JAgentRunInit {
  run_id: string;
  definition_id: string;
  definition_row_id: string;
  status: string;
  project_root: string;
  repo_root?: string | null;
  cwd?: string | null;
  trigger_source?: string | null;
  parent_session_id?: string | null;
  parent_session_row_id?: string | null;
  parent_turn_id?: string | null;
  initiator_session_id?: string | null;
  initiator_session_row_id?: string | null;
  requested_role?: string | null;
  requested_model?: string | null;
  requested_reasoning_effort?: string | null;
  fork_turns?: number | null;
  current_step_key?: string | null;
  input_json?: JsonValue | null;
  output_json?: JsonValue | null;
  error_text?: string | null;
  started_at: Date;
  updated_at: Date;
  completed_at?: Date | null;
}

export interface JAgentStepInit {
  step_id: string;
  run_id: string;
  run_row_id: string;
  sequence: number;
  step_key: string;
  step_kind: string;
  status: string;
  input_json?: JsonValue | null;
  output_json?: JsonValue | null;
  error_text?: string | null;
  started_at: Date;
  updated_at: Date;
  completed_at?: Date | null;
}

export interface JAgentAttemptInit {
  attempt_id: string;
  run_id: string;
  run_row_id: string;
  step_id: string;
  step_row_id: string;
  attempt: number;
  status: string;
  codex_session_id?: string | null;
  codex_session_row_id?: string | null;
  codex_turn_id?: string | null;
  codex_turn_row_id?: string | null;
  fork_turns?: number | null;
  model_name?: string | null;
  reasoning_effort?: string | null;
  started_at: Date;
  completed_at?: Date | null;
  error_text?: string | null;
}

export interface JAgentWaitInit {
  wait_id: string;
  run_id: string;
  run_row_id: string;
  step_id: string;
  step_row_id: string;
  wait_kind: string;
  target_session_id?: string | null;
  target_session_row_id?: string | null;
  target_turn_id?: string | null;
  target_turn_row_id?: string | null;
  resume_condition_json?: JsonValue | null;
  status: string;
  started_at: Date;
  resumed_at?: Date | null;
}

export interface JAgentSessionBindingInit {
  binding_id: string;
  run_id: string;
  run_row_id: string;
  codex_session_id: string;
  codex_session_row_id: string;
  binding_role: string;
  parent_session_id?: string | null;
  parent_session_row_id?: string | null;
  created_at: Date;
}

export interface JAgentArtifactInit {
  artifact_id: string;
  run_id: string;
  run_row_id: string;
  step_id?: string | null;
  step_row_id?: string | null;
  kind: string;
  path: string;
  text_preview?: string | null;
  metadata_json?: JsonValue | null;
  created_at: Date;
}

export interface CodexSessionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  rollout_path?: string | { eq?: string; ne?: string; contains?: string };
  cwd?: string | { eq?: string; ne?: string; contains?: string };
  project_root?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  git_branch?: string | { eq?: string; ne?: string; contains?: string };
  originator?: string | { eq?: string; ne?: string; contains?: string };
  source?: string | { eq?: string; ne?: string; contains?: string };
  cli_version?: string | { eq?: string; ne?: string; contains?: string };
  model_provider?: string | { eq?: string; ne?: string; contains?: string };
  model_name?: string | { eq?: string; ne?: string; contains?: string };
  reasoning_effort?: string | { eq?: string; ne?: string; contains?: string };
  agent_nickname?: string | { eq?: string; ne?: string; contains?: string };
  agent_role?: string | { eq?: string; ne?: string; contains?: string };
  agent_path?: string | { eq?: string; ne?: string; contains?: string };
  first_user_message?: string | { eq?: string; ne?: string; contains?: string };
  latest_user_message?: string | { eq?: string; ne?: string; contains?: string };
  latest_assistant_message?: string | { eq?: string; ne?: string; contains?: string };
  latest_assistant_partial?: string | { eq?: string; ne?: string; contains?: string };
  latest_preview?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  latest_activity_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_user_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_assistant_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_completion_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface CodexTurnWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  turn_id?: string | { eq?: string; ne?: string; contains?: string };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  session_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  status?: string | { eq?: string; ne?: string; contains?: string };
  user_message?: string | { eq?: string; ne?: string; contains?: string };
  assistant_message?: string | { eq?: string; ne?: string; contains?: string };
  assistant_partial?: string | { eq?: string; ne?: string; contains?: string };
  plan_text?: string | { eq?: string; ne?: string; contains?: string };
  reasoning_summary?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  duration_ms?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface CodexSessionPresenceWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  session_row_id?: string | { eq?: string; ne?: string };
  project_root?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  cwd?: string | { eq?: string; ne?: string; contains?: string };
  state?: string | { eq?: string; ne?: string; contains?: string };
  current_turn_id?: string | { eq?: string; ne?: string; contains?: string };
  current_turn_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  current_turn_status?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  latest_activity_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_event_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_user_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_assistant_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_completion_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_synced_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  runtime_pid?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  runtime_tty?: string | { eq?: string; ne?: string; contains?: string };
  runtime_host?: string | { eq?: string; ne?: string; contains?: string };
  last_heartbeat_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface CodexSyncStateWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  source_id?: string | { eq?: string; ne?: string; contains?: string };
  absolute_path?: string | { eq?: string; ne?: string; contains?: string };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  line_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  synced_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentDefinitionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  definition_id?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  version?: string | { eq?: string; ne?: string; contains?: string };
  source_kind?: string | { eq?: string; ne?: string; contains?: string };
  entrypoint?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentRunWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  definition_id?: string | { eq?: string; ne?: string; contains?: string };
  definition_row_id?: string | { eq?: string; ne?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  project_root?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  cwd?: string | { eq?: string; ne?: string; contains?: string };
  trigger_source?: string | { eq?: string; ne?: string; contains?: string };
  parent_session_id?: string | { eq?: string; ne?: string; contains?: string };
  parent_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  parent_turn_id?: string | { eq?: string; ne?: string; contains?: string };
  initiator_session_id?: string | { eq?: string; ne?: string; contains?: string };
  initiator_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  requested_role?: string | { eq?: string; ne?: string; contains?: string };
  requested_model?: string | { eq?: string; ne?: string; contains?: string };
  requested_reasoning_effort?: string | { eq?: string; ne?: string; contains?: string };
  fork_turns?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  current_step_key?: string | { eq?: string; ne?: string; contains?: string };
  input_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  output_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  error_text?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentStepWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  step_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  step_key?: string | { eq?: string; ne?: string; contains?: string };
  step_kind?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  input_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  output_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  error_text?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentAttemptWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  attempt_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  step_id?: string | { eq?: string; ne?: string; contains?: string };
  step_row_id?: string | { eq?: string; ne?: string };
  attempt?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  status?: string | { eq?: string; ne?: string; contains?: string };
  codex_session_id?: string | { eq?: string; ne?: string; contains?: string };
  codex_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  codex_turn_id?: string | { eq?: string; ne?: string; contains?: string };
  codex_turn_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  fork_turns?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  model_name?: string | { eq?: string; ne?: string; contains?: string };
  reasoning_effort?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  completed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  error_text?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentWaitWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  wait_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  step_id?: string | { eq?: string; ne?: string; contains?: string };
  step_row_id?: string | { eq?: string; ne?: string };
  wait_kind?: string | { eq?: string; ne?: string; contains?: string };
  target_session_id?: string | { eq?: string; ne?: string; contains?: string };
  target_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  target_turn_id?: string | { eq?: string; ne?: string; contains?: string };
  target_turn_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  resume_condition_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  status?: string | { eq?: string; ne?: string; contains?: string };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  resumed_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentSessionBindingWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  binding_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  codex_session_id?: string | { eq?: string; ne?: string; contains?: string };
  codex_session_row_id?: string | { eq?: string; ne?: string };
  binding_role?: string | { eq?: string; ne?: string; contains?: string };
  parent_session_id?: string | { eq?: string; ne?: string; contains?: string };
  parent_session_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JAgentArtifactWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  artifact_id?: string | { eq?: string; ne?: string; contains?: string };
  run_id?: string | { eq?: string; ne?: string; contains?: string };
  run_row_id?: string | { eq?: string; ne?: string };
  step_id?: string | { eq?: string; ne?: string; contains?: string };
  step_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  kind?: string | { eq?: string; ne?: string; contains?: string };
  path?: string | { eq?: string; ne?: string; contains?: string };
  text_preview?: string | { eq?: string; ne?: string; contains?: string };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyCodexSessionQueryBuilder<T = any> = { readonly _table: "codex_sessions" } & QueryBuilder<T>;
type AnyCodexTurnQueryBuilder<T = any> = { readonly _table: "codex_turns" } & QueryBuilder<T>;
type AnyCodexSessionPresenceQueryBuilder<T = any> = { readonly _table: "codex_session_presence" } & QueryBuilder<T>;
type AnyCodexSyncStateQueryBuilder<T = any> = { readonly _table: "codex_sync_states" } & QueryBuilder<T>;
type AnyJAgentDefinitionQueryBuilder<T = any> = { readonly _table: "j_agent_definitions" } & QueryBuilder<T>;
type AnyJAgentRunQueryBuilder<T = any> = { readonly _table: "j_agent_runs" } & QueryBuilder<T>;
type AnyJAgentStepQueryBuilder<T = any> = { readonly _table: "j_agent_steps" } & QueryBuilder<T>;
type AnyJAgentAttemptQueryBuilder<T = any> = { readonly _table: "j_agent_attempts" } & QueryBuilder<T>;
type AnyJAgentWaitQueryBuilder<T = any> = { readonly _table: "j_agent_waits" } & QueryBuilder<T>;
type AnyJAgentSessionBindingQueryBuilder<T = any> = { readonly _table: "j_agent_session_bindings" } & QueryBuilder<T>;
type AnyJAgentArtifactQueryBuilder<T = any> = { readonly _table: "j_agent_artifacts" } & QueryBuilder<T>;

export interface CodexSessionInclude {
  codex_turnsViaSession_row?: true | CodexTurnInclude | AnyCodexTurnQueryBuilder<any>;
  codex_session_presenceViaSession_row?: true | CodexSessionPresenceInclude | AnyCodexSessionPresenceQueryBuilder<any>;
  codex_sync_statesViaSession_row?: true | CodexSyncStateInclude | AnyCodexSyncStateQueryBuilder<any>;
  j_agent_runsViaParent_session_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  j_agent_runsViaInitiator_session_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  j_agent_attemptsViaCodex_session_row?: true | JAgentAttemptInclude | AnyJAgentAttemptQueryBuilder<any>;
  j_agent_waitsViaTarget_session_row?: true | JAgentWaitInclude | AnyJAgentWaitQueryBuilder<any>;
  j_agent_session_bindingsViaCodex_session_row?: true | JAgentSessionBindingInclude | AnyJAgentSessionBindingQueryBuilder<any>;
  j_agent_session_bindingsViaParent_session_row?: true | JAgentSessionBindingInclude | AnyJAgentSessionBindingQueryBuilder<any>;
}

export interface CodexTurnInclude {
  session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  codex_session_presenceViaCurrent_turn_row?: true | CodexSessionPresenceInclude | AnyCodexSessionPresenceQueryBuilder<any>;
  j_agent_attemptsViaCodex_turn_row?: true | JAgentAttemptInclude | AnyJAgentAttemptQueryBuilder<any>;
  j_agent_waitsViaTarget_turn_row?: true | JAgentWaitInclude | AnyJAgentWaitQueryBuilder<any>;
}

export interface CodexSessionPresenceInclude {
  session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  current_turn_row?: true | CodexTurnInclude | AnyCodexTurnQueryBuilder<any>;
}

export interface CodexSyncStateInclude {
  session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
}

export interface JAgentDefinitionInclude {
  j_agent_runsViaDefinition_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
}

export interface JAgentRunInclude {
  definition_row?: true | JAgentDefinitionInclude | AnyJAgentDefinitionQueryBuilder<any>;
  parent_session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  initiator_session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  j_agent_stepsViaRun_row?: true | JAgentStepInclude | AnyJAgentStepQueryBuilder<any>;
  j_agent_attemptsViaRun_row?: true | JAgentAttemptInclude | AnyJAgentAttemptQueryBuilder<any>;
  j_agent_waitsViaRun_row?: true | JAgentWaitInclude | AnyJAgentWaitQueryBuilder<any>;
  j_agent_session_bindingsViaRun_row?: true | JAgentSessionBindingInclude | AnyJAgentSessionBindingQueryBuilder<any>;
  j_agent_artifactsViaRun_row?: true | JAgentArtifactInclude | AnyJAgentArtifactQueryBuilder<any>;
}

export interface JAgentStepInclude {
  run_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  j_agent_attemptsViaStep_row?: true | JAgentAttemptInclude | AnyJAgentAttemptQueryBuilder<any>;
  j_agent_waitsViaStep_row?: true | JAgentWaitInclude | AnyJAgentWaitQueryBuilder<any>;
  j_agent_artifactsViaStep_row?: true | JAgentArtifactInclude | AnyJAgentArtifactQueryBuilder<any>;
}

export interface JAgentAttemptInclude {
  run_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  step_row?: true | JAgentStepInclude | AnyJAgentStepQueryBuilder<any>;
  codex_session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  codex_turn_row?: true | CodexTurnInclude | AnyCodexTurnQueryBuilder<any>;
}

export interface JAgentWaitInclude {
  run_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  step_row?: true | JAgentStepInclude | AnyJAgentStepQueryBuilder<any>;
  target_session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  target_turn_row?: true | CodexTurnInclude | AnyCodexTurnQueryBuilder<any>;
}

export interface JAgentSessionBindingInclude {
  run_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  codex_session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
  parent_session_row?: true | CodexSessionInclude | AnyCodexSessionQueryBuilder<any>;
}

export interface JAgentArtifactInclude {
  run_row?: true | JAgentRunInclude | AnyJAgentRunQueryBuilder<any>;
  step_row?: true | JAgentStepInclude | AnyJAgentStepQueryBuilder<any>;
}

export type CodexSessionIncludedRelations<I extends CodexSessionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "codex_turnsViaSession_row"
      ? NonNullable<I["codex_turnsViaSession_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexTurn[]
          : RelationInclude extends AnyCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends CodexTurnInclude
              ? CodexTurnWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "codex_session_presenceViaSession_row"
      ? NonNullable<I["codex_session_presenceViaSession_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSessionPresence[]
          : RelationInclude extends AnyCodexSessionPresenceQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends CodexSessionPresenceInclude
              ? CodexSessionPresenceWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "codex_sync_statesViaSession_row"
      ? NonNullable<I["codex_sync_statesViaSession_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSyncState[]
          : RelationInclude extends AnyCodexSyncStateQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends CodexSyncStateInclude
              ? CodexSyncStateWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_runsViaParent_session_row"
      ? NonNullable<I["j_agent_runsViaParent_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentRun[]
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentRunInclude
              ? JAgentRunWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_runsViaInitiator_session_row"
      ? NonNullable<I["j_agent_runsViaInitiator_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentRun[]
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentRunInclude
              ? JAgentRunWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_attemptsViaCodex_session_row"
      ? NonNullable<I["j_agent_attemptsViaCodex_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentAttempt[]
          : RelationInclude extends AnyJAgentAttemptQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentAttemptInclude
              ? JAgentAttemptWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_waitsViaTarget_session_row"
      ? NonNullable<I["j_agent_waitsViaTarget_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentWait[]
          : RelationInclude extends AnyJAgentWaitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentWaitInclude
              ? JAgentWaitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_session_bindingsViaCodex_session_row"
      ? NonNullable<I["j_agent_session_bindingsViaCodex_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentSessionBinding[]
          : RelationInclude extends AnyJAgentSessionBindingQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentSessionBindingInclude
              ? JAgentSessionBindingWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_session_bindingsViaParent_session_row"
      ? NonNullable<I["j_agent_session_bindingsViaParent_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentSessionBinding[]
          : RelationInclude extends AnyJAgentSessionBindingQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentSessionBindingInclude
              ? JAgentSessionBindingWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type CodexTurnIncludedRelations<I extends CodexTurnInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "session_row"
      ? NonNullable<I["session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? CodexSession : CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? R extends true ? CodexSessionWithIncludes<RelationInclude, false> : CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "codex_session_presenceViaCurrent_turn_row"
      ? NonNullable<I["codex_session_presenceViaCurrent_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSessionPresence[]
          : RelationInclude extends AnyCodexSessionPresenceQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends CodexSessionPresenceInclude
              ? CodexSessionPresenceWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_attemptsViaCodex_turn_row"
      ? NonNullable<I["j_agent_attemptsViaCodex_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentAttempt[]
          : RelationInclude extends AnyJAgentAttemptQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentAttemptInclude
              ? JAgentAttemptWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_waitsViaTarget_turn_row"
      ? NonNullable<I["j_agent_waitsViaTarget_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentWait[]
          : RelationInclude extends AnyJAgentWaitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentWaitInclude
              ? JAgentWaitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type CodexSessionPresenceIncludedRelations<I extends CodexSessionPresenceInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "session_row"
      ? NonNullable<I["session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? CodexSession : CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? R extends true ? CodexSessionWithIncludes<RelationInclude, false> : CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "current_turn_row"
      ? NonNullable<I["current_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexTurn | undefined
          : RelationInclude extends AnyCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexTurnInclude
              ? CodexTurnWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type CodexSyncStateIncludedRelations<I extends CodexSyncStateInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "session_row"
      ? NonNullable<I["session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type JAgentDefinitionIncludedRelations<I extends JAgentDefinitionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "j_agent_runsViaDefinition_row"
      ? NonNullable<I["j_agent_runsViaDefinition_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentRun[]
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentRunInclude
              ? JAgentRunWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type JAgentRunIncludedRelations<I extends JAgentRunInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "definition_row"
      ? NonNullable<I["definition_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentDefinition : JAgentDefinition | undefined
          : RelationInclude extends AnyJAgentDefinitionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentDefinitionInclude
              ? R extends true ? JAgentDefinitionWithIncludes<RelationInclude, false> : JAgentDefinitionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "parent_session_row"
      ? NonNullable<I["parent_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "initiator_session_row"
      ? NonNullable<I["initiator_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "j_agent_stepsViaRun_row"
      ? NonNullable<I["j_agent_stepsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentStep[]
          : RelationInclude extends AnyJAgentStepQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentStepInclude
              ? JAgentStepWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_attemptsViaRun_row"
      ? NonNullable<I["j_agent_attemptsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentAttempt[]
          : RelationInclude extends AnyJAgentAttemptQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentAttemptInclude
              ? JAgentAttemptWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_waitsViaRun_row"
      ? NonNullable<I["j_agent_waitsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentWait[]
          : RelationInclude extends AnyJAgentWaitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentWaitInclude
              ? JAgentWaitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_session_bindingsViaRun_row"
      ? NonNullable<I["j_agent_session_bindingsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentSessionBinding[]
          : RelationInclude extends AnyJAgentSessionBindingQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentSessionBindingInclude
              ? JAgentSessionBindingWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_artifactsViaRun_row"
      ? NonNullable<I["j_agent_artifactsViaRun_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentArtifact[]
          : RelationInclude extends AnyJAgentArtifactQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentArtifactInclude
              ? JAgentArtifactWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type JAgentStepIncludedRelations<I extends JAgentStepInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentRun : JAgentRun | undefined
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentRunInclude
              ? R extends true ? JAgentRunWithIncludes<RelationInclude, false> : JAgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "j_agent_attemptsViaStep_row"
      ? NonNullable<I["j_agent_attemptsViaStep_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentAttempt[]
          : RelationInclude extends AnyJAgentAttemptQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentAttemptInclude
              ? JAgentAttemptWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_waitsViaStep_row"
      ? NonNullable<I["j_agent_waitsViaStep_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentWait[]
          : RelationInclude extends AnyJAgentWaitQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentWaitInclude
              ? JAgentWaitWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "j_agent_artifactsViaStep_row"
      ? NonNullable<I["j_agent_artifactsViaStep_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentArtifact[]
          : RelationInclude extends AnyJAgentArtifactQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends JAgentArtifactInclude
              ? JAgentArtifactWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type JAgentAttemptIncludedRelations<I extends JAgentAttemptInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentRun : JAgentRun | undefined
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentRunInclude
              ? R extends true ? JAgentRunWithIncludes<RelationInclude, false> : JAgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "step_row"
      ? NonNullable<I["step_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentStep : JAgentStep | undefined
          : RelationInclude extends AnyJAgentStepQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentStepInclude
              ? R extends true ? JAgentStepWithIncludes<RelationInclude, false> : JAgentStepWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "codex_session_row"
      ? NonNullable<I["codex_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "codex_turn_row"
      ? NonNullable<I["codex_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexTurn | undefined
          : RelationInclude extends AnyCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexTurnInclude
              ? CodexTurnWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type JAgentWaitIncludedRelations<I extends JAgentWaitInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentRun : JAgentRun | undefined
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentRunInclude
              ? R extends true ? JAgentRunWithIncludes<RelationInclude, false> : JAgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "step_row"
      ? NonNullable<I["step_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentStep : JAgentStep | undefined
          : RelationInclude extends AnyJAgentStepQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentStepInclude
              ? R extends true ? JAgentStepWithIncludes<RelationInclude, false> : JAgentStepWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "target_session_row"
      ? NonNullable<I["target_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "target_turn_row"
      ? NonNullable<I["target_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexTurn | undefined
          : RelationInclude extends AnyCodexTurnQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexTurnInclude
              ? CodexTurnWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type JAgentSessionBindingIncludedRelations<I extends JAgentSessionBindingInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentRun : JAgentRun | undefined
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentRunInclude
              ? R extends true ? JAgentRunWithIncludes<RelationInclude, false> : JAgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "codex_session_row"
      ? NonNullable<I["codex_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? CodexSession : CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? R extends true ? CodexSessionWithIncludes<RelationInclude, false> : CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "parent_session_row"
      ? NonNullable<I["parent_session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? CodexSession | undefined
          : RelationInclude extends AnyCodexSessionQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends CodexSessionInclude
              ? CodexSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type JAgentArtifactIncludedRelations<I extends JAgentArtifactInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "run_row"
      ? NonNullable<I["run_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? JAgentRun : JAgentRun | undefined
          : RelationInclude extends AnyJAgentRunQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JAgentRunInclude
              ? R extends true ? JAgentRunWithIncludes<RelationInclude, false> : JAgentRunWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "step_row"
      ? NonNullable<I["step_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? JAgentStep | undefined
          : RelationInclude extends AnyJAgentStepQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends JAgentStepInclude
              ? JAgentStepWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface CodexSessionRelations {
  codex_turnsViaSession_row: CodexTurn[];
  codex_session_presenceViaSession_row: CodexSessionPresence[];
  codex_sync_statesViaSession_row: CodexSyncState[];
  j_agent_runsViaParent_session_row: JAgentRun[];
  j_agent_runsViaInitiator_session_row: JAgentRun[];
  j_agent_attemptsViaCodex_session_row: JAgentAttempt[];
  j_agent_waitsViaTarget_session_row: JAgentWait[];
  j_agent_session_bindingsViaCodex_session_row: JAgentSessionBinding[];
  j_agent_session_bindingsViaParent_session_row: JAgentSessionBinding[];
}

export interface CodexTurnRelations {
  session_row: CodexSession | undefined;
  codex_session_presenceViaCurrent_turn_row: CodexSessionPresence[];
  j_agent_attemptsViaCodex_turn_row: JAgentAttempt[];
  j_agent_waitsViaTarget_turn_row: JAgentWait[];
}

export interface CodexSessionPresenceRelations {
  session_row: CodexSession | undefined;
  current_turn_row: CodexTurn | undefined;
}

export interface CodexSyncStateRelations {
  session_row: CodexSession | undefined;
}

export interface JAgentDefinitionRelations {
  j_agent_runsViaDefinition_row: JAgentRun[];
}

export interface JAgentRunRelations {
  definition_row: JAgentDefinition | undefined;
  parent_session_row: CodexSession | undefined;
  initiator_session_row: CodexSession | undefined;
  j_agent_stepsViaRun_row: JAgentStep[];
  j_agent_attemptsViaRun_row: JAgentAttempt[];
  j_agent_waitsViaRun_row: JAgentWait[];
  j_agent_session_bindingsViaRun_row: JAgentSessionBinding[];
  j_agent_artifactsViaRun_row: JAgentArtifact[];
}

export interface JAgentStepRelations {
  run_row: JAgentRun | undefined;
  j_agent_attemptsViaStep_row: JAgentAttempt[];
  j_agent_waitsViaStep_row: JAgentWait[];
  j_agent_artifactsViaStep_row: JAgentArtifact[];
}

export interface JAgentAttemptRelations {
  run_row: JAgentRun | undefined;
  step_row: JAgentStep | undefined;
  codex_session_row: CodexSession | undefined;
  codex_turn_row: CodexTurn | undefined;
}

export interface JAgentWaitRelations {
  run_row: JAgentRun | undefined;
  step_row: JAgentStep | undefined;
  target_session_row: CodexSession | undefined;
  target_turn_row: CodexTurn | undefined;
}

export interface JAgentSessionBindingRelations {
  run_row: JAgentRun | undefined;
  codex_session_row: CodexSession | undefined;
  parent_session_row: CodexSession | undefined;
}

export interface JAgentArtifactRelations {
  run_row: JAgentRun | undefined;
  step_row: JAgentStep | undefined;
}

export type CodexSessionWithIncludes<I extends CodexSessionInclude = {}, R extends boolean = false> = CodexSession & CodexSessionIncludedRelations<I, R>;

export type CodexTurnWithIncludes<I extends CodexTurnInclude = {}, R extends boolean = false> = CodexTurn & CodexTurnIncludedRelations<I, R>;

export type CodexSessionPresenceWithIncludes<I extends CodexSessionPresenceInclude = {}, R extends boolean = false> = CodexSessionPresence & CodexSessionPresenceIncludedRelations<I, R>;

export type CodexSyncStateWithIncludes<I extends CodexSyncStateInclude = {}, R extends boolean = false> = CodexSyncState & CodexSyncStateIncludedRelations<I, R>;

export type JAgentDefinitionWithIncludes<I extends JAgentDefinitionInclude = {}, R extends boolean = false> = JAgentDefinition & JAgentDefinitionIncludedRelations<I, R>;

export type JAgentRunWithIncludes<I extends JAgentRunInclude = {}, R extends boolean = false> = JAgentRun & JAgentRunIncludedRelations<I, R>;

export type JAgentStepWithIncludes<I extends JAgentStepInclude = {}, R extends boolean = false> = JAgentStep & JAgentStepIncludedRelations<I, R>;

export type JAgentAttemptWithIncludes<I extends JAgentAttemptInclude = {}, R extends boolean = false> = JAgentAttempt & JAgentAttemptIncludedRelations<I, R>;

export type JAgentWaitWithIncludes<I extends JAgentWaitInclude = {}, R extends boolean = false> = JAgentWait & JAgentWaitIncludedRelations<I, R>;

export type JAgentSessionBindingWithIncludes<I extends JAgentSessionBindingInclude = {}, R extends boolean = false> = JAgentSessionBinding & JAgentSessionBindingIncludedRelations<I, R>;

export type JAgentArtifactWithIncludes<I extends JAgentArtifactInclude = {}, R extends boolean = false> = JAgentArtifact & JAgentArtifactIncludedRelations<I, R>;

export type CodexSessionSelectableColumn = keyof CodexSession | PermissionIntrospectionColumn | "*";
export type CodexSessionOrderableColumn = keyof CodexSession | PermissionIntrospectionColumn;

export type CodexSessionSelected<S extends CodexSessionSelectableColumn = keyof CodexSession> = ("*" extends S ? CodexSession : Pick<CodexSession, Extract<S | "id", keyof CodexSession>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CodexSessionSelectedWithIncludes<I extends CodexSessionInclude = {}, S extends CodexSessionSelectableColumn = keyof CodexSession, R extends boolean = false> = CodexSessionSelected<S> & CodexSessionIncludedRelations<I, R>;

export type CodexTurnSelectableColumn = keyof CodexTurn | PermissionIntrospectionColumn | "*";
export type CodexTurnOrderableColumn = keyof CodexTurn | PermissionIntrospectionColumn;

export type CodexTurnSelected<S extends CodexTurnSelectableColumn = keyof CodexTurn> = ("*" extends S ? CodexTurn : Pick<CodexTurn, Extract<S | "id", keyof CodexTurn>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CodexTurnSelectedWithIncludes<I extends CodexTurnInclude = {}, S extends CodexTurnSelectableColumn = keyof CodexTurn, R extends boolean = false> = CodexTurnSelected<S> & CodexTurnIncludedRelations<I, R>;

export type CodexSessionPresenceSelectableColumn = keyof CodexSessionPresence | PermissionIntrospectionColumn | "*";
export type CodexSessionPresenceOrderableColumn = keyof CodexSessionPresence | PermissionIntrospectionColumn;

export type CodexSessionPresenceSelected<S extends CodexSessionPresenceSelectableColumn = keyof CodexSessionPresence> = ("*" extends S ? CodexSessionPresence : Pick<CodexSessionPresence, Extract<S | "id", keyof CodexSessionPresence>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CodexSessionPresenceSelectedWithIncludes<I extends CodexSessionPresenceInclude = {}, S extends CodexSessionPresenceSelectableColumn = keyof CodexSessionPresence, R extends boolean = false> = CodexSessionPresenceSelected<S> & CodexSessionPresenceIncludedRelations<I, R>;

export type CodexSyncStateSelectableColumn = keyof CodexSyncState | PermissionIntrospectionColumn | "*";
export type CodexSyncStateOrderableColumn = keyof CodexSyncState | PermissionIntrospectionColumn;

export type CodexSyncStateSelected<S extends CodexSyncStateSelectableColumn = keyof CodexSyncState> = ("*" extends S ? CodexSyncState : Pick<CodexSyncState, Extract<S | "id", keyof CodexSyncState>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CodexSyncStateSelectedWithIncludes<I extends CodexSyncStateInclude = {}, S extends CodexSyncStateSelectableColumn = keyof CodexSyncState, R extends boolean = false> = CodexSyncStateSelected<S> & CodexSyncStateIncludedRelations<I, R>;

export type JAgentDefinitionSelectableColumn = keyof JAgentDefinition | PermissionIntrospectionColumn | "*";
export type JAgentDefinitionOrderableColumn = keyof JAgentDefinition | PermissionIntrospectionColumn;

export type JAgentDefinitionSelected<S extends JAgentDefinitionSelectableColumn = keyof JAgentDefinition> = ("*" extends S ? JAgentDefinition : Pick<JAgentDefinition, Extract<S | "id", keyof JAgentDefinition>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentDefinitionSelectedWithIncludes<I extends JAgentDefinitionInclude = {}, S extends JAgentDefinitionSelectableColumn = keyof JAgentDefinition, R extends boolean = false> = JAgentDefinitionSelected<S> & JAgentDefinitionIncludedRelations<I, R>;

export type JAgentRunSelectableColumn = keyof JAgentRun | PermissionIntrospectionColumn | "*";
export type JAgentRunOrderableColumn = keyof JAgentRun | PermissionIntrospectionColumn;

export type JAgentRunSelected<S extends JAgentRunSelectableColumn = keyof JAgentRun> = ("*" extends S ? JAgentRun : Pick<JAgentRun, Extract<S | "id", keyof JAgentRun>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentRunSelectedWithIncludes<I extends JAgentRunInclude = {}, S extends JAgentRunSelectableColumn = keyof JAgentRun, R extends boolean = false> = JAgentRunSelected<S> & JAgentRunIncludedRelations<I, R>;

export type JAgentStepSelectableColumn = keyof JAgentStep | PermissionIntrospectionColumn | "*";
export type JAgentStepOrderableColumn = keyof JAgentStep | PermissionIntrospectionColumn;

export type JAgentStepSelected<S extends JAgentStepSelectableColumn = keyof JAgentStep> = ("*" extends S ? JAgentStep : Pick<JAgentStep, Extract<S | "id", keyof JAgentStep>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentStepSelectedWithIncludes<I extends JAgentStepInclude = {}, S extends JAgentStepSelectableColumn = keyof JAgentStep, R extends boolean = false> = JAgentStepSelected<S> & JAgentStepIncludedRelations<I, R>;

export type JAgentAttemptSelectableColumn = keyof JAgentAttempt | PermissionIntrospectionColumn | "*";
export type JAgentAttemptOrderableColumn = keyof JAgentAttempt | PermissionIntrospectionColumn;

export type JAgentAttemptSelected<S extends JAgentAttemptSelectableColumn = keyof JAgentAttempt> = ("*" extends S ? JAgentAttempt : Pick<JAgentAttempt, Extract<S | "id", keyof JAgentAttempt>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentAttemptSelectedWithIncludes<I extends JAgentAttemptInclude = {}, S extends JAgentAttemptSelectableColumn = keyof JAgentAttempt, R extends boolean = false> = JAgentAttemptSelected<S> & JAgentAttemptIncludedRelations<I, R>;

export type JAgentWaitSelectableColumn = keyof JAgentWait | PermissionIntrospectionColumn | "*";
export type JAgentWaitOrderableColumn = keyof JAgentWait | PermissionIntrospectionColumn;

export type JAgentWaitSelected<S extends JAgentWaitSelectableColumn = keyof JAgentWait> = ("*" extends S ? JAgentWait : Pick<JAgentWait, Extract<S | "id", keyof JAgentWait>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentWaitSelectedWithIncludes<I extends JAgentWaitInclude = {}, S extends JAgentWaitSelectableColumn = keyof JAgentWait, R extends boolean = false> = JAgentWaitSelected<S> & JAgentWaitIncludedRelations<I, R>;

export type JAgentSessionBindingSelectableColumn = keyof JAgentSessionBinding | PermissionIntrospectionColumn | "*";
export type JAgentSessionBindingOrderableColumn = keyof JAgentSessionBinding | PermissionIntrospectionColumn;

export type JAgentSessionBindingSelected<S extends JAgentSessionBindingSelectableColumn = keyof JAgentSessionBinding> = ("*" extends S ? JAgentSessionBinding : Pick<JAgentSessionBinding, Extract<S | "id", keyof JAgentSessionBinding>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentSessionBindingSelectedWithIncludes<I extends JAgentSessionBindingInclude = {}, S extends JAgentSessionBindingSelectableColumn = keyof JAgentSessionBinding, R extends boolean = false> = JAgentSessionBindingSelected<S> & JAgentSessionBindingIncludedRelations<I, R>;

export type JAgentArtifactSelectableColumn = keyof JAgentArtifact | PermissionIntrospectionColumn | "*";
export type JAgentArtifactOrderableColumn = keyof JAgentArtifact | PermissionIntrospectionColumn;

export type JAgentArtifactSelected<S extends JAgentArtifactSelectableColumn = keyof JAgentArtifact> = ("*" extends S ? JAgentArtifact : Pick<JAgentArtifact, Extract<S | "id", keyof JAgentArtifact>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JAgentArtifactSelectedWithIncludes<I extends JAgentArtifactInclude = {}, S extends JAgentArtifactSelectableColumn = keyof JAgentArtifact, R extends boolean = false> = JAgentArtifactSelected<S> & JAgentArtifactIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "codex_sessions": {
    "columns": [
      {
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "rollout_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cwd",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "project_root",
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
        "nullable": true
      },
      {
        "name": "git_branch",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "originator",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "source",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "cli_version",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "model_provider",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "model_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "reasoning_effort",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "agent_nickname",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "agent_role",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "agent_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "first_user_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_user_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_assistant_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_assistant_partial",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_preview",
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
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "latest_activity_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "last_user_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_assistant_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_completion_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      }
    ]
  },
  "codex_turns": {
    "columns": [
      {
        "name": "turn_id",
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
        "nullable": false
      },
      {
        "name": "session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "codex_sessions"
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
        "name": "user_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "assistant_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "assistant_partial",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "plan_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "reasoning_summary",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "started_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "completed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "duration_ms",
        "column_type": {
          "type": "Integer"
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
  "codex_session_presence": {
    "columns": [
      {
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "codex_sessions"
      },
      {
        "name": "project_root",
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
        "nullable": true
      },
      {
        "name": "cwd",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "state",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "current_turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "current_turn_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_turns"
      },
      {
        "name": "current_turn_status",
        "column_type": {
          "type": "Text"
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
        "name": "latest_activity_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "last_event_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "last_user_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_assistant_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_completion_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_synced_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "runtime_pid",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "runtime_tty",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "runtime_host",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "last_heartbeat_at",
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
  "codex_sync_states": {
    "columns": [
      {
        "name": "source_id",
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
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_sessions"
      },
      {
        "name": "line_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "synced_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "j_agent_definitions": {
    "columns": [
      {
        "name": "definition_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "version",
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
        "name": "entrypoint",
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
  "j_agent_runs": {
    "columns": [
      {
        "name": "run_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "definition_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "definition_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "j_agent_definitions"
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "project_root",
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
        "name": "trigger_source",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "parent_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "parent_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_sessions"
      },
      {
        "name": "parent_turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "initiator_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "initiator_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_sessions"
      },
      {
        "name": "requested_role",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "requested_model",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "requested_reasoning_effort",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "fork_turns",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "current_step_key",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "input_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "output_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "error_text",
        "column_type": {
          "type": "Text"
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
  "j_agent_steps": {
    "columns": [
      {
        "name": "step_id",
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
        "references": "j_agent_runs"
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "step_key",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "step_kind",
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
        "name": "input_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "output_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      },
      {
        "name": "error_text",
        "column_type": {
          "type": "Text"
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
  "j_agent_attempts": {
    "columns": [
      {
        "name": "attempt_id",
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
        "references": "j_agent_runs"
      },
      {
        "name": "step_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "step_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "j_agent_steps"
      },
      {
        "name": "attempt",
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
        "name": "codex_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "codex_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_sessions"
      },
      {
        "name": "codex_turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "codex_turn_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_turns"
      },
      {
        "name": "fork_turns",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      },
      {
        "name": "model_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "reasoning_effort",
        "column_type": {
          "type": "Text"
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
      },
      {
        "name": "error_text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      }
    ]
  },
  "j_agent_waits": {
    "columns": [
      {
        "name": "wait_id",
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
        "references": "j_agent_runs"
      },
      {
        "name": "step_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "step_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "j_agent_steps"
      },
      {
        "name": "wait_kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "target_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "target_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_sessions"
      },
      {
        "name": "target_turn_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "target_turn_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_turns"
      },
      {
        "name": "resume_condition_json",
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
        "name": "resumed_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      }
    ]
  },
  "j_agent_session_bindings": {
    "columns": [
      {
        "name": "binding_id",
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
        "references": "j_agent_runs"
      },
      {
        "name": "codex_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "codex_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "codex_sessions"
      },
      {
        "name": "binding_role",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "parent_session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "parent_session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "codex_sessions"
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
  "j_agent_artifacts": {
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
        "references": "j_agent_runs"
      },
      {
        "name": "step_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "step_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "j_agent_steps"
      },
      {
        "name": "kind",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "path",
        "column_type": {
          "type": "Text"
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
      }
    ]
  }
};

export class CodexSessionQueryBuilder<I extends CodexSessionInclude = {}, S extends CodexSessionSelectableColumn = keyof CodexSession, R extends boolean = false> implements QueryBuilder<CodexSessionSelectedWithIncludes<I, S, R>> {
  readonly _table = "codex_sessions";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: CodexSessionSelectedWithIncludes<I, S, R>;
  readonly _initType!: CodexSessionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CodexSessionInclude> = {};
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

  where(conditions: CodexSessionWhereInput): CodexSessionQueryBuilder<I, S, R> {
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

  select<NewS extends CodexSessionSelectableColumn>(...columns: [NewS, ...NewS[]]): CodexSessionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CodexSessionInclude>(relations: NewI): CodexSessionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): CodexSessionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: CodexSessionOrderableColumn, direction: "asc" | "desc" = "asc"): CodexSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CodexSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CodexSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "codex_turnsViaSession_row" | "codex_session_presenceViaSession_row" | "codex_sync_statesViaSession_row" | "j_agent_runsViaParent_session_row" | "j_agent_runsViaInitiator_session_row" | "j_agent_attemptsViaCodex_session_row" | "j_agent_waitsViaTarget_session_row" | "j_agent_session_bindingsViaCodex_session_row" | "j_agent_session_bindingsViaParent_session_row"): CodexSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CodexSessionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CodexSessionQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends CodexSessionInclude = I, CloneS extends CodexSessionSelectableColumn = S, CloneR extends boolean = R>(): CodexSessionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new CodexSessionQueryBuilder<CloneI, CloneS, CloneR>();
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

export class CodexTurnQueryBuilder<I extends CodexTurnInclude = {}, S extends CodexTurnSelectableColumn = keyof CodexTurn, R extends boolean = false> implements QueryBuilder<CodexTurnSelectedWithIncludes<I, S, R>> {
  readonly _table = "codex_turns";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: CodexTurnSelectedWithIncludes<I, S, R>;
  readonly _initType!: CodexTurnInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CodexTurnInclude> = {};
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

  where(conditions: CodexTurnWhereInput): CodexTurnQueryBuilder<I, S, R> {
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

  select<NewS extends CodexTurnSelectableColumn>(...columns: [NewS, ...NewS[]]): CodexTurnQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CodexTurnInclude>(relations: NewI): CodexTurnQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): CodexTurnQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: CodexTurnOrderableColumn, direction: "asc" | "desc" = "asc"): CodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "session_row" | "codex_session_presenceViaCurrent_turn_row" | "j_agent_attemptsViaCodex_turn_row" | "j_agent_waitsViaTarget_turn_row"): CodexTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CodexTurnWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CodexTurnQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends CodexTurnInclude = I, CloneS extends CodexTurnSelectableColumn = S, CloneR extends boolean = R>(): CodexTurnQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new CodexTurnQueryBuilder<CloneI, CloneS, CloneR>();
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

export class CodexSessionPresenceQueryBuilder<I extends CodexSessionPresenceInclude = {}, S extends CodexSessionPresenceSelectableColumn = keyof CodexSessionPresence, R extends boolean = false> implements QueryBuilder<CodexSessionPresenceSelectedWithIncludes<I, S, R>> {
  readonly _table = "codex_session_presence";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: CodexSessionPresenceSelectedWithIncludes<I, S, R>;
  readonly _initType!: CodexSessionPresenceInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CodexSessionPresenceInclude> = {};
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

  where(conditions: CodexSessionPresenceWhereInput): CodexSessionPresenceQueryBuilder<I, S, R> {
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

  select<NewS extends CodexSessionPresenceSelectableColumn>(...columns: [NewS, ...NewS[]]): CodexSessionPresenceQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CodexSessionPresenceInclude>(relations: NewI): CodexSessionPresenceQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): CodexSessionPresenceQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: CodexSessionPresenceOrderableColumn, direction: "asc" | "desc" = "asc"): CodexSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CodexSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CodexSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "session_row" | "current_turn_row"): CodexSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CodexSessionPresenceWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CodexSessionPresenceQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends CodexSessionPresenceInclude = I, CloneS extends CodexSessionPresenceSelectableColumn = S, CloneR extends boolean = R>(): CodexSessionPresenceQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new CodexSessionPresenceQueryBuilder<CloneI, CloneS, CloneR>();
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

export class CodexSyncStateQueryBuilder<I extends CodexSyncStateInclude = {}, S extends CodexSyncStateSelectableColumn = keyof CodexSyncState, R extends boolean = false> implements QueryBuilder<CodexSyncStateSelectedWithIncludes<I, S, R>> {
  readonly _table = "codex_sync_states";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: CodexSyncStateSelectedWithIncludes<I, S, R>;
  readonly _initType!: CodexSyncStateInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CodexSyncStateInclude> = {};
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

  where(conditions: CodexSyncStateWhereInput): CodexSyncStateQueryBuilder<I, S, R> {
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

  select<NewS extends CodexSyncStateSelectableColumn>(...columns: [NewS, ...NewS[]]): CodexSyncStateQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CodexSyncStateInclude>(relations: NewI): CodexSyncStateQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): CodexSyncStateQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: CodexSyncStateOrderableColumn, direction: "asc" | "desc" = "asc"): CodexSyncStateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CodexSyncStateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CodexSyncStateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "session_row"): CodexSyncStateQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CodexSyncStateWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CodexSyncStateQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends CodexSyncStateInclude = I, CloneS extends CodexSyncStateSelectableColumn = S, CloneR extends boolean = R>(): CodexSyncStateQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new CodexSyncStateQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentDefinitionQueryBuilder<I extends JAgentDefinitionInclude = {}, S extends JAgentDefinitionSelectableColumn = keyof JAgentDefinition, R extends boolean = false> implements QueryBuilder<JAgentDefinitionSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_definitions";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentDefinitionSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentDefinitionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentDefinitionInclude> = {};
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

  where(conditions: JAgentDefinitionWhereInput): JAgentDefinitionQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentDefinitionSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentDefinitionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentDefinitionInclude>(relations: NewI): JAgentDefinitionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentDefinitionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentDefinitionOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentDefinitionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentDefinitionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentDefinitionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "j_agent_runsViaDefinition_row"): JAgentDefinitionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentDefinitionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentDefinitionQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentDefinitionInclude = I, CloneS extends JAgentDefinitionSelectableColumn = S, CloneR extends boolean = R>(): JAgentDefinitionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentDefinitionQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentRunQueryBuilder<I extends JAgentRunInclude = {}, S extends JAgentRunSelectableColumn = keyof JAgentRun, R extends boolean = false> implements QueryBuilder<JAgentRunSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_runs";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentRunSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentRunInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentRunInclude> = {};
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

  where(conditions: JAgentRunWhereInput): JAgentRunQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentRunSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentRunQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentRunInclude>(relations: NewI): JAgentRunQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentRunQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentRunOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "definition_row" | "parent_session_row" | "initiator_session_row" | "j_agent_stepsViaRun_row" | "j_agent_attemptsViaRun_row" | "j_agent_waitsViaRun_row" | "j_agent_session_bindingsViaRun_row" | "j_agent_artifactsViaRun_row"): JAgentRunQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentRunWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentRunQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentRunInclude = I, CloneS extends JAgentRunSelectableColumn = S, CloneR extends boolean = R>(): JAgentRunQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentRunQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentStepQueryBuilder<I extends JAgentStepInclude = {}, S extends JAgentStepSelectableColumn = keyof JAgentStep, R extends boolean = false> implements QueryBuilder<JAgentStepSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_steps";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentStepSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentStepInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentStepInclude> = {};
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

  where(conditions: JAgentStepWhereInput): JAgentStepQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentStepSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentStepQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentStepInclude>(relations: NewI): JAgentStepQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentStepQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentStepOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentStepQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentStepQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentStepQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "j_agent_attemptsViaStep_row" | "j_agent_waitsViaStep_row" | "j_agent_artifactsViaStep_row"): JAgentStepQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentStepWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentStepQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentStepInclude = I, CloneS extends JAgentStepSelectableColumn = S, CloneR extends boolean = R>(): JAgentStepQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentStepQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentAttemptQueryBuilder<I extends JAgentAttemptInclude = {}, S extends JAgentAttemptSelectableColumn = keyof JAgentAttempt, R extends boolean = false> implements QueryBuilder<JAgentAttemptSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_attempts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentAttemptSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentAttemptInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentAttemptInclude> = {};
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

  where(conditions: JAgentAttemptWhereInput): JAgentAttemptQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentAttemptSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentAttemptQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentAttemptInclude>(relations: NewI): JAgentAttemptQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentAttemptQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentAttemptOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentAttemptQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentAttemptQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentAttemptQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "step_row" | "codex_session_row" | "codex_turn_row"): JAgentAttemptQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentAttemptWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentAttemptQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentAttemptInclude = I, CloneS extends JAgentAttemptSelectableColumn = S, CloneR extends boolean = R>(): JAgentAttemptQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentAttemptQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentWaitQueryBuilder<I extends JAgentWaitInclude = {}, S extends JAgentWaitSelectableColumn = keyof JAgentWait, R extends boolean = false> implements QueryBuilder<JAgentWaitSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_waits";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentWaitSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentWaitInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentWaitInclude> = {};
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

  where(conditions: JAgentWaitWhereInput): JAgentWaitQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentWaitSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentWaitQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentWaitInclude>(relations: NewI): JAgentWaitQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentWaitQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentWaitOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentWaitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentWaitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentWaitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "step_row" | "target_session_row" | "target_turn_row"): JAgentWaitQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentWaitWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentWaitQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentWaitInclude = I, CloneS extends JAgentWaitSelectableColumn = S, CloneR extends boolean = R>(): JAgentWaitQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentWaitQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentSessionBindingQueryBuilder<I extends JAgentSessionBindingInclude = {}, S extends JAgentSessionBindingSelectableColumn = keyof JAgentSessionBinding, R extends boolean = false> implements QueryBuilder<JAgentSessionBindingSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_session_bindings";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentSessionBindingSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentSessionBindingInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentSessionBindingInclude> = {};
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

  where(conditions: JAgentSessionBindingWhereInput): JAgentSessionBindingQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentSessionBindingSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentSessionBindingQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentSessionBindingInclude>(relations: NewI): JAgentSessionBindingQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentSessionBindingQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentSessionBindingOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentSessionBindingQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentSessionBindingQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentSessionBindingQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "codex_session_row" | "parent_session_row"): JAgentSessionBindingQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentSessionBindingWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentSessionBindingQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentSessionBindingInclude = I, CloneS extends JAgentSessionBindingSelectableColumn = S, CloneR extends boolean = R>(): JAgentSessionBindingQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentSessionBindingQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JAgentArtifactQueryBuilder<I extends JAgentArtifactInclude = {}, S extends JAgentArtifactSelectableColumn = keyof JAgentArtifact, R extends boolean = false> implements QueryBuilder<JAgentArtifactSelectedWithIncludes<I, S, R>> {
  readonly _table = "j_agent_artifacts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JAgentArtifactSelectedWithIncludes<I, S, R>;
  readonly _initType!: JAgentArtifactInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JAgentArtifactInclude> = {};
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

  where(conditions: JAgentArtifactWhereInput): JAgentArtifactQueryBuilder<I, S, R> {
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

  select<NewS extends JAgentArtifactSelectableColumn>(...columns: [NewS, ...NewS[]]): JAgentArtifactQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JAgentArtifactInclude>(relations: NewI): JAgentArtifactQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JAgentArtifactQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JAgentArtifactOrderableColumn, direction: "asc" | "desc" = "asc"): JAgentArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JAgentArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JAgentArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "run_row" | "step_row"): JAgentArtifactQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JAgentArtifactWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JAgentArtifactQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JAgentArtifactInclude = I, CloneS extends JAgentArtifactSelectableColumn = S, CloneR extends boolean = R>(): JAgentArtifactQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JAgentArtifactQueryBuilder<CloneI, CloneS, CloneR>();
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
  codex_sessions: CodexSessionQueryBuilder;
  codex_turns: CodexTurnQueryBuilder;
  codex_session_presence: CodexSessionPresenceQueryBuilder;
  codex_sync_states: CodexSyncStateQueryBuilder;
  j_agent_definitions: JAgentDefinitionQueryBuilder;
  j_agent_runs: JAgentRunQueryBuilder;
  j_agent_steps: JAgentStepQueryBuilder;
  j_agent_attempts: JAgentAttemptQueryBuilder;
  j_agent_waits: JAgentWaitQueryBuilder;
  j_agent_session_bindings: JAgentSessionBindingQueryBuilder;
  j_agent_artifacts: JAgentArtifactQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  codex_sessions: new CodexSessionQueryBuilder(),
  codex_turns: new CodexTurnQueryBuilder(),
  codex_session_presence: new CodexSessionPresenceQueryBuilder(),
  codex_sync_states: new CodexSyncStateQueryBuilder(),
  j_agent_definitions: new JAgentDefinitionQueryBuilder(),
  j_agent_runs: new JAgentRunQueryBuilder(),
  j_agent_steps: new JAgentStepQueryBuilder(),
  j_agent_attempts: new JAgentAttemptQueryBuilder(),
  j_agent_waits: new JAgentWaitQueryBuilder(),
  j_agent_session_bindings: new JAgentSessionBindingQueryBuilder(),
  j_agent_artifacts: new JAgentArtifactQueryBuilder(),
  wasmSchema,
};
