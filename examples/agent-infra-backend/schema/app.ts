// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
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
type AnyTaskRecordQueryBuilder<T = any> = { readonly _table: "task_records" } & QueryBuilder<T>;

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

export type TaskRecordSelectableColumn = keyof TaskRecord | PermissionIntrospectionColumn | "*";
export type TaskRecordOrderableColumn = keyof TaskRecord | PermissionIntrospectionColumn;

export type TaskRecordSelected<S extends TaskRecordSelectableColumn = keyof TaskRecord> = ("*" extends S ? TaskRecord : Pick<TaskRecord, Extract<S | "id", keyof TaskRecord>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

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
  task_records: TaskRecordQueryBuilder;
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
  task_records: new TaskRecordQueryBuilder(),
  wasmSchema,
};
