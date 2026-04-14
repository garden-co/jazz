use std::collections::HashMap;

use clap::Subcommand;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::types::{RowDescriptor, Value};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use uuid::Uuid;

use super::db::{
    DbContextArgs, JsonInputArgs, OpenDb, QuerySnapshot, execute_query_snapshot, open_db,
    print_json, row_json,
};

const DEFAULT_RUN_STATUS: &str = "started";

#[derive(clap::Args)]
pub struct AgentInfraCommand {
    #[command(subcommand)]
    action: AgentInfraAction,
}

#[derive(Subcommand)]
enum AgentInfraAction {
    /// Show the JSON contract for agent-infra operations
    Syntax,
    /// Upsert an agent row
    UpsertAgent {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Create or update an agent run start record
    RecordRunStarted {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Mark an agent run as completed or failed
    RecordRunCompleted {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Create or update a run item start record
    RecordItemStarted {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Mark a run item as completed
    RecordItemCompleted {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert a semantic event
    AppendSemanticEvent {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert a wire event
    AppendWireEvent {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert an artifact row
    RecordArtifact {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert a workspace snapshot row
    RecordWorkspaceSnapshot {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert an agent state snapshot row
    UpdateAgentState {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert a memory-link row
    RecordMemoryLink {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Append or upsert a source-file row
    RecordSourceFile {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Return the latest saved state snapshot for an agent id
    GetAgentState {
        agent_id: String,
        #[command(flatten)]
        args: DbContextArgs,
    },
    /// Return a structured summary for one run id
    GetRunSummary {
        run_id: String,
        #[command(flatten)]
        args: DbContextArgs,
    },
    /// List recent runs ordered by started_at desc
    ListRecentRuns {
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[command(flatten)]
        args: DbContextArgs,
    },
}

#[derive(Clone)]
struct LocatedRow {
    descriptor: RowDescriptor,
    id: ObjectId,
    values: Vec<Value>,
}

impl LocatedRow {
    fn json(&self) -> JsonValue {
        row_json(self.id, &self.values, &self.descriptor)
    }

    fn value(&self, column: &str) -> Option<&Value> {
        self.descriptor
            .columns
            .iter()
            .position(|candidate| candidate.name.as_str() == column)
            .and_then(|index| self.values.get(index))
    }

    fn text(&self, column: &str) -> Option<&str> {
        match self.value(column) {
            Some(Value::Text(value)) => Some(value.as_str()),
            _ => None,
        }
    }

    fn timestamp(&self, column: &str) -> Option<u64> {
        match self.value(column) {
            Some(Value::Timestamp(value)) => Some(*value),
            _ => None,
        }
    }

    fn int(&self, column: &str) -> Option<i32> {
        match self.value(column) {
            Some(Value::Integer(value)) => Some(*value),
            _ => None,
        }
    }
}

pub fn run(command: AgentInfraCommand) -> Result<(), String> {
    match command.action {
        AgentInfraAction::Syntax => {
            print_json(&syntax_json())?;
        }
        AgentInfraAction::UpsertAgent { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "agent payload")?;
            let row = upsert_agent(&db, &payload)?;
            print_json(&mutation_json(&db, "upsertAgent", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordRunStarted { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "run payload")?;
            let row = record_run_started(&db, &payload)?;
            print_json(&mutation_json(&db, "recordRunStarted", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordRunCompleted { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "run payload")?;
            let row = record_run_completed(&db, &payload)?;
            print_json(&mutation_json(&db, "recordRunCompleted", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordItemStarted { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "item payload")?;
            let row = record_item_started(&db, &payload)?;
            print_json(&mutation_json(&db, "recordItemStarted", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordItemCompleted { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "item payload")?;
            let row = record_item_completed(&db, &payload)?;
            print_json(&mutation_json(&db, "recordItemCompleted", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::AppendSemanticEvent { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "semantic event payload")?;
            let row = append_semantic_event(&db, &payload)?;
            print_json(&mutation_json(&db, "appendSemanticEvent", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::AppendWireEvent { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "wire event payload")?;
            let row = append_wire_event(&db, &payload)?;
            print_json(&mutation_json(&db, "appendWireEvent", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordArtifact { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "artifact payload")?;
            let row = record_artifact(&db, &payload)?;
            print_json(&mutation_json(&db, "recordArtifact", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordWorkspaceSnapshot { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "workspace snapshot payload")?;
            let row = record_workspace_snapshot(&db, &payload)?;
            print_json(&mutation_json(&db, "recordWorkspaceSnapshot", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::UpdateAgentState { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "agent state payload")?;
            let row = update_agent_state(&db, &payload)?;
            print_json(&mutation_json(&db, "updateAgentState", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordMemoryLink { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "memory-link payload")?;
            let row = record_memory_link(&db, &payload)?;
            print_json(&mutation_json(&db, "recordMemoryLink", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::RecordSourceFile { args, input } => {
            let db = open_db(&args)?;
            let payload = read_payload_object(&input, "source-file payload")?;
            let row = record_source_file(&db, &payload)?;
            print_json(&mutation_json(&db, "recordSourceFile", row.json()))?;
            db.close()?;
        }
        AgentInfraAction::GetAgentState { agent_id, args } => {
            let db = open_db(&args)?;
            let latest = latest_agent_state(&db, &agent_id)?;
            let output = json!({
                "database": db.metadata_json(),
                "agentId": agent_id,
                "state": latest.map(|row| row.json()),
            });
            print_json(&output)?;
            db.close()?;
        }
        AgentInfraAction::GetRunSummary { run_id, args } => {
            let db = open_db(&args)?;
            let output = run_summary_json(&db, &run_id)?;
            print_json(&output)?;
            db.close()?;
        }
        AgentInfraAction::ListRecentRuns { limit, args } => {
            let db = open_db(&args)?;
            let output = list_recent_runs_json(&db, limit)?;
            print_json(&output)?;
            db.close()?;
        }
    }

    Ok(())
}

fn read_payload_object(input: &JsonInputArgs, label: &str) -> Result<JsonMap<String, JsonValue>, String> {
    let raw = input.read_required(label)?;
    let payload: JsonValue =
        serde_json::from_str(&raw).map_err(|err| format!("invalid {label} JSON: {err}"))?;
    match payload {
        JsonValue::Object(map) => Ok(map),
        _ => Err(format!("{label} must be a JSON object")),
    }
}

fn syntax_json() -> JsonValue {
    json!({
        "overview": {
            "purpose": "Agent-friendly structured control-plane CLI over the run-agent Jazz store.",
            "notes": [
                "All command outputs are JSON.",
                "The store remains file-backed; raw logs and artifacts stay on disk while Jazz stores structured metadata and relationships.",
                "Mutation commands upsert by external ids where possible so callers do not need row ids.",
                "Timestamp payload fields use *_Micros and expect Unix microseconds.",
                "JSON payload columns accept ordinary JSON objects and arrays; the CLI serializes them for Jazz JSON columns."
            ]
        },
        "commands": {
            "recordRunStarted": "jazz-tools agent-infra record-run-started --app-id <app> --data-dir <dir> --json '<payload>'",
            "recordRunCompleted": "jazz-tools agent-infra record-run-completed --app-id <app> --data-dir <dir> --json '<payload>'",
            "recordItemStarted": "jazz-tools agent-infra record-item-started --app-id <app> --data-dir <dir> --json '<payload>'",
            "recordItemCompleted": "jazz-tools agent-infra record-item-completed --app-id <app> --data-dir <dir> --json '<payload>'",
            "appendSemanticEvent": "jazz-tools agent-infra append-semantic-event --app-id <app> --data-dir <dir> --json '<payload>'",
            "recordArtifact": "jazz-tools agent-infra record-artifact --app-id <app> --data-dir <dir> --json '<payload>'",
            "recordWorkspaceSnapshot": "jazz-tools agent-infra record-workspace-snapshot --app-id <app> --data-dir <dir> --json '<payload>'",
            "updateAgentState": "jazz-tools agent-infra update-agent-state --app-id <app> --data-dir <dir> --json '<payload>'",
            "recordSourceFile": "jazz-tools agent-infra record-source-file --app-id <app> --data-dir <dir> --json '<payload>'",
            "getRunSummary": "jazz-tools agent-infra get-run-summary <run-id> --app-id <app> --data-dir <dir>",
            "getAgentState": "jazz-tools agent-infra get-agent-state <agent-id> --app-id <app> --data-dir <dir>",
            "listRecentRuns": "jazz-tools agent-infra list-recent-runs --limit 20 --app-id <app> --data-dir <dir>"
        },
        "examples": {
            "recordRunStarted": {
                "runId": "20260327T123456Z-plan-1a2b3c4d",
                "agentId": "plan",
                "threadId": "thread_123",
                "cwd": "/Users/nikitavoloboev/run",
                "repoRoot": "/Users/nikitavoloboev/run",
                "requestSummary": "write a migration plan",
                "status": "running",
                "startedAtMicros": 1774596000000000u64,
                "contextJson": { "executionMode": "resume" },
                "sourceTracePath": "/Users/nikitavoloboev/run/.ai/internal/agent-runs/plan/run.json",
                "agent": {
                    "lane": "planning",
                    "specPath": "/Users/nikitavoloboev/run/recipes/agents/specs/plan.toml",
                    "promptSurface": "/Users/nikitavoloboev/run/prompts/codex/plan.md"
                }
            },
            "recordSourceFile": {
                "runId": "20260327T123456Z-plan-1a2b3c4d",
                "fileKind": "trace-events-jsonl",
                "absolutePath": "/Users/nikitavoloboev/run/.ai/internal/agent-runs/plan/run.events.jsonl",
                "createdAtMicros": 1774596000000000u64
            }
        }
    })
}

fn mutation_json(db: &OpenDb, action: &str, row: JsonValue) -> JsonValue {
    json!({
        "database": db.metadata_json(),
        "ok": true,
        "action": action,
        "row": row,
    })
}

fn run_summary_json(db: &OpenDb, run_id: &str) -> Result<JsonValue, String> {
    let run = require_one_by_text(db, "agent_runs", "run_id", run_id)?;
    let agent_id = run
        .text("agent_id")
        .ok_or_else(|| format!("run {run_id} is missing agent_id"))?
        .to_string();

    let items = find_many_by_text(db, "run_items", "run_id", run_id)?;
    let semantic_events = find_many_by_text(db, "semantic_events", "run_id", run_id)?;
    let wire_events = find_many_by_text(db, "wire_events", "run_id", run_id)?;
    let artifacts = find_many_by_text(db, "artifacts", "run_id", run_id)?;
    let workspace_snapshots = find_many_by_text(db, "workspace_snapshots", "run_id", run_id)?;
    let memory_links = find_many_by_text(db, "memory_links", "run_id", run_id)?;
    let source_files = find_many_by_text(db, "source_files", "run_id", run_id)?;
    let latest_agent_state = latest_agent_state(db, &agent_id)?;

    let mut items = items;
    items.sort_by_key(|row| row.int("sequence").unwrap_or_default());

    let mut semantic_events = semantic_events;
    semantic_events.sort_by_key(|row| row.timestamp("occurred_at").unwrap_or_default());

    let mut wire_events = wire_events;
    wire_events.sort_by_key(|row| row.timestamp("occurred_at").unwrap_or_default());

    let mut artifacts = artifacts;
    artifacts.sort_by_key(|row| row.timestamp("created_at").unwrap_or_default());

    let mut workspace_snapshots = workspace_snapshots;
    workspace_snapshots.sort_by_key(|row| std::cmp::Reverse(row.timestamp("captured_at").unwrap_or_default()));

    let mut memory_links = memory_links;
    memory_links.sort_by_key(|row| row.timestamp("created_at").unwrap_or_default());

    let mut source_files = source_files;
    source_files.sort_by_key(|row| row.timestamp("created_at").unwrap_or_default());

    Ok(json!({
        "database": db.metadata_json(),
        "runId": run_id,
        "run": run.json(),
        "items": items.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "semanticEvents": semantic_events.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "wireEvents": wire_events.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "artifacts": artifacts.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "workspaceSnapshots": workspace_snapshots.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "memoryLinks": memory_links.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "sourceFiles": source_files.into_iter().map(|row| row.json()).collect::<Vec<_>>(),
        "latestAgentState": latest_agent_state.map(|row| row.json()),
    }))
}

fn list_recent_runs_json(db: &OpenDb, limit: usize) -> Result<JsonValue, String> {
    let mut rows = snapshot_rows(scan_table(db, "agent_runs")?);
    rows.sort_by_key(|row| std::cmp::Reverse(row.timestamp("started_at").unwrap_or_default()));
    let rows = rows.into_iter().take(limit.min(200)).map(|row| row.json()).collect::<Vec<_>>();
    Ok(json!({
        "database": db.metadata_json(),
        "rows": rows,
    }))
}

fn upsert_agent(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let agent_id = required_string(payload, "agentId")?;
    let now = now_micros();
    let lane = optional_string(payload, "lane");
    let spec_path = optional_string(payload, "specPath");
    let prompt_surface = optional_string(payload, "promptSurface");
    let status = optional_string(payload, "status");
    let metadata_json = optional_json_text(payload, "metadataJson")?;
    let created_at = optional_timestamp(payload, "createdAtMicros")?.unwrap_or(now);
    let updated_at = optional_timestamp(payload, "updatedAtMicros")?.unwrap_or(now);

    let existing = find_one_by_text(db, "agents", "agent_id", &agent_id)?;
    if let Some(existing) = existing {
        let mut updates = HashMap::new();
        insert_optional_text(&mut updates, "lane", lane);
        insert_optional_text(&mut updates, "spec_path", spec_path);
        insert_optional_text(&mut updates, "prompt_surface", prompt_surface);
        insert_optional_text(&mut updates, "status", status);
        insert_optional_value(&mut updates, "metadata_json", metadata_json);
        updates.insert("updated_at".to_string(), Value::Timestamp(updated_at));
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update agent '{}': {err}", agent_id))?;
        return require_one_by_text(db, "agents", "agent_id", &agent_id);
    }

    let mut values = HashMap::from([
        ("agent_id".to_string(), Value::Text(agent_id.clone())),
        ("created_at".to_string(), Value::Timestamp(created_at)),
        ("updated_at".to_string(), Value::Timestamp(updated_at)),
    ]);
    insert_optional_text(&mut values, "lane", lane);
    insert_optional_text(&mut values, "spec_path", spec_path);
    insert_optional_text(&mut values, "prompt_surface", prompt_surface);
    insert_optional_text(&mut values, "status", status);
    insert_optional_value(&mut values, "metadata_json", metadata_json);

    db.runtime
        .insert("agents", values, None)
        .map_err(|err| format!("failed to insert agent '{}': {err}", agent_id))?;
    require_one_by_text(db, "agents", "agent_id", &agent_id)
}

fn record_run_started(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let agent_id = required_string(payload, "agentId")?;
    let agent_payload = payload
        .get("agent")
        .and_then(JsonValue::as_object)
        .cloned()
        .unwrap_or_default();
    let mut merged_agent_payload = agent_payload;
    merged_agent_payload.insert("agentId".to_string(), JsonValue::String(agent_id.clone()));
    if let Some(status) = optional_string(payload, "status") {
        merged_agent_payload
            .entry("status".to_string())
            .or_insert(JsonValue::String(status));
    }
    let agent = upsert_agent(db, &merged_agent_payload)?;

    let thread_id = optional_string(payload, "threadId");
    let turn_id = optional_string(payload, "turnId");
    let cwd = optional_string(payload, "cwd");
    let repo_root = optional_string(payload, "repoRoot");
    let request_summary = optional_string(payload, "requestSummary");
    let status = optional_string(payload, "status");
    let started_at = optional_timestamp(payload, "startedAtMicros")?.unwrap_or_else(now_micros);
    let context_json = optional_json_text(payload, "contextJson")?;
    let source_trace_path = optional_string(payload, "sourceTracePath");

    let existing = find_one_by_text(db, "agent_runs", "run_id", &run_id)?;
    if let Some(existing) = existing {
        let mut updates = HashMap::from([
            ("agent_id".to_string(), Value::Text(agent_id.clone())),
            ("agent_row_id".to_string(), Value::Uuid(agent.id)),
        ]);
        insert_optional_text(&mut updates, "thread_id", thread_id);
        insert_optional_text(&mut updates, "turn_id", turn_id);
        insert_optional_text(&mut updates, "cwd", cwd);
        insert_optional_text(&mut updates, "repo_root", repo_root);
        insert_optional_text(&mut updates, "request_summary", request_summary);
        insert_optional_text(&mut updates, "status", status);
        if payload.contains_key("startedAtMicros") {
            updates.insert("started_at".to_string(), Value::Timestamp(started_at));
        }
        insert_optional_value(&mut updates, "context_json", context_json);
        insert_optional_text(&mut updates, "source_trace_path", source_trace_path);
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update run '{}': {err}", run_id))?;
        return require_one_by_text(db, "agent_runs", "run_id", &run_id);
    }

    let mut values = HashMap::from([
        ("run_id".to_string(), Value::Text(run_id.clone())),
        ("agent_id".to_string(), Value::Text(agent_id.clone())),
        ("agent_row_id".to_string(), Value::Uuid(agent.id)),
        (
            "status".to_string(),
            Value::Text(status.unwrap_or_else(|| DEFAULT_RUN_STATUS.to_string())),
        ),
        ("started_at".to_string(), Value::Timestamp(started_at)),
    ]);
    insert_optional_text(&mut values, "thread_id", thread_id);
    insert_optional_text(&mut values, "turn_id", turn_id);
    insert_optional_text(&mut values, "cwd", cwd);
    insert_optional_text(&mut values, "repo_root", repo_root);
    insert_optional_text(&mut values, "request_summary", request_summary);
    insert_optional_value(&mut values, "context_json", context_json);
    insert_optional_text(&mut values, "source_trace_path", source_trace_path);

    db.runtime
        .insert("agent_runs", values, None)
        .map_err(|err| format!("failed to insert run '{}': {err}", run_id))?;
    require_one_by_text(db, "agent_runs", "run_id", &run_id)
}

fn record_run_completed(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let status = optional_string(payload, "status").unwrap_or_else(|| "completed".to_string());
    let ended_at = optional_timestamp(payload, "endedAtMicros")?.unwrap_or_else(now_micros);
    let existing = require_one_by_text(db, "agent_runs", "run_id", &run_id)?;
    let updates = HashMap::from([
        ("status".to_string(), Value::Text(status)),
        ("ended_at".to_string(), Value::Timestamp(ended_at)),
    ]);
    update_row(db, existing.id, updates)
        .map_err(|err| format!("failed to update run '{}': {err}", run_id))?;
    require_one_by_text(db, "agent_runs", "run_id", &run_id)
}

fn record_item_started(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let item_id = required_string(payload, "itemId")?;
    let item_kind = required_string(payload, "itemKind")?;
    let sequence = required_i32(payload, "sequence")?;
    let phase = optional_string(payload, "phase");
    let status = optional_string(payload, "status");
    let summary_json = optional_json_text(payload, "summaryJson")?;
    let started_at = optional_timestamp(payload, "startedAtMicros")?.unwrap_or_else(now_micros);
    let run = require_one_by_text(db, "agent_runs", "run_id", &run_id)?;
    let existing = find_one_by_text_pair(db, "run_items", ("run_id", &run_id), ("item_id", &item_id))?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([
            ("item_kind".to_string(), Value::Text(item_kind)),
            ("sequence".to_string(), Value::Integer(sequence)),
        ]);
        insert_optional_text(&mut updates, "phase", phase);
        insert_optional_text(&mut updates, "status", status);
        insert_optional_value(&mut updates, "summary_json", summary_json);
        if payload.contains_key("startedAtMicros") {
            updates.insert("started_at".to_string(), Value::Timestamp(started_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update item '{}': {err}", item_id))?;
        return require_one_by_text_pair(db, "run_items", ("run_id", &run_id), ("item_id", &item_id))?
            .ok_or_else(|| format!("item '{}' disappeared after update", item_id));
    }

    let mut values = HashMap::from([
        ("item_id".to_string(), Value::Text(item_id.clone())),
        ("run_id".to_string(), Value::Text(run_id.clone())),
        ("run_row_id".to_string(), Value::Uuid(run.id)),
        ("item_kind".to_string(), Value::Text(item_kind)),
        ("sequence".to_string(), Value::Integer(sequence)),
        (
            "status".to_string(),
            Value::Text(status.unwrap_or_else(|| "started".to_string())),
        ),
        ("started_at".to_string(), Value::Timestamp(started_at)),
    ]);
    insert_optional_text(&mut values, "phase", phase);
    insert_optional_value(&mut values, "summary_json", summary_json);
    db.runtime
        .insert("run_items", values, None)
        .map_err(|err| format!("failed to insert item '{}': {err}", item_id))?;
    require_one_by_text_pair(db, "run_items", ("run_id", &run_id), ("item_id", &item_id))?
        .ok_or_else(|| format!("item '{}' not found after insert", item_id))
}

fn record_item_completed(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let item_id = required_string(payload, "itemId")?;
    let status = optional_string(payload, "status").unwrap_or_else(|| "completed".to_string());
    let summary_json = optional_json_text(payload, "summaryJson")?;
    let completed_at = optional_timestamp(payload, "completedAtMicros")?.unwrap_or_else(now_micros);
    let existing = require_one_by_text_pair(db, "run_items", ("run_id", &run_id), ("item_id", &item_id))?
        .ok_or_else(|| format!("run item not found for run '{}' and item '{}'", run_id, item_id))?;
    let mut updates = HashMap::from([
        ("status".to_string(), Value::Text(status)),
        ("completed_at".to_string(), Value::Timestamp(completed_at)),
    ]);
    insert_optional_value(&mut updates, "summary_json", summary_json);
    update_row(db, existing.id, updates)
        .map_err(|err| format!("failed to update item '{}': {err}", item_id))?;
    require_one_by_text_pair(db, "run_items", ("run_id", &run_id), ("item_id", &item_id))?
        .ok_or_else(|| format!("item '{}' disappeared after completion update", item_id))
}

fn append_semantic_event(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let event_type = required_string(payload, "eventType")?;
    let event_id = optional_string(payload, "eventId").unwrap_or_else(random_event_id);
    let item_id = optional_string(payload, "itemId");
    let summary_text = optional_string(payload, "summaryText");
    let payload_json = optional_json_text(payload, "payloadJson")?;
    let occurred_at = optional_timestamp(payload, "occurredAtMicros")?.unwrap_or_else(now_micros);
    let run = require_one_by_text(db, "agent_runs", "run_id", &run_id)?;
    let item = match &item_id {
        Some(item_id) => find_one_by_text_pair(db, "run_items", ("run_id", &run_id), ("item_id", item_id))?,
        None => None,
    };
    let existing = find_one_by_text(db, "semantic_events", "event_id", &event_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([("event_type".to_string(), Value::Text(event_type))]);
        insert_optional_text(&mut updates, "item_id", item_id);
        insert_optional_uuid(&mut updates, "item_row_id", item.as_ref().map(|row| row.id));
        insert_optional_text(&mut updates, "summary_text", summary_text);
        insert_optional_value(&mut updates, "payload_json", payload_json);
        if payload.contains_key("occurredAtMicros") {
            updates.insert("occurred_at".to_string(), Value::Timestamp(occurred_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update semantic event '{}': {err}", event_id))?;
        return require_one_by_text(db, "semantic_events", "event_id", &event_id);
    }

    let mut values = HashMap::from([
        ("event_id".to_string(), Value::Text(event_id.clone())),
        ("run_id".to_string(), Value::Text(run_id.clone())),
        ("run_row_id".to_string(), Value::Uuid(run.id)),
        ("event_type".to_string(), Value::Text(event_type)),
        ("occurred_at".to_string(), Value::Timestamp(occurred_at)),
    ]);
    insert_optional_text(&mut values, "item_id", item_id);
    insert_optional_uuid(&mut values, "item_row_id", item.as_ref().map(|row| row.id));
    insert_optional_text(&mut values, "summary_text", summary_text);
    insert_optional_value(&mut values, "payload_json", payload_json);
    db.runtime
        .insert("semantic_events", values, None)
        .map_err(|err| format!("failed to insert semantic event '{}': {err}", event_id))?;
    require_one_by_text(db, "semantic_events", "event_id", &event_id)
}

fn append_wire_event(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let event_id = optional_string(payload, "eventId").unwrap_or_else(random_event_id);
    let direction = required_string(payload, "direction")?;
    let run_id = optional_string(payload, "runId");
    let connection_id = optional_i32(payload, "connectionId")?;
    let session_id = optional_i32(payload, "sessionId")?;
    let method = optional_string(payload, "method");
    let request_id = optional_string(payload, "requestId");
    let payload_json = optional_json_text(payload, "payloadJson")?;
    let occurred_at = optional_timestamp(payload, "occurredAtMicros")?.unwrap_or_else(now_micros);
    let run = match &run_id {
        Some(run_id) => find_one_by_text(db, "agent_runs", "run_id", run_id)?,
        None => None,
    };
    let existing = find_one_by_text(db, "wire_events", "event_id", &event_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([("direction".to_string(), Value::Text(direction))]);
        insert_optional_text(&mut updates, "run_id", run_id);
        insert_optional_uuid(&mut updates, "run_row_id", run.as_ref().map(|row| row.id));
        insert_optional_i32(&mut updates, "connection_id", connection_id);
        insert_optional_i32(&mut updates, "session_id", session_id);
        insert_optional_text(&mut updates, "method", method);
        insert_optional_text(&mut updates, "request_id", request_id);
        insert_optional_value(&mut updates, "payload_json", payload_json);
        if payload.contains_key("occurredAtMicros") {
            updates.insert("occurred_at".to_string(), Value::Timestamp(occurred_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update wire event '{}': {err}", event_id))?;
        return require_one_by_text(db, "wire_events", "event_id", &event_id);
    }

    let mut values = HashMap::from([
        ("event_id".to_string(), Value::Text(event_id.clone())),
        ("direction".to_string(), Value::Text(direction)),
        ("occurred_at".to_string(), Value::Timestamp(occurred_at)),
    ]);
    insert_optional_text(&mut values, "run_id", run_id);
    insert_optional_uuid(&mut values, "run_row_id", run.as_ref().map(|row| row.id));
    insert_optional_i32(&mut values, "connection_id", connection_id);
    insert_optional_i32(&mut values, "session_id", session_id);
    insert_optional_text(&mut values, "method", method);
    insert_optional_text(&mut values, "request_id", request_id);
    insert_optional_value(&mut values, "payload_json", payload_json);
    db.runtime
        .insert("wire_events", values, None)
        .map_err(|err| format!("failed to insert wire event '{}': {err}", event_id))?;
    require_one_by_text(db, "wire_events", "event_id", &event_id)
}

fn record_artifact(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let artifact_kind = required_string(payload, "artifactKind")?;
    let absolute_path = required_string(payload, "absolutePath")?;
    let artifact_id = optional_string(payload, "artifactId").unwrap_or_else(random_event_id);
    let title = optional_string(payload, "title");
    let checksum = optional_string(payload, "checksum");
    let created_at = optional_timestamp(payload, "createdAtMicros")?.unwrap_or_else(now_micros);
    let run = require_one_by_text(db, "agent_runs", "run_id", &run_id)?;
    let existing = find_one_by_text(db, "artifacts", "artifact_id", &artifact_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([
            ("artifact_kind".to_string(), Value::Text(artifact_kind)),
            ("absolute_path".to_string(), Value::Text(absolute_path)),
        ]);
        insert_optional_text(&mut updates, "title", title);
        insert_optional_text(&mut updates, "checksum", checksum);
        if payload.contains_key("createdAtMicros") {
            updates.insert("created_at".to_string(), Value::Timestamp(created_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update artifact '{}': {err}", artifact_id))?;
        return require_one_by_text(db, "artifacts", "artifact_id", &artifact_id);
    }

    let mut values = HashMap::from([
        ("artifact_id".to_string(), Value::Text(artifact_id.clone())),
        ("run_id".to_string(), Value::Text(run_id.clone())),
        ("run_row_id".to_string(), Value::Uuid(run.id)),
        ("artifact_kind".to_string(), Value::Text(artifact_kind)),
        ("absolute_path".to_string(), Value::Text(absolute_path)),
        ("created_at".to_string(), Value::Timestamp(created_at)),
    ]);
    insert_optional_text(&mut values, "title", title);
    insert_optional_text(&mut values, "checksum", checksum);
    db.runtime
        .insert("artifacts", values, None)
        .map_err(|err| format!("failed to insert artifact '{}': {err}", artifact_id))?;
    require_one_by_text(db, "artifacts", "artifact_id", &artifact_id)
}

fn record_workspace_snapshot(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let run_id = required_string(payload, "runId")?;
    let repo_root = required_string(payload, "repoRoot")?;
    let snapshot_id = optional_string(payload, "snapshotId").unwrap_or_else(random_event_id);
    let branch = optional_string(payload, "branch");
    let head_commit = optional_string(payload, "headCommit");
    let dirty_path_count = optional_i32(payload, "dirtyPathCount")?;
    let snapshot_json = optional_json_text(payload, "snapshotJson")?;
    let captured_at = optional_timestamp(payload, "capturedAtMicros")?.unwrap_or_else(now_micros);
    let run = require_one_by_text(db, "agent_runs", "run_id", &run_id)?;
    let existing = find_one_by_text(db, "workspace_snapshots", "snapshot_id", &snapshot_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([("repo_root".to_string(), Value::Text(repo_root))]);
        insert_optional_text(&mut updates, "branch", branch);
        insert_optional_text(&mut updates, "head_commit", head_commit);
        insert_optional_i32(&mut updates, "dirty_path_count", dirty_path_count);
        insert_optional_value(&mut updates, "snapshot_json", snapshot_json);
        if payload.contains_key("capturedAtMicros") {
            updates.insert("captured_at".to_string(), Value::Timestamp(captured_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update workspace snapshot '{}': {err}", snapshot_id))?;
        return require_one_by_text(db, "workspace_snapshots", "snapshot_id", &snapshot_id);
    }

    let mut values = HashMap::from([
        ("snapshot_id".to_string(), Value::Text(snapshot_id.clone())),
        ("run_id".to_string(), Value::Text(run_id.clone())),
        ("run_row_id".to_string(), Value::Uuid(run.id)),
        ("repo_root".to_string(), Value::Text(repo_root)),
        ("captured_at".to_string(), Value::Timestamp(captured_at)),
    ]);
    insert_optional_text(&mut values, "branch", branch);
    insert_optional_text(&mut values, "head_commit", head_commit);
    insert_optional_i32(&mut values, "dirty_path_count", dirty_path_count);
    insert_optional_value(&mut values, "snapshot_json", snapshot_json);
    db.runtime
        .insert("workspace_snapshots", values, None)
        .map_err(|err| format!("failed to insert workspace snapshot '{}': {err}", snapshot_id))?;
    require_one_by_text(db, "workspace_snapshots", "snapshot_id", &snapshot_id)
}

fn update_agent_state(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let agent_id = required_string(payload, "agentId")?;
    let state_json = required_json_text(payload, "stateJson")?;
    let snapshot_id = optional_string(payload, "snapshotId").unwrap_or_else(random_event_id);
    let state_version = optional_i32(payload, "stateVersion")?;
    let status = optional_string(payload, "status");
    let captured_at = optional_timestamp(payload, "capturedAtMicros")?.unwrap_or_else(now_micros);
    let mut agent_payload = JsonMap::new();
    agent_payload.insert("agentId".to_string(), JsonValue::String(agent_id.clone()));
    if let Some(status) = status.clone() {
        agent_payload.insert("status".to_string(), JsonValue::String(status));
    }
    agent_payload.insert(
        "updatedAtMicros".to_string(),
        JsonValue::Number(serde_json::Number::from(captured_at)),
    );
    let agent = upsert_agent(db, &agent_payload)?;
    let existing = find_one_by_text(db, "agent_state_snapshots", "snapshot_id", &snapshot_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([
            ("state_json".to_string(), state_json),
            ("captured_at".to_string(), Value::Timestamp(captured_at)),
        ]);
        insert_optional_i32(&mut updates, "state_version", state_version);
        insert_optional_text(&mut updates, "status", status);
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update agent state '{}': {err}", snapshot_id))?;
        return require_one_by_text(db, "agent_state_snapshots", "snapshot_id", &snapshot_id);
    }

    let mut values = HashMap::from([
        ("snapshot_id".to_string(), Value::Text(snapshot_id.clone())),
        ("agent_id".to_string(), Value::Text(agent_id.clone())),
        ("agent_row_id".to_string(), Value::Uuid(agent.id)),
        ("state_json".to_string(), state_json),
        ("captured_at".to_string(), Value::Timestamp(captured_at)),
    ]);
    insert_optional_i32(&mut values, "state_version", state_version);
    insert_optional_text(&mut values, "status", status);
    db.runtime
        .insert("agent_state_snapshots", values, None)
        .map_err(|err| format!("failed to insert agent state '{}': {err}", snapshot_id))?;
    require_one_by_text(db, "agent_state_snapshots", "snapshot_id", &snapshot_id)
}

fn record_memory_link(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let link_id = optional_string(payload, "linkId").unwrap_or_else(random_event_id);
    let memory_scope = required_string(payload, "memoryScope")?;
    let run_id = optional_string(payload, "runId");
    let item_id = optional_string(payload, "itemId");
    let memory_ref = optional_string(payload, "memoryRef");
    let query_text = optional_string(payload, "queryText");
    let link_json = optional_json_text(payload, "linkJson")?;
    let created_at = optional_timestamp(payload, "createdAtMicros")?.unwrap_or_else(now_micros);
    let run = match &run_id {
        Some(run_id) => find_one_by_text(db, "agent_runs", "run_id", run_id)?,
        None => None,
    };
    let item = match (&run_id, &item_id) {
        (Some(run_id), Some(item_id)) => find_one_by_text_pair(db, "run_items", ("run_id", run_id), ("item_id", item_id))?,
        _ => None,
    };
    let existing = find_one_by_text(db, "memory_links", "link_id", &link_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([("memory_scope".to_string(), Value::Text(memory_scope))]);
        insert_optional_text(&mut updates, "run_id", run_id);
        insert_optional_uuid(&mut updates, "run_row_id", run.as_ref().map(|row| row.id));
        insert_optional_text(&mut updates, "item_id", item_id);
        insert_optional_uuid(&mut updates, "item_row_id", item.as_ref().map(|row| row.id));
        insert_optional_text(&mut updates, "memory_ref", memory_ref);
        insert_optional_text(&mut updates, "query_text", query_text);
        insert_optional_value(&mut updates, "link_json", link_json);
        if payload.contains_key("createdAtMicros") {
            updates.insert("created_at".to_string(), Value::Timestamp(created_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update memory link '{}': {err}", link_id))?;
        return require_one_by_text(db, "memory_links", "link_id", &link_id);
    }

    let mut values = HashMap::from([
        ("link_id".to_string(), Value::Text(link_id.clone())),
        ("memory_scope".to_string(), Value::Text(memory_scope)),
        ("created_at".to_string(), Value::Timestamp(created_at)),
    ]);
    insert_optional_text(&mut values, "run_id", run_id);
    insert_optional_uuid(&mut values, "run_row_id", run.as_ref().map(|row| row.id));
    insert_optional_text(&mut values, "item_id", item_id);
    insert_optional_uuid(&mut values, "item_row_id", item.as_ref().map(|row| row.id));
    insert_optional_text(&mut values, "memory_ref", memory_ref);
    insert_optional_text(&mut values, "query_text", query_text);
    insert_optional_value(&mut values, "link_json", link_json);
    db.runtime
        .insert("memory_links", values, None)
        .map_err(|err| format!("failed to insert memory link '{}': {err}", link_id))?;
    require_one_by_text(db, "memory_links", "link_id", &link_id)
}

fn record_source_file(db: &OpenDb, payload: &JsonMap<String, JsonValue>) -> Result<LocatedRow, String> {
    let source_file_id = optional_string(payload, "sourceFileId").unwrap_or_else(random_event_id);
    let file_kind = required_string(payload, "fileKind")?;
    let absolute_path = required_string(payload, "absolutePath")?;
    let run_id = optional_string(payload, "runId");
    let checksum = optional_string(payload, "checksum");
    let created_at = optional_timestamp(payload, "createdAtMicros")?.unwrap_or_else(now_micros);
    let run = match &run_id {
        Some(run_id) => find_one_by_text(db, "agent_runs", "run_id", run_id)?,
        None => None,
    };
    let existing = find_one_by_text(db, "source_files", "source_file_id", &source_file_id)?;

    if let Some(existing) = existing {
        let mut updates = HashMap::from([
            ("file_kind".to_string(), Value::Text(file_kind)),
            ("absolute_path".to_string(), Value::Text(absolute_path)),
        ]);
        insert_optional_text(&mut updates, "run_id", run_id);
        insert_optional_uuid(&mut updates, "run_row_id", run.as_ref().map(|row| row.id));
        insert_optional_text(&mut updates, "checksum", checksum);
        if payload.contains_key("createdAtMicros") {
            updates.insert("created_at".to_string(), Value::Timestamp(created_at));
        }
        update_row(db, existing.id, updates)
            .map_err(|err| format!("failed to update source file '{}': {err}", source_file_id))?;
        return require_one_by_text(db, "source_files", "source_file_id", &source_file_id);
    }

    let mut values = HashMap::from([
        ("source_file_id".to_string(), Value::Text(source_file_id.clone())),
        ("file_kind".to_string(), Value::Text(file_kind)),
        ("absolute_path".to_string(), Value::Text(absolute_path)),
        ("created_at".to_string(), Value::Timestamp(created_at)),
    ]);
    insert_optional_text(&mut values, "run_id", run_id);
    insert_optional_uuid(&mut values, "run_row_id", run.as_ref().map(|row| row.id));
    insert_optional_text(&mut values, "checksum", checksum);
    db.runtime
        .insert("source_files", values, None)
        .map_err(|err| format!("failed to insert source file '{}': {err}", source_file_id))?;
    require_one_by_text(db, "source_files", "source_file_id", &source_file_id)
}

fn latest_agent_state(db: &OpenDb, agent_id: &str) -> Result<Option<LocatedRow>, String> {
    let mut rows = find_many_by_text(db, "agent_state_snapshots", "agent_id", agent_id)?;
    rows.sort_by_key(|row| std::cmp::Reverse(row.timestamp("captured_at").unwrap_or_default()));
    Ok(rows.into_iter().next())
}

fn scan_table(db: &OpenDb, table: &str) -> Result<QuerySnapshot, String> {
    execute_query_snapshot(&db.runtime, Query::new(table))
}

fn snapshot_rows(snapshot: QuerySnapshot) -> Vec<LocatedRow> {
    snapshot
        .rows
        .into_iter()
        .map(|(id, values)| LocatedRow {
            descriptor: snapshot.descriptor.clone(),
            id,
            values,
        })
        .collect()
}

fn find_one_by_text(db: &OpenDb, table: &str, column: &str, expected: &str) -> Result<Option<LocatedRow>, String> {
    let mut matches = find_many_by_text(db, table, column, expected)?;
    Ok(matches.pop())
}

fn require_one_by_text(db: &OpenDb, table: &str, column: &str, expected: &str) -> Result<LocatedRow, String> {
    find_one_by_text(db, table, column, expected)?
        .ok_or_else(|| format!("row not found in table '{table}' where {column}='{expected}'"))
}

fn require_one_by_text_pair(
    db: &OpenDb,
    table: &str,
    first: (&str, &str),
    second: (&str, &str),
) -> Result<Option<LocatedRow>, String> {
    find_one_by_text_pair(db, table, first, second)
}

fn find_one_by_text_pair(
    db: &OpenDb,
    table: &str,
    first: (&str, &str),
    second: (&str, &str),
) -> Result<Option<LocatedRow>, String> {
    let mut matches = find_many_by_text_pair(db, table, first, second)?;
    Ok(matches.pop())
}

fn find_many_by_text(db: &OpenDb, table: &str, column: &str, expected: &str) -> Result<Vec<LocatedRow>, String> {
    Ok(snapshot_rows(scan_table(db, table)?)
        .into_iter()
        .filter_map(|row| match row.text(column) {
            Some(value) if value == expected => Some(row),
            _ => None,
        })
        .collect())
}

fn find_many_by_text_pair(
    db: &OpenDb,
    table: &str,
    first: (&str, &str),
    second: (&str, &str),
) -> Result<Vec<LocatedRow>, String> {
    Ok(snapshot_rows(scan_table(db, table)?)
        .into_iter()
        .filter_map(|row| match (row.text(first.0), row.text(second.0)) {
            (Some(left), Some(right)) if left == first.1 && right == second.1 => Some(row),
            _ => None,
        })
        .collect())
}

fn update_row(
    db: &OpenDb,
    object_id: ObjectId,
    updates: HashMap<String, Value>,
) -> Result<(), String> {
    db.runtime
        .update(object_id, updates.into_iter().collect(), None)
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::db::persist_schema_catalogue_for_test;
    use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};

    fn test_schema() -> jazz_tools::query_manager::types::Schema {
        let json = ColumnType::Json { schema: None };

        SchemaBuilder::new()
            .table(
                TableSchema::builder("agents")
                    .column("agent_id", ColumnType::Text)
                    .nullable_column("lane", ColumnType::Text)
                    .nullable_column("spec_path", ColumnType::Text)
                    .nullable_column("prompt_surface", ColumnType::Text)
                    .nullable_column("status", ColumnType::Text)
                    .nullable_column("metadata_json", json.clone())
                    .column("created_at", ColumnType::Timestamp)
                    .column("updated_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("agent_runs")
                    .column("run_id", ColumnType::Text)
                    .column("agent_id", ColumnType::Text)
                    .column("agent_row_id", ColumnType::Uuid)
                    .nullable_column("thread_id", ColumnType::Text)
                    .nullable_column("turn_id", ColumnType::Text)
                    .nullable_column("cwd", ColumnType::Text)
                    .nullable_column("repo_root", ColumnType::Text)
                    .nullable_column("request_summary", ColumnType::Text)
                    .column("status", ColumnType::Text)
                    .column("started_at", ColumnType::Timestamp)
                    .nullable_column("ended_at", ColumnType::Timestamp)
                    .nullable_column("context_json", json.clone())
                    .nullable_column("source_trace_path", ColumnType::Text),
            )
            .table(
                TableSchema::builder("run_items")
                    .column("item_id", ColumnType::Text)
                    .column("run_id", ColumnType::Text)
                    .column("run_row_id", ColumnType::Uuid)
                    .column("item_kind", ColumnType::Text)
                    .column("sequence", ColumnType::Integer)
                    .nullable_column("phase", ColumnType::Text)
                    .column("status", ColumnType::Text)
                    .nullable_column("summary_json", json.clone())
                    .column("started_at", ColumnType::Timestamp)
                    .nullable_column("completed_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("semantic_events")
                    .column("event_id", ColumnType::Text)
                    .column("run_id", ColumnType::Text)
                    .column("run_row_id", ColumnType::Uuid)
                    .column("event_type", ColumnType::Text)
                    .nullable_column("item_id", ColumnType::Text)
                    .nullable_column("item_row_id", ColumnType::Uuid)
                    .nullable_column("summary_text", ColumnType::Text)
                    .nullable_column("payload_json", json.clone())
                    .column("occurred_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("wire_events")
                    .column("event_id", ColumnType::Text)
                    .nullable_column("run_id", ColumnType::Text)
                    .nullable_column("run_row_id", ColumnType::Uuid)
                    .column("direction", ColumnType::Text)
                    .nullable_column("connection_id", ColumnType::Integer)
                    .nullable_column("session_id", ColumnType::Integer)
                    .nullable_column("method", ColumnType::Text)
                    .nullable_column("request_id", ColumnType::Text)
                    .nullable_column("payload_json", json.clone())
                    .column("occurred_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("artifacts")
                    .column("artifact_id", ColumnType::Text)
                    .nullable_column("run_id", ColumnType::Text)
                    .nullable_column("run_row_id", ColumnType::Uuid)
                    .nullable_column("item_id", ColumnType::Text)
                    .nullable_column("item_row_id", ColumnType::Uuid)
                    .nullable_column("artifact_kind", ColumnType::Text)
                    .nullable_column("absolute_path", ColumnType::Text)
                    .nullable_column("metadata_json", json.clone())
                    .column("created_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("workspace_snapshots")
                    .column("snapshot_id", ColumnType::Text)
                    .nullable_column("run_id", ColumnType::Text)
                    .nullable_column("run_row_id", ColumnType::Uuid)
                    .nullable_column("workspace_path", ColumnType::Text)
                    .nullable_column("summary_json", json.clone())
                    .column("captured_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("memory_links")
                    .column("link_id", ColumnType::Text)
                    .column("memory_scope", ColumnType::Text)
                    .nullable_column("run_id", ColumnType::Text)
                    .nullable_column("run_row_id", ColumnType::Uuid)
                    .nullable_column("item_id", ColumnType::Text)
                    .nullable_column("item_row_id", ColumnType::Uuid)
                    .nullable_column("memory_ref", ColumnType::Text)
                    .nullable_column("query_text", ColumnType::Text)
                    .nullable_column("link_json", json.clone())
                    .column("created_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("source_files")
                    .column("source_file_id", ColumnType::Text)
                    .column("file_kind", ColumnType::Text)
                    .column("absolute_path", ColumnType::Text)
                    .nullable_column("run_id", ColumnType::Text)
                    .nullable_column("run_row_id", ColumnType::Uuid)
                    .nullable_column("checksum", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp),
            )
            .table(
                TableSchema::builder("agent_state_snapshots")
                    .column("snapshot_id", ColumnType::Text)
                    .column("agent_id", ColumnType::Text)
                    .column("agent_row_id", ColumnType::Uuid)
                    .column("state_json", json)
                    .nullable_column("state_version", ColumnType::Integer)
                    .nullable_column("status", ColumnType::Text)
                    .column("captured_at", ColumnType::Timestamp),
            )
            .build()
    }

    fn args_for_tempdir(tempdir: &tempfile::TempDir) -> DbContextArgs {
        DbContextArgs {
            app_id: "agent-infra-command-test".to_string(),
            data_dir: tempdir.path().join("agent-data.db").to_string_lossy().to_string(),
            env: "dev".to_string(),
            user_branch: "main".to_string(),
            schema_hash: None,
            schema_dir: None,
        }
    }

    #[test]
    fn agent_infra_round_trip_summary() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let args = args_for_tempdir(&tempdir);
        persist_schema_catalogue_for_test(&args, test_schema()).expect("persist schema");
        let db = open_db(&args).expect("open db");

        let run_payload = json!({
            "runId": "run-test-1",
            "agentId": "plan",
            "threadId": "thread-1",
            "requestSummary": "smoke",
            "status": "running",
            "agent": {
                "specPath": "/tmp/plan.toml",
                "promptSurface": "/tmp/plan.md",
                "metadataJson": { "adapter": "codex-app-server" }
            }
        });
        let item_payload = json!({
            "runId": "run-test-1",
            "itemId": "item-1",
            "itemKind": "agentMessage",
            "sequence": 1,
            "status": "running",
            "summaryJson": { "text_preview": "hello" }
        });
        let semantic_payload = json!({
            "runId": "run-test-1",
            "itemId": "item-1",
            "eventId": "semantic-1",
            "eventType": "server_notification",
            "summaryText": "turn/completed",
            "payloadJson": { "method": "turn/completed" }
        });
        let state_payload = json!({
            "agentId": "plan",
            "snapshotId": "state-1",
            "status": "idle",
            "stateVersion": 1,
            "stateJson": { "last_query": "smoke", "run_id": "run-test-1" }
        });

        record_run_started(&db, run_payload.as_object().expect("run object")).expect("record run");
        record_item_started(&db, item_payload.as_object().expect("item object")).expect("record item");
        append_semantic_event(&db, semantic_payload.as_object().expect("semantic object"))
            .expect("record semantic event");
        update_agent_state(&db, state_payload.as_object().expect("state object"))
            .expect("record state");
        record_run_completed(
            &db,
            json!({ "runId": "run-test-1", "status": "completed" })
                .as_object()
                .expect("completion object"),
        )
        .expect("complete run");

        let summary = run_summary_json(&db, "run-test-1").expect("summary");
        assert_eq!(summary["run"]["record"]["status"], json!("completed"));
        assert_eq!(summary["items"].as_array().map(Vec::len), Some(1));
        assert_eq!(summary["semanticEvents"].as_array().map(Vec::len), Some(1));
        assert_eq!(summary["latestAgentState"]["record"]["status"], json!("idle"));

        db.close().expect("close db");
    }
}

fn required_string(payload: &JsonMap<String, JsonValue>, key: &str) -> Result<String, String> {
    match payload.get(key) {
        Some(JsonValue::String(value)) if !value.trim().is_empty() => Ok(value.trim().to_string()),
        _ => Err(format!("missing required string field '{key}'")),
    }
}

fn optional_string(payload: &JsonMap<String, JsonValue>, key: &str) -> Option<String> {
    match payload.get(key) {
        Some(JsonValue::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        _ => None,
    }
}

fn required_i32(payload: &JsonMap<String, JsonValue>, key: &str) -> Result<i32, String> {
    optional_i32(payload, key)?.ok_or_else(|| format!("missing required integer field '{key}'"))
}

fn optional_i32(payload: &JsonMap<String, JsonValue>, key: &str) -> Result<Option<i32>, String> {
    match payload.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(value)) => {
            let raw = value
                .as_i64()
                .ok_or_else(|| format!("field '{key}' must be an integer"))?;
            let value = i32::try_from(raw)
                .map_err(|_| format!("field '{key}' is outside the i32 range"))?;
            Ok(Some(value))
        }
        Some(JsonValue::String(value)) if !value.trim().is_empty() => {
            let raw = value
                .trim()
                .parse::<i64>()
                .map_err(|err| format!("field '{key}' must be an integer: {err}"))?;
            let value = i32::try_from(raw)
                .map_err(|_| format!("field '{key}' is outside the i32 range"))?;
            Ok(Some(value))
        }
        _ => Err(format!("field '{key}' must be an integer")),
    }
}

fn optional_timestamp(payload: &JsonMap<String, JsonValue>, key: &str) -> Result<Option<u64>, String> {
    match payload.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(value)) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("field '{key}' must be a non-negative integer microsecond timestamp")),
        Some(JsonValue::String(value)) if !value.trim().is_empty() => value
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|err| format!("field '{key}' must be a u64 microsecond timestamp: {err}")),
        _ => Err(format!("field '{key}' must be a non-negative integer microsecond timestamp")),
    }
}

fn required_json_text(payload: &JsonMap<String, JsonValue>, key: &str) -> Result<Value, String> {
    let json = payload
        .get(key)
        .ok_or_else(|| format!("missing required JSON field '{key}'"))?;
    serde_json::to_string(json)
        .map(Value::Text)
        .map_err(|err| format!("failed to serialize field '{key}' as JSON text: {err}"))
}

fn optional_json_text(payload: &JsonMap<String, JsonValue>, key: &str) -> Result<Option<Value>, String> {
    match payload.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(value) => serde_json::to_string(value)
            .map(|json| Some(Value::Text(json)))
            .map_err(|err| format!("failed to serialize field '{key}' as JSON text: {err}")),
    }
}

fn insert_optional_text(target: &mut HashMap<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        target.insert(key.to_string(), Value::Text(value));
    }
}

fn insert_optional_i32(target: &mut HashMap<String, Value>, key: &str, value: Option<i32>) {
    if let Some(value) = value {
        target.insert(key.to_string(), Value::Integer(value));
    }
}

fn insert_optional_uuid(target: &mut HashMap<String, Value>, key: &str, value: Option<ObjectId>) {
    if let Some(value) = value {
        target.insert(key.to_string(), Value::Uuid(value));
    }
}

fn insert_optional_value(target: &mut HashMap<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        target.insert(key.to_string(), value);
    }
}

fn now_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch");
    duration
        .as_secs()
        .saturating_mul(1_000_000)
        .saturating_add(u64::from(duration.subsec_micros()))
}

fn random_event_id() -> String {
    Uuid::new_v4().to_string()
}
