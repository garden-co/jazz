CREATE TABLE agents (
    agent_id TEXT NOT NULL,
    lane TEXT,
    spec_path TEXT,
    prompt_surface TEXT,
    status TEXT,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE agent_runs (
    run_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_row_id UUID REFERENCES agents NOT NULL,
    thread_id TEXT,
    turn_id TEXT,
    cwd TEXT,
    repo_root TEXT,
    request_summary TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    context_json JSON,
    source_trace_path TEXT
);

CREATE TABLE run_items (
    item_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES agent_runs NOT NULL,
    item_kind TEXT NOT NULL,
    phase TEXT,
    sequence INTEGER NOT NULL,
    status TEXT NOT NULL,
    summary_json JSON,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

CREATE TABLE semantic_events (
    event_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES agent_runs NOT NULL,
    item_id TEXT,
    item_row_id UUID REFERENCES run_items,
    event_type TEXT NOT NULL,
    summary_text TEXT,
    payload_json JSON,
    occurred_at TIMESTAMP NOT NULL
);

CREATE TABLE wire_events (
    event_id TEXT NOT NULL,
    run_id TEXT,
    run_row_id UUID REFERENCES agent_runs,
    connection_id INTEGER,
    session_id INTEGER,
    direction TEXT NOT NULL,
    method TEXT,
    request_id TEXT,
    payload_json JSON,
    occurred_at TIMESTAMP NOT NULL
);

CREATE TABLE artifacts (
    artifact_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES agent_runs NOT NULL,
    artifact_kind TEXT NOT NULL,
    title TEXT,
    absolute_path TEXT NOT NULL,
    checksum TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE agent_state_snapshots (
    snapshot_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_row_id UUID REFERENCES agents NOT NULL,
    state_version INTEGER,
    status TEXT,
    state_json JSON NOT NULL,
    captured_at TIMESTAMP NOT NULL
);

CREATE TABLE workspace_snapshots (
    snapshot_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES agent_runs NOT NULL,
    repo_root TEXT NOT NULL,
    branch TEXT,
    head_commit TEXT,
    dirty_path_count INTEGER,
    snapshot_json JSON,
    captured_at TIMESTAMP NOT NULL
);

CREATE TABLE memory_links (
    link_id TEXT NOT NULL,
    run_id TEXT,
    run_row_id UUID REFERENCES agent_runs,
    item_id TEXT,
    item_row_id UUID REFERENCES run_items,
    memory_scope TEXT NOT NULL,
    memory_ref TEXT,
    query_text TEXT,
    link_json JSON,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE source_files (
    source_file_id TEXT NOT NULL,
    run_id TEXT,
    run_row_id UUID REFERENCES agent_runs,
    file_kind TEXT NOT NULL,
    absolute_path TEXT NOT NULL,
    checksum TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE task_records (
    task_id TEXT NOT NULL,
    context TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    placement TEXT NOT NULL,
    focus_rank INTEGER,
    project TEXT NOT NULL,
    issue TEXT,
    branch TEXT,
    workspace TEXT,
    plan TEXT,
    pr TEXT,
    tags_json JSON,
    next_text TEXT,
    context_text TEXT,
    notes_text TEXT,
    annotations_json JSON,
    source_kind TEXT,
    source_path TEXT,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
