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

CREATE TABLE daemon_log_sources (
    source_id TEXT NOT NULL,
    manager TEXT NOT NULL,
    daemon_name TEXT NOT NULL,
    stream TEXT NOT NULL,
    host_id TEXT,
    log_path TEXT NOT NULL,
    config_path TEXT,
    repo_root TEXT,
    workspace_root TEXT,
    owner_agent TEXT,
    flow_daemon_name TEXT,
    launchd_label TEXT,
    retention_class TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE daemon_log_chunks (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_row_id UUID REFERENCES daemon_log_sources NOT NULL,
    daemon_name TEXT NOT NULL,
    stream TEXT NOT NULL,
    host_id TEXT,
    log_path TEXT NOT NULL,
    file_fingerprint TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    first_line_no INTEGER NOT NULL,
    last_line_no INTEGER NOT NULL,
    line_count INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    first_at TIMESTAMP,
    last_at TIMESTAMP,
    sha256 TEXT NOT NULL,
    body_ref TEXT,
    body_preview TEXT,
    compression TEXT NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);

CREATE TABLE daemon_log_events (
    event_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_row_id UUID REFERENCES daemon_log_sources NOT NULL,
    chunk_id TEXT NOT NULL,
    chunk_row_id UUID REFERENCES daemon_log_chunks NOT NULL,
    daemon_name TEXT NOT NULL,
    stream TEXT NOT NULL,
    seq INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    at TIMESTAMP,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    fields_json JSON,
    repo_root TEXT,
    workspace_root TEXT,
    conversation TEXT,
    conversation_hash TEXT,
    run_id TEXT,
    job_id TEXT,
    trace_id TEXT,
    span_id TEXT,
    error_kind TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE daemon_log_checkpoints (
    checkpoint_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_row_id UUID REFERENCES daemon_log_sources NOT NULL,
    host_id TEXT,
    log_path TEXT NOT NULL,
    file_fingerprint TEXT NOT NULL,
    inode TEXT,
    device TEXT,
    offset INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    last_chunk_id TEXT,
    last_event_id TEXT,
    last_seen_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE daemon_log_summaries (
    summary_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_row_id UUID REFERENCES daemon_log_sources NOT NULL,
    daemon_name TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    level_counts_json JSON NOT NULL,
    error_count INTEGER NOT NULL,
    warning_count INTEGER NOT NULL,
    first_error_event_id TEXT,
    last_error_event_id TEXT,
    top_error_kinds_json JSON,
    summary_text TEXT,
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

CREATE TABLE designer_cad_workspaces (
    workspace_id TEXT NOT NULL,
    workspace_key TEXT NOT NULL,
    title TEXT,
    repo_root TEXT,
    workspace_root TEXT,
    status TEXT NOT NULL,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_documents (
    document_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    workspace_row_id UUID REFERENCES designer_cad_workspaces NOT NULL,
    file_path TEXT NOT NULL,
    language TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_hash TEXT,
    status TEXT NOT NULL,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_sessions (
    cad_session_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    workspace_row_id UUID REFERENCES designer_cad_workspaces NOT NULL,
    document_id TEXT NOT NULL,
    document_row_id UUID REFERENCES designer_cad_documents NOT NULL,
    codex_session_id TEXT,
    agent_run_id TEXT,
    status TEXT NOT NULL,
    active_tool_session_id TEXT,
    latest_projection_id TEXT,
    opened_by TEXT,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP
);

CREATE TABLE designer_cad_events (
    event_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    sequence INTEGER NOT NULL,
    event_kind TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    tool_session_id TEXT,
    operation_id TEXT,
    preview_id TEXT,
    source_event_id TEXT,
    payload_json JSON,
    occurred_at TIMESTAMP NOT NULL,
    observed_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_scene_nodes (
    node_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    document_id TEXT NOT NULL,
    document_row_id UUID REFERENCES designer_cad_documents NOT NULL,
    projection_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    label TEXT,
    path TEXT,
    parent_node_id TEXT,
    stable_ref TEXT,
    visibility TEXT,
    source_span_json JSON,
    geometry_ref_json JSON,
    metadata_json JSON,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_selections (
    selection_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    node_id TEXT,
    selection_json JSON,
    status TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_tool_sessions (
    tool_session_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    tool_kind TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    status TEXT NOT NULL,
    input_json JSON,
    state_json JSON,
    started_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

CREATE TABLE designer_cad_operations (
    operation_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    tool_session_id TEXT,
    tool_session_row_id UUID REFERENCES designer_cad_tool_sessions,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    operation_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    operation_json JSON NOT NULL,
    validation_json JSON,
    result_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    applied_at TIMESTAMP
);

CREATE TABLE designer_cad_source_edits (
    edit_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_row_id UUID REFERENCES designer_cad_operations NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    sequence INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    range_json JSON NOT NULL,
    text_preview TEXT,
    text_sha256 TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_preview_handles (
    preview_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    tool_session_id TEXT,
    tool_session_row_id UUID REFERENCES designer_cad_tool_sessions,
    operation_id TEXT,
    operation_row_id UUID REFERENCES designer_cad_operations,
    preview_kind TEXT NOT NULL,
    target_json JSON,
    status TEXT NOT NULL,
    handle_ref TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    disposed_at TIMESTAMP
);

CREATE TABLE designer_cad_preview_updates (
    update_id TEXT NOT NULL,
    preview_id TEXT NOT NULL,
    preview_row_id UUID REFERENCES designer_cad_preview_handles NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    sequence INTEGER NOT NULL,
    params_json JSON,
    mesh_ref_json JSON,
    status TEXT NOT NULL,
    error_text TEXT,
    requested_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

CREATE TABLE designer_cad_widgets (
    widget_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    workspace_row_id UUID REFERENCES designer_cad_workspaces NOT NULL,
    widget_key TEXT NOT NULL,
    title TEXT,
    source_kind TEXT NOT NULL,
    source_path TEXT,
    version TEXT,
    status TEXT NOT NULL,
    manifest_json JSON,
    state_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE designer_cad_steers (
    steer_id TEXT NOT NULL,
    cad_session_id TEXT NOT NULL,
    cad_session_row_id UUID REFERENCES designer_cad_sessions NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    target_agent_id TEXT,
    target_run_id TEXT,
    message_text TEXT NOT NULL,
    context_json JSON,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
