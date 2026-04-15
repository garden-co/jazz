CREATE TABLE codex_sessions (
    session_id TEXT NOT NULL,
    rollout_path TEXT NOT NULL,
    cwd TEXT NOT NULL,
    project_root TEXT NOT NULL,
    repo_root TEXT,
    git_branch TEXT,
    originator TEXT,
    source TEXT,
    cli_version TEXT,
    model_provider TEXT,
    model_name TEXT,
    reasoning_effort TEXT,
    agent_nickname TEXT,
    agent_role TEXT,
    agent_path TEXT,
    first_user_message TEXT,
    latest_user_message TEXT,
    latest_assistant_message TEXT,
    latest_assistant_partial TEXT,
    latest_preview TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    latest_activity_at TIMESTAMP NOT NULL,
    last_user_at TIMESTAMP,
    last_assistant_at TIMESTAMP,
    last_completion_at TIMESTAMP,
    metadata_json JSON
);

CREATE TABLE codex_turns (
    turn_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_row_id UUID REFERENCES codex_sessions NOT NULL,
    sequence INTEGER NOT NULL,
    status TEXT NOT NULL,
    user_message TEXT,
    assistant_message TEXT,
    assistant_partial TEXT,
    plan_text TEXT,
    reasoning_summary TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_ms INTEGER,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE codex_session_presence (
    session_id TEXT NOT NULL,
    session_row_id UUID REFERENCES codex_sessions NOT NULL,
    project_root TEXT NOT NULL,
    repo_root TEXT,
    cwd TEXT NOT NULL,
    state TEXT NOT NULL,
    current_turn_id TEXT,
    current_turn_row_id UUID REFERENCES codex_turns,
    current_turn_status TEXT,
    started_at TIMESTAMP NOT NULL,
    latest_activity_at TIMESTAMP NOT NULL,
    last_event_at TIMESTAMP NOT NULL,
    last_user_at TIMESTAMP,
    last_assistant_at TIMESTAMP,
    last_completion_at TIMESTAMP,
    last_synced_at TIMESTAMP NOT NULL,
    runtime_pid INTEGER,
    runtime_tty TEXT,
    runtime_host TEXT,
    last_heartbeat_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE codex_sync_states (
    source_id TEXT NOT NULL,
    absolute_path TEXT NOT NULL,
    session_id TEXT,
    session_row_id UUID REFERENCES codex_sessions,
    line_count INTEGER NOT NULL,
    synced_at TIMESTAMP NOT NULL
);

CREATE TABLE j_agent_definitions (
    definition_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    entrypoint TEXT NOT NULL,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE j_agent_runs (
    run_id TEXT NOT NULL,
    definition_id TEXT NOT NULL,
    definition_row_id UUID REFERENCES j_agent_definitions NOT NULL,
    status TEXT NOT NULL,
    project_root TEXT NOT NULL,
    repo_root TEXT,
    cwd TEXT,
    trigger_source TEXT,
    parent_session_id TEXT,
    parent_session_row_id UUID REFERENCES codex_sessions,
    parent_turn_id TEXT,
    initiator_session_id TEXT,
    initiator_session_row_id UUID REFERENCES codex_sessions,
    requested_role TEXT,
    requested_model TEXT,
    requested_reasoning_effort TEXT,
    fork_turns INTEGER,
    current_step_key TEXT,
    input_json JSON,
    output_json JSON,
    error_text TEXT,
    started_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

CREATE TABLE j_agent_steps (
    step_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES j_agent_runs NOT NULL,
    sequence INTEGER NOT NULL,
    step_key TEXT NOT NULL,
    step_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    input_json JSON,
    output_json JSON,
    error_text TEXT,
    started_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

CREATE TABLE j_agent_attempts (
    attempt_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES j_agent_runs NOT NULL,
    step_id TEXT NOT NULL,
    step_row_id UUID REFERENCES j_agent_steps NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    codex_session_id TEXT,
    codex_session_row_id UUID REFERENCES codex_sessions,
    codex_turn_id TEXT,
    codex_turn_row_id UUID REFERENCES codex_turns,
    fork_turns INTEGER,
    model_name TEXT,
    reasoning_effort TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_text TEXT
);

CREATE TABLE j_agent_waits (
    wait_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES j_agent_runs NOT NULL,
    step_id TEXT NOT NULL,
    step_row_id UUID REFERENCES j_agent_steps NOT NULL,
    wait_kind TEXT NOT NULL,
    target_session_id TEXT,
    target_session_row_id UUID REFERENCES codex_sessions,
    target_turn_id TEXT,
    target_turn_row_id UUID REFERENCES codex_turns,
    resume_condition_json JSON,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    resumed_at TIMESTAMP
);

CREATE TABLE j_agent_session_bindings (
    binding_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES j_agent_runs NOT NULL,
    codex_session_id TEXT NOT NULL,
    codex_session_row_id UUID REFERENCES codex_sessions NOT NULL,
    binding_role TEXT NOT NULL,
    parent_session_id TEXT,
    parent_session_row_id UUID REFERENCES codex_sessions,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE j_agent_artifacts (
    artifact_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_row_id UUID REFERENCES j_agent_runs NOT NULL,
    step_id TEXT,
    step_row_id UUID REFERENCES j_agent_steps,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    text_preview TEXT,
    metadata_json JSON,
    created_at TIMESTAMP NOT NULL
);
