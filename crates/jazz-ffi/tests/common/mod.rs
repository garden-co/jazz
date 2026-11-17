#![allow(dead_code)]

use serde_json::{Value, json};

pub const STAGE_PLAN_APP: &str =
    include_str!("../../../../examples/stage-plan/apps/react-localfirst/jazz.app.json");
pub const STAGE_PLAN_PERMISSIONS: &str =
    include_str!("../../../../examples/stage-plan/apps/react-localfirst/jazz.permissions.json");
pub const KITCHEN_APP: &str = include_str!("../fixtures/kitchen/jazz.app.json");

/// A local-only StagePlan client kept in memory.
pub fn client_config() -> String {
    json!({ "app": STAGE_PLAN_APP, "storage": "memory" }).to_string()
}

pub fn backend_config(server: &jazz_server::JazzServer, app_id: &str, subject: &str) -> String {
    json!({
        "app": STAGE_PLAN_APP,
        "permissions": STAGE_PLAN_PERMISSIONS,
        "appId": app_id,
        "serverUrl": server.base_url(),
        "backendSecret": server.backend_secret(),
        "adminSecret": server.admin_secret(),
        "session": { "issuer": "jazz:backend", "subject": subject },
    })
    .to_string()
}

pub fn show(chief: &str) -> String {
    json!({
        "name": "Spring Gala",
        "venue": "Main Hall",
        "date": "2026-04-01",
        "doors": "19:00",
        "chiefAccount": chief,
    })
    .to_string()
}

pub fn task(show_id: &str, title: &str, rank: f64) -> String {
    json!({ "showId": show_id, "title": title, "status": "todo", "rank": rank, "assigneeId": null })
        .to_string()
}

pub fn tasks_of(show_id: &str) -> String {
    json!({
        "table": "tasks",
        "where": [{ "column": "showId", "op": "eq", "value": show_id }],
        "orderBy": [{ "column": "rank", "direction": "asc" }],
    })
    .to_string()
}

pub fn parse(text: &str) -> Value {
    serde_json::from_str(text).expect("valid JSON")
}

pub fn titles(rows: &Value) -> Vec<String> {
    rows.as_array()
        .expect("an array of rows")
        .iter()
        .map(|row| row["title"].as_str().expect("a title").to_owned())
        .collect()
}

pub fn nil_uuid() -> &'static str {
    "00000000-0000-0000-0000-000000000000"
}
