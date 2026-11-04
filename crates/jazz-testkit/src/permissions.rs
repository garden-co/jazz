#![allow(dead_code)]

use std::time::{Duration, Instant};

use jazz::tools::{PolicyExpr, Schema, SchemaHash, TableName, TablePolicies};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;

const PUBLISH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const PUBLISH_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// A published permissions bundle: what a backend pins its sessions to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedBundle {
    /// Its hex id.
    pub bundle: String,
}

pub fn allow_all_permissions(schema: &Schema) -> Vec<(TableName, TablePolicies)> {
    schema
        .keys()
        .map(|table_name| (*table_name, jazz::tools::test_support::allow_all_policies()))
        .collect()
}

pub fn deny_all_select_permissions(schema: &Schema) -> Vec<(TableName, TablePolicies)> {
    schema
        .keys()
        .map(|table_name| {
            (
                *table_name,
                TablePolicies::new().with_select(PolicyExpr::False),
            )
        })
        .collect()
}

pub async fn publish_allow_all_permissions(
    base_url: &str,
    app_id: impl std::fmt::Display,
    admin_secret: &str,
    schema: &Schema,
) -> PublishedBundle {
    publish_permissions(
        base_url,
        app_id,
        admin_secret,
        schema,
        allow_all_permissions(schema),
    )
    .await
}

/// Build a complete initial or permission-only deployment for test fixtures.
pub fn schema_deployment(
    schema: &Schema,
    permissions: impl IntoIterator<Item = (TableName, TablePolicies)>,
) -> JsonValue {
    let hash = SchemaHash::compute(schema).to_string();
    json!({
        "targetSchemaHash": hash,
        "schemas": [{ "hash": hash, "schema": schema }],
        "migrations": [],
        "permissions": permissions.into_iter().collect::<HashMap<_, _>>(),
    })
}

pub async fn publish_permissions(
    base_url: &str,
    app_id: impl std::fmt::Display,
    admin_secret: &str,
    schema: &Schema,
    permissions: impl IntoIterator<Item = (TableName, TablePolicies)>,
) -> PublishedBundle {
    let client = Client::new();
    let body = schema_deployment(schema, permissions);
    let deadline = Instant::now() + PUBLISH_RETRY_TIMEOUT;
    loop {
        let response = client
            .post(format!("{base_url}/apps/{app_id}/admin/bundles"))
            .header("X-Jazz-Admin-Secret", admin_secret)
            .json(&body)
            .send()
            .await
            .expect("deploy permissions request");
        let status = response.status();
        if status == StatusCode::NOT_FOUND && Instant::now() < deadline {
            tokio::time::sleep(PUBLISH_RETRY_INTERVAL).await;
            continue;
        }
        assert_eq!(
            status,
            StatusCode::OK,
            "deployment failed: {}",
            response.text().await.unwrap()
        );
        let body: JsonValue = response.json().await.expect("bundle response");
        return PublishedBundle {
            bundle: body["bundle"].as_str().expect("bundle id").to_owned(),
        };
    }
}
