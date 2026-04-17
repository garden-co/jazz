#![allow(dead_code)]

use std::time::{Duration, Instant};

use jazz_tools::Schema;
use jazz_tools::query_manager::types::SchemaHash;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue, json};

const PUBLISH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const PUBLISH_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PublishedPermissionsHead {
    #[serde(rename = "schemaHash")]
    pub schema_hash: String,
    pub version: u64,
    #[serde(rename = "parentBundleObjectId")]
    pub parent_bundle_object_id: Option<String>,
    #[serde(rename = "bundleObjectId")]
    pub bundle_object_id: String,
}

#[derive(Debug, Deserialize)]
struct PermissionsHeadResponse {
    head: Option<PublishedPermissionsHead>,
}

pub fn allow_all_permissions(schema: &Schema) -> Map<String, JsonValue> {
    schema
        .keys()
        .map(|table_name| {
            (
                table_name.to_string(),
                json!({
                    "select": { "using": { "type": "True" } },
                    "insert": { "with_check": { "type": "True" } },
                    "update": {
                        "using": { "type": "True" },
                        "with_check": { "type": "True" }
                    },
                    "delete": { "using": { "type": "True" } }
                }),
            )
        })
        .collect()
}

pub fn deny_all_select_permissions(schema: &Schema) -> Map<String, JsonValue> {
    schema
        .keys()
        .map(|table_name| {
            (
                table_name.to_string(),
                json!({
                    "select": { "using": { "type": "False" } }
                }),
            )
        })
        .collect()
}

pub async fn publish_allow_all_permissions(
    base_url: &str,
    admin_secret: &str,
    schema: &Schema,
) -> PublishedPermissionsHead {
    publish_permissions(
        base_url,
        admin_secret,
        schema,
        allow_all_permissions(schema),
        None,
    )
    .await
}

pub async fn publish_permissions(
    base_url: &str,
    admin_secret: &str,
    schema: &Schema,
    permissions: Map<String, JsonValue>,
    mut expected_parent_bundle_object_id: Option<String>,
) -> PublishedPermissionsHead {
    let client = Client::new();
    let schema_hash = SchemaHash::compute(schema);
    let deadline = Instant::now() + PUBLISH_RETRY_TIMEOUT;
    let mut fetched_parent_after_conflict = false;

    loop {
        let response = client
            .post(format!("{base_url}/admin/permissions"))
            .header("X-Jazz-Admin-Secret", admin_secret)
            .json(&json!({
                "schemaHash": schema_hash.to_string(),
                "permissions": permissions,
                "expectedParentBundleObjectId": expected_parent_bundle_object_id,
            }))
            .send()
            .await
            .expect("publish permissions request");
        let status = response.status();

        match status {
            StatusCode::CREATED => {
                let response_json: PermissionsHeadResponse = response
                    .json()
                    .await
                    .expect("decode permissions publish response");
                return response_json
                    .head
                    .expect("permissions publish response should include head");
            }
            StatusCode::NOT_FOUND if Instant::now() < deadline => {
                tokio::time::sleep(PUBLISH_RETRY_INTERVAL).await;
            }
            StatusCode::CONFLICT if !fetched_parent_after_conflict => {
                expected_parent_bundle_object_id =
                    fetch_current_parent_bundle_object_id(&client, base_url, admin_secret).await;
                fetched_parent_after_conflict = true;
            }
            _ => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<unreadable response body>".to_string());
                panic!("failed to publish permissions: {status} {body}");
            }
        }
    }
}

async fn fetch_current_parent_bundle_object_id(
    client: &Client,
    base_url: &str,
    admin_secret: &str,
) -> Option<String> {
    let response = client
        .get(format!("{base_url}/admin/permissions/head"))
        .header("X-Jazz-Admin-Secret", admin_secret)
        .send()
        .await
        .expect("fetch permissions head");
    let status = response.status();
    assert_eq!(status, StatusCode::OK, "unexpected permissions head status");

    let response_json: PermissionsHeadResponse = response
        .json()
        .await
        .expect("decode permissions head response");
    response_json.head.map(|head| head.bundle_object_id)
}
