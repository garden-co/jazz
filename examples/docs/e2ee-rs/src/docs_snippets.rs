//! Encryption snippets for the docs. Each region is one function, doing what
//! its TypeScript twin in `examples/docs/e2ee-ts` does.
#![allow(dead_code)]

use std::time::Duration;

use jazz_app_support::{AppFiles, AppId, DeviceKey, Result};

use crate::jazz_app::{AccountId, Jazz, ObjectId, Project, SpaceMember};

/// Stands in for your own store: anything that implements
/// `jazz_e2ee_client::PrivateStore`.
struct MyStore;

impl MyStore {
    fn open() -> jazz_e2ee_client::MemoryStore {
        jazz_e2ee_client::MemoryStore::default()
    }
}

// #region e2ee-open-rust
use jazz_app_support::{Auth, Config, EncryptionConfig};
use std::rc::Rc;

pub async fn open_encrypted(files: AppFiles, app_id: AppId, device_key: DeviceKey) -> Result<Jazz> {
    let config = Config::new(app_id)
        .server("https://app.example/jazz")
        .user(Auth::Device(device_key))
        .encryption(EncryptionConfig::store(|| Rc::new(MyStore::open())).environment("production"));
    let jazz = Jazz::open(files, config).await?;
    // The device's status: "active", "awaiting-approval" or "reenrolled".
    let status = jazz.encryption().ready(Duration::from_secs(30)).await?;
    println!("device status: {status}");
    Ok(jazz)
}
// #endregion e2ee-open-rust

// #region e2ee-add-members-rust
use crate::jazz_app::{DurabilityTier, NewProject};

pub async fn share_project(jazz: &Jazz, other_account: AccountId) -> Result<Vec<SpaceMember>> {
    let durable = jazz.durable(DurabilityTier::GlobalServer);
    let project = durable.insert(NewProject::new("Plans")).await?;
    durable
        .add_member::<Project>(project.id, other_account)
        .await?;
    jazz.list_members::<Project>(project.id).await
}
// #endregion e2ee-add-members-rust

// #region e2ee-remove-member-rust
use serde_json::json;

pub async fn remove_member(
    jazz: &Jazz,
    project_id: ObjectId,
    other_account: AccountId,
) -> Result<usize> {
    let removed = jazz
        .remove_member::<Project>(project_id, other_account)
        .await?;
    if removed > 0 {
        jazz.encryption()
            .call(
                "keysRotate",
                json!({ "table": "projects", "id": project_id.to_string() }),
            )
            .await?;
    }
    Ok(removed)
}
// #endregion e2ee-remove-member-rust
