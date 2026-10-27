//! Read-only export hook for the one-time alpha.59 -> SQLite migration.
//!
//! `dev/migrate-alpha59/prepare.sh` copies this file into an alpha.59
//! checkout as `jazz_types::account_registry::migrate_export`, so it can read
//! the account journal with alpha.59's private codec. It never writes.
//! Nothing in released Jazz contains it.
#![allow(missing_docs)]

use serde::Serialize;
use uuid::Uuid;

use super::{AccountCommand, Principal, codec};
use crate::groove::storage::{OrderedKvStorage, RecordStore, ScanBounds};

/// `AccountCommand` as JSON, in serde's default externally tagged shape
/// (the shape the SQLite engine's `AccountCommand` derives).
#[derive(Serialize)]
enum CommandJson<'a> {
    FoundLocalFirst {
        principal: &'a Principal,
        app: Uuid,
    },
    Register {
        principal: &'a Principal,
        account: Uuid,
    },
    RequestLink {
        approver: &'a Principal,
        candidate: &'a Principal,
        nonce: Uuid,
        now: u64,
        expires_at: u64,
    },
    AcceptLink {
        candidate: &'a Principal,
        nonce: Uuid,
        now: u64,
    },
    Revoke {
        approver: &'a Principal,
        target: &'a Principal,
    },
}

fn command_json(command: &AccountCommand) -> CommandJson<'_> {
    match command {
        AccountCommand::FoundLocalFirst { principal, app } => CommandJson::FoundLocalFirst {
            principal,
            app: *app,
        },
        AccountCommand::Register { principal, account } => CommandJson::Register {
            principal,
            account: account.0,
        },
        AccountCommand::RequestLink {
            approver,
            candidate,
            nonce,
            now,
            expires_at,
        } => CommandJson::RequestLink {
            approver,
            candidate,
            nonce: *nonce,
            now: *now,
            expires_at: *expires_at,
        },
        AccountCommand::AcceptLink {
            candidate,
            nonce,
            now,
        } => CommandJson::AcceptLink {
            candidate,
            nonce: *nonce,
            now: *now,
        },
        AccountCommand::Revoke { approver, target } => CommandJson::Revoke { approver, target },
    }
}

/// The journal's commands in revision order, as JSON. Fails on a gap, like
/// `StoredAccountRegistry::open`.
pub async fn migrate_export_account_commands<S: OrderedKvStorage>(
    storage: &S,
) -> Result<Vec<String>, String> {
    let descriptor = codec::descriptor();
    let records = RecordStore::new(storage, "default", &descriptor);
    let mut scan = records
        .scan(ScanBounds::Prefix(b"account-command:".to_vec()))
        .await
        .map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    while let Some(batch) = scan.next_batch().await.map_err(|error| error.to_string())? {
        for (key, bytes) in batch {
            let mut expected = b"account-command:v1:".to_vec();
            expected.extend_from_slice(&(out.len() as u64).to_be_bytes());
            if key != expected {
                return Err("invalid account journal sequence".to_owned());
            }
            let command = codec::decode(&bytes)?;
            out.push(
                serde_json::to_string(&command_json(&command))
                    .map_err(|error| error.to_string())?,
            );
        }
    }
    Ok(out)
}
