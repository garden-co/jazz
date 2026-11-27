//! The v1 configuration contains only routing options. Credentials are read on the runner.
use jazz_server::{BlobStore, s3::S3Options, vercel_blob::VercelBlobOptions};
use serde::{Deserialize, Serialize};

use rivetkit::{
    BindParam, ColumnValue, SqliteDb,
    prelude::{Result as ActorResult, anyhow},
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Destination {
    #[default]
    Sqlite,
    S3 {
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        prefix: Option<String>,
    },
    VercelBlob {
        #[serde(rename = "storeId")]
        store_id: Option<String>,
        prefix: Option<String>,
    },
}

impl Destination {
    // The table holds at most one immutable v1 JSON record. An empty table
    // (including an upgraded actor) means its chunks are in SQLite.
    pub async fn load(db: &SqliteDb) -> ActorResult<Self> {
        db.exec("CREATE TABLE IF NOT EXISTS jazz_rivet_blobs_v1 (id INTEGER PRIMARY KEY CHECK(id = 1), destination TEXT NOT NULL)").await?;
        let result = db
            .query(
                "SELECT destination FROM jazz_rivet_blobs_v1 WHERE id = 1",
                None,
            )
            .await?;
        match result.rows.first().map(Vec::as_slice) {
            None => Ok(Self::Sqlite),
            Some([ColumnValue::Text(s)]) => Ok(serde_json::from_str(s)?),
            _ => Err(anyhow!("invalid stored Rivet blob destination")),
        }
    }

    pub async fn pin(&self, db: &SqliteDb) -> ActorResult<()> {
        db.run(
            "INSERT INTO jazz_rivet_blobs_v1 (id, destination) VALUES (1, ?)",
            Some(vec![BindParam::Text(serde_json::to_string(self)?)]),
        )
        .await?;
        Ok(())
    }

    /// Resolve environment defaults before persisting, so a later deployment
    /// cannot silently redirect existing chunks to another bucket or prefix.
    pub fn resolve(&self, owner: &str) -> Result<(Self, BlobStore), String> {
        let prefix = |v: &Option<String>| {
            Some(
                v.as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
                    .unwrap_or_else(|| format!("jazz/{owner}/")),
            )
        };
        match self {
            Self::Sqlite => Ok((Self::Sqlite, BlobStore::Sqlite)),
            Self::S3 {
                bucket,
                region,
                endpoint,
                prefix: p,
            } => {
                let options = S3Options::from_env(
                    bucket.clone(),
                    region.clone(),
                    endpoint.clone(),
                    prefix(p),
                )?;
                Ok((
                    Self::S3 {
                        bucket: Some(options.bucket.clone()),
                        region: Some(options.region.clone()),
                        endpoint: options.endpoint.clone(),
                        prefix: Some(options.prefix.clone()),
                    },
                    BlobStore::S3(options),
                ))
            }
            Self::VercelBlob {
                store_id,
                prefix: p,
            } => {
                let options = VercelBlobOptions::from_env(store_id.clone(), prefix(p), None)?;
                if options.store_id.is_empty()
                    || !options
                        .store_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                {
                    return Err("vercel blobs: invalid store id".into());
                }
                Ok((
                    Self::VercelBlob {
                        store_id: Some(options.store_id.clone()),
                        prefix: Some(options.prefix.clone()),
                    },
                    BlobStore::VercelBlob(options),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Alice's actor reopens the same destination from the v1 bytes across runner upgrades.
    #[test]
    fn v1_destination_bytes() {
        for bytes in [
            &br#"{"kind":"sqlite"}"#[..],
            br#"{"kind":"s3","bucket":"notes","region":"auto","endpoint":"https://storage.example","prefix":"jazz/alice/"}"#,
            br#"{"kind":"vercel-blob","storeId":"abc","prefix":"jazz/alice/"}"#,
        ] {
            let dest: Destination = serde_json::from_slice(bytes).unwrap();
            assert_eq!(serde_json::to_vec(&dest).unwrap(), bytes);
        }
    }
}
