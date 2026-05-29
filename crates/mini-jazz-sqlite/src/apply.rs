use crate::schema::SchemaDef;
use crate::sync::{Bundle, BUNDLE_PROTOCOL_VERSION};
use crate::time::now_ms;
use crate::{branch, rows, tx, users, Result};
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct BundleApplyPlan {
    policy_tables: BTreeSet<String>,
    touched_tables: BTreeSet<String>,
}

impl BundleApplyPlan {
    pub(crate) fn validate(
        schema: &SchemaDef,
        bundle: &Bundle,
        check_policy_fingerprint: bool,
    ) -> Result<Self> {
        if bundle.protocol_version != BUNDLE_PROTOCOL_VERSION {
            return Err(crate::Error::new(format!(
                "unsupported bundle protocol version {}",
                bundle.protocol_version
            )));
        }
        let local_schema_fingerprint = schema.compatibility_fingerprint();
        if bundle.schema_fingerprint != "legacy"
            && bundle.schema_fingerprint != local_schema_fingerprint
        {
            return Err(crate::Error::new("incompatible schema fingerprint"));
        }

        let policy_tables = bundle_policy_tables(bundle);
        if check_policy_fingerprint {
            for table_name in &policy_tables {
                schema.table_def(table_name)?;
            }
            if !policy_tables.is_empty() {
                let local_policy_fingerprint =
                    schema.policy_fingerprint_for_tables(policy_tables.iter());
                if bundle.policy_fingerprint != "legacy"
                    && bundle.policy_fingerprint != local_policy_fingerprint
                {
                    return Err(crate::Error::new("incompatible policy fingerprint"));
                }
            }
        }

        Ok(Self {
            policy_tables,
            touched_tables: bundle_touched_tables(bundle),
        })
    }

    pub(crate) fn touched_tables(&self) -> &BTreeSet<String> {
        &self.touched_tables
    }

    #[allow(dead_code)]
    pub(crate) fn policy_tables(&self) -> &BTreeSet<String> {
        &self.policy_tables
    }
}

pub(crate) struct AppliedTxs {
    pub(crate) tx_nums_by_id: BTreeMap<String, i64>,
    pub(crate) tx_info_by_num: BTreeMap<i64, ApplyTxInfo>,
}

#[derive(Default)]
pub(crate) struct ApplyCaches {
    row_nums_by_id: BTreeMap<(String, String), i64>,
    row_nums_created_in_apply: BTreeSet<i64>,
    user_nums_by_id: BTreeMap<String, i64>,
}

impl ApplyCaches {
    pub(crate) fn ensure_row_id(
        &mut self,
        db: &Connection,
        table_name: &str,
        row_id: &str,
    ) -> Result<i64> {
        if let Some(row_num) = self
            .row_nums_by_id
            .get(&(table_name.to_owned(), row_id.to_owned()))
        {
            return Ok(*row_num);
        }
        let row_num = rows::ensure_row_id(db, table_name, row_id)?;
        self.row_nums_by_id
            .insert((table_name.to_owned(), row_id.to_owned()), row_num);
        Ok(row_num)
    }

    pub(crate) fn ensure_row_id_with_status(
        &mut self,
        db: &Connection,
        table_name: &str,
        row_id: &str,
    ) -> Result<i64> {
        if let Some(row_num) = self
            .row_nums_by_id
            .get(&(table_name.to_owned(), row_id.to_owned()))
        {
            return Ok(*row_num);
        }
        let (row_num, created) = rows::ensure_row_id_with_status(db, row_id)?;
        self.row_nums_by_id
            .insert((table_name.to_owned(), row_id.to_owned()), row_num);
        if created {
            self.row_nums_created_in_apply.insert(row_num);
        }
        Ok(row_num)
    }

    pub(crate) fn row_created_in_apply(&self, row_num: i64) -> bool {
        self.row_nums_created_in_apply.contains(&row_num)
    }

    pub(crate) fn ensure_user(&mut self, db: &Connection, user_id: &str) -> Result<i64> {
        if let Some(user_num) = self.user_nums_by_id.get(user_id) {
            return Ok(*user_num);
        }
        let user_num = users::ensure_user(db, user_id)?;
        self.user_nums_by_id.insert(user_id.to_owned(), user_num);
        Ok(user_num)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ApplyTxInfo {
    pub(crate) node_num: i64,
    pub(crate) outcome: i64,
    pub(crate) conflict_mode: i64,
}

pub(crate) fn apply_tx_records(db: &Connection, bundle: &Bundle) -> Result<AppliedTxs> {
    let mut tx_nums_by_id = BTreeMap::new();
    let mut tx_info_by_num = BTreeMap::new();
    for tx_record in &bundle.txs {
        let node_num = tx::ensure_node(db, &tx_record.node_id)?;
        let metadata_json = tx_metadata_json(tx_record.auth_user.as_deref())?;
        db.execute(
            "INSERT INTO jazz_tx
             (tx_id, node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tx_id) DO UPDATE SET
               outcome = MAX(jazz_tx.outcome, excluded.outcome),
               global_epoch = CASE
                 WHEN jazz_tx.global_epoch IS NULL THEN excluded.global_epoch
                 WHEN excluded.global_epoch IS NULL THEN jazz_tx.global_epoch
                 ELSE MAX(jazz_tx.global_epoch, excluded.global_epoch)
               END,
               conflict_mode = MAX(jazz_tx.conflict_mode, excluded.conflict_mode),
               metadata_json = CASE
                 WHEN excluded.metadata_json != '{}' THEN excluded.metadata_json
                 ELSE jazz_tx.metadata_json
               END",
            params![
                tx_record.tx_id,
                node_num,
                tx_record.local_epoch,
                tx_record.global_epoch,
                tx::KIND_DATA,
                tx_record.conflict_mode,
                tx_record.outcome,
                tx_record.created_at,
                metadata_json
            ],
        )?;
        let tx_num = tx::tx_num(db, &tx_record.tx_id)?;
        tx_nums_by_id.insert(tx_record.tx_id.clone(), tx_num);
        tx_info_by_num.insert(tx_num, tx_apply_info(db, tx_num)?);
        if tx_record.outcome == tx::OUTCOME_REJECTED {
            if let Some(code) = &tx_record.rejection_code {
                let detail_json = tx::encode_optional_json(tx_record.rejection_detail.as_ref())?;
                db.execute(
                    "INSERT INTO jazz_tx_rejection (tx_num, code, detail_json)
                     VALUES (?, ?, ?)
                     ON CONFLICT(tx_num) DO UPDATE SET
                       code = excluded.code,
                       detail_json = CASE
                         WHEN excluded.detail_json = 'null' AND jazz_tx_rejection.detail_json != 'null' THEN jazz_tx_rejection.detail_json
                         ELSE excluded.detail_json
                       END",
                    params![tx_num, code, detail_json],
                )?;
            }
        }
        if let Some(global_epoch) = tx_record.global_epoch {
            db.execute(
                "INSERT OR REPLACE INTO jazz_tx_receipt
                 (tx_num, tier, observed_at, receipt_json)
                 VALUES (?, ?, ?, '{}')",
                params![tx_num, tx::TIER_GLOBAL, global_epoch],
            )?;
        }
        for tier in &tx_record.receipt_tiers {
            let observed_at = if *tier == tx::TIER_GLOBAL {
                tx_record.global_epoch.unwrap_or(tx_record.created_at)
            } else {
                tx_record.created_at
            };
            db.execute(
                "INSERT OR REPLACE INTO jazz_tx_receipt
                 (tx_num, tier, observed_at, receipt_json)
                 VALUES (?, ?, ?, '{}')",
                params![tx_num, tier, observed_at],
            )?;
        }
    }

    Ok(AppliedTxs {
        tx_nums_by_id,
        tx_info_by_num,
    })
}

pub(crate) fn apply_branch_records(
    db: &Connection,
    bundle: &Bundle,
) -> Result<BTreeMap<String, i64>> {
    for branch_record in &bundle.branches {
        let branch_num = branch::ensure(
            db,
            &branch_record.branch_id,
            branch_record.base_global_epoch,
            now_ms(),
        )?;
        branch::set_sources_from_sync(
            db,
            branch_num,
            &branch_record.source_branch_ids,
            branch_record.source_version,
        )?;
    }
    let mut branch_nums_by_id = BTreeMap::new();
    for branch_record in &bundle.branches {
        let branch_num = branch::checkout(db, &branch_record.branch_id)?;
        branch_nums_by_id.insert(branch_record.branch_id.clone(), branch_num);
    }
    Ok(branch_nums_by_id)
}

pub(crate) fn apply_read_records(
    db: &Connection,
    bundle: &Bundle,
    applied_txs: &AppliedTxs,
    table_nums_by_name: &BTreeMap<String, i64>,
    apply_caches: &mut ApplyCaches,
) -> Result<()> {
    let mut insert_read_stmt = db.prepare(
        "INSERT OR REPLACE INTO jazz_tx_read
         (tx_num, table_num, row_num, reason, observed_tx_num)
         VALUES (?, ?, ?, ?, ?)",
    )?;
    for read_record in &bundle.reads {
        let tx_num = applied_txs
            .tx_nums_by_id
            .get(&read_record.tx_id)
            .copied()
            .ok_or_else(|| crate::Error::new("bundle read references missing tx"))?;
        let row_num =
            apply_caches.ensure_row_id_with_status(db, &read_record.table, &read_record.row_id)?;
        let table_num = table_nums_by_name
            .get(&read_record.table)
            .copied()
            .ok_or_else(|| crate::Error::new("bundle read references missing table"))?;
        let observed_tx_num = read_record
            .observed_tx_id
            .as_deref()
            .map(|observed_tx_id| {
                applied_txs
                    .tx_nums_by_id
                    .get(observed_tx_id)
                    .copied()
                    .ok_or_else(|| crate::Error::new("bundle read references missing observed tx"))
            })
            .transpose()?;
        insert_read_stmt.execute(params![
            tx_num,
            table_num,
            row_num,
            read_record.reason,
            observed_tx_num
        ])?;
    }
    Ok(())
}

pub(crate) fn apply_query_read_records(db: &Connection, bundle: &Bundle) -> Result<()> {
    for query_read in &bundle.query_reads {
        db.execute(
            "INSERT OR REPLACE INTO jazz_query_read
             (branch_id, table_name, field_name, op, value_json, observed_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                query_read.branch_id,
                query_read.table,
                query_read.field,
                query_read.op,
                serde_json::to_string(&query_read.value)
                    .map_err(|err| crate::Error::new(err.to_string()))?,
                now_ms()
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn tx_apply_info(conn: &Connection, tx_num: i64) -> Result<ApplyTxInfo> {
    Ok(conn.query_row(
        "SELECT node_num, outcome, conflict_mode FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| {
            Ok(ApplyTxInfo {
                node_num: row.get(0)?,
                outcome: row.get(1)?,
                conflict_mode: row.get(2)?,
            })
        },
    )?)
}

fn bundle_policy_tables(bundle: &Bundle) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for record in &bundle.history {
        tables.insert(record.table.clone());
    }
    for query_read in &bundle.query_reads {
        tables.insert(query_read.table.clone());
    }
    tables
}

fn tx_metadata_json(auth_user: Option<&str>) -> Result<String> {
    let mut metadata = serde_json::Map::new();
    if let Some(auth_user) = auth_user {
        metadata.insert(
            "auth_user".to_owned(),
            JsonValue::String(auth_user.to_owned()),
        );
    }
    serde_json::to_string(&JsonValue::Object(metadata))
        .map_err(|err| crate::Error::new(err.to_string()))
}

fn bundle_touched_tables(bundle: &Bundle) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for record in &bundle.history {
        tables.insert(record.table.clone());
    }
    for record in &bundle.reads {
        tables.insert(record.table.clone());
    }
    for query_read in &bundle.query_reads {
        tables.insert(query_read.table.clone());
    }
    tables
}
