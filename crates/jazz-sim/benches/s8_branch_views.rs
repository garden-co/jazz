use std::collections::BTreeMap;
use std::time::Instant;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, MergeableTxOps, ReadOpts, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::ContributionMergeRow;
use jazz::protocol::{BranchSelector, BranchViewBase, SnapshotRef};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::time::GlobalTime;
use jazz::tools::public_schema::{ColumnType as PublicColumnType, SchemaBuilder, TableSchema};
use jazz_sim::{emit_json_line, metadata_fields};
use serde_json::{Value as JsonValue, json};

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = if std::env::var_os("JAZZ_SMOKE").is_some() {
        64
    } else {
        env_usize("JAZZ_S8_ROWS", 1_000)
    };
    run(rows.max(1));
}

pub(crate) fn run(row_count: usize) {
    let schema = schema();
    let families = schema.column_families();
    let db = block_on(Db::open_history_complete(
        DbConfig::new(
            schema.clone(),
            MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>())
                .expect("valid memory storage families"),
            DbIdentity {
                node: NodeUuid::from_bytes([0x58; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x5800)),
    ))
    .unwrap();
    let base = selector(0x01);
    let head = selector(0x02);

    let seed_started = Instant::now();
    let seed = block_on(db.mergeable_tx()).unwrap();
    for index in 0..row_count {
        block_on(seed.insert(
            "items",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String(format!("item-{index:08}")),
                ),
                ("rank".to_owned(), Value::I64(index as i64)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row(index)),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let seed_tx = block_on(seed.commit()).unwrap();
    let seed_us = seed_started.elapsed().as_micros() as u64;

    let overlay_started = Instant::now();
    let overlay = block_on(db.mergeable_tx()).unwrap();
    let overlaid = (row_count / 4).max(1);
    for index in 0..overlaid {
        block_on(overlay.update(
            "items",
            row(index),
            BTreeMap::from([(
                "title".to_owned(),
                Value::String(format!("draft-{index:08}")),
            )]),
            jazz::db::UpdateOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(overlay.commit()).unwrap();
    let overlay_us = overlay_started.elapsed().as_micros() as u64;

    let all = db.prepare_query(&Query::from("items")).unwrap();
    let live_opts =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));
    let frozen_opts = ReadOpts::default().branch_view(
        head.clone(),
        Some(BranchViewBase::Snapshot {
            branch: base.clone(),
            snapshot: SnapshotRef {
                owner: seed_tx.node,
                global_base: GlobalTime(0),
                local_base: seed_tx.time,
                dots: Vec::new(),
            },
        }),
    );
    let live_started = Instant::now();
    let live_rows = block_on(db.all(&all, live_opts)).unwrap();
    let live_read_us = live_started.elapsed().as_micros() as u64;
    let frozen_started = Instant::now();
    let frozen_rows = block_on(db.all(&all, frozen_opts)).unwrap();
    let frozen_read_us = frozen_started.elapsed().as_micros() as u64;
    assert_eq!(live_rows.len(), row_count);
    assert_eq!(frozen_rows.len(), row_count);

    let needle = row_count.saturating_sub(1);
    let indexed = db
        .prepare_query(&Query::from("items").filter(eq(
            col("title"),
            lit(Value::String(format!("item-{needle:08}"))),
        )))
        .unwrap();
    let index_started = Instant::now();
    let indexed_rows = block_on(db.all(
        &indexed,
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone()))),
    ))
    .unwrap();
    let indexed_read_us = index_started.elapsed().as_micros() as u64;
    assert_eq!(indexed_rows.len(), usize::from(needle >= overlaid));

    let cross_started = Instant::now();
    let cross = block_on(db.mergeable_tx()).unwrap();
    let cross_row = row(row_count + 1);
    for (branch, title) in [(base.clone(), "base"), (head.clone(), "head")] {
        block_on(cross.insert(
            "items",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("rank".to_owned(), Value::I64(row_count as i64 + 1)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(cross_row),
                target: jazz::db::ExactWriteTarget::Branch(branch),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(cross.commit()).unwrap();
    let cross_branch_tx_us = cross_started.elapsed().as_micros() as u64;

    let merge_rows = (0..overlaid.min(16)).map(|index| ContributionMergeRow {
        table: "items".to_owned(),
        row_uuid: row(index),
    });
    let merge_started = Instant::now();
    let merge_tx = block_on(db.merge_branch_contributions(base, head, merge_rows)).unwrap();
    let contribution_merge_us = merge_started.elapsed().as_micros() as u64;
    assert!(merge_tx.is_some() || overlaid == 0);

    emit_phase(
        "seed_and_overlay",
        row_count,
        json!({
            "seed_us": seed_us,
            "overlay_us": overlay_us,
            "overlay_rows": overlaid,
        }),
    );
    emit_phase(
        "live_and_frozen_base_reads",
        row_count,
        json!({
            "live_read_us": live_read_us,
            "frozen_read_us": frozen_read_us,
            "live_rows": live_rows.len(),
            "frozen_rows": frozen_rows.len(),
        }),
    );
    emit_phase(
        "branch_key_index_and_transactions",
        row_count,
        json!({
            "indexed_read_us": indexed_read_us,
            "indexed_rows": indexed_rows.len(),
            "cross_branch_tx_us": cross_branch_tx_us,
            "contribution_merge_us": contribution_merge_us,
        }),
    );
}

fn schema() -> JazzSchema {
    let public_schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("branch_id", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .column("rank", PublicColumnType::BigInt)
                .branch_by("branch_id")
                .index_only(["title"]),
        )
        .build();
    JazzSchema::new(&public_schema).expect("S8 public schema compiles")
}

fn selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000_u64.to_be_bytes());
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn emit_phase(phase: &str, rows: usize, values: JsonValue) {
    let mut fields = metadata_fields("s8_branch_views", "synchronous", 0x5800_0001, "s8-local");
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("rows".to_owned(), json!(rows));
    if let JsonValue::Object(values) = values {
        fields.extend(values);
    }
    emit_json_line("s8_branch_views", &JsonValue::Object(fields).to_string());
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
