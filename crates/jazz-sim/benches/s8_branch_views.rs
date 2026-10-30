//! S8 branch views: a large base branch, a sparse head overlay, live and
//! frozen bases, indexed reads through the overlay, a transaction that
//! writes two branch keys, and a client that syncs a branch view.
//!
//! Gates: the head view reads the overlay where it exists and the base
//! everywhere else; a frozen base ignores base writes after its snapshot
//! while a live base shows them; an indexed read masks overridden base
//! rows before filtering; both branch rows of a cross-branch transaction
//! land; a synced client's branch view matches the authority's.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::db::{
    Db, ExactWriteTarget, InsertOptions, ReadOpts, UpdateOptions, WriteTarget, block_on,
};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_sim::oracle::{self, Live, Rows};
use jazz_sim::support::{allow_all, cells, compile, config, int, open, settle, text, user};
use jazz_sim::{PeerProfile, Sim, SimulatorTransportCodec, emit_summary, env_usize};
use serde_json::{Value as JsonValue, json};

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    run(env_usize("JAZZ_S8_ROWS", 1_000).max(4), true);
}

#[allow(dead_code)]
pub fn smoke() {
    run(64, false);
}

fn schema() -> JazzSchema {
    compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::BigInt)
                    .branch_by("branch_id")
                    .index_only(["title"])
                    .policies(allow_all()),
            )
            .build(),
    )
}

fn selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn row(index: usize) -> RowUuid {
    jazz_sim::support::row(0x80, index as u64)
}

fn item(title: String, rank: usize) -> BTreeMap<String, Value> {
    cells([("title", text(title)), ("rank", int(rank as i64))])
}

/// What a branch view reads, without the branch key column.
fn titles(rows: &Rows) -> BTreeMap<RowUuid, Value> {
    rows.iter()
        .map(|(id, cells)| (*id, cells.get("title").cloned().expect("title")))
        .collect()
}

fn run(row_count: usize, emit: bool) {
    let mut sim = Sim::new(0x5800_0001, SimulatorTransportCodec::WireFrames);
    let core = block_on(Db::open_history_complete(config(
        schema(),
        0,
        AuthorSubject::SYSTEM,
    )))
    .expect("open authority");
    sim.add("core", core);
    let db = sim.db("core");
    let base = selector(0x01);
    let head = selector(0x02);
    let live_base =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));
    let all = Query::from("items");

    // Seed the base branch.
    let seed_started = Instant::now();
    let seed = block_on(db.mergeable_tx()).expect("seed transaction");
    for index in 0..row_count {
        block_on(seed.insert(
            "items",
            item(format!("item-{index:08}"), index),
            InsertOptions {
                row_id: Some(row(index)),
                target: ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        ))
        .expect("seed row");
    }
    block_on(seed.commit()).expect("commit seed");
    let seed_us = seed_started.elapsed().as_micros() as u64;
    let mut expected_base: BTreeMap<RowUuid, Value> = (0..row_count)
        .map(|i| (row(i), text(format!("item-{i:08}"))))
        .collect();

    // A sparse overlay: every fourth row gets a draft title in the head.
    let overlay_started = Instant::now();
    let overlay = block_on(db.mergeable_tx()).expect("overlay transaction");
    let overlaid: Vec<usize> = (0..row_count).step_by(4).collect();
    for index in &overlaid {
        block_on(overlay.update(
            "items",
            row(*index),
            cells([("title", text(format!("draft-{index:08}")))]),
            UpdateOptions {
                target: WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        ))
        .expect("overlay update");
    }
    block_on(overlay.commit()).expect("commit overlay");
    let overlay_us = overlay_started.elapsed().as_micros() as u64;
    let mut expected_head = expected_base.clone();
    for index in &overlaid {
        expected_head.insert(row(*index), text(format!("draft-{index:08}")));
    }

    // Freeze the base, then keep writing to it.
    let snapshot = sim.drive(db.capture_snapshot()).expect("snapshot");
    let frozen_base = ReadOpts::default().branch_view(
        head.clone(),
        Some(BranchViewBase::Snapshot {
            branch: base.clone(),
            snapshot,
        }),
    );
    let frozen_expected = expected_head.clone();
    let moved = row_count - 1;
    assert!(
        !overlaid.contains(&moved),
        "the moved base row is inherited, not overridden"
    );
    block_on(db.update(
        "items",
        row(moved),
        cells([("title", text("base-after-freeze"))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: base.clone(),
                base: None,
            },
            ..Default::default()
        },
    ))
    .expect("base write after the freeze");
    expected_base.insert(row(moved), text("base-after-freeze"));
    expected_head.insert(row(moved), text("base-after-freeze"));

    let live_started = Instant::now();
    let live = titles(&oracle::read(db, &all, live_base.clone()));
    let live_read_us = live_started.elapsed().as_micros() as u64;
    assert_eq!(live, expected_head, "the head over a live base");
    let frozen_started = Instant::now();
    let frozen = titles(&oracle::read(db, &all, frozen_base));
    let frozen_read_us = frozen_started.elapsed().as_micros() as u64;
    assert_eq!(
        frozen, frozen_expected,
        "the head over a frozen base ignores later base writes"
    );
    assert_eq!(
        titles(&oracle::read(
            db,
            &all,
            ReadOpts::default().branch_view(base.clone(), None)
        )),
        expected_base,
        "the base alone never shows the overlay"
    );

    // An indexed read masks an overridden base row before filtering.
    let overridden_title =
        Query::from("items").filter(eq(col("title"), lit(text("item-00000000"))));
    let inherited = 1;
    let inherited_title =
        Query::from("items").filter(eq(col("title"), lit(text(format!("item-{inherited:08}")))));
    let index_started = Instant::now();
    assert!(
        oracle::read(db, &overridden_title, live_base.clone()).is_empty(),
        "the overridden base row does not match through the head"
    );
    assert_eq!(
        oracle::ids(db, &inherited_title, live_base.clone()),
        [row(inherited)].into(),
        "an inherited base row matches through the head"
    );
    let indexed_read_us = index_started.elapsed().as_micros() as u64;

    // One transaction writes the same row id under both branch keys.
    let cross_started = Instant::now();
    let cross_row = row(row_count + 1);
    let cross = block_on(db.mergeable_tx()).expect("cross-branch transaction");
    for (branch, title) in [(base.clone(), "base"), (head.clone(), "head")] {
        block_on(cross.insert(
            "items",
            item(title.to_owned(), row_count + 1),
            InsertOptions {
                row_id: Some(cross_row),
                target: ExactWriteTarget::Branch(branch),
                ..Default::default()
            },
        ))
        .expect("cross-branch insert");
    }
    block_on(cross.commit()).expect("commit cross-branch transaction");
    let cross_branch_tx_us = cross_started.elapsed().as_micros() as u64;
    expected_base.insert(cross_row, text("base"));
    expected_head.insert(cross_row, text("head"));
    assert_eq!(
        titles(&oracle::read(db, &all, live_base.clone())),
        expected_head
    );
    assert_eq!(
        titles(&oracle::read(
            db,
            &all,
            ReadOpts::default().branch_view(base.clone(), None)
        )),
        expected_base
    );

    // A client syncs the head view and edits through it.
    sim.add("client", open(schema(), 1, user(1)));
    sim.connect("client", "core", user(1), PeerProfile::new("s8", 2, 1, 0));
    let mut synced = Live::subscribe(sim.db("client"), &all, live_base.clone());
    sim.settle();
    synced.poll();
    assert_eq!(
        titles(&synced.rows),
        expected_head,
        "the client's head view matches the authority's"
    );
    let edited = row(inherited);
    let handle = block_on(sim.db("client").update(
        "items",
        edited,
        cells([("title", text("client-draft"))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
    ))
    .expect("client edits through the head");
    settle(&sim, sim.db("client"), &handle);
    sim.settle();
    expected_head.insert(edited, text("client-draft"));
    synced.poll();
    assert_eq!(
        titles(&synced.rows),
        expected_head,
        "the client's edit shows in its view"
    );
    assert_eq!(
        titles(&oracle::read(sim.db("core"), &all, live_base)),
        expected_head,
        "the authority holds the client's head edit"
    );
    assert_eq!(
        titles(&oracle::read(
            sim.db("core"),
            &all,
            ReadOpts::default().branch_view(base, None)
        )),
        expected_base,
        "the client's head edit copied the row, leaving the base alone"
    );

    if emit {
        let mut fields = sim.json_fields();
        fields.extend(
            json!({
                "phase": "summary",
                "rows": row_count,
                "overlay_rows": overlaid.len(),
                "seed_us": seed_us,
                "overlay_us": overlay_us,
                "live_read_us": live_read_us,
                "frozen_read_us": frozen_read_us,
                "indexed_read_us": indexed_read_us,
                "cross_branch_tx_us": cross_branch_tx_us,
            })
            .as_object()
            .expect("object")
            .clone(),
        );
        emit_summary(
            "s8_branch_views",
            "deterministic",
            0x5800_0001,
            "s8",
            JsonValue::Object(fields),
        );
    }
}
