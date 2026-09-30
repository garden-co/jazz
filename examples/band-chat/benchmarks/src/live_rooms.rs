//! Many open room views, one new message: the cost of a write that only one
//! of many live subscriptions of the same query shape needs to see.
//!
//! Moved from `crates/jazz/benches/route_subscription_curve.rs`
//! (`matching_write_fanout[100]`), reframed in BandChat terms. The fixture,
//! query shape, write and assertions are unchanged: routes are rooms,
//! documents are messages, `updated_at` is `sent_at`. The crate bench keeps
//! the scale receipt (`JAZZ_ROUTE_CURVE_RECEIPT`) for diagnosis.
//!
//! 1,001 rooms. The busy room holds 1,000 messages; every other room holds
//! one. `ROOMS_OPEN` room views bind one prepared shape (room = param, newest
//! first, limit 100) to rooms `0..ROOMS_OPEN`, as that many members each keep
//! a different room open. One new message lands in the busy room: its view
//! gains the message and drops its oldest shown one; every other view stays
//! quiet.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, param};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const MESSAGES: &str = "live_room_messages";
const ROOMS: usize = 1_001;
const BUSY_ROOM_MESSAGES: usize = 1_000;
const PAGE_SIZE: usize = 100;
const MAX_OPEN_ROOMS: usize = 1_000;
const WRITER: AuthorSubject = AuthorSubject::SYSTEM;

/// Seeded rooms; no views open yet.
pub struct LiveRoomsFixture {
    db: Db,
    rooms_open: usize,
    query: Query,
}

/// `rooms_open` hydrated room views, ready for the new message.
pub struct OpenRooms {
    fixture: LiveRoomsFixture,
    streams: Vec<SubscriptionStream>,
}

impl LiveRoomsFixture {
    pub fn seeded(rooms_open: usize) -> Self {
        assert!((1..=MAX_OPEN_ROOMS).contains(&rooms_open));
        let db = open_db(rooms_open as u64);
        for ordinal in 0..BUSY_ROOM_MESSAGES {
            insert_message(&db, message_row(0, ordinal), 0, ordinal as u64);
        }
        for room in 1..ROOMS {
            insert_message(&db, message_row(room, 0), room, room as u64);
        }
        Self {
            db,
            rooms_open,
            query: room_view_query(),
        }
    }

    /// Open every room view and consume its exact initial page. Also asserts
    /// that storage-backed views retain no source-version witnesses but keep
    /// replacement witnesses for delete/restore winner delivery.
    pub fn open_all(self) -> OpenRooms {
        let mut streams = Vec::with_capacity(self.rooms_open);
        let mut shape_id = None;
        for room in 0..self.rooms_open {
            let prepared = self
                .db
                .prepare_query_bound(
                    &self.query,
                    BTreeMap::from([("room".to_owned(), Value::Uuid(room_row(room).0))]),
                )
                .expect("prepare room view");
            if let Some(expected_shape) = shape_id {
                assert_eq!(prepared.shape().shape_id(), expected_shape);
            } else {
                shape_id = Some(prepared.shape().shape_id());
            }
            let mut stream =
                block_on(self.db.subscribe(&prepared, local_opts())).expect("open room view");
            assert_eq!(take_initial_reset(&mut stream), expected_initial_rows(room));
            streams.push(stream);
        }
        let stats = self.db.runtime_stats_for_test();
        assert_eq!(stats.active_subscriptions, self.rooms_open);
        let receipts = self.db.maintained_subscription_size_receipts_for_test();
        assert_eq!(receipts.len(), self.rooms_open);
        let total = |field: &dyn Fn(usize) -> usize| (0..receipts.len()).map(field).sum::<usize>();
        let source_witnesses = total(&|i| receipts[i].footprint.version_identities)
            + total(&|i| receipts[i].footprint.versions_bytes);
        assert_eq!(
            source_witnesses, 0,
            "storage-backed room views retain source witnesses"
        );
        assert!(
            total(&|i| receipts[i].footprint.replacement_entries) > 0
                && total(&|i| receipts[i].footprint.replacements_bytes) > 0,
            "storage-backed room views must retain replacement witnesses"
        );
        OpenRooms {
            fixture: self,
            streams,
        }
    }
}

impl OpenRooms {
    /// The timed operation: one new message in the busy room, then drain every
    /// open view. Asserts the busy room's exact delta and quiet elsewhere.
    pub fn new_message(mut self) -> Self {
        let new_row = message_row(0, BUSY_ROOM_MESSAGES + 1);
        insert_message(&self.fixture.db, new_row, 0, BUSY_ROOM_MESSAGES as u64 + 1);
        let deltas = drain_events(&mut self.streams);
        assert!(deltas.first().is_some_and(|delta| {
            delta.events == 1
                && delta.added == BTreeSet::from([new_row])
                && delta.removed == BTreeSet::from([message_row(0, BUSY_ROOM_MESSAGES - PAGE_SIZE)])
                && delta.updated.is_empty()
                && !delta.reset
        }));
        assert!(deltas.iter().skip(1).all(|delta| delta.events == 0));
        self
    }

    /// Untimed oracle: a message in a room nobody has open, and an older-than-
    /// the-page message in the busy room, wake no view.
    pub fn unrelated_messages_are_quiet(mut self) -> bool {
        insert_message(
            &self.fixture.db,
            message_row(MAX_OPEN_ROOMS, 1),
            MAX_OPEN_ROOMS,
            1,
        );
        let unrelated = drain_events(&mut self.streams);
        insert_message(
            &self.fixture.db,
            message_row(0, BUSY_ROOM_MESSAGES + 2),
            0,
            0,
        );
        let below_page = drain_events(&mut self.streams);
        unrelated
            .iter()
            .chain(&below_page)
            .all(|delta| delta.events == 0)
    }
}

#[derive(Default)]
struct Delta {
    events: usize,
    reset: bool,
    added: BTreeSet<RowUuid>,
    updated: BTreeSet<RowUuid>,
    removed: BTreeSet<RowUuid>,
}

fn drain_events(streams: &mut [SubscriptionStream]) -> Vec<Delta> {
    streams
        .iter_mut()
        .map(|stream| {
            let mut delta = Delta::default();
            while let Some(event) = stream.try_next_event() {
                delta.events += 1;
                match event {
                    SubscriptionEvent::Delta {
                        reset,
                        added,
                        updated,
                        removed,
                        ..
                    } => {
                        delta.reset |= reset;
                        delta
                            .added
                            .extend(added.into_iter().map(|row| row.row_uuid()));
                        delta
                            .updated
                            .extend(updated.into_iter().map(|row| row.row_uuid()));
                        delta
                            .removed
                            .extend(removed.into_iter().map(|row| row.row_uuid));
                    }
                    SubscriptionEvent::Rejected { reason } => {
                        panic!("room view rejected: {reason:?}")
                    }
                    SubscriptionEvent::Closed => panic!("room view closed"),
                }
            }
            delta
        })
        .collect()
}

fn take_initial_reset(stream: &mut SubscriptionStream) -> BTreeSet<RowUuid> {
    match stream
        .try_next_event()
        .expect("room view did not emit an initial reset")
    {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            added.into_iter().map(|row| row.row_uuid()).collect()
        }
        other => panic!("expected initial reset, got {other:?}"),
    }
}

fn expected_initial_rows(room: usize) -> BTreeSet<RowUuid> {
    if room == 0 {
        (BUSY_ROOM_MESSAGES - PAGE_SIZE..BUSY_ROOM_MESSAGES)
            .map(|ordinal| message_row(room, ordinal))
            .collect()
    } else {
        BTreeSet::from([message_row(room, 0)])
    }
}

fn open_db(seed: u64) -> Db {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(MESSAGES)
                    .column("room", ColumnType::Uuid)
                    .column("sent_at", ColumnType::Timestamp)
                    .column("body", ColumnType::Text),
            )
            .build(),
    )
    .expect("live-rooms schema compiles");
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs).expect("valid memory storage families"),
            DbIdentity {
                node: NodeUuid::from_bytes((0x7600_u128 + seed as u128).to_be_bytes()),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x7600 + seed)),
    ))
    .expect("open live-rooms db")
}

fn insert_message(db: &Db, row: RowUuid, room: usize, sent_at: u64) {
    block_on(db.insert(
        MESSAGES,
        BTreeMap::from([
            ("room".to_owned(), Value::Uuid(room_row(room).0)),
            ("sent_at".to_owned(), Value::U64(sent_at)),
            (
                "body".to_owned(),
                Value::String(format!("room {room} message {sent_at}")),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("insert live-rooms message");
}

fn room_row(room: usize) -> RowUuid {
    tagged_row(0x7601, room as u64)
}

fn message_row(room: usize, ordinal: usize) -> RowUuid {
    tagged_row(0x7602 + room as u64, ordinal as u64)
}

fn tagged_row(namespace: u64, value: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&namespace.to_be_bytes());
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn local_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn room_view_query() -> Query {
    Query::from(MESSAGES)
        .filter(eq(col("room"), param("room")))
        .order_by("sent_at", OrderDirection::Desc)
        .limit(PAGE_SIZE)
}
