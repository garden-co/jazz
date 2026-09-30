//! A Global read orders its open after the local writes it must observe.
//!
//! A Global read is answered from the authority's state, so it sees this
//! node's own writes only when they reach the authority first. An ordinary
//! commit queued behind a large value that is still uploading is held back
//! while an open is not (garden-co/jazz#3839). These tests drive the upload at
//! the peer protocol boundary to hold it deterministically: the public client
//! API cannot pause a large value mid-upload or script an upload rejection.

use super::*;
use crate::model::test_support::AllowAll;

fn music_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("tracks").column("title", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("albums").column("title", PublicColumnType::Text))
            .allow_all(),
    )
}

#[derive(Clone)]
struct ManualUploadRetryClock(Rc<Cell<u64>>);

impl UploadRetryClock for ManualUploadRetryClock {
    fn now_ms(&self) -> u64 {
        self.0.get()
    }
}

fn title(value: impl Into<String>) -> RowCells {
    BTreeMap::from([("title".to_owned(), Value::String(value.into()))])
}

/// Large enough to travel as a chunked large value.
fn large_title(label: &str) -> RowCells {
    title(format!("{label}/").repeat(8_000))
}

type ReadFuture<'a> = Pin<Box<dyn Future<Output = Result<SerializedReadResult, Error>> + 'a>>;

/// A default-tier Global read with local updates, as a backend issues it.
fn global_read<'a>(
    db: &'a Db,
    table: &str,
    budget_expired: &'a dyn Fn() -> bool,
) -> ReadFuture<'a> {
    let query = postcard::to_allocvec(&Query::from(table)).unwrap();
    Box::pin(async move {
        db.all_serialized_query(
            &query,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
            None,
            None,
            None,
            true,
            budget_expired,
            |attachment| db.detach_query(attachment),
        )
        .await
    })
}

fn poll_read(read: &mut ReadFuture<'_>) -> Poll<Result<SerializedReadResult, Error>> {
    read.as_mut().poll(&mut Context::from_waker(Waker::noop()))
}

fn row_count(result: SerializedReadResult) -> usize {
    match result {
        SerializedReadResult::Rows(rows) => rows.len(),
        SerializedReadResult::Relation(snapshot) => snapshot.root_count,
    }
}

struct Fixture {
    writer: Db,
    core: CoreDb,
    clock: Rc<Cell<u64>>,
    _links: Vec<Box<dyn std::any::Any>>,
}

impl Fixture {
    /// A writer whose Core accepts an upload start but rate-limits its bytes,
    /// holding a large value until [`Fixture::release_uploads`].
    fn holding_large_values(node: u8) -> Self {
        let schema = music_schema();
        let author = AuthorSubject::for_test_bytes([node; 16]);
        let core = open_core(node + 1, AuthorSubject::SYSTEM, &schema);
        core.node().borrow_mut().set_large_value_staging_policy(
            crate::node::LargeValueStagingPolicy {
                incoming_bytes_per_window:
                    crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES + 1,
                window_ms: 60_000,
                max_age_ms: 10 * 60 * 1_000,
            },
        );
        let writer = open_db(node, author, &schema);
        let clock = Rc::new(Cell::new(10_000));
        writer
            .node
            .set_upload_retry_clock_for_test(Rc::new(ManualUploadRetryClock(Rc::clone(&clock))));
        writer.set_tick_scheduler(Some(Rc::new(RecordingScheduler::default())));
        let (writer_transport, core_transport, _) =
            duplex_with_admitted_session_context_and_client_outbound_tap(
                author,
                NodeUuid::from_bytes([node; 16]),
                1,
                NodeUuid::from_bytes([node + 1; 16]),
                1,
            );
        let upstream = crate::local_executor::block_on(writer.connect_upstream(writer_transport));
        let subscriber = core.accept_subscriber(core_transport, author);
        Self {
            writer,
            core,
            clock,
            _links: vec![Box::new(upstream), Box::new(subscriber)],
        }
    }

    fn pump(&self, rounds: usize) {
        for _ in 0..rounds {
            self.writer.tick().unwrap();
            self.core.tick().unwrap();
        }
    }

    fn release_uploads(&self) {
        self.core
            .node()
            .borrow_mut()
            .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy::default());
        self.clock.set(self.clock.get() + 60_000);
    }

    fn is_global(&self, tx_id: TxId) -> bool {
        self.writer.write_state(tx_id).unwrap().durability == DurabilityTier::Global
    }
}

/// alice streams a track, then writes another; her Global read of `tracks`
/// waits until both are on the wire, without spending its coverage budget,
/// and then sees both.
///
/// ```text
/// alice ──large track──► (held by core) ··· release ──► core
/// alice ──plain track──────────────────────────────────► core
/// alice ──read tracks── waits ─────────────── open ────► core ──► 2 rows
/// ```
#[test]
fn same_table_global_read_waits_for_a_held_large_value_and_then_sees_it() {
    let fixture = Fixture::holding_large_values(0xd1);
    let streamed = fixture
        .writer
        .insert("tracks", large_title("streamed"), Default::default())
        .unwrap();
    fixture.pump(6);
    assert!(
        !fixture.is_global(streamed.tx_id),
        "Core holds the large value back"
    );
    fixture
        .writer
        .insert("tracks", title("plain"), Default::default())
        .unwrap();

    let budget_checks = Cell::new(0_usize);
    let budget_expired = || {
        budget_checks.set(budget_checks.get() + 1);
        false
    };
    let mut read = global_read(&fixture.writer, "tracks", &budget_expired);
    for _ in 0..8 {
        assert!(
            poll_read(&mut read).is_pending(),
            "the read waits while its table's writes are held back"
        );
        fixture.pump(1);
    }
    assert_eq!(
        budget_checks.get(),
        0,
        "waiting for local writes to go out does not spend the coverage budget"
    );

    fixture.release_uploads();
    let mut rows = None;
    for _ in 0..256 {
        fixture.pump(1);
        if let Poll::Ready(result) = poll_read(&mut read) {
            rows = Some(row_count(
                result.expect("the read resolves once its writes are out"),
            ));
            break;
        }
    }
    assert_eq!(
        rows,
        Some(2),
        "the read sees the streamed row and the write queued behind it"
    );
}

/// alice's Global read of `albums` answers while her `tracks` large value is
/// still held: writes to other tables never delay a read.
#[test]
fn global_read_of_an_unrelated_table_proceeds_while_a_large_value_uploads() {
    let fixture = Fixture::holding_large_values(0xd3);
    let streamed = fixture
        .writer
        .insert("tracks", large_title("streamed"), Default::default())
        .unwrap();
    fixture.pump(6);

    let budget_expired = || false;
    let mut read = global_read(&fixture.writer, "albums", &budget_expired);
    let mut rows = None;
    for _ in 0..256 {
        if let Poll::Ready(result) = poll_read(&mut read) {
            rows = Some(row_count(result.expect("an unrelated read is not held")));
            break;
        }
        fixture.pump(1);
    }
    assert_eq!(rows, Some(0), "the albums read answers during the upload");
    assert!(
        !fixture.is_global(streamed.tx_id),
        "the tracks upload is still held when the albums read answers"
    );
}

/// alice imports albums and streamed tracks in turn. A Global read of
/// `albums` waits for the album written before it, not for the writes the
/// import issues after the read started, so a long import cannot starve it.
#[test]
fn interleaved_import_read_waits_only_for_the_writes_before_it() {
    let fixture = Fixture::holding_large_values(0xd5);
    fixture.release_uploads();
    fixture
        .writer
        .insert("tracks", large_title("first"), Default::default())
        .unwrap();
    // Start the large value's upload; later commits now queue behind it.
    fixture.writer.tick().unwrap();
    fixture
        .writer
        .insert("albums", title("first album"), Default::default())
        .unwrap();

    let budget_expired = || false;
    let mut read = global_read(&fixture.writer, "albums", &budget_expired);
    assert!(
        poll_read(&mut read).is_pending(),
        "the album queued behind the large value has not gone out"
    );

    // The import carries on after the read started. The read must not wait
    // for these.
    let later_track = fixture
        .writer
        .insert("tracks", large_title("second"), Default::default())
        .unwrap();
    fixture
        .writer
        .insert("albums", title("second album"), Default::default())
        .unwrap();

    let mut rows = None;
    for _ in 0..256 {
        fixture.pump(1);
        if let Poll::Ready(result) = poll_read(&mut read) {
            rows = Some(row_count(result.expect("the read resolves")));
            break;
        }
    }
    let rows = rows.expect("the read is not starved by writes issued after it");
    assert!(rows >= 1, "the read sees the album written before it");
    let _ = later_track;
}

/// When the upstream rejects alice's large value, her Global read of that
/// table, waiting on it, fails at once with an explicit error rather than
/// answering without the write.
#[test]
fn failed_large_value_upload_rejects_a_waiting_global_read() {
    let schema = music_schema();
    let author = AuthorSubject::for_test_bytes([0xd7; 16]);
    let writer = open_db(0xd7, author, &schema);
    writer.set_tick_scheduler(Some(Rc::new(RecordingScheduler::default())));
    // A scripted upstream: it answers the upload start with a rejection.
    let (writer_transport, mut server_end, _) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            NodeUuid::from_bytes([0xd7; 16]),
            1,
            NodeUuid::from_bytes([0xd8; 16]),
            1,
        );
    let _upstream = crate::local_executor::block_on(writer.connect_upstream(writer_transport));
    writer
        .insert("tracks", large_title("rejected"), Default::default())
        .unwrap();
    writer
        .insert("tracks", title("behind it"), Default::default())
        .unwrap();
    writer.tick().unwrap();
    writer.tick().unwrap();

    let budget_expired = || false;
    let mut read = global_read(&writer, "tracks", &budget_expired);
    assert!(poll_read(&mut read).is_pending());

    let mut start = None;
    while let Some(message) = server_end.try_recv() {
        if let SyncMessage::ChunkUploadStart(message) = message {
            start = Some(message);
        }
    }
    let start = start.expect("the writer starts the large-value upload");
    server_end
        .send(SyncMessage::ChunkUploadResult(
            crate::protocol::ChunkUploadResult {
                value_ref: start.value_ref,
                status: crate::protocol::ChunkUploadStatus::Rejected,
            },
        ))
        .unwrap();

    let mut outcome = None;
    for _ in 0..16 {
        writer.tick().unwrap();
        if let Poll::Ready(result) = poll_read(&mut read) {
            outcome = Some(result);
            break;
        }
    }
    let error = match outcome.expect("the read settles once the upload fails") {
        Ok(_) => panic!("a read must not answer without the writes it waits on"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Global read waits on local writes to `tracks` that could not be uploaded"),
        "unexpected error: {error}"
    );
}
