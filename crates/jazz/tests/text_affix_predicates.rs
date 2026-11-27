//! Text prefix and suffix predicates (`starts_with`, `ends_with`): bytewise
//! on UTF-8, case-sensitive, NULL never matches; text columns only; sent
//! upstream only over a link that negotiated
//! `FEATURE_TEXT_AFFIX_PREDICATES`.

use jazz_types::legacy_test_future::ResultFutureExt;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SyncMessage, Transport,
    TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    Query, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationQuery, RelationValueRef, col, ends_with, lit, param, starts_with,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("people")
                    .nullable_column("name", ColumnType::Text)
                    .column("age", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x55; 16])
}

/// Rows 1..: Ann, ann, Annabel, Joann, Émile, Zoé, NULL, "".
fn seed(db: &Db) {
    let names: [Option<&str>; 8] = [
        Some("Ann"),
        Some("ann"),
        Some("Annabel"),
        Some("Joann"),
        Some("Émile"),
        Some("Zoé"),
        None,
        Some(""),
    ];
    for (index, name) in names.into_iter().enumerate() {
        let byte = index as u8 + 1;
        block_on(db.insert(
            "people",
            BTreeMap::from([
                (
                    "name".to_owned(),
                    name.map_or(Value::Nullable(None), |name| {
                        Value::Nullable(Some(Box::new(Value::String(name.to_owned()))))
                    }),
                ),
                ("age".to_owned(), Value::I32(i32::from(byte))),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([byte; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
    }
}

fn ids(db: &Db, query: &Query, tier: ReadTier) -> Result<Vec<u8>, jazz::db::Error> {
    let prepared = db.prepare_query(query)?;
    let opts = ReadOpts {
        tier,
        ..Default::default()
    };
    let mut ids: Vec<u8> = block_on(db.all(&prepared, opts))?
        .iter()
        .map(|row| row.row_uuid().as_bytes()[0])
        .collect();
    ids.sort();
    Ok(ids)
}

fn local(db: &Db, query: &Query) -> Vec<u8> {
    ids(db, query, ReadTier::LocalOnly).unwrap()
}

fn text(value: &str) -> jazz::query::Operand {
    lit(Value::String(value.to_owned()))
}

/// Alice filters by prefix and suffix. The match is bytewise on UTF-8:
/// case counts, an accented letter is its own bytes, the empty needle
/// matches every non-NULL name, and NULL matches nothing.
#[test]
fn starts_with_and_ends_with_match_bytewise_and_skip_null() {
    let db = open(0x01, alice());
    seed(&db);
    let people = || Query::from("people");

    assert_eq!(
        local(&db, &people().filter(starts_with(col("name"), text("Ann")))),
        [1, 3]
    );
    assert_eq!(
        local(&db, &people().filter(starts_with(col("name"), text("É")))),
        [5]
    );
    assert_eq!(
        local(&db, &people().filter(ends_with(col("name"), text("ann")))),
        [2, 4]
    );
    assert_eq!(
        local(&db, &people().filter(ends_with(col("name"), text("é")))),
        [6]
    );
    // A needle longer than the value never matches.
    assert_eq!(
        local(&db, &people().filter(ends_with(col("name"), text("xAnn")))),
        Vec::<u8>::new()
    );
    assert_eq!(
        local(&db, &people().filter(starts_with(col("name"), text("")))),
        [1, 2, 3, 4, 5, 6, 8]
    );
    assert_eq!(
        local(&db, &people().filter(ends_with(col("name"), text("")))),
        [1, 2, 3, 4, 5, 6, 8]
    );
}

/// The same test reached through the relation IR (as the TypeScript query
/// layer sends it) gives the same rows.
#[test]
fn relation_starts_with_and_ends_with_match_like_the_flat_predicates() {
    let db = open(0x01, alice());
    seed(&db);
    let scoped = |column: &str| RelationColumnRef {
        scope: Some("people".into()),
        column: column.into(),
    };
    let filtered = |predicate: RelationPredicate| {
        let mut query = Query::from("people");
        query.relation = Some(RelationQuery {
            rel: RelationExpr::Project {
                input: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "people".into(),
                        alias: None,
                    }),
                    predicate,
                }),
                columns: ["id", "name", "age"]
                    .into_iter()
                    .map(|column| RelationProjectColumn {
                        alias: column.to_owned(),
                        expr: RelationProjectExpr::Column(scoped(column)),
                    })
                    .collect(),
            },
        });
        local(&db, &query)
    };
    let name = || scoped("name");
    let needle = |value: &str| RelationValueRef::Literal(serde_json::json!(value));

    assert_eq!(
        filtered(RelationPredicate::StartsWith {
            left: name(),
            right: needle("Ann"),
        }),
        [1, 3]
    );
    assert_eq!(
        filtered(RelationPredicate::EndsWith {
            left: name(),
            right: needle("ann"),
        }),
        [2, 4]
    );
}

/// A prefix of a number is not defined: the query is refused, not
/// answered by its text form.
#[test]
fn starts_with_on_a_non_text_column_is_refused() {
    let db = open(0x01, alice());
    let error = db
        .prepare_query(&Query::from("people").filter(starts_with(col("age"), text("1"))))
        .unwrap_err();
    assert!(error.to_string().to_lowercase().contains("type"), "{error}");
}

/// A duplex whose `Hello` negotiated `features`.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    features: jazz::wire::WireFeatures,
    /// Counts the prefix/suffix reads sent this way.
    affix_reads: Rc<RefCell<usize>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let SyncMessage::Read { query, .. } = &message
            && query.uses_text_affix()
        {
            *self.affix_reads.borrow_mut() += 1;
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn features(&self) -> jazz::wire::WireFeatures {
        self.features
    }
}

/// A server holding the seeded rows and Alice's replica, linked over a
/// connection that negotiated `features`; the count of prefix/suffix reads
/// Alice's replica sent.
fn connected(features: jazz::wire::WireFeatures) -> (Db, Db, Rc<RefCell<usize>>) {
    let server = open(0x01, AuthorSubject::SYSTEM);
    jazz::db::block_on(server.set_authority()).unwrap();
    seed(&server);
    let client = open(0x02, alice());
    let (upstream, affix_reads) = link(&server, &client, features);
    std::mem::forget(upstream);
    // A held subscription makes the link ready before reading over it.
    let ready = client
        .prepare_query(&Query::from("people").limit(1))
        .unwrap();
    std::mem::forget(block_on(client.subscribe(&ready, ReadOpts::default())).unwrap());
    pump(&server, &client);
    (server, client, affix_reads)
}

/// Link Alice's replica to the server over a connection that negotiated
/// `features`; the count of prefix/suffix reads the replica sent on it.
fn link(
    server: &Db,
    client: &Db,
    features: jazz::wire::WireFeatures,
) -> (jazz::db::Upstream, Rc<RefCell<usize>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let affix_reads = Rc::new(RefCell::new(0));
    let upstream = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
        features,
        affix_reads: affix_reads.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up,
                features,
                affix_reads: Rc::new(RefCell::new(0)),
            }),
            alice(),
        )
        .unwrap();
    (upstream, affix_reads)
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
}

/// Poll `future`, ticking both ends between polls, until it completes.
fn drive<T>(server: &Db, client: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..64 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    panic!("did not complete");
}

/// Read `query` at the server's tier (as a binding's one-shot read sends
/// it), driving both ends until it settles.
fn remote(server: &Db, client: &Db, query: &Query) -> Result<Vec<u8>, jazz::db::Error> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::Remote,
        ..Default::default()
    };
    let mut read = pin!(client.all_serialized_query(&bytes, opts, None, None, None));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..64 {
        if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
            let rows = match result? {
                jazz::db::SerializedReadResult::Rows(rows) => rows,
                other => panic!("{other:?}"),
            };
            let mut ids: Vec<u8> = rows
                .iter()
                .map(|row| row.row_uuid().as_bytes()[0])
                .collect();
            ids.sort();
            return Ok(ids);
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    panic!("did not complete");
}

/// Alice reads by prefix from a server that negotiated the feature: the
/// server answers it.
///
/// ```text
/// alice ──Read(starts_with)──► server (affix feature) ──rows──► alice
/// ```
#[test]
fn a_server_that_negotiated_affix_predicates_answers_them() {
    let (server, client, sent) = connected(jazz::wire::current_wire_features());
    let query = Query::from("people").filter(starts_with(col("name"), text("Ann")));
    assert_eq!(remote(&server, &client, &query).unwrap(), [1, 3]);
    assert_eq!(*sent.borrow(), 1);
}

/// Over a link that did not negotiate the feature (an older server, which
/// would drop the whole link on a message it cannot decode), Alice's read
/// is refused on her side and never sent; other reads still go through.
///
/// ```text
/// alice ──Read(starts_with)──✗ (refused locally, nothing sent)
/// alice ──Read(eq)───────────► older server ──rows──► alice
/// ```
#[test]
fn an_affix_read_needs_a_server_that_negotiated_it() {
    let (server, client, sent) = connected(jazz::wire::FEATURE_NONE);
    let query = Query::from("people").filter(ends_with(col("name"), text("ann")));
    let error = remote(&server, &client, &query).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not supported yet by this server"),
        "{error}"
    );
    assert_eq!(*sent.borrow(), 0, "the read stayed on the client");
    let plain = Query::from("people").filter(jazz::query::eq(col("name"), text("Zoé")));
    assert_eq!(remote(&server, &client, &plain).unwrap(), [6]);
}

/// Offline, Alice's serializable transaction reads by prefix (from the rows
/// her replica holds) and writes. Back online over a link without the
/// feature, the commit is refused on her side as a rejected fate and never
/// reaches the older server, which could not decode its read set.
///
/// ```text
/// alice (offline) ──tx: read starts_with, insert──► commit (local)
/// alice ──link without affix feature──✗ commit refused, nothing sent
/// ```
#[test]
fn a_serializable_commit_reading_by_prefix_needs_a_server_that_negotiated_it() {
    let server = open(0x01, AuthorSubject::SYSTEM);
    jazz::db::block_on(server.set_authority()).unwrap();
    seed(&server);
    let client = open(0x02, alice());
    let (upstream, _) = link(&server, &client, jazz::wire::current_wire_features());
    let everyone = client.prepare_query(&Query::from("people")).unwrap();
    let _live = block_on(client.subscribe(&everyone, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(client.detach_connection(&upstream));

    let tx = jazz::model::transaction::OpenTransactionId::new();
    drive(&server, &client, client.begin_serializable(tx)).unwrap();
    let by_prefix = Query::from("people").filter(starts_with(col("name"), text("Ann")));
    let bytes = postcard::to_allocvec(&by_prefix).unwrap();
    let read = drive(
        &server,
        &client,
        client.all_serialized_query(&bytes, ReadOpts::default(), Some(tx), None, None),
    )
    .unwrap();
    match read {
        jazz::db::SerializedReadResult::Rows(rows) => assert_eq!(rows.len(), 2),
        other => panic!("{other:?}"),
    }
    drive(
        &server,
        &client,
        client.tx_ref(tx).insert(
            "people",
            BTreeMap::from([
                ("name".to_owned(), Value::String("Anneke".into())),
                ("age".to_owned(), Value::I32(40)),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([0x40; 16])),
                ..Default::default()
            },
        ),
    )
    .unwrap();
    let receipt = drive(&server, &client, client.commit_serializable_handle(tx)).unwrap();

    let (upstream, _) = link(&server, &client, jazz::wire::FEATURE_NONE);
    std::mem::forget(upstream);
    let error = drive(
        &server,
        &client,
        client.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not supported yet by this server"),
        "{error}"
    );
    assert!(!local(&server, &Query::from("people")).contains(&0x40));
}

/// Model validation (`Query::validate`, which policy validation also
/// runs) takes prefix/suffix tests like any comparison: a parameter is
/// typed as text from the column, and a non-text column is an error, not
/// a panic.
#[test]
fn validation_types_prefix_and_suffix_tests() {
    let schema = schema();
    let validated = Query::from("people")
        .filter(starts_with(col("name"), param("prefix")))
        .filter(ends_with(col("name"), text("n")))
        .validate(&schema);
    assert!(validated.is_ok(), "{validated:?}");
    let error = Query::from("people")
        .filter(ends_with(col("age"), text("1")))
        .validate(&schema)
        .unwrap_err();
    assert!(
        matches!(error, jazz::query::QueryError::OperandTypeMismatch),
        "{error:?}"
    );
}
