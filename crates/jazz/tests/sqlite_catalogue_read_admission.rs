//! Remote reads can refresh an older catalogue cache without admitting drafts.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, FirstLoad, ReadOpts, ReadTier,
    SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, MigrationLens, PhysicalIdentityManifest,
    SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::Cell,
    collections::BTreeMap,
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn author(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}
fn schema(extra: Option<&str>) -> JazzSchema {
    let mut builder = SchemaBuilder::new().table(
        TableSchemaBuilder::new("projects")
            .column("owner", ColumnType::Uuid)
            .column("title", ColumnType::Text)
            .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                "owner",
                vec!["user".into(), "account".into()],
            ))),
    );
    if let Some(table) = extra {
        builder = builder.table(TableSchemaBuilder::new(table).fk_column("projectId", "projects"));
    }
    JazzSchema::new(&builder.build()).unwrap()
}
fn open(node: u8, schema: JazzSchema) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if node == 90 {
                AuthorSubject::SYSTEM
            } else {
                author(node)
            },
        },
    )))
    .unwrap()
}
struct Counting {
    inner: Box<dyn Transport>,
    requests: Rc<Cell<usize>>,
}
impl Transport for Counting {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::CatalogueFetchV1 { .. }) {
            self.requests.set(self.requests.get() + 1);
        }
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("read did not settle")
}
fn fixture(node: u8, unpublished: bool) -> (Db, Db, Rc<Cell<usize>>) {
    let before = schema(None);
    let after = schema(Some("notes"));
    let server = open(90, before.clone());
    server.set_authority();
    let mut catalogue = CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&before),
        schemas: vec![SchemaVersion::new(before.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: before.version_id(),
        },
    };
    server.install_authority_catalogue(&catalogue).unwrap();
    block_on(server.insert(
        "projects",
        BTreeMap::from([
            (
                "owner".into(),
                Value::Uuid(author(1).account_id().unwrap().0),
            ),
            (
                "title".into(),
                Value::String("Created before migration".into()),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([10; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    let cache = server
        .take_authenticated_catalogue_state()
        .unwrap()
        .capture
        .unwrap();
    let lens = MigrationLens::new(
        before.version_id(),
        after.version_id(),
        vec![TableLens {
            source_table: "projects".into(),
            target_table: "projects".into(),
            ops: vec![],
        }],
    )
    .unwrap();
    let publication = SchemaLineagePublication::author_from_prior(
        &before,
        &catalogue.genesis_physical_identities,
        SchemaVersion::new(after.clone()),
        lens.clone(),
        vec!["notes".to_owned()],
        Vec::<String>::new(),
    )
    .unwrap();
    catalogue.schemas.push(SchemaVersion::new(after.clone()));
    catalogue.lineages.push((1, publication));
    catalogue.current_write_schema = CurrentWriteSchema {
        revision: 2,
        schema: after.version_id(),
    };
    block_on(server.activate_schema_with_lenses(after.clone(), &[lens])).unwrap();
    server.install_authority_catalogue(&catalogue).unwrap();
    let replica = open(
        node,
        if unpublished {
            schema(Some("drafts"))
        } else {
            after
        }
        .without_permissions(),
    );
    // SAFETY: the cache was captured from this exact fixture's authority above.
    unsafe { replica.install_cached_catalogue(&cache) }.unwrap();
    assert!(!replica.has_authenticated_catalogue().unwrap());
    let requests = Rc::new(Cell::new(0));
    let (up, down) = duplex();
    block_on(replica.connect_upstream(Box::new(Counting {
        inner: up,
        requests: requests.clone(),
    })));
    server.accept_subscriber(down, author(node));
    (server, replica, requests)
}
async fn read(replica: &Db, options: ReadOpts) -> Result<SerializedReadResult, Error> {
    replica
        .all_serialized_query_as(
            &postcard::to_allocvec(&Query::from("projects")).unwrap(),
            options,
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        )
        .await
}

/// Alice opens the newly deployed schema using the previous host cache. Her
/// first remote read obtains exact authority admission and preserves old rows;
/// Mallory can obtain schema metadata but cannot read Alice's project.
#[test]
fn first_remote_read_refreshes_an_older_catalogue_cache() {
    for node in [1, 2] {
        for tier in [ReadTier::Remote, ReadTier::LocalFirst] {
            let (server, replica, requests) = fixture(node, false);
            let mut options = ReadOpts::for_read_tier(tier);
            options.first_load = FirstLoad::WaitForRemote { timeout_ms: 1_000 };
            let result = drive(&[&server, &replica], read(&replica, options)).unwrap();
            let SerializedReadResult::Rows(rows) = result else {
                panic!("expected rows")
            };
            assert_eq!(rows.len(), usize::from(node == 1));
            if node == 1 {
                assert_eq!(rows[0].row_uuid(), RowUuid::from_bytes([10; 16]));
            }
            assert!(replica.has_authenticated_catalogue().unwrap());
            assert_eq!(requests.get(), 1);
        }
    }
}

/// Alice's local-only read cannot fetch admission, and Mallory's unpublished
/// schema stays unusable even after asking the authority through a remote read.
#[test]
fn local_only_and_unpublished_reads_do_not_gain_schema_admission() {
    for unpublished in [false, true] {
        let (server, replica, requests) = fixture(if unpublished { 2 } else { 1 }, unpublished);
        let mut immediate = ReadOpts::default();
        immediate.first_load = FirstLoad::WaitForRemote { timeout_ms: 0 };
        for options in [
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            ReadOpts::default(),
            immediate,
        ] {
            let error = block_on(read(&replica, options)).unwrap_err();
            assert_eq!(error.code, ErrorCode::Schema);
            assert_eq!(requests.get(), 0);
            assert!(!replica.has_authenticated_catalogue().unwrap());
        }
        if unpublished {
            let mut options = ReadOpts::default();
            options.first_load = FirstLoad::WaitForRemote { timeout_ms: 5 };
            let error = drive(&[&server, &replica], read(&replica, options)).unwrap_err();
            assert!(
                matches!(error.code, ErrorCode::Schema | ErrorCode::NotObserved),
                "{error}"
            );
            assert!(!replica.has_authenticated_catalogue().unwrap());
            assert!(replica.prepare_query(&Query::from("projects")).is_err());
            assert_eq!(requests.get(), 1);
        }
    }
}

/// Alice's short local-first wait bounds catalogue admission as well as rows.
/// An unreachable authority cannot turn that wait into the default 15 seconds.
#[test]
fn catalogue_admission_respects_the_read_deadline() {
    let (_server, replica, _requests) = fixture(1, false);
    let mut options = ReadOpts::for_read_tier(ReadTier::LocalFirst);
    options.first_load = FirstLoad::WaitForRemote { timeout_ms: 1 };
    let mut pending = pin!(read(&replica, options));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    // No ticks deliver the authority's metadata during this bounded wait.
    std::thread::sleep(std::time::Duration::from_millis(5));
    let Poll::Ready(Err(error)) = pending.as_mut().poll(&mut cx) else {
        panic!("catalogue admission exceeded the read deadline")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(!replica.has_authenticated_catalogue().unwrap());
}
