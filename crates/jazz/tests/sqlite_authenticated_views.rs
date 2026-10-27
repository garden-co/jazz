//! Typed views may name accepted policies, but cannot retain a revoked grant.
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    SubscriptionEvent, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, PhysicalIdentityManifest, SchemaVersion,
};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn schema(public: bool) -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(if public {
                        PolicyExpr::True
                    } else {
                        PolicyExpr::eq_session("owner", vec!["user".into(), "account".into()])
                    })),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, schema: JazzSchema) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if node == 90 {
                AuthorSubject::SYSTEM
            } else {
                alice()
            },
        },
    )))
    .unwrap()
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
        tick(dbs);
    }
    panic!("operation did not settle")
}
fn tick(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
struct Gate {
    inner: Box<dyn Transport>,
    hold: Rc<Cell<bool>>,
    corrupt: Rc<Cell<bool>>,
    delayed: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Transport for Gate {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if !self.hold.get() {
            let message = self
                .delayed
                .borrow_mut()
                .pop_front()
                .or_else(|| self.inner.try_recv());
            if let Some(SyncMessage::PolicyDescriptorV1(descriptor)) = &message
                && self.corrupt.replace(false)
            {
                let mut invalid = descriptor.clone();
                invalid.head.root[0] ^= 1;
                self.delayed
                    .borrow_mut()
                    .push_front(SyncMessage::PolicyDescriptorV1(invalid));
            }
            return message;
        }
        while let Some(message) = self.inner.try_recv() {
            if matches!(message, SyncMessage::CatalogueMetadataV1 { .. }) {
                return Some(message);
            }
            self.delayed.borrow_mut().push_back(message);
        }
        None
    }
}
fn reject_view(db: &Db, schema: JazzSchema) {
    let error = match block_on(db.register_schema_view(schema)) {
        Ok(_) => panic!("unaccepted policy view registered"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::Schema);
    assert!(error.message.contains("policy metadata conflicts"));
}

/// Alice registers the authority's exact policy through a policy-free replica.
/// Mallory's broader rule and Alice's formerly accepted, now revoked rule fail.
/// The retained view, prepared read and subscription lose access on the new
/// catalogue head, before its policy descriptor or row evictions arrive.
#[test]
fn accepted_views_follow_authority_revocation() {
    for private_owner in [false, true] {
        let allowed = schema(false);
        let bare = allowed.without_permissions();
        let server = open(90, allowed.clone());
        server.set_authority();
        let mut catalogue = CatalogueSnapshot {
            genesis_physical_identities: PhysicalIdentityManifest::allocate(&allowed),
            schemas: vec![SchemaVersion::new(allowed.clone())],
            lineages: vec![],
            current_write_schema: CurrentWriteSchema {
                revision: 1,
                schema: allowed.version_id(),
            },
        };
        server.install_authority_catalogue(&catalogue).unwrap();
        block_on(server.insert(
            "notes",
            BTreeMap::from([("owner".into(), Value::Uuid(alice().account_id().unwrap().0))]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([10; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
        let replica = open(1, bare);
        if private_owner {
            replica.set_scope_isolated_storage_owner().unwrap();
        }
        reject_view(&replica, allowed.clone());
        let hold = Rc::new(Cell::new(false));
        let corrupt = Rc::new(Cell::new(false));
        let delayed = Rc::new(RefCell::new(VecDeque::new()));
        let (up, down) = duplex();
        block_on(replica.connect_upstream(Box::new(Gate {
            inner: up,
            hold: hold.clone(),
            corrupt: corrupt.clone(),
            delayed: delayed.clone(),
        })));
        server.accept_subscriber(down, alice());
        let read = drive(
            &[&replica, &server],
            replica.all_serialized_query_as(
                &postcard::to_allocvec(&Query::from("notes")).unwrap(),
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                None,
                None,
                false,
                || false,
                |_| {},
            ),
        )
        .unwrap();
        assert!(matches!(read, SerializedReadResult::Rows(rows) if rows.len() == 1));
        reject_view(&replica, schema(true));
        let view = block_on(replica.register_schema_view(allowed.clone())).unwrap();
        let query = view.prepare_query(&Query::from("notes")).unwrap();
        let local = ReadOpts::for_read_tier(ReadTier::LocalOnly);
        assert_eq!(
            block_on(view.all_for_identity(&query, local.clone(), alice()))
                .unwrap()
                .len(),
            1
        );
        let mut subscription =
            block_on(view.subscribe_for_identity(&query, local.clone(), alice())).unwrap();
        assert!(
            matches!(subscription.try_next_event(), Some(SubscriptionEvent::Delta { added, .. }) if added.len() == 1)
        );
        hold.set(true);
        let denied = allowed.without_permissions();
        block_on(server.activate_schema(denied.clone())).unwrap();
        catalogue.schemas = vec![SchemaVersion::new(denied)];
        catalogue.current_write_schema.revision += 1;
        server.install_authority_catalogue(&catalogue).unwrap();
        for _ in 0..12 {
            tick(&[&server, &replica]);
        }
        assert!(
            delayed
                .borrow()
                .iter()
                .any(|message| matches!(message, SyncMessage::PolicyDescriptorV1(_)))
        );
        assert_eq!(
            view.read(&query).unwrap().len(),
            1,
            "accepted row is still physically present"
        );
        assert!(
            block_on(view.all_for_identity(&query, local.clone(), alice()))
                .unwrap()
                .is_empty()
        );
        let fresh = view.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(view.all_for_identity(&fresh, local.clone(), alice()))
                .unwrap()
                .is_empty()
        );
        assert!(
            matches!(subscription.try_next_event(), Some(SubscriptionEvent::Delta { removed, .. }) if removed.len() == 1)
        );
        assert!(
            matches!(subscription.try_next_event(), Some(SubscriptionEvent::Delta { reset: true, added, .. }) if added.is_empty())
        );
        reject_view(&replica, allowed.clone());
        hold.set(false);
        for _ in 0..12 {
            tick(&[&server, &replica]);
        }
        assert!(
            block_on(view.all_for_identity(&query, local, alice()))
                .unwrap()
                .is_empty()
        );
        reject_view(&replica, allowed.clone());

        // A grant followed by invalid metadata in one tick rolls back as a
        // unit. Neither a retained read nor queued events may expose that grant.
        assert_eq!(view.read(&query).unwrap().len(), 1);
        corrupt.set(true);
        block_on(server.activate_schema(allowed.clone())).unwrap();
        catalogue.schemas = vec![SchemaVersion::new(allowed)];
        catalogue.current_write_schema.revision += 1;
        server.install_authority_catalogue(&catalogue).unwrap();
        block_on(server.tick()).unwrap();
        let error = block_on(replica.tick()).unwrap_err();
        assert!(
            error
                .message
                .contains("policy descriptor does not match accepted catalogue head")
        );
        match block_on(view.all_for_identity(
            &query,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            alice(),
        )) {
            Ok(rows) => assert!(rows.is_empty()),
            Err(error) => {
                assert!(private_owner);
                assert_eq!(error.code, ErrorCode::Storage);
            }
        }
        while let Some(event) = subscription.try_next_event() {
            match event {
                SubscriptionEvent::Delta { added, updated, .. } => {
                    assert!(added.is_empty());
                    assert!(updated.is_empty());
                }
                SubscriptionEvent::Rejected { .. } if private_owner => {}
                other => panic!("unexpected event after metadata rollback: {other:?}"),
            }
        }
    }
}
