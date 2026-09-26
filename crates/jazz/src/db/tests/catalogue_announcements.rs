//! Catalogue protocol checks at the public Node/Transport boundary.
//!
//! These live with the core tests because catalogue envelope emission and a
//! transport's failed-send retry are not observable through application query
//! results. The in-memory transport exposes those protocol events; the tests
//! still run real Node state, public schema builders, and the production sync
//! path. Application integration tests in crates/jazz/tests use JazzServer.
#![cfg(feature = "testing")]

use crate::{
    block_on,
    db::{Node, Transport},
    groove::{records::Value, storage::MemoryStorage},
    ids::{AuthorSubject, NodeUuid},
    node::{CommitUnitTrust, NodeState},
    protocol::{CatalogueSnapshot, LensOp, MigrationLens, SchemaVersion, SyncMessage, TableLens},
    schema::JazzSchema,
    tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder},
    wire::TransportError,
};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
};

#[derive(Default, Clone)]
struct Outbox {
    messages: Rc<RefCell<VecDeque<SyncMessage>>>,
    fail_once: Rc<Cell<bool>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Outbox {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if self.fail_once.replace(false) {
            return Err(TransportError::Backpressure);
        }
        self.messages.borrow_mut().push_back(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

impl Outbox {
    fn snapshot(&self) -> CatalogueSnapshot {
        let mut messages = self.messages.borrow_mut();
        assert_eq!(messages.len(), 1, "one changed catalogue announcement");
        let Some(SyncMessage::CatalogueSnapshot(snapshot)) = messages.pop_front() else {
            panic!("expected catalogue snapshot");
        };
        *snapshot
    }
}

fn schema(extra: bool, allowed: bool) -> JazzSchema {
    let table = TableSchemaBuilder::new("items").column("title", ColumnType::Text);
    let table = if extra {
        table.column("notes", ColumnType::Text)
    } else {
        table
    };
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(table.policies(TablePolicies::new().with_select(if allowed {
                PolicyExpr::True
            } else {
                PolicyExpr::False
            })))
            .build(),
    )
    .unwrap()
}

fn open(schema: JazzSchema, id: u8) -> Node<MemoryStorage> {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    Node::new(
        block_on(NodeState::new_with_shared_test_catalogue(
            NodeUuid::from_bytes([id; 16]),
            schema,
            MemoryStorage::new(&refs).unwrap(),
        ))
        .unwrap(),
    )
}

fn apply(node: &Node<MemoryStorage>, message: SyncMessage) {
    let state = node.node();
    block_on(async {
        let mut state = state.lock().await;
        let pending = state
            .apply_trusted_catalogue_message(message)
            .await
            .unwrap();
        state.persist_and_settle_outcome(pending).await.unwrap();
    });
}

#[test]
fn catalogue_announcements_follow_lineage_permissions_and_relay_replacement() {
    let base = schema(false, true);
    let evolved = schema(true, true);
    let authority = open(base.clone(), 0x51);
    let relay = open(base.clone(), 0x52);
    let out = Outbox::default();
    let relay_out = Outbox::default();
    let incoming = Outbox::default();
    let upstream = block_on(relay.connect_upstream(Box::new(incoming.clone())));
    let _connection = authority.accept_subscriber_with_trust(
        Box::new(out.clone()),
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let _relay_connection = relay.accept_subscriber_with_trust(
        Box::new(relay_out.clone()),
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    block_on(authority.tick()).unwrap();
    let initial = out.snapshot();
    incoming
        .inbound
        .borrow_mut()
        .push_back(SyncMessage::CatalogueSnapshot(Box::new(initial.clone())));
    block_on(async {
        upstream.lock().await.tick().await.unwrap();
    });
    block_on(relay.tick()).unwrap();
    assert_eq!(
        serde_json::to_vec(&relay_out.snapshot()).unwrap(),
        serde_json::to_vec(&initial).unwrap()
    );
    for _ in 0..3 {
        block_on(authority.tick()).unwrap();
        block_on(relay.tick()).unwrap();
    }
    assert!(out.messages.borrow().is_empty());
    assert!(relay_out.messages.borrow().is_empty());

    let target = SchemaVersion::new(evolved.clone());
    let lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "items".into(),
            target_table: "items".into(),
            ops: vec![LensOp::AddColumn {
                column: "notes".into(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .unwrap();
    let publication = block_on(async {
        authority
            .node()
            .lock()
            .await
            .author_schema_lineage_publication(
                target.clone(),
                lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap()
    });
    apply(
        &authority,
        SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        },
    );
    block_on(authority.tick()).unwrap();
    let grown = out.snapshot();
    assert_eq!(grown.schemas.len(), 2);
    assert_eq!(grown.lineages.len(), 1);
    incoming
        .inbound
        .borrow_mut()
        .push_back(SyncMessage::CatalogueSnapshot(Box::new(grown.clone())));
    block_on(async {
        upstream.lock().await.tick().await.unwrap();
    });
    block_on(relay.tick()).unwrap();
    assert_eq!(
        serde_json::to_vec(&relay_out.snapshot()).unwrap(),
        serde_json::to_vec(&grown).unwrap()
    );

    let denied = schema(true, false);
    assert_eq!(
        denied.version_id(),
        evolved.version_id(),
        "policy-only change keeps structural identity"
    );
    block_on(async {
        authority
            .node()
            .lock()
            .await
            .activate_schema_for_test(20, denied.clone())
            .await
            .unwrap();
    });
    block_on(authority.tick()).unwrap();
    let changed = out.snapshot();
    assert_eq!(changed.current_write_schema.schema, target.id);
    assert_eq!(changed.current_write_schema.revision, 20);
    assert_eq!(
        changed
            .schemas
            .iter()
            .find(|s| s.id == target.id)
            .unwrap()
            .schema,
        denied
    );
    incoming
        .inbound
        .borrow_mut()
        .push_back(SyncMessage::CatalogueSnapshot(Box::new(changed.clone())));
    block_on(async {
        upstream.lock().await.tick().await.unwrap();
    });
    block_on(relay.tick()).unwrap();
    assert_eq!(
        serde_json::to_vec(&relay_out.snapshot()).unwrap(),
        serde_json::to_vec(&changed).unwrap()
    );
    block_on(authority.tick()).unwrap();
    assert!(out.messages.borrow().is_empty());

    let newcomer = Outbox::default();
    let _new_connection = authority.accept_subscriber_with_trust(
        Box::new(newcomer.clone()),
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    block_on(authority.tick()).unwrap();
    assert_eq!(
        serde_json::to_vec(&newcomer.snapshot()).unwrap(),
        serde_json::to_vec(&changed).unwrap()
    );
    assert!(
        out.messages.borrow().is_empty(),
        "announcement state stays per peer"
    );
}

#[test]
fn backpressured_catalogue_retries_with_the_current_permissions() {
    let base = schema(false, true);
    let node = open(base.clone(), 0x53);
    let out = Outbox::default();
    out.fail_once.set(true);
    let connection = node.accept_subscriber_with_trust(
        Box::new(out.clone()),
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    block_on(async {
        connection.lock().await.tick().await.unwrap();
    });
    assert!(out.messages.borrow().is_empty());
    let denied = schema(false, false);
    block_on(async {
        node.node()
            .lock()
            .await
            .activate_schema_for_test(21, denied.clone())
            .await
            .unwrap();
    });
    block_on(async {
        connection.lock().await.tick().await.unwrap();
    });
    let snapshot = out.snapshot();
    assert_eq!(snapshot.current_write_schema.revision, 21);
    assert_eq!(
        snapshot
            .schemas
            .iter()
            .find(|s| s.id == base.version_id())
            .unwrap()
            .schema,
        denied
    );
    block_on(async {
        connection.lock().await.tick().await.unwrap();
    });
    assert!(out.messages.borrow().is_empty());
}
