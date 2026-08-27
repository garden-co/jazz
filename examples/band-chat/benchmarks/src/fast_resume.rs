//! Caught-up BandChat timeline resume receipt.
//!
//! This is intentionally a small protocol fixture rather than an application
//! runtime fixture: it exercises the same settled message-history shape while
//! making the fast-known-state boundary observable without relying on private
//! benchmark infrastructure.

use std::collections::BTreeMap;

use jazz::db::block_on;
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::{
    KnownStateCompleteness, KnownStateDeclaration, RegisterShapeOptions, SubscriptionKey,
    SyncMessage,
};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::time::GlobalTime;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::Fate;

const TABLE: &str = "messages";

/// The externally observable result of attaching a fully caught-up peer.
#[derive(Debug, PartialEq, Eq)]
pub struct FastResumeReceipt {
    pub reset_result_set: bool,
    pub result_member_adds: usize,
    pub result_member_removes: usize,
    pub version_carriers: usize,
    pub version_bundles: usize,
}

impl FastResumeReceipt {
    pub fn is_caught_up_noop(&self) -> bool {
        !self.reset_result_set
            && self.result_member_adds == 0
            && self.result_member_removes == 0
            && self.version_carriers == 0
            && self.version_bundles == 0
    }
}

/// A seeded BandChat message-history view with its settled membership frontier.
///
/// Construction represents established server state and is deliberately outside
/// the benchmark timing closure. `caught_up_fast_resume` measures only attaching
/// a peer which has already observed that exact frontier.
pub struct FastResumeFixture {
    core: NodeState<MemoryStorage>,
    shape: jazz::query::ValidatedQuery,
    binding: jazz::query::Binding,
    subscription: SubscriptionKey,
    settled_through: GlobalTime,
}

impl FastResumeFixture {
    pub fn new(message_count: usize) -> Self {
        assert!(message_count > 0, "fixture requires at least one message");
        let schema = schema();
        let mut writer = open_node(node(1), schema.clone());
        let mut core = open_node(node(2), schema.clone());

        for index in 0..message_count {
            let (published, unit) = block_on(
                writer.commit_mergeable_unit(
                    MergeableCommit::new(TABLE, row(index), 1_000 + index as u64)
                        .cells(message_cells(index)),
                ),
            )
            .expect("create BandChat message fixture commit");
            block_on(writer.persist_and_settle_transaction(published))
                .expect("persist BandChat message fixture commit");

            let SyncMessage::CommitUnit { tx, versions } = unit else {
                panic!("fixture commit must publish a commit unit");
            };
            let outcome =
                block_on(core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS))
                    .expect("ingest BandChat message fixture commit");
            let [fate] = block_on(core.persist_and_settle_outcome(outcome))
                .expect("settle BandChat message fixture commit")
                .try_into()
                .expect("fixture commit has one fate");
            assert!(matches!(
                fate,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            ));
        }

        let shape = Query::from(TABLE)
            .validate(&schema)
            .expect("validate BandChat message-history query");
        let binding = shape
            .bind(BTreeMap::new())
            .expect("bind BandChat message-history query");
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        };

        // The cursor is learned from an actual settled publication, never from
        // fixture timestamps. That keeps this receipt honest about the protocol
        // frontier used by a reconnecting peer.
        let mut warm_peer = PeerState::relay();
        let warm_update = block_on(warm_peer.rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        ))
        .expect("rehydrate warm BandChat message-history peer")
        .expect("warm peer receives an initial view update");
        let SyncMessage::ViewUpdate(payload) = warm_update else {
            panic!("warm peer must receive a view update");
        };

        Self {
            core,
            shape,
            binding,
            subscription,
            settled_through: payload.settled_through,
        }
    }

    /// Attach a peer whose `FastCurrentMembership` declaration is exactly at
    /// the observed settled frontier. The update must carry no reset,
    /// membership transition, or version payload; CPU cost is what Divan and
    /// CodSpeed measure across fixture scales.
    pub fn caught_up_fast_resume(&mut self) -> FastResumeReceipt {
        let mut peer = PeerState::relay();
        peer.declare_known_state(
            self.subscription,
            Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: self.settled_through,
            }),
        );
        let update = block_on(peer.rehydrate_query_for_subscription_with_opts(
            &mut self.core,
            self.subscription,
            &self.shape,
            &self.binding,
            RegisterShapeOptions::default(),
        ))
        .expect("rehydrate caught-up BandChat peer")
        .expect("caught-up peer receives an initial view update");
        let SyncMessage::ViewUpdate(payload) = update else {
            panic!("caught-up peer must receive a view update");
        };
        FastResumeReceipt {
            reset_result_set: payload.reset_result_set,
            result_member_adds: payload.result_member_adds.len(),
            result_member_removes: payload.result_member_removes.len(),
            version_carriers: payload.version_carriers.len(),
            version_bundles: payload.version_bundles.len(),
        }
    }
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new(TABLE)
                .column("body", ColumnType::Text)
                .column("sent_at", ColumnType::Timestamp)
                .index_only(["sent_at"]),
        )
        .build();
    JazzSchema::new(&source).expect("BandChat fast-resume schema compiles")
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> NodeState<MemoryStorage> {
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(NodeState::new(
        node_uuid,
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
    ))
    .expect("open BandChat fast-resume fixture node")
}

fn message_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "body".to_owned(),
            Value::String(format!("Message {index:06}")),
        ),
        ("sent_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [4; 16];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    RowUuid::from_bytes(bytes)
}
