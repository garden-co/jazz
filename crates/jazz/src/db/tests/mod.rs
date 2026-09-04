use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use groove::records::{EnumValue, RecordDescriptor, ValueType};
use groove::storage::{OrderedKvStorage, ReopenableStorage, YieldingStorage};
use jazz_storage_rocksdb::RocksDbStorage as ImmediateRocksDbStorage;
use std::path::Path;

type RocksDbStorage = YieldingStorage<ImmediateRocksDbStorage>;

trait TestRocksOpen: Sized {
    fn open(
        path: impl AsRef<Path>,
        column_families: &[&str],
    ) -> Result<Self, groove::storage::Error>;
}

impl TestRocksOpen for RocksDbStorage {
    fn open(
        path: impl AsRef<Path>,
        column_families: &[&str],
    ) -> Result<Self, groove::storage::Error> {
        ImmediateRocksDbStorage::open(path, column_families).map(YieldingStorage::wrap)
    }
}

use super::*;
use crate::ids::{AuthorSubject, NodeUuid};
use crate::legacy_test_future::{
    FutureResolveExt as _, OptionFutureExt as _, ResultFutureExt as _, SettledNodeTestExt as _,
};
use crate::protocol::{
    AuthorizationScopePurpose, AuthorizationScopeReceipt, AuthorizationSupportScopeKey,
    CatalogueAck, KnownStateCompleteness, KnownStateDeclaration, LensOp, PeerPayloadInventory,
    PermissionAdviceAction, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions, RowVersionRef,
    ShapeAst, SnapshotRef, Subscribe, SubscribeRejectReason, SubscribeServerFailureCode, TableLens,
    VersionBundle, VersionBundleScope, VersionCarrier,
};
use crate::protocol_limits::{
    MAX_FETCH_ROW_VERSIONS, MAX_FRAGMENT_REASSEMBLY_AGE_MS, MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
    MAX_INFLIGHT_LOGICAL_MESSAGES, MAX_KNOWN_STATE_EXACT_REFS, MAX_LOGICAL_MESSAGE_BYTES,
    MAX_SHAPE_REGISTRATION_BYTES, MAX_WIRE_FRAME_BYTES,
};
use crate::query::{
    ArraySubquery, BindingId, Include, JoinMode, OrderDirection, Predicate, RelationOrderBy,
    ShapeId, all_of, any_of, claim, col, contains, eq, gt, in_list, is_null, lit, lte, ne, not,
    param,
};
use crate::schema::WritePolicies;
use crate::time::{GlobalTime, TxTime};
use crate::tools::ObjectId as PublicObjectId;
use crate::tools::public_schema::{
    CmpOp as PublicCmpOp, ColumnDescriptor as PublicColumnDescriptor,
    ColumnType as PublicColumnType, EnumCaseDescriptor as PublicEnumCaseDescriptor,
    Operation as PublicOperation, PolicyExpr as PublicPolicyExpr, PolicyValue as PublicPolicyValue,
    Schema as PublicSchema, SchemaBuilder as PublicSchemaBuilder,
    TablePolicies as PublicTablePolicies, TableSchemaBuilder as PublicTableSchemaBuilder,
    Value as PublicValue,
};
use crate::tools::public_schema::{
    RelColumnRef as PublicRelColumnRef, RelExpr as PublicRelExpr,
    RelJoinCondition as PublicRelJoinCondition, RelJoinKind as PublicRelJoinKind,
    RelKeyRef as PublicRelKeyRef, RelPredicateCmpOp as PublicRelPredicateCmpOp,
    RelPredicateExpr as PublicRelPredicateExpr, RelProjectColumn as PublicRelProjectColumn,
    RelProjectExpr as PublicRelProjectExpr, RelRecursionBound as PublicRelRecursionBound,
    RelValueRef as PublicRelValueRef, RowIdRef as PublicRelRowIdRef,
};
use crate::tx::TxId;
use crate::wire::{
    FEATURE_MESSAGE_FRAGMENTATION, FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD,
    WIRE_PROTOCOL_VERSION, WireEnvelope, WireError, WireErrorCode, WireFrame, WireMessageFragment,
    WireRetry, WireSession, WireStreamDecoder, WireTransport, current_wire_features, decode_frame,
    decode_sync_message, encode_frame,
};

use super::peer_connection::{
    PendingRowVersionRepair, SubscriberConnectionState, UpstreamConnectionState,
    aggregate_authorization_scope_bounds, authorization_progress_for_view_receipt,
    authorization_scope_receipt_matches_transport_context,
    authorization_scope_support_options_match, remove_scope_aggregate_member,
    send_subscriber_with_sync_context, view_update_is_empty,
};

#[test]
fn terminal_root_binding_fields_preserve_hybrid_public_slot_provenance() {
    // Local maintained-view reset snapshots must use the same slot mapping as
    // terminal deltas. In particular, a logical `user_check` may coexist with
    // physical application column `check` stored as `user_check`.
    let layout = TerminalRootLayout {
        id: "hybrid-test-layout".to_owned(),
        root_descriptor: RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("user_check".to_owned(), ValueType::Bool),
            ("user_check".to_owned(), ValueType::Bool),
        ]),
        root_key_slot: 0,
        root_key_field_name: "row_uuid".to_owned(),
        public_fields: vec![
            TerminalRootPublicField {
                publication: crate::node::CurrentRowPublicationField::ResultField {
                    name: "user_check".to_owned(),
                    visible: true,
                },
                name: "user_check".to_owned(),
                descriptor_field_name: "user_check".to_owned(),
                slot: 1,
                carrier: TerminalRootCarrier::Logical,
            },
            TerminalRootPublicField {
                publication: crate::node::CurrentRowPublicationField::StoredColumn {
                    id: crate::ids::PhysicalColumnId(7),
                    output_name: "check".to_owned(),
                },
                name: "check".to_owned(),
                descriptor_field_name: "user_check".to_owned(),
                slot: 2,
                carrier: TerminalRootCarrier::CurrentRow,
            },
        ],
        carrier: TerminalRootCarrier::Logical,
    };

    assert_eq!(
        terminal_root_binding_fields(&layout),
        vec![
            CurrentRowBindingRole::LogicalField,
            CurrentRowBindingRole::LogicalField,
            CurrentRowBindingRole::PhysicalColumn,
        ]
    );
}
use catalogue::assert_authority_rejects_staged_write;
use support::block_on;
use support::*;
use wire_transport::byte_duplex_with_session;

mod catalogue;
mod chunk_io_pump;
mod lifecycle;
mod mutations;
mod node_runtime;
mod peer_connection;
mod reads;
mod subscriptions;
mod support;
mod transactions;
mod wire_transport;
