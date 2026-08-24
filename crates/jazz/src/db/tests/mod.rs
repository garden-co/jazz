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
use crate::ids::{AuthorId, NodeUuid};
use crate::legacy_test_future::{
    FutureResolveExt as _, OptionFutureExt as _, ResultFutureExt as _, SettledNodeTestExt as _,
};
use crate::protocol::{
    AuthorizationScopePurpose, AuthorizationScopeReceipt, AuthorizationSupportScopeKey,
    BindingViewKey, CatalogueAck, KnownStateCompleteness, KnownStateDeclaration, LensOp,
    PermissionAdviceAction, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
    ResultMemberEntry, RowVersionRef, ShapeAst, SnapshotRef, Subscribe, SubscribeRejectReason,
    SubscribeServerFailureCode, TableLens,
};
use crate::protocol_limits::{
    MAX_FETCH_ROW_VERSIONS, MAX_INFLIGHT_LOGICAL_MESSAGES, MAX_KNOWN_STATE_EXACT_REFS,
    MAX_LOGICAL_MESSAGE_BYTES, MAX_SHAPE_AST_BYTES, MAX_WIRE_FRAME_BYTES,
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
    aggregate_authorization_scope_bounds, authorization_scope_receipt_matches_transport_context,
    authorization_scope_support_options_match, remove_scope_aggregate_member, view_update_is_empty,
};

#[test]
fn retryable_chunk_response_keeps_the_original_waiter_until_the_scheduled_reissue() {
    use groove::chunks::MissingChunkResolver;

    let resolver = PeerChunkResolver::default();
    resolver.register_connection(7, true);
    let request = groove::chunks::ChunkRequest {
        object_hash: [0x31; 32],
        locator: b"retryable-binding-receipt".to_vec(),
    };
    let waiter = resolver.resolve(request.clone());
    let response_id = resolver.take_outbound(1)[0].request_id;
    resolver.complete(ChunkResponseEntry {
        request_id: response_id,
        result: ChunkResponse::Retryable { retry_after_ms: 1 },
    });
    assert!(resolver.take_outbound(1).is_empty(), "no eager reissue");
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(resolver.promote_due_retries(), 1, "one scheduled reissue");
    let retry = resolver.take_outbound(1);
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].request_id, response_id, "same pending request id");
    resolver.complete(ChunkResponseEntry {
        request_id: response_id,
        result: ChunkResponse::Found(vec![0x42]),
    });
    assert_eq!(crate::db::block_on(waiter).unwrap().as_ref(), &[0x42]);
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
