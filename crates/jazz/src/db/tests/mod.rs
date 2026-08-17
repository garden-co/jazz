use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use groove::records::{EnumCase, EnumSchema, EnumValue, RecordDescriptor, ValueType};
use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{OrderedKvStorage, ReopenableStorage};
use jazz_storage_rocksdb::RocksDbStorage;

use super::*;
use crate::ids::{AuthorId, BranchId, NodeUuid};
use crate::protocol::{
    AuthorizationScopePurpose, AuthorizationScopeReceipt, AuthorizationSupportScopeKey,
    BindingViewKey, BranchMetadata, CatalogueAck, KnownStateCompleteness, KnownStateDeclaration,
    LensOp, PermissionAdviceAction, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
    ResultMemberEntry, RowVersionRef, ShapeAst, SnapshotRef, Subscribe, SubscribeRejectReason,
    SubscribeServerFailureCode, TableLens,
};
use crate::protocol_limits::{
    MAX_FETCH_ROW_VERSIONS, MAX_INFLIGHT_LOGICAL_MESSAGES, MAX_KNOWN_STATE_EXACT_REFS,
    MAX_LOGICAL_MESSAGE_BYTES, MAX_SHAPE_AST_BYTES, MAX_WIRE_FRAME_BYTES,
};
use crate::query::{
    ArraySubquery, BindingId, Include, JoinMode, OrderDirection, PolicyBranch, Predicate,
    RelationOrderBy, ShapeId, all_of, any_of, claim, col, contains, eq, gt, in_list, is_null, lit,
    lte, ne, not, param,
};
use crate::schema::{Policy, TableSchema, WritePolicies};
use crate::time::{GlobalSeq, TxTime};
use crate::tx::TxId;
use crate::wire::{
    FEATURE_MESSAGE_FRAGMENTATION, FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD,
    WIRE_PROTOCOL_VERSION, WireEnvelope, WireError, WireErrorCode, WireFrame, WireMessageFragment,
    WireRetry, WireSession, WireStreamDecoder, WireTransport, current_wire_features, decode_frame,
    decode_sync_message, encode_frame,
};

use super::peer_connection::{
    PendingRowVersionRepair, aggregate_authorization_scope_bounds,
    authorization_scope_receipt_matches_transport_context,
    authorization_scope_support_options_match, remove_scope_aggregate_member, view_update_is_empty,
};
use catalogue::assert_authority_rejects_staged_write;
use support::block_on;
use support::*;
use wire_transport::byte_duplex_with_session;

mod catalogue;
mod lifecycle;
mod mutations;
mod node_runtime;
mod peer_connection;
mod reads;
mod subscriptions;
mod support;
mod transactions;
mod wire_transport;
