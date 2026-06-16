//! Sync-bundle composer and applier.
//!
//! A *bundle* is the server's own outbound payloads for a single query — the
//! catalogue head, the query settlement, and one CRDT [`StoredRowBatch`] per
//! visible row — frozen into a serialisable envelope. A cold client applies it
//! to seed its store before any live sync connects, so the rows are present on
//! the first paint and the later live replay reconciles as a content-addressed
//! no-op rather than a flash-to-empty.
//!
//! The composer drives a *synthetic client* through the server's ordinary
//! subscription-delivery path, then harvests that client's outbox. Reusing the
//! live path means the rows are permission-filtered by the server's own policy
//! evaluation and byte-faithful to what it would have sent over a socket — no
//! parallel serialisation logic to drift.
//!
//! [`StoredRowBatch`]: crate::row_histories::StoredRowBatch

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::query_manager::manager::QueryManager;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::storage::Storage;
use crate::sync_manager::{
    ClientId, Destination, InboxEntry, QueryId, QueryPropagation, ServerId, Source, SyncPayload,
};

/// Current bundle envelope version, stamped on every composed bundle. The
/// payload shape is internal; [`SyncBundle::from_bytes`] rejects any version it
/// does not recognise, so the envelope can evolve without a client mis-applying
/// bytes it cannot read.
const BUNDLE_VERSION: u8 = 1;

/// Upper bound on compose rounds, so a server that never settles cannot spin
/// forever. A cold single-query subscription settles in one or two rounds.
const MAX_COMPOSE_ROUNDS: usize = 32;

/// Query id for the synthetic subscription that drives delivery. It is scoped to
/// a freshly-minted synthetic client id, so it cannot collide with a real
/// downstream client's queries.
const SYNTHETIC_QUERY_ID: QueryId = QueryId(1);

/// An opaque, versioned envelope carrying a server's CRDT state for one query,
/// query-scoped and permission-filtered, ready to seed a cold client's store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBundle {
    // INVARIANT: `version` must stay the first field and be checked before any
    // `payloads` are trusted. postcard is non-self-describing, so a future
    // version's bytes can decode as a current-shape `SyncBundle`; the version
    // gate in `from_bytes` is the only thing that catches it.
    version: u8,
    /// The server's outbound payloads for the synthetic subscription, in the
    /// order the server emitted them.
    payloads: Vec<SyncPayload>,
}

impl SyncBundle {
    /// The envelope version this bundle was composed with.
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Serialise the bundle to its wire bytes for shipping to a client.
    pub fn to_bytes(&self) -> Result<Vec<u8>, SyncBundleError> {
        postcard::to_allocvec(self).map_err(SyncBundleError::Serialize)
    }

    /// Deserialise a bundle from its wire bytes, rejecting any envelope version
    /// this build does not understand.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SyncBundleError> {
        let bundle: Self = postcard::from_bytes(bytes).map_err(SyncBundleError::Deserialize)?;
        if bundle.version != BUNDLE_VERSION {
            return Err(SyncBundleError::UnsupportedVersion(bundle.version));
        }
        Ok(bundle)
    }
}

/// Why a [`SyncBundle`] could not cross the wire boundary.
#[derive(Debug)]
pub enum SyncBundleError {
    /// The bundle could not be serialised to wire bytes.
    Serialize(postcard::Error),
    /// The bytes were not a decodable bundle.
    Deserialize(postcard::Error),
    /// The bundle's envelope version is not understood by this build.
    UnsupportedVersion(u8),
}

impl std::fmt::Display for SyncBundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize(e) => write!(f, "failed to serialise sync bundle: {e}"),
            Self::Deserialize(e) => write!(f, "failed to deserialise sync bundle: {e}"),
            Self::UnsupportedVersion(v) => write!(
                f,
                "unsupported sync bundle version {v} (this build understands {BUNDLE_VERSION})"
            ),
        }
    }
}

impl std::error::Error for SyncBundleError {}

/// Compose a bundle of the server's CRDT state for `query` under `session`.
///
/// Registers a synthetic client, drives the server's subscription delivery for
/// it, and captures the resulting outbound payloads. The synthetic client is
/// removed before returning, so the composing server keeps no phantom state.
pub fn compose_query_bundle<H: Storage>(
    server: &mut QueryManager,
    server_io: &mut H,
    query: Query,
    session: Option<Session>,
) -> SyncBundle {
    let client_id = ClientId(new_synthetic_id());
    server
        .sync_manager_mut()
        .add_client_with_storage(server_io, client_id);

    server.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: SYNTHETIC_QUERY_ID,
            query: Box::new(query),
            session,
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    // Drive the subscription to completion. Against synchronous storage the
    // server settles a single cold query within a couple of rounds, emitting its
    // catalogue head, settlement and row payloads before the first round that
    // produces nothing further — that empty round is the quiescence signal. The
    // round cap is a runaway backstop, not the expected exit.
    let mut payloads = Vec::new();
    let mut settled = false;
    for _ in 0..MAX_COMPOSE_ROUNDS {
        server.process(server_io);
        let mut produced = false;
        let mut retained = Vec::new();
        for entry in server.sync_manager_mut().take_outbox() {
            if matches!(entry.destination, Destination::Client(id) if id == client_id) {
                payloads.push(entry.payload);
                produced = true;
            } else {
                // The composer may share a runtime with live peers; leave their
                // queued outbound sync untouched.
                retained.push(entry);
            }
        }
        server.sync_manager_mut().prepend_outbox(retained);
        if !produced {
            settled = true;
            break;
        }
    }
    if !settled {
        // The bundle may be partial: surface it rather than seeding a client
        // with a silently-incomplete result.
        tracing::warn!(
            max_rounds = MAX_COMPOSE_ROUNDS,
            "sync bundle compose hit the round cap before settling; bundle may be incomplete"
        );
    }

    // Tear down the synthetic subscription so the composing server keeps no
    // phantom state. remove_client alone reaps only SyncManager-owned state; the
    // QueryManager's server_subscriptions entry is removed by processing an
    // unsubscription, so a long-lived server does not re-settle it every tick.
    server.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QueryUnsubscription {
            query_id: SYNTHETIC_QUERY_ID,
        },
    });
    server.process(server_io);
    server.sync_manager_mut().remove_client(client_id);

    SyncBundle {
        version: BUNDLE_VERSION,
        payloads,
    }
}

/// Apply a composed bundle to a cold client, seeding its store.
///
/// The bundle's payloads are replayed through the client's inbox as if a server
/// delivered them, so they travel the exact live-sync ingest path: catalogue
/// and rows land in storage with server provenance (not as pending local
/// writes), keyed by their original batch ids. A later live connection then
/// reconciles them as a no-op.
pub fn apply_query_bundle<H: Storage>(
    client: &mut QueryManager,
    client_io: &mut H,
    bundle: &SyncBundle,
) {
    let server_id = ServerId(new_synthetic_id());
    for payload in &bundle.payloads {
        client.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(server_id),
            payload: payload.clone(),
        });
    }
    client.process(client_io);
}

fn new_synthetic_id() -> Uuid {
    Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext))
}
