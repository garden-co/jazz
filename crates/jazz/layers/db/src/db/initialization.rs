//! Private initialization capabilities. Reserved identities are journal linkage,
//! never public committed identities or permission to choose a commit identity.

use super::*;
use crate::wire::encode_sync_message;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReservedTxId(pub(crate) TxId);

impl ReservedTxId {
    /// Stable private journal encoding: version, node UUID, HLC milliseconds and counter.
    pub fn encode(self) -> String {
        format!(
            "jazz-init-v1:{}:{}:{}",
            self.0.node.0,
            self.0.time.physical_ms(),
            self.0.time.counter()
        )
    }

    pub fn decode(value: &str) -> Result<Self, Error> {
        let invalid = || {
            Error::new(
                ErrorCode::Protocol,
                "invalid reserved initialization identity",
            )
        };
        let mut parts = value.split(':');
        if parts.next() != Some("jazz-init-v1") {
            return Err(invalid());
        }
        let node = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<uuid::Uuid>()
            .map_err(|_| invalid())?;
        let physical = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        let counter = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<u32>()
            .map_err(|_| invalid())?;
        if parts.next().is_some()
            || physical > crate::time::HLC_MAX_PHYSICAL_MS
            || counter > crate::time::HLC_MAX_LOGICAL_COUNTER
        {
            return Err(invalid());
        }
        let id = Self(TxId::new(TxTime::new(physical, counter), NodeUuid(node)));
        if id.encode() != value {
            return Err(invalid());
        }
        Ok(id)
    }
}

#[doc(hidden)]
pub struct InitializationSeal {
    reservation: ReservedTxId,
    open: OpenTransactionId,
    nonce: OpenTransactionId,
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl InitializationSeal {
    pub fn reservation(&self) -> ReservedTxId {
        self.reservation
    }
}

impl Drop for InitializationSeal {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

#[doc(hidden)]
pub use crate::node::InitializationTransactionStatus;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Call after the existing owner preparation queue has drained. Freezes the
    /// same exclusive state; it does not enqueue or publish a commit.
    #[doc(hidden)]
    pub async fn seal_initialization_transaction(
        &self,
        open: OpenTransactionId,
    ) -> Result<InitializationSeal, Error> {
        self.seal_initialization_transaction_at_ms(open, self.next_now_ms())
            .await
    }

    #[doc(hidden)]
    pub async fn seal_initialization_transaction_at_ms(
        &self,
        open: OpenTransactionId,
        now_ms: u64,
    ) -> Result<InitializationSeal, Error> {
        if let Some(error) = self.queued_transaction_error(open) {
            return Err(error);
        }
        let mut node = self.lock_for_transaction_operation(open).await?;
        node.check_staged_transaction_identity(open, self.identity.author, self.identity.author)?;
        let reservation = ReservedTxId(self.reserve_transaction_id_at_ms(now_ms)?);
        let nonce = OpenTransactionId::new();
        node.seal_initialization_transaction(open, reservation.0, nonce, self.identity.author)?;
        let owner = Rc::downgrade(&self.node);
        Ok(InitializationSeal {
            reservation,
            open,
            nonce,
            cleanup: Some(Box::new(move || {
                if let Some(owner) = owner.upgrade() {
                    owner.abandon_or_enqueue_transaction(open);
                }
            })),
        })
    }

    #[doc(hidden)]
    pub async fn publish_initialization_transaction(
        &self,
        mut seal: InitializationSeal,
    ) -> Result<WriteHandle<S>, Error> {
        self.lock_for_transaction_operation(seal.open)
            .await?
            .check_initialization_seal(seal.open, seal.reservation.0, seal.nonce)?;
        let open = seal.open;
        let tx_id = seal.reservation.0;
        let nonce = seal.nonce;
        let db = self.clone_for_reserved_transaction(tx_id);
        let status = self.node.enqueue_transaction_commit(
            open,
            tx_id,
            TxKind::Exclusive,
            Box::pin(async move {
                let (published, unit) = db
                    .lock_for_transaction_operation(open)
                    .await?
                    .publish_initialization_transaction(open, tx_id, nonce)
                    .await?;
                db.finish_exclusive_publication(published, unit).await?;
                Ok(())
            }),
        );
        seal.cleanup = None;
        Ok(self.queued_write_handle(RowUuid::from_bytes([0; 16]), tx_id, status, None))
    }

    #[doc(hidden)]
    pub async fn cancel_initialization_transaction(
        &self,
        mut seal: InitializationSeal,
    ) -> Result<(), Error> {
        let mut node = self.lock_for_transaction_operation(seal.open).await?;
        node.check_initialization_seal(seal.open, seal.reservation.0, seal.nonce)?;
        node.abandon_tx(seal.open)?;
        seal.cleanup = None;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn prepare_initialization_insert(
        &self,
        open: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.lock_for_transaction_operation(open)
            .await?
            .prepare_initialization_insert(open, self.schema_version_id, table, row)
            .await?;
        Ok(())
    }

    /// Exact, bounded queries derive author scope from this already admitted
    /// owner. A foreign record and a missing record are indistinguishable.
    #[doc(hidden)]
    pub async fn initialization_transaction_status(
        &self,
        ids: &[ReservedTxId],
    ) -> Result<Vec<InitializationTransactionStatus>, Error> {
        if ids.len() > 64 {
            return Err(Error::new(
                ErrorCode::Protocol,
                "initialization status accepts at most 64 identities",
            ));
        }
        let mut node = self.node.node.lock().await;
        if node
            .client_relay_scope()
            .is_some_and(|scope| !scope.admits_bound_session(self.identity.author))
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                "initialization status requires the admitted relay session",
            ));
        }
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            results.push(
                node.initialization_transaction_status(id.0, self.identity.author)
                    .await?,
            );
        }
        Ok(results)
    }
    /// Restore exact pending units after a direct persistent owner's foreground
    /// lease rotates. This is not a subscriber admission or a replay protocol.
    ///
    /// # Safety
    /// The host must have proof-admitted this author's exclusive storage scope
    /// and call this during open, before attaching transport or admitting writes.
    #[doc(hidden)]
    pub async unsafe fn restore_initialization_owner_pending_uploads(&self) -> Result<(), Error> {
        let mut node = self.node.node.lock().await;
        if node
            .client_relay_scope()
            .is_some_and(|scope| !scope.admits_bound_session(self.identity.author))
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                "initialization replay requires the admitted owner",
            ));
        }
        let pending = node
            .pending_transaction_ids_for_author(self.identity.author)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let (statuses, _, units) = plan_local_replay_commit_units(
            &mut node,
            &pending,
            &pending,
            &BTreeMap::new(),
            LocalReplayMode::InitializationOwner,
        )
        .await?;
        if let Some(id) = pending
            .iter()
            .find(|id| !matches!(statuses.get(id), Some(LocalReplayStatus::Complete)))
        {
            return Err(Error::new(
                ErrorCode::Storage,
                format!(
                    "incomplete owned pending transaction during initialization recovery: {}",
                    ReservedTxId(*id).encode(),
                ),
            ));
        }
        drop(node);
        // No queue side effect occurs until every owned pending root is proven
        // complete. The planner's dependency order and exact units are retained.
        for (id, unit) in units {
            if pending.contains(&id) {
                self.node.queue_pending_upload(id, Some(unit));
            }
        }
        Ok(())
    }
}

// Version 1 embeds the maintained, canonical wire-v3 CatalogueSnapshot codec.
// The source is the stable authenticated authority node, not a local schema ID.
const CATALOGUE_CAPTURE_HEADER: &[u8] = b"JAZZ-CATALOGUE\0\x01\x03";

pub(super) fn encode_catalogue_capture(
    source: NodeUuid,
    message: &SyncMessage,
) -> Result<Vec<u8>, Error> {
    let payload = encode_sync_message(message).map_err(|error| {
        Error::new(
            ErrorCode::Protocol,
            format!("catalogue cache encoding failed: {error}"),
        )
    })?;
    let mut bytes = Vec::with_capacity(CATALOGUE_CAPTURE_HEADER.len() + 16 + payload.len());
    bytes.extend_from_slice(CATALOGUE_CAPTURE_HEADER);
    bytes.extend_from_slice(source.0.as_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(super) fn decode_catalogue_capture(
    bytes: &[u8],
) -> Result<crate::protocol::CatalogueSnapshot, Error> {
    let invalid = || Error::new(ErrorCode::Protocol, "invalid authenticated catalogue cache");
    let body = bytes
        .strip_prefix(CATALOGUE_CAPTURE_HEADER)
        .ok_or_else(invalid)?;
    if body.len() <= 16 {
        return Err(invalid());
    }
    match crate::wire::decode_sync_message(&body[16..]).map_err(|_| invalid())? {
        SyncMessage::CatalogueSnapshot(snapshot) => Ok(*snapshot),
        _ => Err(invalid()),
    }
}

/// Atomic single-owner observation of authenticated catalogue availability.
/// Captured bytes move once; the host must retain them until durable publication.
#[doc(hidden)]
#[derive(Debug)]
pub struct AuthenticatedCatalogueState {
    pub capture: Option<Vec<u8>>,
    pub ready: bool,
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Await the catalogue owner, then observe readiness and drain its capture
    /// under the same lock. Contention is pending, never an empty observation.
    #[doc(hidden)]
    pub async fn take_authenticated_catalogue_state(
        &self,
    ) -> Result<AuthenticatedCatalogueState, Error> {
        let mut node = self.node.node.lock().await;
        let (capture, ready) = node.take_authenticated_catalogue_state();
        Ok(AuthenticatedCatalogueState { capture, ready })
    }
}

/// Check monotone replacement of a host's authenticated captures. Source-node
/// rotation is allowed: authority identity is authenticated at capture time,
/// while the host independently binds both captures to the same app scope.
#[doc(hidden)]
pub fn validate_catalogue_capture_replacement(previous: &[u8], next: &[u8]) -> Result<(), Error> {
    let previous = decode_catalogue_capture(previous)?;
    let next = decode_catalogue_capture(next)?;
    crate::node::validate_catalogue_snapshot_replacement(&previous, &next)?;
    Ok(())
}
