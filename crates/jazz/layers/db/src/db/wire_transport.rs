//! Wire-frame adaptation, bounded-message fragmentation, and reassembly.
//!
//! This stays below the database facade: it converts authenticated byte frames
//! into logical sync messages without changing peer dispatch semantics.

use super::{ConnectionSessionContext, Transport};
use crate::protocol::SyncMessage;
use crate::protocol_limits::validate_wire_frame_len;
use crate::wire::{
    TransportError, WIRE_PROTOCOL_VERSION, WireError, WireErrorCode, WireFeatures, WireFrame,
    WireInboundContext, WireRetry, WireSession, WireTransport, current_wire_features,
};
use std::collections::BTreeMap;

/// Outcome of one bounded transport-output turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFlushStatus {
    /// No queued output remains.
    Idle,
    /// More output can be driven in another cooperative turn.
    MoreReady,
    /// A lower queue rejected this exact retained frame; await writable wake.
    Backpressured,
}

/// Outcome of [`WireTransportAdapter::offer`].
#[derive(Debug)]
pub enum WireSendOutcome {
    /// The adapter owns the message and will deliver it in order.
    Accepted,
    /// Backpressure rejected the message before admission; it is returned
    /// unchanged to the caller.
    Rejected(SyncMessage),
}

/// Converts logical messages to mandatory wire-v3 ordered channels.
pub struct WireTransportAdapter<T> {
    inner: T,
    inbound_context: WireInboundContext,
    session_context: Option<ConnectionSessionContext>,
    permits_delegated_sessions: bool,
    endpoint: super::channel_endpoint::ChannelEndpoint,
    auxiliary: super::SharedAuxiliaryEndpoint,
    routes: BTreeMap<Vec<u8>, (u16, u64)>,
    canonical_turns: u8,
    terminal_error: Option<TransportError>,
    last_wire_error: Option<WireError>,
    received_wire_error: bool,
}

impl<T: WireTransport> WireTransportAdapter<T> {
    /// Wrap an admitted byte transport with current wire defaults.
    pub fn current(inner: T) -> Self {
        Self::new(inner, WIRE_PROTOCOL_VERSION, current_wire_features(), None)
    }
    /// Wrap a byte transport with negotiated metadata.
    pub fn new(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
    ) -> Self {
        Self::new_with_session_context(inner, protocol_version, features, session, None)
    }
    /// Wrap a transport with immutable authenticated endpoint facts.
    pub fn new_with_session_context(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
        session_context: Option<ConnectionSessionContext>,
    ) -> Self {
        Self::new_with_session_context_and_delegated_sessions(
            inner,
            protocol_version,
            features,
            session,
            session_context,
            false,
        )
    }
    /// Wrap an admitted trusted backend permitted to forward session bindings.
    pub fn new_with_session_context_and_delegated_sessions(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
        session_context: Option<ConnectionSessionContext>,
        permits_delegated_sessions: bool,
    ) -> Self {
        let context = WireInboundContext::new(protocol_version, features, session);
        let endpoint = super::channel_endpoint::ChannelEndpoint::new(context.clone())
            .expect("valid channel context");
        let mut auxiliary =
            super::AuxiliaryChannelEndpoint::new(context.clone()).expect("valid auxiliary context");
        auxiliary.set_channel_credits(endpoint.channel_credits());
        Self {
            inner,
            inbound_context: context.clone(),
            session_context,
            permits_delegated_sessions,
            endpoint,
            auxiliary: std::sync::Arc::new(std::sync::Mutex::new(auxiliary)),
            routes: BTreeMap::new(),
            canonical_turns: 0,
            terminal_error: None,
            last_wire_error: None,
            received_wire_error: false,
        }
    }
    /// Return the underlying byte transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    #[cfg(test)]
    pub(super) fn set_reassembly_elapsed_for_test(&mut self, elapsed_ms: u64) {
        self.endpoint.set_elapsed_for_test(elapsed_ms);
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn set_incomplete_receive_timeout_for_test(&mut self, timeout_ms: u64) {
        self.endpoint
            .set_incomplete_receive_timeout_for_test(timeout_ms);
    }

    fn route(
        &mut self,
        message: &SyncMessage,
    ) -> Result<(u16, u64, crate::wire::channels::ChannelClass, bool), TransportError> {
        use crate::wire::channels::ChannelClass;
        let (class, barrier) = super::channel_endpoint::message_class(message);
        let fixed = match class {
            ChannelClass::Control => Some(0),
            ChannelClass::Requests => Some(1),
            ChannelClass::Writes => Some(2),
            ChannelClass::Progress => Some(crate::wire::channels::PROGRESS_CHANNEL),
            _ => None,
        };
        if let Some(slot) = fixed {
            return Ok((slot, 0, class, barrier));
        }
        let key = delivery_route_key(message);
        if let Some(&(slot, generation)) = self.routes.get(&key) {
            return Ok((slot, generation, class, barrier));
        }
        let slot = (3..crate::wire::channels::PROGRESS_CHANNEL)
            .find(|slot| self.endpoint.is_idle(*slot))
            .ok_or(TransportError::Backpressure)?;
        let generation = self
            .endpoint
            .next_idle_generation(slot)
            .map_err(TransportError::Failed)?;
        Ok((slot, generation, class, barrier))
    }

    fn flush_turn(&mut self, turns: usize) -> Result<WireFlushStatus, TransportError> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        self.endpoint.expire().map_err(TransportError::Failed)?;
        self.auxiliary
            .lock()
            .map_err(|_| TransportError::Failed("auxiliary mutex poisoned".into()))?
            .expire_incomplete_receive()
            .map_err(TransportError::Failed)?;
        for _ in 0..turns {
            let pump_owned = self
                .auxiliary
                .lock()
                .map_err(|_| TransportError::Failed("auxiliary mutex poisoned".into()))?
                .pump_owned();
            if !pump_owned {
                let credits = self.endpoint.channel_credits();
                let mut credits = credits
                    .lock()
                    .map_err(|_| TransportError::Failed("credit mutex poisoned".into()))?;
                if let Some(grant) = credits.peek_grant().map_err(TransportError::Failed)? {
                    match self.inner.send_frame(grant) {
                        Ok(()) => {
                            credits.accept_grant().map_err(TransportError::Failed)?;
                            continue;
                        }
                        Err(TransportError::Backpressure) => {
                            return Ok(WireFlushStatus::Backpressured);
                        }
                        Err(error) => {
                            self.terminal_error = Some(error.clone());
                            return Err(error);
                        }
                    }
                }
            }
            if !pump_owned && self.canonical_turns >= 4 {
                let mut aux = self
                    .auxiliary
                    .lock()
                    .map_err(|_| TransportError::Failed("auxiliary mutex poisoned".into()))?;
                if aux.outbound_is_ready() {
                    if let Some(frame) = aux.peek_outbound().map_err(TransportError::Failed)? {
                        match self.inner.send_frame(frame) {
                            Ok(()) => {
                                aux.accept_outbound().map_err(TransportError::Failed)?;
                                self.canonical_turns = 0;
                                continue;
                            }
                            Err(TransportError::Backpressure) => {
                                return Ok(WireFlushStatus::Backpressured);
                            }
                            Err(error) => {
                                self.terminal_error = Some(error.clone());
                                return Err(error);
                            }
                        }
                    }
                }
            }
            let canonical = self
                .endpoint
                .peek_outbound()
                .map_err(TransportError::Failed)?;
            if let Some(frame) = canonical {
                match self.inner.send_frame(frame) {
                    Ok(()) => {
                        self.endpoint
                            .accept_outbound()
                            .map_err(TransportError::Failed)?;
                        self.canonical_turns = self.canonical_turns.saturating_add(1);
                    }
                    Err(TransportError::Backpressure) => return Ok(WireFlushStatus::Backpressured),
                    Err(error) => {
                        self.terminal_error = Some(error.clone());
                        return Err(error);
                    }
                }
            } else {
                let mut aux = self.auxiliary.lock().map_err(|_| {
                    TransportError::Failed("auxiliary channel mutex poisoned".into())
                })?;
                if aux.pump_owned() {
                    return Ok(if self.endpoint.has_pending() {
                        WireFlushStatus::Backpressured
                    } else {
                        WireFlushStatus::Idle
                    });
                }
                let Some(frame) = aux.peek_outbound().map_err(TransportError::Failed)? else {
                    return Ok(
                        if self.endpoint.has_pending() || aux.has_pending_outbound() {
                            WireFlushStatus::Backpressured
                        } else {
                            WireFlushStatus::Idle
                        },
                    );
                };
                match self.inner.send_frame(frame) {
                    Ok(()) => {
                        aux.accept_outbound().map_err(TransportError::Failed)?;
                        self.canonical_turns = 0;
                    }
                    Err(TransportError::Backpressure) => return Ok(WireFlushStatus::Backpressured),
                    Err(error) => {
                        self.terminal_error = Some(error.clone());
                        return Err(error);
                    }
                }
            }
        }
        let aux = self
            .auxiliary
            .lock()
            .map_err(|_| TransportError::Failed("auxiliary channel mutex poisoned".into()))?;
        Ok(
            if self.endpoint.has_pending() || (!aux.pump_owned() && aux.has_pending_outbound()) {
                WireFlushStatus::MoreReady
            } else {
                WireFlushStatus::Idle
            },
        )
    }

    /// Strict receive for bootstrap and live channels. Stateful stream errors
    /// terminate the connection; skipping a corrupt compressed extent is unsafe.
    pub fn try_recv_strict(&mut self) -> Result<Option<SyncMessage>, WireError> {
        self.try_recv_result().map_err(|error| {
            self.last_wire_error.clone().unwrap_or_else(|| {
                WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("{error:?}"),
                )
            })
        })
    }

    // Terminal diagnostics are best effort: a full physical queue may prevent
    // delivery, but must not prevent local termination or require an unbounded
    // error queue. The caller retains the exact typed error locally.
    fn send_wire_error(&mut self, error: &WireError) {
        if let Ok(frame) = crate::wire::encode_frame(&WireFrame::Error(error.clone())) {
            let _ = self.inner.send_frame(frame);
        }
    }

    fn receive(&mut self) -> Result<Option<super::ReceivedSyncMessage>, TransportError> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        // Outbound backpressure does not block independent inbound progress.
        let _ = self.flush_turn(1)?;
        if let Some(message) = self.endpoint.pop() {
            return Ok(Some(message));
        }
        while let Some(bytes) = self.inner.try_recv_frame() {
            validate_wire_frame_len(bytes.len()).map_err(TransportError::Failed)?;
            let frame = self
                .inbound_context
                .decode_frame(&bytes)
                .map_err(|error| TransportError::Failed(error.to_string()))?;
            if let WireFrame::Channel(envelope) = &frame {
                if let Err(error) = self.inbound_context.validate_channel_metadata(envelope) {
                    self.last_wire_error = Some(error.clone());
                    return Err(TransportError::Failed(error.message));
                }
            }
            let message = match frame {
                WireFrame::Channel(frame)
                    if frame.extent.channel == crate::wire::channels::AUXILIARY_CHANNEL =>
                {
                    self.auxiliary
                        .lock()
                        .map_err(|_| {
                            TransportError::Failed("auxiliary channel mutex poisoned".into())
                        })?
                        .receive(frame, bytes.len())
                        .map(|message| message.map(super::ReceivedSyncMessage::unleased))
                        .map_err(TransportError::Failed)?
                }
                WireFrame::Channel(frame) => self
                    .endpoint
                    .receive(frame, bytes.len())
                    .map_err(TransportError::Failed)?,
                WireFrame::ChannelCredit(grant) => {
                    self.endpoint
                        .channel_credits()
                        .lock()
                        .map_err(|_| TransportError::Failed("credit mutex poisoned".into()))?
                        .receive_credit(grant)
                        .map_err(TransportError::Failed)?;
                    None
                }
                WireFrame::Error(error) => {
                    self.received_wire_error = true;
                    self.last_wire_error = Some(error.clone());
                    return Err(TransportError::Failed(format!(
                        "remote wire error: {error:?}"
                    )));
                }
                _ => {
                    return Err(TransportError::Failed(
                        "live wire-v3 transport requires ordered channel frames".into(),
                    ));
                }
            };
            if message.is_some() {
                let _ = self.flush_turn(1)?;
                return Ok(message);
            }
        }
        let _ = self.flush_turn(1)?;
        Ok(None)
    }
}

impl<T: WireTransport> WireTransportAdapter<T> {
    /// The structured error the remote peer sent before ending this link.
    ///
    /// Local failures (a closed socket, a frame this side rejected) return
    /// `None`: only the peer's own retry guidance belongs here.
    pub fn remote_wire_error(&self) -> Option<&WireError> {
        if self.received_wire_error {
            self.last_wire_error.as_ref()
        } else {
            None
        }
    }

    /// Offer one logical message, handing it back if it was not admitted.
    ///
    /// `Ok(WireSendOutcome::Rejected(message))` is the only backpressure
    /// outcome: the adapter did not take semantic ownership, so the caller
    /// still owns `message` and must retry it before any later message to
    /// preserve its order. [`Transport::send`] reports the same case as
    /// `TransportError::Backpressure` and drops the message.
    pub fn offer(&mut self, message: SyncMessage) -> Result<WireSendOutcome, TransportError> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        if let Err(error) = crate::wire::ensure_sync_message_features(
            &message,
            self.inbound_context.negotiated_features(),
        ) {
            self.send_wire_error(&error);
            return Ok(WireSendOutcome::Accepted);
        }
        if super::channel_endpoint::message_class(&message).0
            == crate::wire::channels::ChannelClass::Auxiliary
        {
            let admitted = self
                .auxiliary
                .lock()
                .map_err(|_| TransportError::Failed("auxiliary channel mutex poisoned".into()))?
                .try_enqueue(message);
            if let Err(rejected) = admitted {
                return match *rejected {
                    (TransportError::Backpressure, message) => {
                        Ok(WireSendOutcome::Rejected(message))
                    }
                    (error, _) => Err(error),
                };
            }
        } else {
            let (slot, generation, class, barrier) = match self.route(&message) {
                Ok(route) => route,
                Err(TransportError::Backpressure) => {
                    return Ok(WireSendOutcome::Rejected(message));
                }
                Err(error) => return Err(error),
            };
            match self
                .endpoint
                .enqueue(slot, generation, class, &message, barrier)
            {
                Ok(()) => {}
                Err(TransportError::Backpressure) => {
                    return Ok(WireSendOutcome::Rejected(message));
                }
                Err(error) => return Err(error),
            }
            if (3..crate::wire::channels::PROGRESS_CHANNEL).contains(&slot) {
                self.routes.retain(|_, (existing, _)| *existing != slot);
                self.routes
                    .insert(delivery_route_key(&message), (slot, generation));
            }
        }
        // Semantic ownership is now accepted. A rejected physical extent is
        // retained exactly and must never be retried by the semantic caller.
        let result = self.flush_turn(1);
        if let Err(error) = result {
            self.terminal_error = Some(error.clone());
            return Err(error);
        }
        Ok(WireSendOutcome::Accepted)
    }
}

impl<T: WireTransport> Transport for WireTransportAdapter<T> {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        match self.offer(message)? {
            WireSendOutcome::Accepted => Ok(()),
            WireSendOutcome::Rejected(_) => Err(TransportError::Backpressure),
        }
    }
    fn try_send(&mut self, message: SyncMessage) -> Result<Option<SyncMessage>, TransportError> {
        match self.offer(message)? {
            WireSendOutcome::Accepted => Ok(None),
            WireSendOutcome::Rejected(message) => Ok(Some(message)),
        }
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.try_recv_result().ok().flatten()
    }
    fn try_recv_result(&mut self) -> Result<Option<SyncMessage>, TransportError> {
        self.try_recv_owned_result()
            .map(|message| message.map(|message| message.message))
    }
    fn try_recv_owned_result(
        &mut self,
    ) -> Result<Option<super::ReceivedSyncMessage>, TransportError> {
        let was_terminal = self.terminal_error.is_some();
        let result = self.receive();
        if let Err(error) = &result {
            if !was_terminal {
                let wire_error = self
                    .last_wire_error
                    .clone()
                    .or_else(|| self.endpoint.last_wire_error())
                    .or_else(|| {
                        self.auxiliary
                            .lock()
                            .ok()
                            .and_then(|aux| aux.last_wire_error())
                    })
                    .unwrap_or_else(|| {
                        WireError::new(
                            WireErrorCode::MalformedFrame,
                            WireRetry::Never,
                            format!("{error:?}"),
                        )
                    });
                if !self.received_wire_error {
                    self.send_wire_error(&wire_error);
                }
                self.last_wire_error = Some(wire_error);
            }
            self.terminal_error = Some(error.clone());
        }
        result
    }
    fn poll_flush(&mut self) -> Result<WireFlushStatus, TransportError> {
        let result = self.flush_turn(8);
        if let Err(error) = &result {
            self.terminal_error = Some(error.clone());
        }
        result
    }
    #[cfg(any(test, feature = "testing"))]
    fn set_incomplete_receive_timeout_for_test(&mut self, timeout_ms: u64) {
        self.endpoint
            .set_incomplete_receive_timeout_for_test(timeout_ms);
    }

    fn has_terminal_failure(&self) -> bool {
        self.terminal_error.is_some()
    }
    fn remote_wire_error(&self) -> Option<WireError> {
        WireTransportAdapter::remote_wire_error(self).cloned()
    }
    fn incomplete_receive_timeout_ms(&self) -> Option<u64> {
        let auxiliary = self
            .auxiliary
            .lock()
            .ok()
            .and_then(|endpoint| endpoint.incomplete_receive_timeout_ms());
        self.endpoint
            .incomplete_receive_timeout_ms()
            .into_iter()
            .chain(auxiliary)
            .min()
    }
    fn shared_auxiliary_endpoint(&self) -> Option<super::SharedAuxiliaryEndpoint> {
        Some(std::sync::Arc::clone(&self.auxiliary))
    }
    fn set_trusted_encoder(&mut self, trusted: bool) {
        self.inbound_context.set_trusted_encoder(trusted);
        self.endpoint.set_trusted_encoder(trusted);
        self.auxiliary
            .lock()
            .expect("auxiliary mutex poisoned")
            .set_trusted_encoder(trusted);
    }
    fn wire_inbound_context(&self) -> Option<WireInboundContext> {
        Some(self.inbound_context.clone())
    }
    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.session_context
    }
    fn permits_delegated_sessions(&self) -> bool {
        self.permits_delegated_sessions
    }
}

fn delivery_route_key(message: &SyncMessage) -> Vec<u8> {
    use SyncMessage::*;
    let (tag, bytes) = match message {
        ViewUpdate(view) => (0, postcard::to_allocvec(&view.subscription).unwrap()),
        SubscribeRejected { subscription, .. } | AuthorizationScopeReceipt { subscription, .. } => {
            (0, postcard::to_allocvec(subscription).unwrap())
        }
        AuthorizationScopeView { request_id, .. }
        | AuthorizationScopeAggregateReceipt { request_id, .. }
        | AuthorizationScopeUnavailable { request_id }
        | AuthorizationScopeDecision { request_id, .. }
        | PermissionAdviceResponse { request_id, .. } => (1, request_id.0.to_vec()),
        CurrentRowsReceipt(receipt) => (2, receipt.request_id.0.to_vec()),
        ChunkUploadStart(upload) => (3, upload.value_ref.root.object_hash.0.to_vec()),
        ChunkUploadNodes(upload) => (3, upload.value_ref.root.object_hash.0.to_vec()),
        ChunkUploadResult(upload) => (3, upload.value_ref.root.object_hash.0.to_vec()),
        _ => (4, Vec::new()),
    };
    let mut key = vec![tag];
    key.extend(bytes);
    key
}
