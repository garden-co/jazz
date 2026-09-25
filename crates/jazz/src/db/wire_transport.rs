//! Wire-frame adaptation, bounded-message fragmentation, and reassembly.
//!
//! This stays below the database facade: it converts authenticated byte frames
//! into logical sync messages without changing peer dispatch semantics.

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::{BTreeSet, HashMap, VecDeque};

use super::{ConnectionSessionContext, Transport};
use crate::protocol::SyncMessage;
use crate::protocol_limits::validate_wire_frame_len;
#[cfg(test)]
use crate::protocol_limits::{
    MAX_FRAGMENT_REASSEMBLY_AGE_MS, MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
    MAX_INFLIGHT_ENCODED_MESSAGE_BYTES, MAX_INFLIGHT_LOGICAL_MESSAGES,
    validate_encoded_message_len,
};
use crate::wire::{
    TransportError, WIRE_PROTOCOL_VERSION, WireError, WireErrorCode, WireFeatures, WireFrame,
    WireInboundContext, WireRetry, WireSession, WireTransport, current_wire_features,
};
#[cfg(test)]
use crate::wire::{WireEnvelope, WireMessageFragment};

#[cfg(test)]
pub(super) const RECENT_COMPLETED_LOGICAL_MESSAGES: usize = 64;

/// Adapter from postcard wire frames to the internal sync-message transport.
#[cfg(test)]
pub(super) struct IncompleteLogicalMessage {
    protocol_version: u16,
    features: WireFeatures,
    session: Option<WireSession>,
    message_digest: [u8; 32],
    total_len: usize,
    received_len: usize,
    extents: BTreeMap<usize, Vec<u8>>,
    absolute_deadline_ms: u64,
    deadline_ms: u64,
}

#[cfg(test)]
pub(super) struct LogicalMessageReassembler {
    pub(super) incomplete: HashMap<u64, IncompleteLogicalMessage>,
    pub(super) staged_bytes: usize,
    deadlines: BTreeSet<(u64, u64)>,
    staging_budget: usize,
    recently_completed: VecDeque<(u64, [u8; 32])>,
}

#[cfg(test)]
impl Default for LogicalMessageReassembler {
    fn default() -> Self {
        Self {
            incomplete: HashMap::new(),
            staged_bytes: 0,
            deadlines: BTreeSet::new(),
            staging_budget: MAX_INFLIGHT_ENCODED_MESSAGE_BYTES,
            recently_completed: VecDeque::new(),
        }
    }
}

#[cfg(test)]
impl LogicalMessageReassembler {
    #[cfg(test)]
    pub(super) fn with_staging_budget_for_test(staging_budget: usize) -> Self {
        Self {
            staging_budget,
            ..Self::default()
        }
    }

    pub(super) fn discard(&mut self, message_id: u64) {
        if let Some(state) = self.incomplete.remove(&message_id) {
            self.deadlines.remove(&(state.deadline_ms, message_id));
            self.staged_bytes = self.staged_bytes.saturating_sub(state.received_len);
        }
    }

    pub(super) fn expire(&mut self, now_ms: u64) {
        while let Some(&(deadline_ms, message_id)) = self.deadlines.first() {
            if deadline_ms > now_ms {
                break;
            }
            self.deadlines.pop_first();
            if self
                .incomplete
                .get(&message_id)
                .is_some_and(|state| state.deadline_ms == deadline_ms)
            {
                let state = self
                    .incomplete
                    .remove(&message_id)
                    .expect("expired logical message state exists");
                self.staged_bytes = self.staged_bytes.saturating_sub(state.received_len);
            }
        }
    }

    pub(super) fn push(
        &mut self,
        fragment: WireMessageFragment,
        now_ms: u64,
    ) -> Result<Option<WireEnvelope>, String> {
        self.expire(now_ms);
        if let Some((_, digest)) = self
            .recently_completed
            .iter()
            .find(|(message_id, _)| *message_id == fragment.message_id)
        {
            return if digest == &fragment.message_digest {
                Ok(None)
            } else {
                Err("completed logical message id was reused with another digest".to_owned())
            };
        }
        let total_len = usize::try_from(fragment.total_len)
            .map_err(|_| "encoded message length does not fit this receiver".to_owned())?;
        validate_encoded_message_len(total_len)?;
        let offset = usize::try_from(fragment.offset)
            .map_err(|_| "encoded message fragment offset does not fit this receiver".to_owned())?;
        let end = offset
            .checked_add(fragment.payload.len())
            .ok_or_else(|| "encoded message fragment range overflow".to_owned())?;
        if fragment.payload.is_empty() || end > total_len {
            return Err("logical message fragment has an empty or out-of-range extent".to_owned());
        }
        if !self.incomplete.contains_key(&fragment.message_id) {
            if self.incomplete.len() >= MAX_INFLIGHT_LOGICAL_MESSAGES {
                return Err("too many incomplete logical messages for peer".to_owned());
            }
            let absolute_deadline_ms = now_ms.saturating_add(MAX_FRAGMENT_REASSEMBLY_AGE_MS);
            let deadline_ms =
                absolute_deadline_ms.min(now_ms.saturating_add(MAX_FRAGMENT_REASSEMBLY_IDLE_MS));
            self.incomplete.insert(
                fragment.message_id,
                IncompleteLogicalMessage {
                    protocol_version: fragment.protocol_version,
                    features: fragment.features,
                    session: fragment.session.clone(),
                    message_digest: fragment.message_digest,
                    total_len,
                    received_len: 0,
                    extents: BTreeMap::new(),
                    absolute_deadline_ms,
                    deadline_ms,
                },
            );
            self.deadlines.insert((deadline_ms, fragment.message_id));
        }
        let state = self
            .incomplete
            .get_mut(&fragment.message_id)
            .expect("logical message state inserted");
        if state.total_len != total_len
            || state.protocol_version != fragment.protocol_version
            || state.features != fragment.features
            || state.session != fragment.session
            || state.message_digest != fragment.message_digest
        {
            return Err("logical message fragments disagree on metadata".to_owned());
        }
        if let Some(existing) = state.extents.get(&offset) {
            return if existing == &fragment.payload {
                Ok(None)
            } else {
                Err("conflicting duplicate logical message fragment".to_owned())
            };
        }
        if state
            .extents
            .range(..=offset)
            .next_back()
            .is_some_and(|(start, bytes)| *start + bytes.len() > offset)
            || state
                .extents
                .range(offset..)
                .next()
                .is_some_and(|(start, _)| *start < end)
        {
            return Err("overlapping logical message fragments".to_owned());
        }
        let next_staged = self
            .staged_bytes
            .checked_add(fragment.payload.len())
            .ok_or_else(|| "logical message staging byte count overflow".to_owned())?;
        if next_staged > self.staging_budget {
            return Err("incomplete logical messages exceed peer staging budget".to_owned());
        }
        self.staged_bytes = next_staged;
        state.received_len += fragment.payload.len();
        state.extents.insert(offset, fragment.payload);
        if state.received_len != state.total_len {
            let previous_deadline_ms = state.deadline_ms;
            let deadline_ms = state
                .absolute_deadline_ms
                .min(now_ms.saturating_add(MAX_FRAGMENT_REASSEMBLY_IDLE_MS));
            if deadline_ms != previous_deadline_ms {
                state.deadline_ms = deadline_ms;
                self.deadlines
                    .remove(&(previous_deadline_ms, fragment.message_id));
                self.deadlines.insert((deadline_ms, fragment.message_id));
            }
            return Ok(None);
        }

        let state = self
            .incomplete
            .remove(&fragment.message_id)
            .expect("completed logical message state exists");
        self.deadlines
            .remove(&(state.deadline_ms, fragment.message_id));
        self.staged_bytes -= state.received_len;
        let mut cursor = 0;
        let mut payload = Vec::with_capacity(state.total_len);
        for (offset, extent) in state.extents {
            if offset != cursor {
                return Err(
                    "logical message fragments do not provide contiguous coverage".to_owned(),
                );
            }
            cursor += extent.len();
            payload.extend_from_slice(&extent);
        }
        if cursor != state.total_len || *blake3::hash(&payload).as_bytes() != state.message_digest {
            return Err("logical message digest mismatch".to_owned());
        }
        self.recently_completed
            .push_back((fragment.message_id, state.message_digest));
        if self.recently_completed.len() > RECENT_COMPLETED_LOGICAL_MESSAGES {
            self.recently_completed.pop_front();
        }
        Ok(Some(WireEnvelope {
            protocol_version: state.protocol_version,
            features: state.features,
            session: state.session,
            payload,
        }))
    }
}

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
    // Retained only for historical fragment corpus tests, not live admission.
    #[cfg(test)]
    pub(super) reassembler: LogicalMessageReassembler,
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
            #[cfg(test)]
            reassembler: LogicalMessageReassembler::default(),
        }
    }
    /// Return the underlying byte transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    #[cfg(test)]
    pub(super) fn set_reassembly_elapsed_for_test(&mut self, elapsed_ms: u64) {
        self.reassembler.expire(elapsed_ms);
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
        RemoteReadResponse(response) => (2, response.request_id.0.to_vec()),
        ChunkUploadStart(upload) => (3, upload.value_ref.root.object_hash.0.to_vec()),
        ChunkUploadNodes(upload) => (3, upload.value_ref.root.object_hash.0.to_vec()),
        ChunkUploadResult(upload) => (3, upload.value_ref.root.object_hash.0.to_vec()),
        _ => (4, Vec::new()),
    };
    let mut key = vec![tag];
    key.extend(bytes);
    key
}
