use crate::protocol::{
    ClientMessage, CloseReason, ProtocolError, ServerMessage, SettlementTier, SubscriptionId,
};
use crate::session::{DownstreamSession, UpstreamSession};
use crate::{BuiltQuery, Error, Result, Runtime};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

pub trait DownstreamEndpoint {
    fn send_client_message(&mut self, message: ClientMessage);
    fn receive_server_message(&mut self) -> Option<ServerMessage>;
}

pub trait UpstreamEndpoint {
    fn send_server_message(&mut self, message: ServerMessage);
    fn receive_client_message(&mut self) -> Option<ClientMessage>;
}

#[derive(Debug, Default)]
struct Queues {
    downstream_to_upstream: VecDeque<ClientMessage>,
    upstream_to_downstream: VecDeque<ServerMessage>,
}

#[derive(Clone, Debug)]
pub struct DownstreamConnection {
    queues: Rc<RefCell<Queues>>,
}

#[derive(Clone, Debug)]
pub struct UpstreamConnection {
    queues: Rc<RefCell<Queues>>,
}

pub fn in_memory_connection_pair() -> (DownstreamConnection, UpstreamConnection) {
    let queues = Rc::new(RefCell::new(Queues::default()));
    (
        DownstreamConnection {
            queues: Rc::clone(&queues),
        },
        UpstreamConnection { queues },
    )
}

impl DownstreamEndpoint for DownstreamConnection {
    fn send_client_message(&mut self, message: ClientMessage) {
        self.queues
            .borrow_mut()
            .downstream_to_upstream
            .push_back(message);
    }

    fn receive_server_message(&mut self) -> Option<ServerMessage> {
        self.queues.borrow_mut().upstream_to_downstream.pop_front()
    }
}

impl UpstreamEndpoint for UpstreamConnection {
    fn send_server_message(&mut self, message: ServerMessage) {
        self.queues
            .borrow_mut()
            .upstream_to_downstream
            .push_back(message);
    }

    fn receive_client_message(&mut self) -> Option<ClientMessage> {
        self.queues.borrow_mut().downstream_to_upstream.pop_front()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DownstreamConnectionSubscription {
    id: SubscriptionId,
}

impl DownstreamConnectionSubscription {
    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }
}

pub struct DownstreamConnectionManager {
    session: DownstreamSession,
    pending_subscriptions: BTreeMap<SubscriptionId, PendingSubscription>,
    dropped_subscriptions: BTreeSet<SubscriptionId>,
    in_flight_uploads: BTreeSet<String>,
    sent_uploads: BTreeSet<String>,
    max_in_flight_uploads: usize,
    next_subscription_id: u64,
}

pub struct UpstreamConnectionManager {
    session: UpstreamSession,
}

#[derive(Clone, Debug)]
struct PendingSubscription {
    query: BuiltQuery,
    requested_tier: SettlementTier,
}

impl DownstreamConnectionManager {
    pub fn new(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            session: DownstreamSession::new(
                session_id,
                node_id,
                schema_fingerprint,
                policy_fingerprint,
            ),
            pending_subscriptions: BTreeMap::new(),
            dropped_subscriptions: BTreeSet::new(),
            in_flight_uploads: BTreeSet::new(),
            sent_uploads: BTreeSet::new(),
            max_in_flight_uploads: 1000,
            next_subscription_id: 0,
        }
    }

    pub fn open(&mut self) -> Result<Vec<ClientMessage>> {
        let mut batch = DownstreamMessageBatch::empty();
        self.in_flight_uploads.clear();
        self.sent_uploads.clear();
        self.session.open(&mut batch)?;
        Ok(batch.into_client_messages())
    }

    pub fn set_max_in_flight_uploads_for_test(&mut self, max: usize) {
        self.max_in_flight_uploads = max;
    }

    pub fn subscribe(
        &mut self,
        query: BuiltQuery,
        requested_tier: SettlementTier,
    ) -> Result<(DownstreamConnectionSubscription, Vec<ClientMessage>)> {
        if self.session.is_closed() {
            return Err(Error::new("session is closed"));
        }
        let subscription = self.next_subscription()?;
        let id = subscription.id.clone();
        self.dropped_subscriptions.remove(&id);

        if !self.is_ready() {
            self.pending_subscriptions.insert(
                id,
                PendingSubscription {
                    query,
                    requested_tier,
                },
            );
            return Ok((subscription, Vec::new()));
        }

        let mut batch = DownstreamMessageBatch::empty();
        self.session
            .subscribe(&mut batch, id, query, requested_tier)?;
        Ok((subscription, batch.into_client_messages()))
    }

    pub fn replay(&mut self) -> Result<Vec<ClientMessage>> {
        if self.session.is_closed() {
            return Err(Error::new("session is closed"));
        }
        if !self.is_ready() {
            return Ok(Vec::new());
        }

        let mut batch = DownstreamMessageBatch::empty();
        self.session.replay(&mut batch)?;
        Ok(batch.into_client_messages())
    }

    pub fn flush(&mut self, runtime: &mut Runtime) -> Result<Vec<ClientMessage>> {
        if self.session.is_closed() {
            return Err(Error::new("session is closed"));
        }
        if !self.is_ready() {
            return Ok(Vec::new());
        }

        let mut batch = DownstreamMessageBatch::empty();
        self.flush_pending_subscriptions(&mut batch)?;
        self.flush_uploads(runtime, &mut batch)?;
        Ok(batch.into_client_messages())
    }

    pub fn receive(
        &mut self,
        runtime: &mut Runtime,
        server_messages: Vec<ServerMessage>,
    ) -> Result<Vec<ClientMessage>> {
        let server_messages = self.filter_dropped_server_messages(server_messages);
        let should_process_upload_acks = self.is_ready();
        if should_process_upload_acks {
            for message in &server_messages {
                if let ServerMessage::UploadAck { tx_id } = message {
                    if self.in_flight_uploads.remove(tx_id) {
                        runtime.mark_upload_ack(tx_id)?;
                    }
                }
            }
        }
        let protocol_error = server_messages.iter().find_map(|message| match message {
            ServerMessage::Error(error) => Some(error.clone()),
            _ => None,
        });
        let mut batch = DownstreamMessageBatch::with_server_messages(server_messages);
        self.session.pump(runtime, &mut batch)?;
        if let Some(error) = protocol_error {
            return Err(Error::new(format!(
                "protocol error {}: {}",
                error.code, error.message
            )));
        }
        if self.is_ready() {
            batch.extend_client_messages(self.flush(runtime)?);
        }
        Ok(batch.into_client_messages())
    }

    pub fn unsubscribe(
        &mut self,
        subscription: &DownstreamConnectionSubscription,
    ) -> Result<Vec<ClientMessage>> {
        self.pending_subscriptions.remove(subscription.id());
        let was_active = self.session.has_active_subscription(subscription.id());
        self.session.drop_subscription(subscription.id());
        self.dropped_subscriptions.insert(subscription.id().clone());
        if !was_active || !self.is_ready() {
            return Ok(Vec::new());
        }

        let mut batch = DownstreamMessageBatch::empty();
        batch.send_client_message(ClientMessage::Unsubscribe {
            subscription_id: subscription.id().clone(),
        });
        Ok(batch.into_client_messages())
    }

    pub fn is_settled(
        &self,
        subscription: &DownstreamConnectionSubscription,
        tier: SettlementTier,
    ) -> bool {
        self.session.is_settled(subscription.id(), tier)
    }

    pub fn is_ready(&self) -> bool {
        self.session.is_handshake_established() && !self.session.is_closed()
    }

    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    pub fn last_error(&self) -> Option<&ProtocolError> {
        self.session.last_error()
    }

    pub fn close(&mut self, reason: CloseReason) -> Result<Vec<ClientMessage>> {
        let mut batch = DownstreamMessageBatch::empty();
        self.pending_subscriptions.clear();
        self.in_flight_uploads.clear();
        self.sent_uploads.clear();
        self.session.close(&mut batch, reason)?;
        Ok(batch.into_client_messages())
    }

    fn next_subscription(&mut self) -> Result<DownstreamConnectionSubscription> {
        let id = self.next_subscription_id;
        self.next_subscription_id = self
            .next_subscription_id
            .checked_add(1)
            .ok_or_else(|| Error::new("subscription id overflow"))?;
        Ok(DownstreamConnectionSubscription {
            id: SubscriptionId::new(format!("downstream-subscription-{id}")),
        })
    }

    fn flush_pending_subscriptions(&mut self, batch: &mut DownstreamMessageBatch) -> Result<()> {
        let pending = std::mem::take(&mut self.pending_subscriptions);
        for (subscription_id, subscription) in pending {
            self.session.subscribe(
                batch,
                subscription_id,
                subscription.query,
                subscription.requested_tier,
            )?;
        }
        Ok(())
    }

    fn flush_uploads(
        &mut self,
        runtime: &mut Runtime,
        batch: &mut DownstreamMessageBatch,
    ) -> Result<()> {
        let available = self
            .max_in_flight_uploads
            .saturating_sub(self.in_flight_uploads.len());
        if available == 0 {
            return Ok(());
        }
        for upload in runtime.active_uploads(available, &self.sent_uploads)? {
            let tx_id = upload.tx.tx_id.clone();
            batch.send_client_message(ClientMessage::UploadTx {
                tx: upload.tx,
                data: upload.data,
                reads: upload.reads,
            });
            runtime.mark_upload_attempt(&tx_id)?;
            self.in_flight_uploads.insert(tx_id.clone());
            self.sent_uploads.insert(tx_id);
        }
        Ok(())
    }

    fn filter_dropped_server_messages(
        &self,
        server_messages: Vec<ServerMessage>,
    ) -> Vec<ServerMessage> {
        server_messages
            .into_iter()
            .filter(|message| match message {
                ServerMessage::Data {
                    subscription_id: Some(subscription_id),
                    ..
                } => !self.dropped_subscriptions.contains(subscription_id),
                ServerMessage::Settled {
                    subscription_id, ..
                } => !self.dropped_subscriptions.contains(subscription_id),
                ServerMessage::Error(error) => {
                    error
                        .subscription_id
                        .as_ref()
                        .is_none_or(|subscription_id| {
                            !self.dropped_subscriptions.contains(subscription_id)
                        })
                }
                ServerMessage::Hello(_)
                | ServerMessage::Data {
                    subscription_id: None,
                    ..
                }
                | ServerMessage::UploadAck { .. }
                | ServerMessage::TxStatus { .. }
                | ServerMessage::Close(_) => true,
            })
            .collect()
    }
}

impl UpstreamConnectionManager {
    pub fn new(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            session: UpstreamSession::new(
                session_id,
                node_id,
                schema_fingerprint,
                policy_fingerprint,
            ),
        }
    }

    pub fn new_authenticated_for_test(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
        connection_auth_user: impl Into<String>,
    ) -> Self {
        Self::new_authenticated(
            session_id,
            node_id,
            schema_fingerprint,
            policy_fingerprint,
            connection_auth_user,
        )
    }

    pub fn new_authenticated(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
        connection_auth_user: impl Into<String>,
    ) -> Self {
        Self {
            session: UpstreamSession::new_authenticated(
                session_id,
                node_id,
                schema_fingerprint,
                policy_fingerprint,
                connection_auth_user,
            ),
        }
    }

    pub fn receive(
        &mut self,
        runtime: &mut Runtime,
        client_messages: Vec<ClientMessage>,
    ) -> Result<Vec<ServerMessage>> {
        let mut batch = UpstreamMessageBatch::new(client_messages);
        self.session.pump(runtime, &mut batch)?;
        Ok(batch.into_server_messages())
    }

    pub fn refresh_active_subscriptions(
        &mut self,
        runtime: &Runtime,
    ) -> Result<Vec<ServerMessage>> {
        let mut batch = UpstreamMessageBatch::new(Vec::new());
        self.session
            .refresh_active_subscriptions(runtime, &mut batch)?;
        Ok(batch.into_server_messages())
    }

    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    pub fn has_active_subscription(&self, subscription_id: &SubscriptionId) -> bool {
        self.session.has_active_subscription(subscription_id)
    }

    pub fn last_error(&self) -> Option<&ProtocolError> {
        self.session.last_error()
    }
}

#[derive(Debug, Default)]
pub struct DownstreamMessageBatch {
    client_messages: Vec<ClientMessage>,
    server_messages: VecDeque<ServerMessage>,
}

impl DownstreamMessageBatch {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_server_messages(server_messages: Vec<ServerMessage>) -> Self {
        Self {
            client_messages: Vec::new(),
            server_messages: server_messages.into(),
        }
    }

    pub fn into_client_messages(self) -> Vec<ClientMessage> {
        self.client_messages
    }

    fn extend_client_messages(&mut self, messages: Vec<ClientMessage>) {
        self.client_messages.extend(messages);
    }
}

impl DownstreamEndpoint for DownstreamMessageBatch {
    fn send_client_message(&mut self, message: ClientMessage) {
        self.client_messages.push(message);
    }

    fn receive_server_message(&mut self) -> Option<ServerMessage> {
        self.server_messages.pop_front()
    }
}

#[derive(Debug)]
pub struct UpstreamMessageBatch {
    client_messages: VecDeque<ClientMessage>,
    server_messages: Vec<ServerMessage>,
}

impl UpstreamMessageBatch {
    pub fn new(client_messages: Vec<ClientMessage>) -> Self {
        Self {
            client_messages: client_messages.into(),
            server_messages: Vec::new(),
        }
    }

    pub fn into_server_messages(self) -> Vec<ServerMessage> {
        self.server_messages
    }
}

impl UpstreamEndpoint for UpstreamMessageBatch {
    fn send_server_message(&mut self, message: ServerMessage) {
        self.server_messages.push(message);
    }

    fn receive_client_message(&mut self) -> Option<ClientMessage> {
        self.client_messages.pop_front()
    }
}
