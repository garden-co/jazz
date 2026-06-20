use crate::connection::{DownstreamEndpoint, UpstreamEndpoint};
use crate::protocol::{
    ClientHello, ClientMessage, CloseReason, MessageId, ProtocolCapabilities, ProtocolError,
    ProtocolVersion, ReplayCursor, ReplaySubscription, RetryHint, ServerHello, ServerMessage,
    SessionId, SettlementTier, SubscriptionId, TxStatusKind, SUPPORTED_PROTOCOL_VERSION,
};
use crate::{BuiltQuery, Error, Result, Runtime};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
struct ActiveSubscription {
    query: BuiltQuery,
    requested_tier: SettlementTier,
    last_applied_cursor: Option<ReplayCursor>,
}

pub struct DownstreamSession {
    hello: ClientHello,
    awaiting_server_hello: bool,
    upstream_hello: Option<ServerHello>,
    active_subscriptions: BTreeMap<SubscriptionId, ActiveSubscription>,
    settled: BTreeMap<(SubscriptionId, SettlementTier), ReplayCursor>,
    closed: bool,
    last_error: Option<ProtocolError>,
}

pub struct UpstreamSession {
    hello: ServerHello,
    schema_fingerprint: String,
    policy_fingerprint: String,
    connection_auth_user: Option<String>,
    peer_hello: Option<ClientHello>,
    active_subscriptions: BTreeMap<SubscriptionId, ActiveSubscription>,
    pending_messages: BTreeMap<MessageId, (SubscriptionId, ReplayCursor)>,
    last_acknowledged: BTreeMap<SubscriptionId, ReplayCursor>,
    last_error: Option<ProtocolError>,
    next_message_id: u64,
    next_cursor: u64,
    closed: bool,
}

impl DownstreamSession {
    pub fn new(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        Self::new_with_protocol_version(
            session_id,
            node_id,
            SUPPORTED_PROTOCOL_VERSION.0,
            schema_fingerprint,
            policy_fingerprint,
        )
    }

    pub fn new_with_protocol_version(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        protocol_version: u32,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            hello: ClientHello {
                protocol_version: ProtocolVersion(protocol_version),
                session_id: SessionId::new(session_id),
                node_id: node_id.into(),
                schema_fingerprint: schema_fingerprint.into(),
                policy_fingerprint: policy_fingerprint.into(),
            },
            awaiting_server_hello: false,
            upstream_hello: None,
            active_subscriptions: BTreeMap::new(),
            settled: BTreeMap::new(),
            closed: false,
            last_error: None,
        }
    }

    pub fn open(&mut self, conn: &mut impl DownstreamEndpoint) -> Result<()> {
        ensure_open(self.closed)?;
        self.upstream_hello = None;
        self.awaiting_server_hello = true;
        self.settled.clear();
        self.last_error = None;
        conn.send_client_message(ClientMessage::Hello(self.hello.clone()));
        Ok(())
    }

    pub fn subscribe(
        &mut self,
        conn: &mut impl DownstreamEndpoint,
        subscription_id: SubscriptionId,
        query: BuiltQuery,
        requested_tier: SettlementTier,
    ) -> Result<()> {
        ensure_open(self.closed)?;
        ensure_handshake(self.upstream_hello.is_some())?;
        if self.active_subscriptions.contains_key(&subscription_id) {
            return Err(Error::new("subscription is already active"));
        }
        self.active_subscriptions.insert(
            subscription_id.clone(),
            ActiveSubscription {
                query: query.clone(),
                requested_tier,
                last_applied_cursor: None,
            },
        );
        self.settled
            .remove(&(subscription_id.clone(), requested_tier));
        conn.send_client_message(ClientMessage::Subscribe {
            subscription_id,
            query,
            requested_tier,
        });
        Ok(())
    }

    pub fn replay(&mut self, conn: &mut impl DownstreamEndpoint) -> Result<()> {
        ensure_open(self.closed)?;
        ensure_handshake(self.upstream_hello.is_some())?;
        let subscriptions = self
            .active_subscriptions
            .iter()
            .map(|(subscription_id, subscription)| ReplaySubscription {
                subscription_id: subscription_id.clone(),
                query: subscription.query.clone(),
                requested_tier: subscription.requested_tier,
                last_applied_cursor: subscription.last_applied_cursor,
            })
            .collect();
        conn.send_client_message(ClientMessage::Replay { subscriptions });
        Ok(())
    }

    pub fn close(&mut self, conn: &mut impl DownstreamEndpoint, reason: CloseReason) -> Result<()> {
        ensure_open(self.closed)?;
        self.clear_session_state();
        self.closed = true;
        conn.send_client_message(ClientMessage::Close(reason));
        Ok(())
    }

    pub fn pump(
        &mut self,
        runtime: &mut Runtime,
        conn: &mut impl DownstreamEndpoint,
    ) -> Result<()> {
        while let Some(message) = conn.receive_server_message() {
            if self.closed {
                break;
            }
            match message {
                ServerMessage::Hello(hello) => {
                    self.receive_server_hello(hello, conn);
                }
                ServerMessage::Data {
                    message_id,
                    subscription_id,
                    cursor,
                    bundle,
                } => {
                    if self.upstream_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        return Err(Error::new("handshake is not established"));
                    }
                    if subscription_id.as_ref().is_some_and(|subscription_id| {
                        !self.active_subscriptions.contains_key(subscription_id)
                    }) {
                        self.close_with_error(conn, "unknown_subscription", "unknown subscription");
                        return Err(Error::new("unknown subscription"));
                    }
                    if let Err(error) = runtime.apply_bundle(&bundle) {
                        self.close_with_error(conn, "bundle_apply_failed", &error.to_string());
                        return Err(error);
                    }
                    if let Some(subscription_id) = subscription_id {
                        let advanced = {
                            let subscription = self
                                .active_subscriptions
                                .get_mut(&subscription_id)
                                .expect("validated active subscription before bundle apply");
                            let previous_cursor = subscription.last_applied_cursor;
                            let next_cursor = previous_cursor
                                .map(|current| current.max(cursor))
                                .unwrap_or(cursor);
                            subscription.last_applied_cursor = Some(next_cursor);
                            Some(next_cursor) != previous_cursor
                        };
                        if advanced {
                            self.settled.retain(|(settled_subscription_id, _), _| {
                                settled_subscription_id != &subscription_id
                            });
                        }
                    }
                    conn.send_client_message(ClientMessage::Ack {
                        message_id,
                        cursor: Some(cursor),
                    });
                }
                ServerMessage::Settled {
                    subscription_id,
                    tier,
                    cursor,
                } => {
                    if self.upstream_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        return Err(Error::new("handshake is not established"));
                    }
                    if self
                        .active_subscriptions
                        .get(&subscription_id)
                        .is_some_and(|subscription| {
                            subscription.requested_tier == tier
                                && subscription.last_applied_cursor == Some(cursor)
                        })
                    {
                        self.settled.insert((subscription_id, tier), cursor);
                    }
                }
                ServerMessage::UploadAck { .. } => {
                    if self.upstream_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        return Err(Error::new("handshake is not established"));
                    }
                }
                ServerMessage::TxStatus { tx_id, status } => {
                    if self.upstream_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        return Err(Error::new("handshake is not established"));
                    }
                    runtime.apply_tx_status(&tx_id, status)?;
                }
                ServerMessage::Error(error) => {
                    let fatal = error.retry_hint == RetryHint::Fatal;
                    if !fatal {
                        if let Some(subscription_id) = &error.subscription_id {
                            self.active_subscriptions.remove(subscription_id);
                            self.settled.retain(|(settled_subscription_id, _), _| {
                                settled_subscription_id != subscription_id
                            });
                        }
                    }
                    self.last_error = Some(error);
                    if fatal {
                        self.clear_session_state();
                        self.closed = true;
                        break;
                    }
                }
                ServerMessage::Close(_) => {
                    self.clear_session_state();
                    self.closed = true;
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn is_settled(&self, subscription_id: &SubscriptionId, tier: SettlementTier) -> bool {
        self.active_subscriptions
            .get(subscription_id)
            .is_some_and(|subscription| {
                if subscription.requested_tier != tier {
                    return false;
                }
                let Some(cursor) = subscription.last_applied_cursor else {
                    return false;
                };
                self.settled.get(&(subscription_id.clone(), tier)).copied() == Some(cursor)
            })
    }

    pub fn has_active_subscription(&self, subscription_id: &SubscriptionId) -> bool {
        self.active_subscriptions.contains_key(subscription_id)
    }

    pub fn drop_subscription(&mut self, subscription_id: &SubscriptionId) {
        self.active_subscriptions.remove(subscription_id);
        self.settled
            .retain(|(settled_subscription_id, _), _| settled_subscription_id != subscription_id);
    }

    pub fn is_handshake_established(&self) -> bool {
        self.upstream_hello.is_some()
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn last_error(&self) -> Option<&ProtocolError> {
        self.last_error.as_ref()
    }

    fn receive_server_hello(&mut self, hello: ServerHello, conn: &mut impl DownstreamEndpoint) {
        if !self.awaiting_server_hello {
            self.close_with_error(conn, "protocol_error", "unexpected server hello");
            return;
        }
        if hello.protocol_version != SUPPORTED_PROTOCOL_VERSION {
            self.close_with_error(
                conn,
                "unsupported_protocol_version",
                "unsupported protocol version",
            );
            return;
        }
        if !hello.capabilities.replay
            || !hello.capabilities.acknowledgements
            || !hello.capabilities.query_settlement
            || !hello.capabilities.tx_upload
        {
            self.close_with_error(
                conn,
                "unsupported_capability",
                "server is missing a required protocol capability",
            );
            return;
        }
        self.awaiting_server_hello = false;
        self.upstream_hello = Some(hello);
    }

    fn close_with_error(&mut self, conn: &mut impl DownstreamEndpoint, code: &str, message: &str) {
        self.last_error = Some(ProtocolError::new(code, message, RetryHint::Fatal));
        self.clear_session_state();
        self.closed = true;
        conn.send_client_message(ClientMessage::Close(CloseReason::ProtocolError));
    }

    fn clear_session_state(&mut self) {
        self.active_subscriptions.clear();
        self.settled.clear();
    }
}

impl UpstreamSession {
    pub fn new(
        session_id: impl Into<String>,
        node_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            hello: ServerHello {
                protocol_version: SUPPORTED_PROTOCOL_VERSION,
                session_id: SessionId::new(session_id),
                node_id: node_id.into(),
                capabilities: ProtocolCapabilities::default(),
            },
            schema_fingerprint: schema_fingerprint.into(),
            policy_fingerprint: policy_fingerprint.into(),
            connection_auth_user: None,
            peer_hello: None,
            active_subscriptions: BTreeMap::new(),
            pending_messages: BTreeMap::new(),
            last_acknowledged: BTreeMap::new(),
            last_error: None,
            next_message_id: 1,
            next_cursor: 1,
            closed: false,
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
        let mut session = Self::new(session_id, node_id, schema_fingerprint, policy_fingerprint);
        session.connection_auth_user = Some(connection_auth_user.into());
        session
    }

    pub fn pump(&mut self, runtime: &mut Runtime, conn: &mut impl UpstreamEndpoint) -> Result<()> {
        while let Some(message) = conn.receive_client_message() {
            if self.closed {
                break;
            }
            match message {
                ClientMessage::Hello(hello) => self.receive_hello(hello, conn)?,
                ClientMessage::Subscribe {
                    subscription_id,
                    query,
                    requested_tier,
                } => {
                    ensure_open(self.closed)?;
                    if self.peer_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        continue;
                    }
                    if self.active_subscriptions.contains_key(&subscription_id) {
                        self.send_protocol_error(
                            conn,
                            "duplicate_subscription",
                            "subscription is already active",
                            Some(subscription_id),
                            None,
                            RetryHint::Retryable,
                        );
                        continue;
                    }
                    if requested_tier != SettlementTier::Local {
                        self.send_protocol_error(
                            conn,
                            "unsupported_settlement_tier",
                            "only local settlement is supported",
                            Some(subscription_id),
                            None,
                            RetryHint::Retryable,
                        );
                        continue;
                    }
                    if let Err(error) = self.send_subscription_data(
                        runtime,
                        conn,
                        subscription_id.clone(),
                        query,
                        requested_tier,
                    ) {
                        self.send_scoped_error(conn, "query_rejected", error, subscription_id);
                    }
                }
                ClientMessage::Replay { subscriptions } => {
                    ensure_open(self.closed)?;
                    if self.peer_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        continue;
                    }
                    let replay_subscription_ids = subscriptions
                        .iter()
                        .map(|subscription| subscription.subscription_id.clone())
                        .collect::<BTreeSet<_>>();
                    self.active_subscriptions.retain(|subscription_id, _| {
                        replay_subscription_ids.contains(subscription_id)
                    });
                    self.pending_messages.retain(|_, (subscription_id, _)| {
                        replay_subscription_ids.contains(subscription_id)
                    });
                    self.last_acknowledged.retain(|subscription_id, _| {
                        replay_subscription_ids.contains(subscription_id)
                    });
                    for subscription in subscriptions {
                        if subscription.requested_tier != SettlementTier::Local {
                            self.send_protocol_error(
                                conn,
                                "unsupported_settlement_tier",
                                "only local settlement is supported",
                                Some(subscription.subscription_id),
                                None,
                                RetryHint::Retryable,
                            );
                            continue;
                        }
                        if let Err(error) = self.send_subscription_data(
                            runtime,
                            conn,
                            subscription.subscription_id.clone(),
                            subscription.query,
                            subscription.requested_tier,
                        ) {
                            self.send_scoped_error(
                                conn,
                                "query_rejected",
                                error,
                                subscription.subscription_id,
                            );
                        }
                    }
                }
                ClientMessage::Ack {
                    message_id,
                    cursor: _,
                } => {
                    if self.peer_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        continue;
                    }
                    if let Some((subscription_id, pending_cursor)) =
                        self.pending_messages.remove(&message_id)
                    {
                        let acknowledged_cursor = self
                            .last_acknowledged
                            .get(&subscription_id)
                            .copied()
                            .map(|current| current.max(pending_cursor))
                            .unwrap_or(pending_cursor);
                        self.last_acknowledged
                            .insert(subscription_id, acknowledged_cursor);
                    }
                }
                ClientMessage::UploadTx { tx, data, reads } => {
                    ensure_open(self.closed)?;
                    if self.peer_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        continue;
                    }
                    let Some(connection_auth_user) = self.connection_auth_user.as_deref() else {
                        self.close_with_error(
                            conn,
                            "auth_required",
                            "transaction upload requires authenticated connection",
                        );
                        continue;
                    };
                    conn.send_server_message(ServerMessage::UploadAck {
                        tx_id: tx.tx_id.clone(),
                    });
                    let peer_node_id = self
                        .peer_hello
                        .as_ref()
                        .expect("handshake checked before upload")
                        .node_id
                        .clone();
                    match runtime.apply_upload_tx_as_user(
                        &tx,
                        &data,
                        &reads,
                        &peer_node_id,
                        connection_auth_user,
                    ) {
                        Ok(Some(status)) => conn.send_server_message(ServerMessage::TxStatus {
                            tx_id: tx.tx_id,
                            status,
                        }),
                        Ok(None) => {}
                        Err(error) => conn.send_server_message(ServerMessage::TxStatus {
                            tx_id: tx.tx_id,
                            status: TxStatusKind::Rejected {
                                code: "upload_rejected".to_owned(),
                                detail: Some(json!({
                                    "message": error.to_string(),
                                })),
                            },
                        }),
                    }
                }
                ClientMessage::Unsubscribe { subscription_id } => {
                    ensure_open(self.closed)?;
                    if self.peer_hello.is_none() {
                        self.close_with_error(
                            conn,
                            "protocol_error",
                            "handshake is not established",
                        );
                        continue;
                    }
                    self.active_subscriptions.remove(&subscription_id);
                    self.pending_messages
                        .retain(|_, (pending_subscription_id, _)| {
                            pending_subscription_id != &subscription_id
                        });
                    self.last_acknowledged.remove(&subscription_id);
                }
                ClientMessage::Close(_) => {
                    self.clear_session_state();
                    self.closed = true;
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn last_acknowledged_cursor(
        &self,
        subscription_id: &SubscriptionId,
    ) -> Option<ReplayCursor> {
        self.last_acknowledged.get(subscription_id).copied()
    }

    pub fn refresh_active_subscriptions(
        &mut self,
        runtime: &Runtime,
        conn: &mut impl UpstreamEndpoint,
    ) -> Result<()> {
        ensure_open(self.closed)?;
        if self.peer_hello.is_none() {
            return Err(Error::new("handshake is not established"));
        }

        let subscriptions = self
            .active_subscriptions
            .iter()
            .map(|(subscription_id, subscription)| {
                (
                    subscription_id.clone(),
                    subscription.query.clone(),
                    subscription.requested_tier,
                )
            })
            .collect::<Vec<_>>();

        for (subscription_id, query, requested_tier) in subscriptions {
            self.send_subscription_data(runtime, conn, subscription_id, query, requested_tier)?;
        }
        Ok(())
    }

    pub fn has_active_subscription(&self, subscription_id: &SubscriptionId) -> bool {
        self.active_subscriptions.contains_key(subscription_id)
    }

    pub fn last_error(&self) -> Option<&ProtocolError> {
        self.last_error.as_ref()
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn receive_hello(
        &mut self,
        hello: ClientHello,
        conn: &mut impl UpstreamEndpoint,
    ) -> Result<()> {
        if hello.protocol_version != SUPPORTED_PROTOCOL_VERSION {
            self.close_with_error(
                conn,
                "unsupported_protocol_version",
                "unsupported protocol version",
            );
            return Ok(());
        }
        if hello.schema_fingerprint != self.schema_fingerprint {
            self.close_with_error(
                conn,
                "incompatible_schema_fingerprint",
                "incompatible schema fingerprint",
            );
            return Ok(());
        }
        if hello.policy_fingerprint != self.policy_fingerprint {
            self.close_with_error(
                conn,
                "incompatible_policy_fingerprint",
                "incompatible policy fingerprint",
            );
            return Ok(());
        }
        self.clear_session_state();
        self.last_error = None;
        self.peer_hello = Some(hello);
        conn.send_server_message(ServerMessage::Hello(self.hello.clone()));
        Ok(())
    }

    fn send_subscription_data(
        &mut self,
        runtime: &Runtime,
        conn: &mut impl UpstreamEndpoint,
        subscription_id: SubscriptionId,
        query: BuiltQuery,
        requested_tier: SettlementTier,
    ) -> Result<()> {
        let bundle = runtime.export_query(query.clone())?;
        let cursor = self.next_cursor();
        let message_id = self.next_message_id();
        self.active_subscriptions.insert(
            subscription_id.clone(),
            ActiveSubscription {
                query,
                requested_tier,
                last_applied_cursor: Some(cursor),
            },
        );
        self.pending_messages
            .insert(message_id, (subscription_id.clone(), cursor));
        conn.send_server_message(ServerMessage::Data {
            message_id,
            subscription_id: Some(subscription_id.clone()),
            cursor,
            bundle,
        });
        if requested_tier == SettlementTier::Local {
            conn.send_server_message(ServerMessage::Settled {
                subscription_id,
                tier: SettlementTier::Local,
                cursor,
            });
        }
        Ok(())
    }

    fn close_with_error(&mut self, conn: &mut impl UpstreamEndpoint, code: &str, message: &str) {
        self.clear_session_state();
        self.closed = true;
        let error = ProtocolError::new(code, message, RetryHint::Fatal);
        self.last_error = Some(error.clone());
        conn.send_server_message(ServerMessage::Error(error));
        conn.send_server_message(ServerMessage::Close(CloseReason::ProtocolError));
    }

    fn send_protocol_error(
        &mut self,
        conn: &mut impl UpstreamEndpoint,
        code: &str,
        message: impl Into<String>,
        subscription_id: Option<SubscriptionId>,
        message_id: Option<MessageId>,
        retry_hint: RetryHint,
    ) {
        let protocol_error = ProtocolError {
            code: code.to_owned(),
            message: message.into(),
            subscription_id,
            message_id,
            retry_hint,
        };
        self.last_error = Some(protocol_error.clone());
        conn.send_server_message(ServerMessage::Error(protocol_error));
    }

    fn send_scoped_error(
        &mut self,
        conn: &mut impl UpstreamEndpoint,
        code: &str,
        error: Error,
        subscription_id: SubscriptionId,
    ) {
        self.send_protocol_error(
            conn,
            code,
            error.to_string(),
            Some(subscription_id),
            None,
            RetryHint::Retryable,
        );
    }

    fn next_message_id(&mut self) -> MessageId {
        let id = self.next_message_id;
        self.next_message_id += 1;
        MessageId(id)
    }

    fn next_cursor(&mut self) -> ReplayCursor {
        let cursor = self.next_cursor;
        self.next_cursor += 1;
        ReplayCursor(cursor)
    }

    fn clear_session_state(&mut self) {
        self.active_subscriptions.clear();
        self.pending_messages.clear();
        self.last_acknowledged.clear();
    }
}

fn ensure_open(closed: bool) -> Result<()> {
    if closed {
        return Err(Error::new("session is closed"));
    }
    Ok(())
}

fn ensure_handshake(established: bool) -> Result<()> {
    if !established {
        return Err(Error::new("handshake is not established"));
    }
    Ok(())
}
