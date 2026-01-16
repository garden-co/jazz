//! Native Driver for the runtime-less sync engine.
//!
//! This module provides a tokio-based driver that:
//! - Owns a `SyncEngine` from groove
//! - Handles SSE streams and HTTP requests via spawned tasks
//! - Runs a tick timer via tokio::time
//!
//! The driver follows the same inbox/outbox pattern as the WASM driver,
//! but uses tokio's async runtime for I/O.
//!
//! Note: HTTP client implementation is TODO - requires adding hyper-util client feature
//! or using a different HTTP client like reqwest.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;

use groove::ObjectId;
use groove::sync::{
    ConnectionEvent, ConnectionEventKind, Encode, Inboxes, LocalWriteEvent, Notification,
    OutboundRequest, Outboxes, PushRequest, PushResponse, PushResponseEvent, SseEvent,
    SseInboxEvent, StorageRequest, StreamAction, SubscribeRequestEvent, SubscriptionOptions,
    SyncEngine, TickEvent, UpstreamId,
};

// ============================================================================
// Driver Events (internal)
// ============================================================================

/// Events that flow from async tasks back to the driver.
enum DriverEvent {
    /// SSE event received
    Sse {
        upstream_id: UpstreamId,
        subscription_id: u32,
        event: SseEvent,
    },
    /// SSE stream opened
    StreamOpened {
        upstream_id: UpstreamId,
        subscription_id: u32,
    },
    /// SSE stream closed/errored
    StreamClosed {
        upstream_id: UpstreamId,
        subscription_id: u32,
        error: Option<String>,
    },
    /// Push response received
    PushResponse {
        upstream_id: UpstreamId,
        object_id: ObjectId,
        result: Result<PushResponse, String>,
    },
    /// Tick timer fired
    Tick { now_ms: u64 },
}

// ============================================================================
// NativeSyncDriver
// ============================================================================

/// A native driver for the runtime-less sync engine.
///
/// This driver runs in a tokio runtime and handles all async I/O,
/// feeding events into the sync engine via inboxes.
pub struct NativeSyncDriver {
    /// The sync engine
    engine: SyncEngine,
    /// Server URL for sync
    server_url: String,
    /// Auth token for requests
    auth_token: String,
    /// Channel for receiving events from async tasks
    event_rx: mpsc::UnboundedReceiver<DriverEvent>,
    /// Channel sender (cloned for each async task)
    event_tx: mpsc::UnboundedSender<DriverEvent>,
    /// Active stream handles (for cancellation)
    active_streams: HashMap<(u64, u32), tokio::task::JoinHandle<()>>,
    /// Upstream server ID
    upstream_id: UpstreamId,
    /// Callback for notifications
    on_notification: Option<Box<dyn Fn(&Notification) + Send>>,
}

impl NativeSyncDriver {
    /// Create a new native sync driver.
    pub fn new(server_url: String, auth_token: String, engine: SyncEngine) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let upstream_id = UpstreamId(0); // Will be set when add_upstream is called

        Self {
            engine,
            server_url,
            auth_token,
            event_rx,
            event_tx,
            active_streams: HashMap::new(),
            upstream_id,
            on_notification: None,
        }
    }

    /// Set a callback for notifications.
    pub fn set_on_notification<F>(&mut self, callback: F)
    where
        F: Fn(&Notification) + Send + 'static,
    {
        self.on_notification = Some(Box::new(callback));
    }

    /// Add an upstream server and return its ID.
    pub fn add_upstream(&mut self) -> UpstreamId {
        self.upstream_id = self.engine.add_upstream();
        self.upstream_id
    }

    /// Connect to the sync server with a query.
    pub fn connect(&mut self, query: String) {
        // Create subscribe request
        let inboxes = Inboxes {
            subscribe_requests: vec![SubscribeRequestEvent {
                upstream_id: self.upstream_id,
                query,
                options: SubscriptionOptions::default(),
            }],
            ..Default::default()
        };

        // Run a pass
        let outboxes = self.engine.pass(inboxes);

        // Handle outboxes
        self.handle_outboxes(outboxes);
    }

    /// Process a local write (e.g., from Database layer).
    pub fn write(&mut self, object_id: ObjectId, branch: String, content: Vec<u8>) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let inboxes = Inboxes {
            local_writes: vec![LocalWriteEvent {
                object_id,
                branch,
                content,
                author: "native".to_string(),
                timestamp: now_ms,
            }],
            ..Default::default()
        };

        let outboxes = self.engine.pass(inboxes);
        self.handle_outboxes(outboxes);
    }

    /// Run the driver event loop.
    ///
    /// This processes events from async tasks and runs tick passes.
    pub async fn run(&mut self) {
        // Start tick timer
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if tx.send(DriverEvent::Tick { now_ms }).is_err() {
                    break;
                }
            }
        });

        // Process events
        while let Some(event) = self.event_rx.recv().await {
            let inboxes = match event {
                DriverEvent::Sse {
                    upstream_id,
                    subscription_id,
                    event,
                } => Inboxes {
                    sse_events: vec![SseInboxEvent {
                        upstream_id,
                        subscription_id,
                        event,
                    }],
                    ..Default::default()
                },
                DriverEvent::StreamOpened {
                    upstream_id,
                    subscription_id,
                } => Inboxes {
                    connection_events: vec![ConnectionEvent {
                        upstream_id,
                        event: ConnectionEventKind::StreamOpened { subscription_id },
                    }],
                    ..Default::default()
                },
                DriverEvent::StreamClosed {
                    upstream_id,
                    subscription_id,
                    error,
                } => Inboxes {
                    connection_events: vec![ConnectionEvent {
                        upstream_id,
                        event: ConnectionEventKind::StreamClosed {
                            subscription_id,
                            error,
                        },
                    }],
                    ..Default::default()
                },
                DriverEvent::PushResponse {
                    upstream_id,
                    object_id,
                    result,
                } => Inboxes {
                    push_responses: vec![PushResponseEvent {
                        upstream_id,
                        object_id,
                        result,
                    }],
                    ..Default::default()
                },
                DriverEvent::Tick { now_ms } => Inboxes {
                    tick: Some(TickEvent { now_ms }),
                    ..Default::default()
                },
            };

            let outboxes = self.engine.pass(inboxes);
            self.handle_outboxes(outboxes);
        }
    }

    /// Handle outboxes from a pass.
    fn handle_outboxes(&mut self, outboxes: Outboxes) {
        // Handle stream actions
        for action in outboxes.stream_actions {
            match action {
                StreamAction::Open {
                    upstream_id,
                    subscription_id,
                    query,
                    options: _,
                } => {
                    self.open_sse_stream(upstream_id, subscription_id, &query);
                }
                StreamAction::Close {
                    upstream_id,
                    subscription_id,
                } => {
                    let key = (upstream_id.0, subscription_id);
                    if let Some(handle) = self.active_streams.remove(&key) {
                        handle.abort();
                    }
                }
            }
        }

        // Handle outbound requests
        for request in outboxes.requests {
            match request {
                OutboundRequest::Push {
                    upstream_id,
                    request,
                } => {
                    self.send_push_request(upstream_id, request);
                }
                OutboundRequest::Reconcile { .. } => {
                    // TODO: Implement reconcile
                }
                OutboundRequest::Unsubscribe { .. } => {
                    // TODO: Implement unsubscribe
                }
            }
        }

        // Handle notifications
        if let Some(ref callback) = self.on_notification {
            for notification in &outboxes.notifications {
                callback(notification);
            }
        }

        // Handle storage requests
        // For MemoryEnvironment, we run synchronously since it's instant.
        // A production driver with async storage would need Arc<dyn Environment + Send + Sync>.
        if !outboxes.storage.is_empty() {
            let env = self.engine.local_node.env().expect("env required").clone();
            futures::executor::block_on(async {
                for request in outboxes.storage {
                    match request {
                        StorageRequest::PutCommit { commit } => {
                            env.put_commit(&commit).await;
                        }
                        StorageRequest::SetFrontier {
                            object_id,
                            branch,
                            frontier,
                        } => {
                            env.set_frontier(object_id.into(), &branch, &frontier).await;
                        }
                        StorageRequest::PutChunk { data, .. } => {
                            use bytes::Bytes;
                            env.put_chunk(Bytes::from(data)).await;
                        }
                        StorageRequest::GetChunk { .. } => {
                            // TODO: For native driver, we would need to handle async
                            // responses. For now, native driver doesn't use lazy loading.
                        }
                        StorageRequest::LoadObject { .. } => {
                            // TODO: For native driver, we would need to handle async
                            // responses. For now, native driver doesn't use lazy loading.
                        }
                    }
                }
            });
        }
    }

    /// Open an SSE stream to the server.
    fn open_sse_stream(&mut self, upstream_id: UpstreamId, subscription_id: u32, query: &str) {
        let url = format!(
            "{}/sync/events?token={}&query={}",
            self.server_url,
            urlencoding::encode(&self.auth_token),
            urlencoding::encode(query)
        );

        let tx = self.event_tx.clone();
        let handle = tokio::spawn(async move {
            // Notify stream opened
            let _ = tx.send(DriverEvent::StreamOpened {
                upstream_id,
                subscription_id,
            });

            // TODO: Implement SSE stream using hyper
            // For now, this is a placeholder that immediately closes
            // A full implementation would:
            // 1. Create a hyper client
            // 2. Send GET request to the SSE endpoint
            // 3. Read the streaming response
            // 4. Parse SSE events and send them via tx

            let _ = tx.send(DriverEvent::StreamClosed {
                upstream_id,
                subscription_id,
                error: Some("SSE not yet implemented".to_string()),
            });
        });

        let key = (upstream_id.0, subscription_id);
        self.active_streams.insert(key, handle);
    }

    /// Send a push request to the server.
    fn send_push_request(&mut self, upstream_id: UpstreamId, request: PushRequest) {
        let url = format!("{}/sync/push", self.server_url);
        let auth_token = self.auth_token.clone();
        let object_id = request.object_id;
        let tx = self.event_tx.clone();

        tokio::spawn(async move {
            let body = request.to_bytes();

            // TODO: Implement HTTP POST using hyper
            // For now, this is a placeholder that returns an error
            // A full implementation would:
            // 1. Create a hyper client
            // 2. Send POST request with the body
            // 3. Read the response and decode PushResponse

            let _ = tx.send(DriverEvent::PushResponse {
                upstream_id,
                object_id,
                result: Err("Push not yet implemented".to_string()),
            });
        });
    }

    /// Get a reference to the sync engine.
    pub fn engine(&self) -> &SyncEngine {
        &self.engine
    }

    /// Get a mutable reference to the sync engine.
    pub fn engine_mut(&mut self) -> &mut SyncEngine {
        &mut self.engine
    }
}
