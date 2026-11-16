//! Thin sessions' transactions, served by the backend (SPEC 8.14).
//!
//! A thin client's socket is relayed to the core as for any client, except
//! that its transaction requests stay here: [`ThinRouter`] splits the
//! client's frames, and a [`SessionClient`] serves the transaction ones. It
//! is an in-memory client linked to the core as the socket's session (the
//! same backend-vouched session, pinned to the same bundle), so the core
//! reads and checks for it exactly as for the session, and only ever
//! accepts finished transactions from it. The client starts on the
//! session's first transaction request and stops with the socket, rolling
//! back what it left open.

use std::sync::Arc;
use std::time::Duration;

use jazz::thin::{ThinBody, ThinOp, ThinRequest, ThinResponse};
use jazz::tools::{AppContext, ClientStorage, JazzClient, Schema, Session};
use tokio::sync::mpsc;

pub(crate) use jazz::thin_transactions::{ThinRouter, answer_frames};

/// How long a transaction request waits for the core (a commit, for its
/// verdict) before it is answered without it.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a thin socket's requests may wait on an answer, from the core
/// or the backend, before the socket is closed: past the backend's own
/// [`REQUEST_TIMEOUT`], an answer is lost, and the client reconnects.
pub(crate) const ANSWER_DEADLINE: Duration = Duration::from_secs(60);

/// Who the session's client connects to the core as: what the socket's
/// prelude stated, on the backend's word.
#[derive(Clone)]
pub(crate) enum SessionAuth {
    /// A signed-in user's session, vouched for by the backend.
    User(Session),
    /// A device account, on its own proof, relayed by the backend.
    DeviceAccount(String),
}

/// Where the session's client connects, and as whom.
#[derive(Clone)]
pub(crate) struct SessionTarget {
    pub server: String,
    pub app_id: jazz::tools::AppId,
    pub backend_secret: String,
    pub bundle: String,
    pub auth: SessionAuth,
}

impl SessionTarget {
    fn context(&self, schema: Schema) -> AppContext {
        let mut context = AppContext {
            app_id: self.app_id,
            client_id: None,
            schema,
            server_url: self.server.clone(),
            data_dir: std::path::PathBuf::new(),
            storage: ClientStorage::Memory,
            account_id: None,
            jwt_token: None,
            backend_secret: Some(self.backend_secret.clone()),
            backend_session: None,
            bundle: Some(self.bundle.clone()),
            admin_secret: None,
            migrations: Default::default(),
        };
        match &self.auth {
            SessionAuth::User(session) => {
                context.account_id = session.account_id;
                context.backend_session = Some(session.clone());
            }
            SessionAuth::DeviceAccount(token) => context.jwt_token = Some(token.clone()),
        }
        context
    }
}

/// A thin session's transactions, served by a client of its own on a
/// thread of its own (a client is not `Send`). Requests are served one at
/// a time, in order; their answers come back on the socket's channel.
pub(crate) struct SessionClient {
    requests: mpsc::UnboundedSender<ThinRequest>,
    /// The socket's answers, to refuse a request no client will serve.
    answers: mpsc::UnboundedSender<ThinResponse>,
}

impl SessionClient {
    /// Start serving: the client connects on the first request, with
    /// `schema` (the public schema JSON the thin client bound).
    pub fn start(
        target: SessionTarget,
        schema: Vec<u8>,
        answers: mpsc::UnboundedSender<ThinResponse>,
    ) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        let spawned = std::thread::Builder::new()
            .name("jazz-thin-session".into())
            .spawn({
                let answers = answers.clone();
                move || run(target, schema, receiver, answers)
            });
        if let Err(error) = spawned {
            // Its receiver is gone with the closure: every request is
            // refused in `send`.
            tracing::warn!(%error, "cannot start a thin session's client thread");
        }
        Self { requests, answers }
    }

    /// Serve `request`; its answer comes on the socket's channel.
    pub fn send(&self, request: ThinRequest) {
        if let Err(unsent) = self.requests.send(request) {
            let _ = self.answers.send(refused(
                unsent.0.id,
                "storage",
                "the backend's client for this session stopped",
            ));
        }
    }
}

fn refused(id: u64, code: &str, message: impl Into<String>) -> ThinResponse {
    ThinResponse {
        id,
        body: ThinBody::Error {
            code: code.to_owned(),
            message: message.into(),
        },
    }
}

fn run(
    target: SessionTarget,
    schema: Vec<u8>,
    mut requests: mpsc::UnboundedReceiver<ThinRequest>,
    answers: mpsc::UnboundedSender<ThinResponse>,
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        refuse_all(
            &mut requests,
            &answers,
            "the backend could not start the session's client",
        );
        return;
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let Some(first) = requests.recv().await else {
            return;
        };
        let opened = match serde_json::from_slice::<Schema>(&schema) {
            Ok(schema) => {
                JazzClient::connect_with_native_transport(
                    target.context(schema),
                    Arc::new(jazz_native_transport::NativeWebSocketConnector),
                )
                .await
            }
            Err(error) => Err(jazz::tools::JazzError::Schema(error.to_string())),
        };
        let client = match opened {
            Ok(client) => client,
            Err(error) => {
                let message = format!("the backend could not open the session's client: {error}");
                let _ = answers.send(refused(first.id, "storage", message.clone()));
                requests.close();
                while let Some(request) = requests.recv().await {
                    let _ = answers.send(refused(request.id, "storage", message.clone()));
                }
                return;
            }
        };
        let mut transactions = match client.thin_transactions() {
            Ok(transactions) => transactions,
            Err(error) => {
                let message = format!("the session's client cannot serve transactions: {error}");
                let _ = answers.send(refused(first.id, "Argument", message.clone()));
                requests.close();
                while let Some(request) = requests.recv().await {
                    let _ = answers.send(refused(request.id, "Argument", message.clone()));
                }
                let _ = client.shutdown().await;
                return;
            }
        };
        let mut next = Some(first);
        while let Some(request) = match next.take() {
            Some(request) => Some(request),
            None => requests.recv().await,
        } {
            // A core that does not answer must not hold the session's later
            // requests forever. A commit that times out may still reach the
            // core: the client is told its fate is unknown, not that it
            // failed.
            let served = tokio::time::timeout(
                REQUEST_TIMEOUT,
                client.serve_thin_transaction(&mut transactions, &request),
            )
            .await;
            let body = match served {
                Ok(Ok(body)) => body,
                Ok(Err(error)) => ThinBody::Error {
                    code: "storage".to_owned(),
                    message: error.to_string(),
                },
                Err(_) => ThinBody::Error {
                    code: "not_observed".to_owned(),
                    message: match request.op {
                        ThinOp::Commit { .. } => format!(
                            "the core did not confirm the commit within {}s: it may still apply",
                            REQUEST_TIMEOUT.as_secs()
                        ),
                        _ => format!(
                            "the core did not answer within {}s",
                            REQUEST_TIMEOUT.as_secs()
                        ),
                    },
                },
            };
            if answers
                .send(ThinResponse {
                    id: request.id,
                    body,
                })
                .is_err()
            {
                break;
            }
        }
        // The socket ended: what it left open rolls back.
        let _ = client.release_thin_transactions(transactions).await;
        let _ = client.shutdown().await;
    });
}

fn refuse_all(
    requests: &mut mpsc::UnboundedReceiver<ThinRequest>,
    answers: &mpsc::UnboundedSender<ThinResponse>,
    message: &str,
) {
    requests.close();
    while let Ok(request) = requests.try_recv() {
        let _ = answers.send(refused(request.id, "storage", message));
    }
}
