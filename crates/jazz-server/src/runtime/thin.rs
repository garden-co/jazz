//! Thin sessions (SPEC 8.14): connections that hold no replica. The owner
//! evaluates their queries under the session's identity and claims, sends
//! finished results, and runs their writes as that session.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jazz::binding_codec;
use jazz::binding_host::{EventChunk, decode_cells, decode_schema, event_chunk};
use jazz::db::{
    DeleteOptions, Error, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    SubscriptionStream, UpdateOptions, UpsertOptions, WriteHandle, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, RowUuid, SchemaVersionId};
use jazz::protocol::ReadViewSpec;
use jazz::records::Value;
use jazz::thin::{ThinBody, ThinOp, ThinReadTier, ThinRequest, ThinResponse};
use jazz::wire::{WireErrorCode, WireFrame, WireRetry, encode_frame};
use tokio::sync::mpsc as tokio_mpsc;

use super::{Outbound, Owner, SESSION_QUEUE_BUDGET, ServerSession};

pub(super) struct ThinSession {
    identity: AuthorSubject,
    /// The identity's claims, bound to every read; `None` for SYSTEM.
    claims: Option<BTreeMap<String, Value>>,
    /// A session under its own identity (not a trusted backend link).
    untrusted: bool,
    /// The schema version the client declared.
    version: Option<SchemaVersionId>,
    /// Live queries, by the id of the request that opened them.
    subscriptions: BTreeMap<u64, SubscriptionStream>,
    /// Answers produced while the session's egress budget is spent.
    held: Vec<ThinResponse>,
    output: tokio_mpsc::UnboundedSender<Outbound>,
    egress: Arc<AtomicUsize>,
}

impl ThinSession {
    pub(super) fn new(
        identity: AuthorSubject,
        claims: Option<BTreeMap<String, Value>>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            identity,
            claims,
            untrusted: identity != AuthorSubject::SYSTEM,
            version: None,
            subscriptions: BTreeMap::new(),
            held: Vec::new(),
            output,
            egress,
        }
    }

    fn has_room(&self) -> bool {
        self.egress.load(Ordering::Acquire) < SESSION_QUEUE_BUDGET
    }

    /// Send `responses` as one batch. False once the socket side is gone.
    fn send(&self, responses: Vec<ThinResponse>) -> bool {
        if responses.is_empty() {
            return true;
        }
        let mut frames = Vec::with_capacity(responses.len());
        for response in responses {
            match encode_frame(&WireFrame::ThinResponse(response)) {
                Ok(frame) => frames.push(frame),
                Err(error) => {
                    tracing::error!(%error, "encode thin response");
                    let _ = self.output.send((
                        Err(jazz::wire::WireError::new(
                            WireErrorCode::Internal,
                            WireRetry::Never,
                            format!("thin session closed: {error}"),
                        )),
                        0,
                    ));
                    return false;
                }
            }
        }
        let bytes = frames.iter().map(Vec::len).sum::<usize>();
        self.egress.fetch_add(bytes, Ordering::AcqRel);
        self.output.send((Ok(frames), bytes)).is_ok()
    }
}

fn error_body(error: &Error) -> ThinBody {
    ThinBody::Error {
        code: format!("{:?}", error.code),
        message: error.message.clone(),
    }
}

fn argument(message: impl Into<String>) -> ThinBody {
    ThinBody::Error {
        code: "Argument".to_owned(),
        message: message.into(),
    }
}

fn read_opts(tier: ThinReadTier, include_deleted: bool) -> ReadOpts {
    ReadOpts {
        tier: match tier {
            ThinReadTier::LocalFirst => ReadTier::LocalFirst,
            ThinReadTier::Remote => ReadTier::Remote,
        },
        read_view: ReadViewSpec {
            include_deleted,
            ..ReadViewSpec::default()
        },
        ..ReadOpts::default()
    }
}

fn written(write: Result<WriteHandle, Error>) -> ThinBody {
    match write {
        Ok(write) => ThinBody::Written {
            row: *write.row_uuid().as_bytes(),
            seq: write.mergeable_tx_id(),
        },
        Err(error) => error_body(&error),
    }
}

impl Owner {
    pub(super) fn open_thin_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) {
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        self.thin
            .insert(session, ThinSession::new(identity, claims, output, egress));
    }

    pub(super) fn close_thin_session(&mut self, session: ServerSession) {
        // Dropping the streams ends the subscriptions.
        self.thin.remove(&session);
    }

    pub(super) fn thin_request(&mut self, session: ServerSession, request: ThinRequest) {
        let Some(thin) = self.thin.get(&session) else {
            return;
        };
        let body = if self.awaiting_permissions && thin.untrusted {
            ThinBody::Error {
                code: "NotReady".to_owned(),
                message: "no permissions are published for this app yet; deploy them before \
                          connecting thin clients"
                    .to_owned(),
            }
        } else {
            match self.serve_thin(session, request.id, request.op) {
                Some(body) => body,
                None => return,
            }
        };
        let response = ThinResponse {
            id: request.id,
            body,
        };
        let thin = self.thin.get_mut(&session).expect("checked above");
        if thin.has_room() && thin.held.is_empty() {
            if !thin.send(vec![response]) {
                self.close_thin_session(session);
            }
        } else {
            thin.held.push(response);
        }
    }

    /// Serve one request; `None` when it has no immediate answer (a live
    /// query answers from [`Owner::pump_thin`]).
    fn serve_thin(&mut self, session: ServerSession, id: u64, op: ThinOp) -> Option<ThinBody> {
        let thin = self.thin.get_mut(&session)?;
        if let ThinOp::Schema { schema } = op {
            // The version a replica built against this schema would declare.
            let version = match decode_schema(&schema) {
                Ok(schema) => schema.version_id(),
                Err(error) => return Some(argument(format!("{error:?}"))),
            };
            return Some(match self.db.governed_view(version) {
                Some(_) => {
                    thin.version = Some(version);
                    ThinBody::Ok
                }
                None => ThinBody::Error {
                    code: "SchemaNotDeployed".to_owned(),
                    message: "the client's schema version is not deployed to this server"
                        .to_owned(),
                },
            });
        }
        if let ThinOp::Unsubscribe { subscription } = op {
            thin.subscriptions.remove(&subscription);
            // The live query's own id hears that it ended.
            if subscription != id {
                thin.held.push(ThinResponse {
                    id: subscription,
                    body: ThinBody::Closed,
                });
            }
            return Some(ThinBody::Ok);
        }
        let Some(version) = thin.version else {
            return Some(argument("a thin session declares its schema version first"));
        };
        // Resolved per request: an activation changes the policies that
        // govern the declared version.
        let Some(db) = self.db.governed_view(version) else {
            return Some(ThinBody::Error {
                code: "SchemaNotDeployed".to_owned(),
                message: "the client's schema version is no longer deployed".to_owned(),
            });
        };
        let identity = thin.identity;
        let scope = thin.claims.clone().map(|claims| (identity, claims));
        let write_identity = WriteIdentity::Session(identity);
        Some(match op {
            ThinOp::Schema { .. } | ThinOp::Unsubscribe { .. } => unreachable!("served above"),
            ThinOp::Subscribe {
                query,
                include_deleted,
                tier,
            } => {
                if thin.subscriptions.contains_key(&id) {
                    return Some(argument("a live query with this id is already open"));
                }
                let query: jazz::query::Query = match postcard::from_bytes(&query) {
                    Ok(query) => query,
                    Err(error) => return Some(argument(format!("undecodable query: {error}"))),
                };
                let opened = db.prepare_query(&query).and_then(|mut prepared| {
                    if let Some((author, claims)) = scope {
                        prepared = prepared.with_identity_claims(author, claims);
                    }
                    block_on(db.subscribe_for_identity(
                        &prepared,
                        read_opts(tier, include_deleted),
                        identity,
                    ))
                });
                match opened {
                    Ok(stream) => {
                        thin.subscriptions.insert(id, stream);
                        return None;
                    }
                    Err(error) => error_body(&error),
                }
            }
            ThinOp::Query {
                query,
                include_deleted,
                tier,
            } => {
                // The authority holds everything a read can ask for: it
                // never waits.
                let read = block_on(db.all_serialized_query(
                    &query,
                    read_opts(tier, include_deleted),
                    None,
                    scope,
                    None,
                ));
                let encoded = match read {
                    Ok(SerializedReadResult::Rows(rows)) => {
                        binding_codec::encode_rows(&rows).map(|rows| (rows, false))
                    }
                    Ok(SerializedReadResult::Relation(snapshot)) => {
                        binding_codec::encode_relation_snapshot(&snapshot).map(|rows| (rows, true))
                    }
                    Err(error) => return Some(error_body(&error)),
                };
                match encoded {
                    Ok((rows, relation)) => ThinBody::Rows { rows, relation },
                    Err(error) => argument(format!("encode result: {error}")),
                }
            }
            ThinOp::Insert {
                table,
                row,
                cells,
                updated_at_ms,
            } => match decode_cells(&cells) {
                Ok(cells) => written(block_on(db.insert(
                    &table,
                    cells,
                    InsertOptions {
                        row_id: row.map(RowUuid::from_bytes),
                        identity: write_identity,
                        updated_at_ms,
                        ..InsertOptions::default()
                    },
                ))),
                Err(error) => argument(format!("{error:?}")),
            },
            ThinOp::Update {
                table,
                row,
                patch,
                updated_at_ms,
            } => match decode_cells(&patch) {
                Ok(patch) => written(block_on(db.update(
                    &table,
                    RowUuid::from_bytes(row),
                    patch,
                    UpdateOptions {
                        identity: write_identity,
                        updated_at_ms,
                        ..UpdateOptions::default()
                    },
                ))),
                Err(error) => argument(format!("{error:?}")),
            },
            ThinOp::Upsert {
                table,
                row,
                cells,
                updated_at_ms,
            } => match decode_cells(&cells) {
                Ok(cells) => written(block_on(db.upsert(
                    &table,
                    RowUuid::from_bytes(row),
                    cells,
                    UpsertOptions {
                        identity: write_identity,
                        updated_at_ms,
                        ..UpsertOptions::default()
                    },
                ))),
                Err(error) => argument(format!("{error:?}")),
            },
            ThinOp::Delete {
                table,
                row,
                updated_at_ms,
            } => written(block_on(db.delete(
                &table,
                RowUuid::from_bytes(row),
                DeleteOptions {
                    identity: write_identity,
                    updated_at_ms,
                    ..DeleteOptions::default()
                },
            ))),
        })
    }

    /// After a tick: every thin session's held answers and its live
    /// queries' new events, up to its egress budget.
    pub(super) fn pump_thin(&mut self) {
        let mut gone = Vec::new();
        for (session, thin) in &mut self.thin {
            let mut batch = std::mem::take(&mut thin.held);
            let mut ended = Vec::new();
            if thin.has_room() {
                for (id, stream) in &mut thin.subscriptions {
                    while let Some(event) = stream.try_next_event() {
                        let body = match event_chunk(event) {
                            Ok(EventChunk::Delta {
                                delta,
                                reset,
                                settled,
                                tier,
                            }) => ThinBody::Event {
                                delta,
                                reset,
                                settled,
                                tier,
                            },
                            Ok(EventChunk::Closed) => {
                                ended.push(*id);
                                ThinBody::Closed
                            }
                            Ok(EventChunk::Rejected { detail }) => {
                                ended.push(*id);
                                ThinBody::Error {
                                    code: "Rejected".to_owned(),
                                    message: detail,
                                }
                            }
                            Err(error) => {
                                ended.push(*id);
                                argument(format!("{error:?}"))
                            }
                        };
                        let last = !matches!(body, ThinBody::Event { .. });
                        batch.push(ThinResponse { id: *id, body });
                        if last {
                            break;
                        }
                    }
                }
            }
            for id in ended {
                thin.subscriptions.remove(&id);
            }
            if !thin.send(batch) {
                gone.push(*session);
            }
        }
        for session in gone {
            self.close_thin_session(session);
        }
    }
}
