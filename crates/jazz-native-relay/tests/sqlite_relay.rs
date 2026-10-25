//! SQLite relay behaviour through the native host's public command boundary.
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_native_relay::*;
use rusqlite::OptionalExtension;
use std::{cell::RefCell, collections::VecDeque, path::Path, rc::Rc};

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap()
}
fn identity(node: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
    }
}
fn cells(text: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
        descriptor.create(&[Value::String(text.into())]).unwrap(),
        descriptor,
    ))
    .unwrap()
}
fn empty() -> JazzNativeRelayBytes {
    JazzNativeRelayBytes {
        data: std::ptr::null_mut(),
        len: 0,
    }
}
unsafe fn take(out: &mut JazzNativeRelayBytes) -> Vec<u8> {
    let bytes = unsafe { std::slice::from_raw_parts(out.data, out.len) }.to_vec();
    unsafe { jazz_native_relay_bytes_free(out) };
    bytes
}
struct Host {
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
    relay: u64,
    foreground: u64,
}
impl Host {
    fn open(path: &Path) -> Self {
        Self::try_open(path, "alice", identity(2), schema()).unwrap()
    }
    fn try_open(
        path: &Path,
        auth_scope: &str,
        id: DbIdentity,
        schema: JazzSchema,
    ) -> Result<Self, JazzNativeRelayStatus> {
        let host = jazz_native_relay_host_new();
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        let mut this = Self {
            host,
            lease,
            relay: 0,
            foreground: 0,
        };
        let request=serde_json::to_vec(&serde_json::json!({"scope":{"app_namespace":"sqlite-native-relay","storage_namespace":"default","auth_scope":auth_scope},
            "sqlite_path":path.to_str().unwrap(),"schema_json":serde_json::to_string(schema.public_schema()).unwrap(),"identity":id,"claims":{}})).unwrap();
        let mut output = empty();
        let status = unsafe {
            jazz_native_relay_host_admit_scope_json(
                host,
                request.as_ptr(),
                request.len(),
                &mut output,
            )
        };
        if status != JazzNativeRelayStatus::Ok {
            return Err(status);
        }
        let capability = unsafe { take(&mut output) };
        let bytes = postcard::to_allocvec(&RelayCommandRequest::Open {
            supported_abi_minimum: NATIVE_RELAY_ABI_V1,
            supported_abi_maximum: NATIVE_RELAY_ABI_V1,
            admitted_scope: postcard::from_bytes(&capability).unwrap(),
        })
        .unwrap();
        let status = unsafe {
            jazz_native_relay_host_execute(host, bytes.as_ptr(), bytes.len(), &mut output)
        };
        if status != JazzNativeRelayStatus::Ok {
            return Err(status);
        }
        let response: RelayCommandResponse =
            postcard::from_bytes(&unsafe { take(&mut output) }).unwrap();
        let RelayCommandResponse::Opened { relay } = response else {
            panic!("{response:?}")
        };
        this.relay = relay;
        let status = unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                capability.as_ptr(),
                capability.len(),
                &mut this.foreground,
            )
        };
        if status != JazzNativeRelayStatus::Ok {
            return Err(status);
        }
        Ok(this)
    }
    fn relay_command(&self, command: RelayCommandRequest) -> RelayCommandResponse {
        let bytes = postcard::to_allocvec(&command).unwrap();
        let mut output = empty();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_execute(self.host, bytes.as_ptr(), bytes.len(), &mut output)
            },
            JazzNativeRelayStatus::Ok,
            "relay {command:?}"
        );
        postcard::from_bytes(&unsafe { take(&mut output) }).unwrap()
    }
    fn command(&self, command: ForegroundDbCommandRequest) -> ForegroundDbCommandResponse {
        let bytes = postcard::to_allocvec(&command).unwrap();
        let mut output = empty();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    self.lease,
                    self.foreground,
                    bytes.as_ptr(),
                    bytes.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok,
            "foreground {command:?}"
        );
        postcard::from_bytes(&unsafe { take(&mut output) }).unwrap()
    }
    fn pump(&self) {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_tick_attached_foreground(self.lease, self.foreground)
            },
            JazzNativeRelayStatus::Ok
        );
        self.relay_command(RelayCommandRequest::Pump { relay: self.relay });
    }
    fn insert(&self, row: u8, text: &str) -> [u8; 16] {
        let response = self.command(ForegroundDbCommandRequest::DirectMutation {
            mutation: ForegroundMutationKind::Insert,
            table: "notes".into(),
            row_id: Some([row; 16]),
            cells: cells(text),
            options_json: "{}".into(),
        });
        match response {
            ForegroundDbCommandResponse::MutationCommitted { tx_id, .. } => tx_id,
            other => panic!("{other:?}"),
        }
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        unsafe {
            jazz_native_relay_host_lease_free(self.lease);
            jazz_native_relay_host_free(self.host);
        }
    }
}
struct Queues {
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Transport for Queues {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}
struct Authority {
    db: Db,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Authority {
    fn new() -> Self {
        Self::with_schema(schema())
    }
    fn with_schema(schema: JazzSchema) -> Self {
        let db = block_on(Db::open_history_complete(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([9; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let this = Self {
            db,
            inbound: Rc::default(),
            outbound: Rc::default(),
        };
        this.db.accept_subscriber(
            Box::new(Queues {
                inbound: this.inbound.clone(),
                outbound: this.outbound.clone(),
            }),
            identity(2).author,
        );
        this
    }
    fn drive(&self, host: &Host) {
        for _ in 0..16 {
            host.pump();
            let RelayCommandResponse::Frames { frames } =
                host.relay_command(RelayCommandRequest::ReceiveRelayFrames { relay: host.relay })
            else {
                panic!("frames")
            };
            for frame in frames {
                self.inbound
                    .borrow_mut()
                    .push_back(postcard::from_bytes(&frame).unwrap());
            }
            block_on(self.db.tick()).unwrap();
            for message in self.outbound.borrow_mut().drain(..) {
                host.relay_command(RelayCommandRequest::SendRelayFrame {
                    relay: host.relay,
                    frame: postcard::to_allocvec(&message).unwrap(),
                });
            }
        }
    }
    fn texts(&self) -> Vec<String> {
        let q = self.db.prepare_query(&Query::from("notes")).unwrap();
        let mut rows = block_on(self.db.all(&q, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|r| match r.cell_at(0).unwrap() {
                Value::String(text) => text,
                _ => panic!("text"),
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }
    fn complete(
        &self,
        host: &Host,
        mut response: ForegroundDbCommandResponse,
    ) -> ForegroundDbCommandResponse {
        for _ in 0..30 {
            let ForegroundDbCommandResponse::Pending { operation } = response else {
                return response;
            };
            self.drive(host);
            response = host.command(ForegroundDbCommandRequest::Poll { operation });
        }
        panic!("pending operation did not finish: {response:?}")
    }
}
/// Alice's native foreground writes through its durable relay. The write reaches
/// local durability offline and receives global acceptance only from the authority.
#[test]
fn native_foreground_write_waits_for_real_authority_and_survives_owner_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relay.sqlite");
    let authority = Authority::new();
    let host = Host::open(&path);
    authority.drive(&host);
    let tx = host.insert(3, "alice");
    for _ in 0..12 {
        host.pump();
    }
    let global = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: tx,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(
        matches!(global, ForegroundDbCommandResponse::Pending { .. }),
        "{global:?}"
    );
    assert!(authority.texts().is_empty());
    assert!(
        matches!(authority.complete(&host,global),ForegroundDbCommandResponse::TransactionSettled{tx_id} if tx_id==tx)
    );
    assert_eq!(authority.texts(), vec!["alice"]);
    drop(host);
    let reopened = Host::open(&path);
    authority.drive(&reopened);
    let next = reopened.insert(4, "bob");
    assert_ne!(next, tx);
    let global = reopened.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: next,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(
        matches!(authority.complete(&reopened,global),ForegroundDbCommandResponse::TransactionSettled{tx_id} if tx_id==next)
    );
    assert_eq!(authority.texts(), vec!["alice", "bob"]);
}
/// Alice seals through her durable owner before publication. Cancellation consumes
/// the reserved identity; a later published initialization reaches the authority once.
#[test]
fn native_initialization_reserves_at_the_owner_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("init.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    let ready = host.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::HasAuthenticatedCatalogue,
    });
    assert!(
        matches!(authority.complete(&host,ready),ForegroundDbCommandResponse::Rows{rows} if rows==b"true")
    );
    let mut reservations = Vec::new();
    for cancel in [true, false] {
        let opened = host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        });
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = opened else {
            panic!("{opened:?}")
        };
        let absence = host.command(ForegroundDbCommandRequest::InitializationV1 {
            version: 1,
            action: InitializationAction::RecordAbsence {
                transaction,
                table: "notes".into(),
                row: [5; 16],
            },
        });
        assert!(matches!(
            authority.complete(&host, absence),
            ForegroundDbCommandResponse::Rows { .. }
        ));
        assert!(matches!(
            host.command(ForegroundDbCommandRequest::Insert {
                transaction,
                table: "notes".into(),
                cells: cells("founder"),
                row_id: Some([5; 16])
            }),
            ForegroundDbCommandResponse::Inserted { .. }
        ));
        let response = host.command(ForegroundDbCommandRequest::InitializationV1 {
            version: 1,
            action: InitializationAction::Seal { transaction },
        });
        let ForegroundDbCommandResponse::Rows { rows } = authority.complete(&host, response) else {
            panic!("seal")
        };
        let value: serde_json::Value = serde_json::from_slice(&rows).unwrap();
        reservations.push(value["reservedTxId"].as_str().unwrap().to_owned());
        assert!(authority.texts().is_empty());
        let token = value["token"].as_str().unwrap().to_owned();
        let response = host.command(ForegroundDbCommandRequest::InitializationV1 {
            version: 1,
            action: if cancel {
                InitializationAction::Cancel { token }
            } else {
                InitializationAction::Publish { token }
            },
        });
        let response = authority.complete(&host, response);
        if !cancel {
            let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = response else {
                panic!("{response:?}")
            };
            let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
                tx_id,
                tier: "global".into(),
                observe_only: false,
            });
            assert!(matches!(
                authority.complete(&host, wait),
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ));
        }
    }
    assert_ne!(reservations[0], reservations[1]);
    assert_eq!(authority.texts(), vec!["founder"]);
}

/// Alice receives stable rejected receipts for duplicate inserts, while Bob's
/// authority retains the original row. An empty transaction settles normally.
#[test]
fn native_rejected_receipts_and_empty_transactions_keep_their_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("receipts.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    let first = host.insert(6, "original");
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: first,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        authority.complete(&host, wait),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    let rejected = host.insert(6, "duplicate");
    assert_ne!(rejected, first);
    let ForegroundDbCommandResponse::WriteState { state_json } =
        host.command(ForegroundDbCommandRequest::WriteState { tx_id: rejected })
    else {
        panic!("state")
    };
    let state: jazz::db::WriteState = serde_json::from_str(&state_json).unwrap();
    assert!(matches!(state.fate, jazz::tx::Fate::Rejected(_)));
    assert_eq!(state.durability, jazz::tx::DurabilityTier::None);
    for _ in 0..2 {
        let result = host.command(ForegroundDbCommandRequest::WaitForTransaction {
            tx_id: rejected,
            tier: "global".into(),
            observe_only: false,
        });
        assert!(
            matches!(authority.complete(&host,result),ForegroundDbCommandResponse::CodedOperationError{ref code,..} if code=="write_rejected"),
            "rejection"
        );
    }
    let ForegroundDbCommandResponse::MutationErrors { events_json } =
        host.command(ForegroundDbCommandRequest::DrainMutationErrors)
    else {
        panic!("events")
    };
    assert_eq!(
        events_json, "[]",
        "claimed errors must not be reported again"
    );
    let observed = host.insert(6, "observed duplicate");
    let result = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: observed,
        tier: "global".into(),
        observe_only: true,
    });
    assert!(matches!(
        authority.complete(&host, result),
        ForegroundDbCommandResponse::CodedOperationError { .. }
    ));
    host.pump();
    let ForegroundDbCommandResponse::MutationErrors { events_json } =
        host.command(ForegroundDbCommandRequest::DrainMutationErrors)
    else {
        panic!("events")
    };
    let events: Vec<serde_json::Value> = serde_json::from_str(&events_json).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["code"], "write_rejected");
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        })
    else {
        panic!("open")
    };
    let result = host.command(ForegroundDbCommandRequest::CommitTransaction { transaction });
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
        authority.complete(&host, result)
    else {
        panic!("commit")
    };
    let result = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        authority.complete(&host, result),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    assert_eq!(authority.texts(), vec!["original"]);
}

/// Alice catches a refused staged write after a valid insert. Bob's authority
/// must receive neither write: every native mutation kind rejects the whole unit.
#[test]
fn native_staged_refusals_reject_the_whole_transaction() {
    for kind in [
        ForegroundTransactionKind::Mergeable,
        ForegroundTransactionKind::Serializable,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let host = Host::open(&dir.path().join("staged-refusals.sqlite"));
        let authority = Authority::new();
        authority.drive(&host);
        host.insert(1, "original");
        host.insert(2, "deleted");
        assert!(matches!(
            host.command(ForegroundDbCommandRequest::DirectMutation {
                mutation: ForegroundMutationKind::Delete,
                table: "notes".into(),
                row_id: Some([2; 16]),
                cells: Vec::new(),
                options_json: "{}".into(),
            }),
            ForegroundDbCommandResponse::MutationCommitted { .. }
        ));
        authority.drive(&host);
        for (index, mutation) in [
            ForegroundMutationKind::Insert,
            ForegroundMutationKind::Update,
            ForegroundMutationKind::Upsert,
            ForegroundMutationKind::Delete,
            ForegroundMutationKind::Restore,
        ]
        .into_iter()
        .enumerate()
        {
            let ForegroundDbCommandResponse::TransactionOpened { transaction } =
                host.command(ForegroundDbCommandRequest::BeginTransaction { kind })
            else {
                panic!("open transaction")
            };
            assert!(matches!(
                host.command(ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "notes".into(),
                    row_id: Some([10 + index as u8; 16]),
                    cells: cells("must not escape a refused unit"),
                }),
                ForegroundDbCommandResponse::Inserted { .. }
            ));
            let row = if matches!(
                mutation,
                ForegroundMutationKind::Insert | ForegroundMutationKind::Restore
            ) {
                [1; 16]
            } else {
                [2; 16]
            };
            let staged = host.command(ForegroundDbCommandRequest::StageMutation {
                transaction,
                mutation,
                table: "notes".into(),
                row_id: Some(row),
                cells: cells("refused"),
                options_json: "{}".into(),
            });
            // A caller can catch a synchronous error. That must never make the
            // transaction's earlier valid write publishable on its own.
            let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
                host.command(ForegroundDbCommandRequest::CommitTransaction { transaction })
            else {
                panic!("commit returns a durable outcome handle")
            };
            let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
                tx_id,
                tier: "global".into(),
                observe_only: false,
            });
            let outcome = authority.complete(&host, wait);
            assert!(
                matches!(&outcome, ForegroundDbCommandResponse::CodedOperationError { code, .. } if code == "write_rejected"),
                "{kind:?} {mutation:?}: {outcome:?}; staged response: {staged:?}"
            );
            let expected = if mutation == ForegroundMutationKind::Insert {
                ForegroundDbCommandResponse::Inserted { row_id: row }
            } else {
                ForegroundDbCommandResponse::MutationStaged
            };
            assert_eq!(staged, expected);
            assert_eq!(authority.texts(), vec!["original"]);
        }
    }
}

/// Alice rolls back a refused unit and starts another. Bob's authority sees
/// only the new transaction; the old refusal and its staged rows are retired.
#[test]
fn native_rollback_retires_a_staged_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("rollback-refusal.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    host.insert(1, "original");
    authority.drive(&host);
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        })
    else {
        panic!("open transaction")
    };
    for row in [2, 1] {
        assert_eq!(
            host.command(ForegroundDbCommandRequest::Insert {
                transaction,
                table: "notes".into(),
                row_id: Some([row; 16]),
                cells: cells("rolled back"),
            }),
            ForegroundDbCommandResponse::Inserted { row_id: [row; 16] }
        );
    }
    assert_eq!(
        host.command(ForegroundDbCommandRequest::RollbackTransaction { transaction }),
        ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
    );
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        })
    else {
        panic!("open replacement")
    };
    assert!(matches!(
        host.command(ForegroundDbCommandRequest::Insert {
            transaction,
            table: "notes".into(),
            row_id: Some([2; 16]),
            cells: cells("replacement"),
        }),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
        host.command(ForegroundDbCommandRequest::CommitTransaction { transaction })
    else {
        panic!("commit replacement")
    };
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        authority.complete(&host, wait),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    assert_eq!(authority.texts(), vec!["original", "replacement"]);
}

/// Alice's authority refuses her write. Unwaited and observe-only writes
/// report once, while an explicit waiter owns its rejection.
#[test]
fn native_authority_rejection_is_delivered_once_unless_claimed() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("rejections.sqlite"));
    let denied = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False),
                    ),
            )
            .build(),
    )
    .unwrap();
    let authority = Authority::with_schema(denied);
    authority.drive(&host);
    let drain = || {
        let ForegroundDbCommandResponse::MutationErrors { events_json } =
            host.command(ForegroundDbCommandRequest::DrainMutationErrors)
        else {
            panic!("errors")
        };
        serde_json::from_str::<Vec<serde_json::Value>>(&events_json).unwrap()
    };
    host.insert(70, "unwaited");
    authority.drive(&host);
    let errors = drain();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["code"], "permission_denied");
    assert!(drain().is_empty());
    for observe_only in [false, true] {
        let tx_id = host.insert(if observe_only { 72 } else { 71 }, "waited");
        let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
            tx_id,
            tier: "global".into(),
            observe_only,
        });
        assert!(matches!(
            authority.complete(&host, wait),
            ForegroundDbCommandResponse::CodedOperationError { .. }
        ));
        assert_eq!(drain().len(), usize::from(observe_only));
        assert!(drain().is_empty());
    }
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        })
    else {
        panic!("transaction")
    };
    assert!(matches!(
        host.command(ForegroundDbCommandRequest::Insert {
            transaction,
            table: "notes".into(),
            cells: cells("transaction"),
            row_id: Some([74; 16]),
        }),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    assert!(matches!(
        host.command(ForegroundDbCommandRequest::CommitTransaction { transaction }),
        ForegroundDbCommandResponse::TransactionCommitted { .. }
    ));
    authority.drive(&host);
    let errors = drain();
    assert_eq!(errors.len(), 1, "unwaited transaction rejection");
    assert_eq!(errors[0]["code"], "permission_denied");
    assert!(authority.texts().is_empty());
}

/// Alice sees her optimistic row, then the authority accepts unchanged bytes.
/// Only the E2EE dependency observer receives the acceptance notification.
/// alice: local delta -> authority accepts -> empty evidence notification
#[test]
fn native_accepted_evidence_notifies_only_opted_in_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("evidence.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    let open = |observe| {
        let ForegroundDbCommandResponse::Subscribed { subscription } =
            host.command(ForegroundDbCommandRequest::Subscribe {
                query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
                options_json:
                    serde_json::json!({"tier":"local-only", "observe_accepted_evidence":observe})
                        .to_string(),
            })
        else {
            panic!("subscribe")
        };
        subscription
    };
    let drain = |subscription| {
        let ForegroundDbCommandResponse::SubscriptionEvents { events } =
            host.command(ForegroundDbCommandRequest::DrainSubscription { subscription })
        else {
            panic!("events")
        };
        events
    };
    let plain = open(false);
    let observed = open(true);
    assert!(!drain(plain).is_empty());
    assert!(!drain(observed).is_empty());
    host.insert(73, "accepted grant");
    assert!(!drain(plain).is_empty());
    assert!(!drain(observed).is_empty());
    authority.drive(&host);
    let events = drain(observed);
    assert!(
        !events.is_empty(),
        "acceptance must notify the opted-in observer"
    );
    let empty_delta = jazz::binding_codec::encode_subscription_delta(&[], &[], &[]).unwrap();
    for event in events {
        let ForegroundSubscriptionEvent::Delta { reset, delta, .. } = event else {
            panic!("{event:?}")
        };
        assert!(!reset);
        assert_eq!(delta, empty_delta);
    }
    assert!(
        drain(plain).is_empty(),
        "ordinary subscriptions must not duplicate rows"
    );
    authority.drive(&host);
    assert!(drain(observed).is_empty(), "evidence must notify only once");
}

/// Alice's empty offline update keeps the original durable upload intact.
/// Neither the no-op nor a local receipt can manufacture authority acceptance.
#[test]
fn native_empty_update_preserves_the_pending_upload() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("empty-update.sqlite"));
    let original = host.insert(42, "unchanged");
    let settle_local = |tx_id| {
        let mut response = host.command(ForegroundDbCommandRequest::WaitForTransaction {
            tx_id,
            tier: "local".into(),
            observe_only: false,
        });
        for _ in 0..32 {
            if let ForegroundDbCommandResponse::Pending { operation } = response {
                host.pump();
                response = host.command(ForegroundDbCommandRequest::Poll { operation });
            } else {
                break;
            }
        }
        assert!(
            matches!(
                response,
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ),
            "{response:?}"
        );
    };
    settle_local(original);
    let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
    let empty_cells = jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
        descriptor.create(&[]).unwrap(),
        descriptor,
    ))
    .unwrap();
    let response = host.command(ForegroundDbCommandRequest::DirectMutation {
        mutation: ForegroundMutationKind::Update,
        table: "notes".into(),
        row_id: Some([42; 16]),
        cells: empty_cells,
        options_json: "{}".into(),
    });
    let ForegroundDbCommandResponse::MutationCommitted { tx_id, .. } = response else {
        panic!("{response:?}");
    };
    settle_local(tx_id);
    let global = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: original,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        global,
        ForegroundDbCommandResponse::Pending { .. }
    ));
    let authority = Authority::new();
    assert!(matches!(
        authority.complete(&host, global),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    assert_eq!(authority.texts(), vec!["unchanged"]);
}

/// Alice's offline write reaches durable owner storage, survives an owner
/// restart, and is uploaded once when the authority reconnects.
#[test]
fn native_pending_upload_survives_owner_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("offline.sqlite");
    let authority = Authority::new();
    let host = Host::open(&path);
    authority.drive(&host);
    let tx = host.insert(7, "offline");
    let mut local = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: tx,
        tier: "local".into(),
        observe_only: false,
    });
    for _ in 0..30 {
        let ForegroundDbCommandResponse::Pending { operation } = local else {
            break;
        };
        host.pump();
        local = host.command(ForegroundDbCommandRequest::Poll { operation });
    }
    assert!(
        matches!(
            local,
            ForegroundDbCommandResponse::TransactionSettled { .. }
        ),
        "{local:?}"
    );
    assert!(authority.texts().is_empty());
    drop(host);
    let reopened = Host::open(&path);
    authority.drive(&reopened);
    assert_eq!(authority.texts(), vec!["offline"]);
}

/// The authority evaluates Alice's native advice request under her admitted
/// identity and the relay preserves the allowed result.
#[test]
fn native_advice_uses_the_authenticated_authority() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("advice.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    let response = host.command(ForegroundDbCommandRequest::PermissionAdvice {
        action: ForegroundPermissionAdviceAction::Insert {
            table: "notes".into(),
            cells: cells("advised"),
        },
    });
    assert_eq!(
        authority.complete(&host, response),
        ForegroundDbCommandResponse::PermissionAdvice {
            advice: ForegroundPermissionAdvice::Allowed
        }
    );
    assert!(authority.texts().is_empty());
}

/// Alice's relay refuses Mallory's scope or identity and a second simultaneous
/// owner; a legacy SQLite layout is rejected without changing its contents.
#[test]
fn native_file_admission_preserves_scope_identity_and_old_layouts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owned.sqlite");
    let host = Host::open(&path);
    assert!(matches!(
        Host::try_open(&path, "alice", identity(2), schema()),
        Err(JazzNativeRelayStatus::LifecycleFailure)
    ));
    drop(host);
    let before = std::fs::read(&path).unwrap();
    for (scope, id) in [
        ("mallory", identity(2)),
        ("alice", identity(3)),
        (
            "alice",
            DbIdentity {
                author: AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap(),
                ..identity(2)
            },
        ),
    ] {
        assert!(matches!(
            Host::try_open(&path, scope, id, schema()),
            Err(JazzNativeRelayStatus::LifecycleFailure)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    drop(Host::open(&path));
    let legacy = dir.path().join("legacy.sqlite");
    let db = rusqlite::Connection::open(&legacy).unwrap();
    db.execute_batch("CREATE TABLE legacy_kv(k BLOB PRIMARY KEY,v BLOB); INSERT INTO legacy_kv VALUES(X'01',X'0203')").unwrap();
    drop(db);
    let bytes = std::fs::read(&legacy).unwrap();
    assert!(matches!(
        Host::try_open(&legacy, "alice", identity(2), schema()),
        Err(JazzNativeRelayStatus::UnsupportedStorageCodecs)
    ));
    assert_eq!(std::fs::read(&legacy).unwrap(), bytes);
}

fn generic_config(path: &Path) -> RelayOpenConfig {
    RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "sqlite-native-relay".into(),
            storage_namespace: "default".into(),
            auth_scope: Some("alice".into()),
        },
        sqlite_path: path.into(),
        schema: schema(),
        identity: identity(2),
    }
}

fn seed_generic_pending(path: &Path, author: AuthorSubject) {
    let db = block_on(Db::open_at(
        DbConfig::new(schema(), identity(2)),
        path,
        Default::default(),
    ))
    .unwrap();
    block_on(db.insert(
        "notes",
        std::collections::BTreeMap::from([(
            "text".into(),
            Value::String("before admission".into()),
        )]),
        jazz::db::InsertOptions {
            identity: jazz::db::WriteIdentity::Session(author),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(db.close()).unwrap();
}

fn native_marker_owner(path: &Path) -> Option<String> {
    rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap()
        .query_row(
            "SELECT owner FROM _jazz_native_relay_v1 WHERE slot=1",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

// Public row reads cannot prove that promotion preserved the original upload's
// complete transaction identity, attribution and timestamps byte-for-byte.
fn pending_commit_bytes(path: &Path) -> Vec<Vec<u8>> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    conn.prepare("SELECT commit_msg FROM _jazz_pending ORDER BY tx")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn assert_native_promotion_state(path: &Path, state: &str) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite_native_owner_promotion_v1.json"
    ))
    .unwrap();
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let (version, owner): (i64, Option<String>) = conn.query_row(
        "SELECT version, CASE WHEN owner IS NULL THEN NULL ELSE lower(hex(owner)) END FROM _jazz_native_relay_v1 WHERE slot=1",
        [], |r| Ok((r.get(0)?, r.get(1)?)),
    ).unwrap();
    let subject: Option<String> = conn
        .query_row(
            "SELECT lower(hex(subject)) FROM _jazz_owner_scope_v1 WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()
        .unwrap();
    assert_eq!(version, 1);
    assert_eq!(owner.as_deref(), fixture[state]["owner_utf8"].as_str());
    assert_eq!(subject.as_deref(), fixture[state]["subject_utf8"].as_str());
}

/// Alice closes her generic relay before trusted native admission promotes it.
/// Her pending row survives promotion and reopen, and only the authority accepts
/// it. Mallory, another node and another scope cannot claim the generic root.
#[test]
fn native_generic_root_promotion_preserves_pending_writes_and_exact_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("promotion.sqlite");
    let config = generic_config(&path);
    let registry = NativeRelayRegistry::default();
    let raw = registry.open(config.clone()).unwrap();
    assert!(Host::try_open(&path, "alice", identity(2), schema()).is_err());
    registry.close(&config.scope).unwrap();
    assert!(
        raw.pump().is_err(),
        "retired aliases cannot survive promotion"
    );
    drop(raw);
    seed_generic_pending(&path, identity(2).author);
    let pending = pending_commit_bytes(&path);
    assert_eq!(pending.len(), 1);
    assert_eq!(native_marker_owner(&path), None);
    assert_native_promotion_state(&path, "generic");
    let before = std::fs::read(&path).unwrap();
    for (scope, id) in [
        ("mallory", identity(2)),
        ("alice", identity(3)),
        (
            "alice",
            DbIdentity {
                author: AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap(),
                ..identity(2)
            },
        ),
    ] {
        assert!(Host::try_open(&path, scope, id, schema()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    drop(Host::open(&path));
    assert_eq!(pending_commit_bytes(&path), pending);
    assert_native_promotion_state(&path, "admitted");
    let owner = native_marker_owner(&path).unwrap();
    assert_eq!(
        owner.as_bytes(),
        unhex(frozen()["file_marker"]["owner_utf8"].as_str().unwrap())
    );
    assert!(matches!(
        registry.open(config),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    let reopened = Host::open(&path);
    let authority = Authority::new();
    assert!(authority.texts().is_empty());
    authority.drive(&reopened);
    assert_eq!(authority.texts(), vec!["before admission"]);
    let tx = reopened.insert(0x91, "after admission");
    let wait = reopened.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: tx,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        authority.complete(&reopened, wait),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    assert_eq!(
        authority.texts(),
        vec!["after admission", "before admission"]
    );
}

/// Alice cannot promote a generic relay containing Mallory's pending work.
/// Refusal leaves the generic mode available and does not bind an owner subject.
#[test]
fn native_generic_root_promotion_rejects_another_subjects_pending_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed-promotion.sqlite");
    let config = generic_config(&path);
    let registry = NativeRelayRegistry::default();
    drop(registry.open(config.clone()).unwrap());
    registry.close(&config.scope).unwrap();
    seed_generic_pending(
        &path,
        AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap(),
    );
    let pending = pending_commit_bytes(&path);
    assert_eq!(pending.len(), 1);
    assert!(Host::try_open(&path, "alice", identity(2), schema()).is_err());
    assert_eq!(pending_commit_bytes(&path), pending);
    assert_eq!(native_marker_owner(&path), None);
    assert_native_promotion_state(&path, "generic");
    let conn =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM _jazz_owner_scope_v1", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(conn);
    assert!(registry.open(config.clone()).unwrap().pump().is_ok());
    registry.close(&config.scope).unwrap();
}

/// Alice's process stops after the core binds its private owner but before the
/// native marker records admission. This lower-level fixture is needed to
/// reproduce that durable boundary; public native opens then prove recovery
/// cannot expose the private store through generic mode.
#[test]
fn native_generic_root_promotion_recovers_without_reopening_generic_access() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("interrupted-promotion.sqlite");
    let config = generic_config(&path);
    let registry = NativeRelayRegistry::default();
    drop(registry.open(config.clone()).unwrap());
    registry.close(&config.scope).unwrap();
    seed_generic_pending(&path, identity(2).author);
    let core = block_on(Db::open_at(
        DbConfig::new(schema(), identity(2)),
        &path,
        Default::default(),
    ))
    .unwrap();
    core.set_scope_isolated_storage_owner().unwrap();
    block_on(core.close()).unwrap();
    drop(core);
    assert_eq!(native_marker_owner(&path), None);
    assert_native_promotion_state(&path, "core_bound");
    assert!(matches!(
        registry.open(config),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    let admitted = Host::open(&path);
    let authority = Authority::new();
    authority.drive(&admitted);
    assert_eq!(authority.texts(), vec!["before admission"]);
    drop(admitted);
    assert!(native_marker_owner(&path).is_some());
    assert_native_promotion_state(&path, "admitted");
    drop(Host::open(&path));
}

fn frozen() -> serde_json::Value {
    serde_json::from_str(include_str!("../fixtures/sqlite_native_relay_v1.json")).unwrap()
}
fn unhex(value: &str) -> Vec<u8> {
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
        .collect()
}
fn cache_path(root: &Path) -> std::path::PathBuf {
    std::fs::read_dir(root)
        .unwrap()
        .map(|p| p.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("catalogue-")
                && p.extension().is_some_and(|e| e == "sqlite")
        })
        .unwrap()
}

/// Alice's stored owner identity and offline catalogue use frozen versioned
/// bytes. An older cache envelope is refused before the account file is opened.
/// Physical inspection is necessary here to pin the durable encoding contract.
#[test]
fn native_storage_formats_match_the_frozen_byte_fixture() {
    let fixture = frozen();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("format.sqlite");
    drop(Host::open(&path));
    let db = rusqlite::Connection::open(&path).unwrap();
    let row:(i64,i64,String,String,String,String)=db.query_row("SELECT slot,version,lower(hex(scope)),lower(hex(owner)),lower(hex(node)),lower(hex(author)) FROM _jazz_native_relay_v1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).unwrap();
    let marker = &fixture["file_marker"];
    assert_eq!(row.0, marker["slot"]);
    assert_eq!(row.1, marker["version"]);
    assert_eq!(row.2, marker["scope_utf8"]);
    assert_eq!(row.3, marker["owner_utf8"]);
    assert_eq!(row.4, marker["node"]);
    assert_eq!(row.5, marker["author_utf8"]);
    drop(db);
    let cache = cache_path(dir.path());
    let db = rusqlite::Connection::open(&cache).unwrap();
    let envelope = unhex(fixture["cache_envelope"].as_str().unwrap());
    db.execute(
        "INSERT INTO _jazz_native_catalogue_v2 VALUES(1,?1)",
        [&envelope],
    )
    .unwrap();
    drop(db);
    let plain = JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("text", ColumnType::Text))
            .build(),
    )
    .unwrap();
    let restored = Host::try_open(
        &dir.path().join("restored.sqlite"),
        "alice",
        identity(2),
        plain,
    )
    .unwrap();
    let ready = restored.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::HasAuthenticatedCatalogue,
    });
    assert_eq!(
        ready,
        ForegroundDbCommandResponse::Rows {
            rows: b"true".to_vec()
        }
    );
    drop(restored);
    let db = rusqlite::Connection::open(&cache).unwrap();
    assert_eq!(
        db.query_row("SELECT capture FROM _jazz_native_catalogue_v2", [], |r| r
            .get::<_, Vec<
            u8,
        >>(
            0
        ))
        .unwrap(),
        envelope
    );
    let mut old = envelope;
    old[3] = 1;
    db.execute("UPDATE _jazz_native_catalogue_v2 SET capture=?1", [old])
        .unwrap();
    drop(db);
    let cache_before = std::fs::read(&cache).unwrap();
    let account = dir.path().join("must-not-create.sqlite");
    assert!(Host::try_open(&account, "alice", identity(2), schema()).is_err());
    assert!(!account.exists());
    assert_eq!(std::fs::read(&cache).unwrap(), cache_before);
}

impl Host {
    async fn account(
        root: &Path,
        server: &jazz_server::JazzServer,
        account: Option<serde_json::Value>,
    ) -> (Self, serde_json::Value) {
        let bearer = jazz_server::TestJwtIssuer::jwt_for_user("native-alice");
        let registry = format!("{}/apps/{}/accounts", server.base_url(), server.app_id());
        let account = match account {
            Some(account) => account,
            None => {
                let response = reqwest::Client::new()
                    .post(format!("{registry}/register"))
                    .bearer_auth(&bearer)
                    .send()
                    .await
                    .unwrap();
                assert!(
                    response.status().is_success(),
                    "registration {}",
                    response.status()
                );
                response.json::<serde_json::Value>().await.unwrap()["account"].clone()
            }
        };
        let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(&bearer).unwrap();
        let request=serde_json::to_vec(&serde_json::json!({"server_url":server.base_url(),"registry":registry,"app_id":server.app_id().to_string(),"env":"test","account_id":account,"issuer":issuer,"subject":subject,"jwt":bearer,"claims":{}})).unwrap();
        let host = jazz_native_relay_host_new();
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        let mut this = Self {
            host,
            lease,
            relay: 0,
            foreground: 0,
        };
        let mut out = empty();
        let root = root.to_str().unwrap().as_bytes();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_begin_account_session_json(
                    lease,
                    request.as_ptr(),
                    request.len(),
                    root.as_ptr(),
                    root.len(),
                    &mut out,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let setup = unsafe { take(&mut out) };
        let schema = serde_json::to_vec(schema().public_schema()).unwrap();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_attach_account_schema_json(
                    lease,
                    setup.as_ptr(),
                    setup.len(),
                    schema.as_ptr(),
                    schema.len(),
                    &mut out,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let admitted = unsafe { take(&mut out) };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    admitted.as_ptr(),
                    admitted.len(),
                    &mut this.foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        (this, account)
    }
    async fn socket_ready(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let status = self.command(ForegroundDbCommandRequest::NativeConnectionStatus);
            if matches!(
                status,
                ForegroundDbCommandResponse::NativeConnectionStatus {
                    connected: true,
                    ..
                }
            ) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "socket not connected: {status:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    async fn complete_socket(
        &self,
        mut response: ForegroundDbCommandResponse,
    ) -> ForegroundDbCommandResponse {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let ForegroundDbCommandResponse::Pending { operation } = response else {
                return response;
            };
            assert!(
                std::time::Instant::now() < deadline,
                "pending socket operation {operation}"
            );
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_tick_attached_foreground(
                        self.lease,
                        self.foreground,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            response = self.command(ForegroundDbCommandRequest::Poll { operation });
        }
    }
}

/// Alice uses a real JWT/account registry and WebSocket authority through the
/// public native ABI. Her durable owner reopens under the same account, and the
/// new foreground can read the globally accepted write and request advice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_account_socket_reopens_and_reads_accepted_writes() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (host, account) = Host::account(dir.path(), &server, None).await;
    host.socket_ready().await;
    let tx = host.insert(8, "socket write");
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: tx,
        tier: "global".into(),
        observe_only: false,
    });
    let result = host.complete_socket(wait).await;
    assert!(
        matches!(
            result,
            ForegroundDbCommandResponse::TransactionSettled { .. }
        ),
        "{result:?}"
    );
    drop(host);
    let (reopened, _) = Host::account(dir.path(), &server, Some(account)).await;
    reopened.socket_ready().await;
    let result = reopened.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"remote"}"#.into(),
        transaction: None,
    });
    let ForegroundDbCommandResponse::Rows { rows } = reopened.complete_socket(result).await else {
        panic!("read")
    };
    let batches = jazz::binding_codec::decode_rows(&rows).unwrap();
    assert_eq!(
        batches
            .iter()
            .flat_map(|b| &b.rows)
            .map(|r| r.cells["text"].clone())
            .collect::<Vec<_>>(),
        vec![Value::String("socket write".into())]
    );
    let advice = reopened.command(ForegroundDbCommandRequest::PermissionAdvice {
        action: ForegroundPermissionAdviceAction::Insert {
            table: "notes".into(),
            cells: cells("advice"),
        },
    });
    assert_eq!(
        reopened.complete_socket(advice).await,
        ForegroundDbCommandResponse::PermissionAdvice {
            advice: ForegroundPermissionAdvice::Allowed
        }
    );
    drop(reopened);
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
}

/// Alice streams a text value through the native ABI. The authority receives
/// every chunk before acceptance, and her subscription publishes hydrated text.
#[test]
fn native_streamed_text_reaches_authority_and_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("stream.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let ForegroundDbCommandResponse::Subscribed { subscription } =
        host.command(ForegroundDbCommandRequest::Subscribe {
            query,
            options_json: "{}".into(),
        })
    else {
        panic!("subscribe")
    };
    authority.drive(&host);
    let before = host.command(ForegroundDbCommandRequest::DrainSubscription { subscription });
    assert!(matches!(
        authority.complete(&host, before),
        ForegroundDbCommandResponse::SubscriptionEvents { .. }
    ));
    let descriptor = RecordDescriptor::new(Vec::<(&str, ValueType)>::new());
    let empty_cells = jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
        descriptor.create(&[]).unwrap(),
        descriptor,
    ))
    .unwrap();
    let ForegroundDbCommandResponse::StreamingMutationOpened { upload } =
        host.command(ForegroundDbCommandRequest::BeginStreamingMutation {
            mutation: ForegroundMutationKind::Insert,
            table: "notes".into(),
            row_id: [10; 16],
            cells: empty_cells,
            column: "text".into(),
            options_json: "{}".into(),
        })
    else {
        panic!("upload")
    };
    let content = "Alice's streamed text. ".repeat(8192);
    for chunk in content.as_bytes().chunks(32 * 1024) {
        let result = host.command(ForegroundDbCommandRequest::PushStreamingMutation {
            upload,
            chunk: chunk.to_vec(),
        });
        assert!(matches!(
            authority.complete(&host, result),
            ForegroundDbCommandResponse::StreamingMutationPushed
        ));
    }
    let result = host.command(ForegroundDbCommandRequest::FinishStreamingMutation { upload });
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
        authority.complete(&host, result)
    else {
        panic!("finish")
    };
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        authority.complete(&host, wait),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    assert_eq!(authority.texts(), vec![content.clone()]);
    let response = host.command(ForegroundDbCommandRequest::DrainSubscription { subscription });
    let ForegroundDbCommandResponse::SubscriptionEvents { events } =
        authority.complete(&host, response)
    else {
        panic!("events")
    };
    let mut hydrated = false;
    for event in events {
        if let ForegroundSubscriptionEvent::Delta { delta, .. } = event {
            let (added, updated) =
                jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
            hydrated |= added
                .iter()
                .chain(updated.iter())
                .flat_map(|batch| &batch.rows)
                .any(|row| row.cells.get("text") == Some(&Value::String(content.clone())));
        }
    }
    assert!(
        hydrated,
        "subscription must carry the complete streamed value"
    );
    assert_eq!(
        host.command(ForegroundDbCommandRequest::Unsubscribe { subscription }),
        ForegroundDbCommandResponse::Unsubscribed { closed: true }
    );
}

/// Alice stages a large upload through her admitted private owner. Bob's
/// authority sees it only after commit; rollback and a duplicate attachment
/// publish no companion writes, and every attachment consumes its capability.
#[test]
fn native_staged_upload_commit_rollback_and_refusal_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("staged-atomic.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    let content = "Alice's staged text. ".repeat(8192);
    let stage = |row: u8| {
        let descriptor = RecordDescriptor::new(Vec::<(&str, ValueType)>::new());
        let empty_cells = jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
            descriptor.create(&[]).unwrap(),
            descriptor,
        ))
        .unwrap();
        let ForegroundDbCommandResponse::StreamingMutationOpened { upload } =
            host.command(ForegroundDbCommandRequest::BeginStreamingMutation {
                mutation: ForegroundMutationKind::Insert,
                table: "notes".into(),
                row_id: [row; 16],
                cells: empty_cells,
                column: "text".into(),
                options_json: "{}".into(),
            })
        else {
            panic!("open upload")
        };
        for chunk in content.as_bytes().chunks(32 * 1024) {
            let pushed = host.command(ForegroundDbCommandRequest::PushStreamingMutation {
                upload,
                chunk: chunk.to_vec(),
            });
            assert_eq!(
                authority.complete(&host, pushed),
                ForegroundDbCommandResponse::StreamingMutationPushed
            );
        }
        let staged = host.command(ForegroundDbCommandRequest::StageStreamingMutation { upload });
        let ForegroundDbCommandResponse::StreamingMutationStaged { staged } =
            authority.complete(&host, staged)
        else {
            panic!("stage upload")
        };
        staged
    };
    for (row, commit) in [(60, true), (61, false)] {
        let before = authority.texts();
        let staged = stage(row);
        let ForegroundDbCommandResponse::TransactionOpened { transaction } =
            host.command(ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Serializable,
            })
        else {
            panic!("open transaction")
        };
        let attached = host.command(ForegroundDbCommandRequest::AttachStagedStreamingMutation {
            staged,
            transaction,
        });
        assert_eq!(
            authority.complete(&host, attached),
            ForegroundDbCommandResponse::StagedStreamingMutationAttached
        );
        authority.drive(&host);
        assert_eq!(authority.texts(), before, "attachment does not publish");
        if commit {
            let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
                host.command(ForegroundDbCommandRequest::CommitTransaction { transaction })
            else {
                panic!("commit")
            };
            let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
                tx_id,
                tier: "global".into(),
                observe_only: false,
            });
            assert!(matches!(
                authority.complete(&host, wait),
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ));
        } else {
            assert_eq!(
                host.command(ForegroundDbCommandRequest::RollbackTransaction { transaction }),
                ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
            );
        }
        let aborted =
            host.command(ForegroundDbCommandRequest::AbortStagedStreamingMutation { staged });
        assert_eq!(
            authority.complete(&host, aborted),
            ForegroundDbCommandResponse::StagedStreamingMutationAborted { aborted: false }
        );
        authority.drive(&host);
        assert_eq!(authority.texts(), vec![content.clone()]);
    }
    let duplicate = stage(60);
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        })
    else {
        panic!("open refused transaction")
    };
    assert!(matches!(
        host.command(ForegroundDbCommandRequest::Insert {
            transaction,
            table: "notes".into(),
            row_id: Some([62; 16]),
            cells: cells("must not escape"),
        }),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    let attached = host.command(ForegroundDbCommandRequest::AttachStagedStreamingMutation {
        staged: duplicate,
        transaction,
    });
    let attached = authority.complete(&host, attached);
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
        host.command(ForegroundDbCommandRequest::CommitTransaction { transaction })
    else {
        panic!("refused receipt")
    };
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    let result = authority.complete(&host, wait);
    assert!(
        matches!(&result, ForegroundDbCommandResponse::CodedOperationError { code, .. } if code == "write_rejected"),
        "caught attachment refusal must reject the unit: {result:?}, attachment: {attached:?}"
    );
    assert_eq!(
        attached,
        ForegroundDbCommandResponse::StagedStreamingMutationAttached
    );
    let aborted = host
        .command(ForegroundDbCommandRequest::AbortStagedStreamingMutation { staged: duplicate });
    assert_eq!(
        authority.complete(&host, aborted),
        ForegroundDbCommandResponse::StagedStreamingMutationAborted { aborted: false }
    );
    assert_eq!(authority.texts(), vec![content]);
}

/// Alice commits while a cold serializable read still awaits Bob's authority.
/// SQLite consumes and refuses that batch. Repeated refused attempts must not
/// occupy native transaction slots or publish any staged companion write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_failed_commits_release_transaction_capacity() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (host, _) = Host::account(dir.path(), &server, None).await;
    host.socket_ready().await;
    for attempt in 0..96 {
        let opened = host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        });
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = opened else {
            panic!("attempt {attempt} could not open a fresh transaction: {opened:?}")
        };
        assert!(matches!(
            host.command(ForegroundDbCommandRequest::Insert {
                transaction,
                table: "notes".into(),
                row_id: Some([attempt; 16]),
                cells: cells("refused companion"),
            }),
            ForegroundDbCommandResponse::Inserted { .. }
        ));
        let read = host.command(ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
            options_json: "{}".into(),
            transaction: Some(transaction),
        });
        let ForegroundDbCommandResponse::Pending { operation } = read else {
            panic!("authority must still be required: {read:?}")
        };
        let committed = host.command(ForegroundDbCommandRequest::CommitTransaction { transaction });
        assert!(
            matches!(&committed, ForegroundDbCommandResponse::CodedOperationError { code, .. } if code == "not_observed"),
            "unfinished read must refuse commit: {committed:?}"
        );
        assert_eq!(
            host.command(ForegroundDbCommandRequest::Cancel { operation }),
            ForegroundDbCommandResponse::Cancelled { cancelled: true }
        );
    }
    let tx_id = host.insert(200, "usable after refused commits");
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        host.complete_socket(wait).await,
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    let read = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"Remote"}"#.into(),
        transaction: None,
    });
    let ForegroundDbCommandResponse::Rows { rows } = host.complete_socket(read).await else {
        panic!("authority read did not complete")
    };
    let rows = jazz::binding_codec::decode_rows(&rows).unwrap();
    let texts: Vec<_> = rows
        .iter()
        .flat_map(|batch| &batch.rows)
        .map(|row| row.cells.get("text").unwrap().clone())
        .collect();
    assert_eq!(
        texts,
        [Value::String("usable after refused commits".into())],
        "only the subsequent accepted write may reach the authority"
    );
}

/// Alice cancels every pending observer before committing or rolling back.
/// Real authority reads hold the fences; cancellation must release all of them
/// without poisoning the commit or publishing a rolled-back companion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cancelled_reads_release_transaction_fences() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (host, _) = Host::account(dir.path(), &server, None).await;
    host.socket_ready().await;
    let mut expected = Vec::new();
    for attempt in 0..96_u8 {
        let ForegroundDbCommandResponse::TransactionOpened { transaction } =
            host.command(ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Serializable,
            })
        else {
            panic!("attempt {attempt} exhausted transaction capacity")
        };
        let text = format!("cancelled observer {attempt:02}");
        assert!(matches!(
            host.command(ForegroundDbCommandRequest::Insert {
                transaction,
                table: "notes".into(),
                row_id: Some([attempt; 16]),
                cells: cells(&text),
            }),
            ForegroundDbCommandResponse::Inserted { .. }
        ));
        let mut operations = Vec::new();
        for _ in 0..3 {
            let read = host.command(ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
                options_json: "{}".into(),
                transaction: Some(transaction),
            });
            let ForegroundDbCommandResponse::Pending { operation } = read else {
                panic!("read must remain pending until polled: {read:?}")
            };
            operations.push(operation);
        }
        // Cancel a queued observer first, then the active observer, then the
        // remaining one. Neither queue position may retain a commit fence.
        for index in [1, 0, 2] {
            assert_eq!(
                host.command(ForegroundDbCommandRequest::Cancel {
                    operation: operations[index],
                }),
                ForegroundDbCommandResponse::Cancelled { cancelled: true }
            );
        }
        if attempt % 2 == 0 {
            let committed =
                host.command(ForegroundDbCommandRequest::CommitTransaction { transaction });
            let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = committed else {
                panic!("cancelled ordinary reads must release the fence: {committed:?}")
            };
            let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
                tx_id,
                tier: "global".into(),
                observe_only: false,
            });
            assert!(matches!(
                host.complete_socket(wait).await,
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ));
            expected.push(text);
        } else {
            assert_eq!(
                host.command(ForegroundDbCommandRequest::RollbackTransaction { transaction }),
                ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
            );
        }
    }
    let read = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"Remote"}"#.into(),
        transaction: None,
    });
    let ForegroundDbCommandResponse::Rows { rows } = host.complete_socket(read).await else {
        panic!("authority read")
    };
    let rows = jazz::binding_codec::decode_rows(&rows).unwrap();
    let mut actual = rows
        .iter()
        .flat_map(|batch| &batch.rows)
        .map(|row| match row.cells.get("text").unwrap() {
            Value::String(text) => text.clone(),
            value => panic!("unexpected title {value:?}"),
        })
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(actual, expected);
}

/// Alice rolls back while authority reads are still pending. Every observer
/// must finish with a transaction error, release its operation slot, and leave
/// the authority empty, even after more iterations than the native handle cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_rollback_terminates_pending_reads_without_publication() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (host, _) = Host::account(dir.path(), &server, None).await;
    host.socket_ready().await;
    for attempt in 0..96_u8 {
        let ForegroundDbCommandResponse::TransactionOpened { transaction } =
            host.command(ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Serializable,
            })
        else {
            panic!("attempt {attempt} exhausted transaction capacity")
        };
        assert!(matches!(
            host.command(ForegroundDbCommandRequest::Insert {
                transaction,
                table: "notes".into(),
                row_id: Some([attempt; 16]),
                cells: cells("rolled back before observation"),
            }),
            ForegroundDbCommandResponse::Inserted { .. }
        ));
        let mut operations = Vec::new();
        for _ in 0..3 {
            let read = host.command(ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
                options_json: "{}".into(),
                transaction: Some(transaction),
            });
            let ForegroundDbCommandResponse::Pending { operation } = read else {
                panic!("read must remain pending until polled: {read:?}")
            };
            operations.push(operation);
        }
        assert_eq!(
            host.command(ForegroundDbCommandRequest::RollbackTransaction { transaction }),
            ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
        );
        for operation in operations {
            let read = host.command(ForegroundDbCommandRequest::Poll { operation });
            assert!(
                matches!(&read, ForegroundDbCommandResponse::CodedOperationError { code, .. } if code == "not_observed"),
                "rollback must terminate every pending read: {read:?}"
            );
            assert_eq!(
                host.command(ForegroundDbCommandRequest::Cancel { operation }),
                ForegroundDbCommandResponse::Cancelled { cancelled: false },
                "delivering the terminal error must retire the operation"
            );
        }
    }
    let read = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"Remote"}"#.into(),
        transaction: None,
    });
    let ForegroundDbCommandResponse::Rows { rows } = host.complete_socket(read).await else {
        panic!("authority read")
    };
    assert!(
        jazz::binding_codec::decode_rows(&rows)
            .unwrap()
            .iter()
            .all(|batch| batch.rows.is_empty()),
        "rolling back before observation must not publish any companion"
    );
}

/// Alice closes while a serializable read, an uncommitted row and a streamed
/// upload are unfinished. A locally durable write survives the account reopen;
/// the old handles cannot publish anything and the authority sees no abandoned
/// companions. All lifecycle operations use the public native C ABI.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_close_retires_pending_work_and_reopens_durable_rows() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (host, account) = Host::account(dir.path(), &server, None).await;
    host.socket_ready().await;
    let tx_id = host.insert(201, "durable before close");
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "local".into(),
        observe_only: false,
    });
    assert!(matches!(
        host.complete_socket(wait).await,
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        })
    else {
        panic!("open transaction")
    };
    assert!(matches!(
        host.command(ForegroundDbCommandRequest::Insert {
            transaction,
            table: "notes".into(),
            row_id: Some([202; 16]),
            cells: cells("uncommitted before close"),
        }),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    let read = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: "{}".into(),
        transaction: Some(transaction),
    });
    let ForegroundDbCommandResponse::Pending { operation } = read else {
        panic!("authority read must remain pending until polled: {read:?}")
    };
    let descriptor = RecordDescriptor::new(Vec::<(&str, ValueType)>::new());
    let empty_cells = jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
        descriptor.create(&[]).unwrap(),
        descriptor,
    ))
    .unwrap();
    let ForegroundDbCommandResponse::StreamingMutationOpened { upload } =
        host.command(ForegroundDbCommandRequest::BeginStreamingMutation {
            mutation: ForegroundMutationKind::Insert,
            table: "notes".into(),
            row_id: [203; 16],
            cells: empty_cells,
            column: "text".into(),
            options_json: "{}".into(),
        })
    else {
        panic!("open upload")
    };
    let pushed = host.command(ForegroundDbCommandRequest::PushStreamingMutation {
        upload,
        chunk: vec![b'x'; 64 * 1024],
    });
    assert!(matches!(
        host.complete_socket(pushed).await,
        ForegroundDbCommandResponse::StreamingMutationPushed
    ));
    assert_eq!(
        host.command(ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    for command in [
        ForegroundDbCommandRequest::Poll { operation },
        ForegroundDbCommandRequest::Cancel { operation },
        ForegroundDbCommandRequest::CommitTransaction { transaction },
        ForegroundDbCommandRequest::FinishStreamingMutation { upload },
    ] {
        let bytes = postcard::to_allocvec(&command).unwrap();
        let mut output = empty();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    host.lease,
                    host.foreground,
                    bytes.as_ptr(),
                    bytes.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidHandle,
            "retired command {command:?}"
        );
        assert_eq!(output.len, 0);
    }
    drop(host);

    let (reopened, _) = Host::account(dir.path(), &server, Some(account)).await;
    reopened.socket_ready().await;
    let next = reopened.insert(204, "usable after reopen");
    let wait = reopened.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: next,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        reopened.complete_socket(wait).await,
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    let read = reopened.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"Remote"}"#.into(),
        transaction: None,
    });
    let ForegroundDbCommandResponse::Rows { rows } = reopened.complete_socket(read).await else {
        panic!("authority read")
    };
    let rows = jazz::binding_codec::decode_rows(&rows).unwrap();
    let mut actual = rows
        .iter()
        .flat_map(|batch| &batch.rows)
        .map(|row| match row.cells.get("text").unwrap() {
            Value::String(text) => text.clone(),
            value => panic!("unexpected title {value:?}"),
        })
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(actual, ["durable before close", "usable after reopen"]);
}

/// Alice catches a duplicate-row staging refusal and tries to seal the batch.
/// The refused batch cannot publish, and repeated failures must release native
/// slots so a later valid initialization can still be sealed and cancelled.
#[test]
fn native_failed_initialization_seals_release_transaction_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("failed-seal-capacity.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    host.insert(220, "existing");
    authority.drive(&host);
    for attempt in 0..96 {
        let opened = host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        });
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = opened else {
            panic!("attempt {attempt} could not open a fresh transaction: {opened:?}")
        };
        for (row, text) in [(attempt, "refused companion"), (220, "duplicate")] {
            assert!(matches!(
                host.command(ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "notes".into(),
                    row_id: Some([row; 16]),
                    cells: cells(text),
                }),
                ForegroundDbCommandResponse::Inserted { .. }
            ));
        }
        let seal = host.command(ForegroundDbCommandRequest::InitializationV1 {
            version: 1,
            action: InitializationAction::Seal { transaction },
        });
        assert!(
            matches!(&seal, ForegroundDbCommandResponse::CodedOperationError { code, .. } if code == "write_rejected"),
            "staging refusal must prevent sealing: {seal:?}"
        );
    }
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        })
    else {
        panic!("valid initialization must still be possible")
    };
    let seal = host.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::Seal { transaction },
    });
    let ForegroundDbCommandResponse::Rows { rows } = authority.complete(&host, seal) else {
        panic!("seal must complete")
    };
    let token = serde_json::from_slice::<serde_json::Value>(&rows).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let cancel = host.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::Cancel { token },
    });
    assert!(matches!(
        authority.complete(&host, cancel),
        ForegroundDbCommandResponse::Rows { .. }
    ));
    authority.drive(&host);
    assert_eq!(
        authority.texts(),
        ["existing"],
        "refused companions cannot escape"
    );
}

/// Alice's seal preflight rejects an unfinished read without consuming its
/// transaction. Once Bob's authority answers that read, the same transaction
/// must still seal, publish and receive genuine Global acceptance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_initialization_preflight_can_retry_after_read_completes() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (host, _) = Host::account(dir.path(), &server, None).await;
    host.socket_ready().await;
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        })
    else {
        panic!("open")
    };
    assert!(matches!(
        host.command(ForegroundDbCommandRequest::Insert {
            transaction,
            table: "notes".into(),
            row_id: Some([201; 16]),
            cells: cells("after preflight retry"),
        }),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    let read = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: "{}".into(),
        transaction: Some(transaction),
    });
    assert!(matches!(read, ForegroundDbCommandResponse::Pending { .. }));
    let preflight = host.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::Seal { transaction },
    });
    assert!(
        matches!(preflight, ForegroundDbCommandResponse::CodedOperationError { ref code, .. } if code == "not_observed")
    );
    assert!(matches!(
        host.complete_socket(read).await,
        ForegroundDbCommandResponse::Rows { .. }
    ));
    let seal = host.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::Seal { transaction },
    });
    let ForegroundDbCommandResponse::Rows { rows } = host.complete_socket(seal).await else {
        panic!("the same transaction must remain sealable")
    };
    let token = serde_json::from_slice::<serde_json::Value>(&rows).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let published = host.command(ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::Publish { token },
    });
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
        host.complete_socket(published).await
    else {
        panic!("publish")
    };
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        host.complete_socket(wait).await,
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    let read = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"Remote"}"#.into(),
        transaction: None,
    });
    let ForegroundDbCommandResponse::Rows { rows } = host.complete_socket(read).await else {
        panic!("authority read")
    };
    let rows = jazz::binding_codec::decode_rows(&rows).unwrap();
    let texts: Vec<_> = rows
        .iter()
        .flat_map(|batch| &batch.rows)
        .map(|row| row.cells.get("text").unwrap().clone())
        .collect();
    assert_eq!(texts, [Value::String("after preflight retry".into())]);
}

/// Alice's pending and completed seals retain the existing 64-slot bound.
/// Cancelling either form releases every slot without publishing its staged
/// rows, and Bob's authority can then accept a subsequent ordinary write.
#[test]
fn native_initialization_seals_hold_capacity_until_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("seal-capacity.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    for complete in [false, true] {
        let mut operations = Vec::new();
        let mut tokens = Vec::new();
        for row in 0..64 {
            let ForegroundDbCommandResponse::TransactionOpened { transaction } =
                host.command(ForegroundDbCommandRequest::BeginTransaction {
                    kind: ForegroundTransactionKind::Serializable,
                })
            else {
                panic!("open transaction {row}")
            };
            assert!(matches!(
                host.command(ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "notes".into(),
                    row_id: Some([row; 16]),
                    cells: cells("cancelled initialization"),
                }),
                ForegroundDbCommandResponse::Inserted { .. }
            ));
            let seal = host.command(ForegroundDbCommandRequest::InitializationV1 {
                version: 1,
                action: InitializationAction::Seal { transaction },
            });
            if complete {
                let ForegroundDbCommandResponse::Rows { rows } = authority.complete(&host, seal)
                else {
                    panic!("seal")
                };
                tokens.push(
                    serde_json::from_slice::<serde_json::Value>(&rows).unwrap()["token"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            } else {
                let ForegroundDbCommandResponse::Pending { operation } = seal else {
                    panic!("reservation must first await its durable owner: {seal:?}")
                };
                operations.push(operation);
            }
        }
        let over_capacity = host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        });
        assert!(
            matches!(over_capacity, ForegroundDbCommandResponse::OperationError { ref reason }
            if reason.contains("transaction capacity 64 exceeded")),
            "{over_capacity:?}"
        );
        for operation in operations {
            assert_eq!(
                host.command(ForegroundDbCommandRequest::Cancel { operation }),
                ForegroundDbCommandResponse::Cancelled { cancelled: true }
            );
        }
        for token in tokens {
            let cancelled = host.command(ForegroundDbCommandRequest::InitializationV1 {
                version: 1,
                action: InitializationAction::Cancel { token },
            });
            assert!(matches!(
                authority.complete(&host, cancelled),
                ForegroundDbCommandResponse::Rows { .. }
            ));
        }
        authority.drive(&host);
        assert!(
            authority.texts().is_empty(),
            "sealing and cancellation must not publish"
        );
    }
    let tx_id = host.insert(202, "after cancellations");
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        authority.complete(&host, wait),
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    assert_eq!(authority.texts(), ["after cancellations"]);
}

struct WakeProbe<'a> {
    host: &'a Host,
    count: Box<std::sync::atomic::AtomicUsize>,
}
unsafe extern "C" fn count_wake(
    context: *mut std::ffi::c_void,
    _foreground: u64,
    kind: u8,
    _delay_ms: u64,
) {
    if kind != 3 {
        unsafe { &*context.cast::<std::sync::atomic::AtomicUsize>() }
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}
impl<'a> WakeProbe<'a> {
    fn new(host: &'a Host) -> Self {
        let mut this = Self {
            host,
            count: Box::default(),
        };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    host.lease,
                    host.foreground,
                    Some(count_wake),
                    (&mut *this.count as *mut std::sync::atomic::AtomicUsize).cast(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
        this
    }
    fn take(&self) -> usize {
        self.count.swap(0, std::sync::atomic::Ordering::AcqRel)
    }
}
impl Drop for WakeProbe<'_> {
    fn drop(&mut self) {
        // Unregister synchronously before freeing the callback's stable address.
        unsafe {
            jazz_native_relay_host_lease_set_foreground_wake_callback(
                self.host.lease,
                self.host.foreground,
                None,
                std::ptr::null_mut(),
            )
        };
    }
}

/// Alice's drained local subscription becomes quiescent. New row changes still
/// request a platform callback; unrelated idle socket/owner turns do not.
#[test]
fn native_drained_subscriptions_do_not_keep_waking_the_platform() {
    let dir = tempfile::tempdir().unwrap();
    let host = Host::open(&dir.path().join("idle-wakes.sqlite"));
    let wake = WakeProbe::new(&host);
    let ForegroundDbCommandResponse::Subscribed { subscription } =
        host.command(ForegroundDbCommandRequest::Subscribe {
            query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
            options_json: r#"{"tier":"LocalOnly"}"#.into(),
        })
    else {
        panic!("subscription")
    };
    let drain = || {
        let ForegroundDbCommandResponse::SubscriptionEvents { events } =
            host.command(ForegroundDbCommandRequest::DrainSubscription { subscription })
        else {
            panic!("events")
        };
        events
    };
    assert!(!drain().is_empty());
    // An unopened Remote subscription must also stay parked while no
    // authority is available; its queued local preview is not deliverable.
    let ForegroundDbCommandResponse::Subscribed {
        subscription: remote,
    } = host.command(ForegroundDbCommandRequest::Subscribe {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"Remote"}"#.into(),
    })
    else {
        panic!("remote subscription")
    };
    assert!(
        matches!(host.command(ForegroundDbCommandRequest::DrainSubscription {
        subscription: remote,
    }), ForegroundDbCommandResponse::SubscriptionEvents { events } if events.is_empty())
    );
    for changed in [false, true] {
        if changed {
            host.insert(90, "new event");
            for _ in 0..4 {
                host.pump();
            }
            assert!(wake.take() > 0, "a new row must wake its observer");
            assert!(!drain().is_empty());
        }
        for _ in 0..4 {
            host.pump();
        }
        wake.take();
        for _ in 0..16 {
            host.pump();
        }
        assert!(
            wake.take() <= 4,
            "drained subscription must reach wake quiescence"
        );
    }
}

/// Alice repeatedly abandons units containing either a schema error or a
/// stored-row refusal. Each rollback releases native capacity and its staged
/// companion. Bob's authority sees only later valid replacements, for both
/// mergeable and serializable transactions, after more than the 64-slot limit.
#[test]
fn native_repeated_failed_staging_rollbacks_release_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let host = Host::open(&directory.path().join("rollback-capacity.sqlite"));
    let authority = Authority::new();
    authority.drive(&host);
    host.insert(1, "original");
    authority.drive(&host);
    let mut expected = vec!["original".to_owned()];
    let mut replacement_row = 10;
    for kind in [
        ForegroundTransactionKind::Mergeable,
        ForegroundTransactionKind::Serializable,
    ] {
        for schema_error in [true, false] {
            for attempt in 0..96 {
                let opened = host.command(ForegroundDbCommandRequest::BeginTransaction { kind });
                let ForegroundDbCommandResponse::TransactionOpened { transaction } = opened else {
                    panic!("{kind:?}, schema_error={schema_error}, attempt {attempt}: {opened:?}")
                };
                assert_eq!(
                    host.command(ForegroundDbCommandRequest::Insert {
                        transaction,
                        table: "notes".into(),
                        row_id: Some([2; 16]),
                        cells: cells("must remain rolled back"),
                    }),
                    ForegroundDbCommandResponse::Inserted { row_id: [2; 16] }
                );
                let refused = host.command(ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: if schema_error {
                        "missing_table"
                    } else {
                        "notes"
                    }
                    .into(),
                    row_id: Some([1; 16]),
                    cells: cells("refused"),
                });
                if schema_error {
                    assert!(
                        matches!(&refused, ForegroundDbCommandResponse::CodedOperationError { code, .. } if code == "query"),
                        "schema lookup fails before staging: {refused:?}"
                    );
                } else {
                    assert_eq!(
                        refused,
                        ForegroundDbCommandResponse::Inserted { row_id: [1; 16] },
                        "a stored-row refusal is retained for the transaction outcome"
                    );
                }
                assert_eq!(
                    host.command(ForegroundDbCommandRequest::RollbackTransaction { transaction }),
                    ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
                );
            }
            authority.drive(&host);
            assert_eq!(
                authority.texts(),
                expected,
                "rolled-back companions never publish"
            );
            let ForegroundDbCommandResponse::TransactionOpened { transaction } =
                host.command(ForegroundDbCommandRequest::BeginTransaction { kind })
            else {
                panic!("rollback must release capacity for a replacement")
            };
            let title = format!("replacement {replacement_row}");
            assert_eq!(
                host.command(ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "notes".into(),
                    row_id: Some([replacement_row; 16]),
                    cells: cells(&title),
                }),
                ForegroundDbCommandResponse::Inserted {
                    row_id: [replacement_row; 16]
                }
            );
            let ForegroundDbCommandResponse::TransactionCommitted { tx_id } =
                host.command(ForegroundDbCommandRequest::CommitTransaction { transaction })
            else {
                panic!("previous rollback must not poison the replacement")
            };
            let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
                tx_id,
                tier: "global".into(),
                observe_only: false,
            });
            assert!(matches!(
                authority.complete(&host, wait),
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ));
            expected.push(title);
            assert_eq!(authority.texts(), expected);
            replacement_row += 1;
        }
    }
}

/// Alice starts two serializable reads with a write between their admissions.
/// Bob's authority replies after later staging: each read must retain the
/// staged prefix at its own admission, and the final commit publishes the last
/// value. Both the active and queued reads use the real native C ABI and socket.
///
/// Alice: stage first -> read A (pending) -> stage middle -> read B (queued)
///        -> stage last -> receive A=first, B=middle -> commit last to Bob.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_pending_transaction_reads_preserve_their_staged_prefixes() {
    let issuer = jazz_server::TestJwtIssuer::start().await;
    let server = jazz_server::JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (host, _) = Host::account(directory.path(), &server, None).await;
    host.socket_ready().await;
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        host.command(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        })
    else {
        panic!("open serializable transaction")
    };
    let row_id = [0xd7; 16];
    assert_eq!(
        host.command(ForegroundDbCommandRequest::Insert {
            transaction,
            table: "notes".into(),
            row_id: Some(row_id),
            cells: cells("first"),
        }),
        ForegroundDbCommandResponse::Inserted { row_id }
    );
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut reads = Vec::new();
    for later in ["middle", "last"] {
        let read = host.command(ForegroundDbCommandRequest::All {
            query: query.clone(),
            options_json: "{}".into(),
            transaction: Some(transaction),
        });
        assert!(
            matches!(read, ForegroundDbCommandResponse::Pending { .. }),
            "the socket read must remain pending until polled: {read:?}"
        );
        reads.push(read);
        assert_eq!(
            host.command(ForegroundDbCommandRequest::Update {
                transaction,
                table: "notes".into(),
                row_id,
                patch: cells(later),
            }),
            ForegroundDbCommandResponse::MutationStaged
        );
    }
    let texts = |response: ForegroundDbCommandResponse| {
        let ForegroundDbCommandResponse::Rows { rows } = response else {
            panic!("read did not return rows: {response:?}")
        };
        jazz::binding_codec::decode_rows(&rows)
            .unwrap()
            .iter()
            .flat_map(|batch| &batch.rows)
            .map(|row| row.cells.get("text").unwrap().clone())
            .collect::<Vec<_>>()
    };
    for (read, expected) in reads.into_iter().zip(["first", "middle"]) {
        assert_eq!(
            texts(host.complete_socket(read).await),
            [Value::String(expected.into())],
            "later staging must not change an earlier admitted read"
        );
    }
    let committed = host.command(ForegroundDbCommandRequest::CommitTransaction { transaction });
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = committed else {
        panic!("completed reads must permit commit: {committed:?}")
    };
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(matches!(
        host.complete_socket(wait).await,
        ForegroundDbCommandResponse::TransactionSettled { .. }
    ));
    let read = host.command(ForegroundDbCommandRequest::All {
        query,
        options_json: r#"{"tier":"Remote"}"#.into(),
        transaction: None,
    });
    assert_eq!(
        texts(host.complete_socket(read).await),
        [Value::String("last".into())]
    );
}
