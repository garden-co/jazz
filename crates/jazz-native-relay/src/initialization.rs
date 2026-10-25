use super::*;
use jazz::db::ReservedTxId;

/// The core may fail before taking a batch, or consume it before an owner
/// reservation fails/is cancelled. Match that actual lifetime on every exit,
/// including dropping an unpolled or suspended native operation.
struct SealingTransaction {
    db: Rc<Db>,
    transactions: Rc<RefCell<BTreeMap<u64, ForegroundTransaction>>>,
    handle: u64,
    open: OpenTransactionId,
    retained_by_token: bool,
}

impl SealingTransaction {
    fn retain_by_token(&mut self) {
        self.retained_by_token = true;
    }
}

impl Drop for SealingTransaction {
    fn drop(&mut self) {
        if !self.retained_by_token && !self.db.has_open_transaction(self.open) {
            self.transactions.borrow_mut().remove(&self.handle);
        }
    }
}

/// Frozen local V1 tags: Seal=0, Publish=1, Cancel=2, RecordAbsence=3,
/// Status=4, HasAuthenticatedCatalogue=5.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InitializationAction {
    Seal {
        transaction: u64,
    },
    Publish {
        token: String,
    },
    Cancel {
        token: String,
    },
    RecordAbsence {
        transaction: u64,
        table: String,
        row: [u8; 16],
    },
    Status {
        ids: Vec<String>,
    },
    /// Admitted host-cache readiness, not local schema bootstrap. Contention
    /// returns a foreground pending operation; true requires durable capture
    /// publication. Generic relay handles without a cache owner are unsupported.
    HasAuthenticatedCatalogue,
}

impl NativeRelayClient {
    pub(super) fn initialization_command(
        &self,
        action: InitializationAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.initialization_command(id, action))
    }
}

impl RelayWorker {
    fn initialization_command(
        &mut self,
        client: u64,
        action: InitializationAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        // This lookup checks foreground admission before even decoding reserved IDs.
        let foreground = self.foreground_client(client)?;
        let db = Rc::clone(&foreground.db);
        let seals = Rc::clone(&foreground.initialization_seals);
        let writes = Rc::clone(&foreground.mutations.writes);
        let future: ForegroundOperationFuture = match action {
            InitializationAction::HasAuthenticatedCatalogue => {
                let cache = self
                    .catalogue_cache
                    .as_ref()
                    .map(Rc::clone)
                    .ok_or_else(|| {
                        RelayError::ForegroundCommand(
                            "authenticated catalogue requires an admitted cache owner".into(),
                        )
                    })?;
                let owner = Rc::clone(&self.persistent);
                Box::pin(async move {
                    let requested_ready =
                        db.has_authenticated_catalogue().map_err(RelayError::Db)?;
                    let ready = std::future::poll_fn(move |context| {
                        cache.borrow_mut().poll_refresh(&owner, context)
                    })
                    .await?;
                    Ok(ForegroundOperationResult::Rows(
                        if requested_ready && ready {
                            b"true".to_vec()
                        } else {
                            b"false".to_vec()
                        },
                    ))
                })
            }
            InitializationAction::Seal { transaction } => {
                let (_, tx) = self.foreground_transaction(client, transaction)?;
                if tx.kind != ForegroundTransactionKind::Serializable {
                    return Err(RelayError::ForegroundCommand(
                        "initialization requires a serializable transaction".into(),
                    ));
                }
                if seals.borrow().len() >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX {
                    return Err(RelayError::ForegroundCommand(
                        "initialization seal capacity exceeded".into(),
                    ));
                }
                let mut transaction_lifetime = SealingTransaction {
                    db: Rc::clone(&db),
                    transactions: Rc::clone(&self.foreground_client(client)?.transactions),
                    handle: transaction,
                    open: tx.open_tx_id,
                    retained_by_token: false,
                };
                Box::pin(async move {
                    let seal = db
                        .binding
                        .seal_initialization_transaction_with_owner(&tx.open_tx_id.to_string())
                        .await
                        .map_err(engine::host_error)?;
                    let token = OpenTransactionId::new().to_string();
                    let result =
                        serde_json::json!({ "token": token, "reservedTxId": seal.reserved_tx_id });
                    seals.borrow_mut().insert(token, (transaction, seal.token));
                    // A successful seal keeps its native slot until the token
                    // is published or cancelled; it cannot evade the cap.
                    transaction_lifetime.retain_by_token();
                    Ok(ForegroundOperationResult::Rows(
                        result.to_string().into_bytes(),
                    ))
                })
            }
            InitializationAction::Publish { token } => {
                let (transaction, seal) =
                    seals.borrow_mut().remove(&token).ok_or_else(unknown_seal)?;
                self.foreground_client_mut(client)?
                    .transactions
                    .borrow_mut()
                    .remove(&transaction);
                Box::pin(async move {
                    let write = db
                        .binding
                        .publish_initialization_transaction(&seal)
                        .map_err(engine::host_error)?;
                    let id = write.tx_id;
                    writes.borrow_mut().insert(id, write);
                    Ok(ForegroundOperationResult::TransactionCommitted(id))
                })
            }
            InitializationAction::Cancel { token } => {
                let (transaction, seal) =
                    seals.borrow_mut().remove(&token).ok_or_else(unknown_seal)?;
                self.foreground_client_mut(client)?
                    .transactions
                    .borrow_mut()
                    .remove(&transaction);
                Box::pin(async move {
                    db.binding
                        .cancel_initialization_transaction(&seal)
                        .map_err(engine::host_error)?;
                    Ok(ForegroundOperationResult::Rows(Vec::new()))
                })
            }
            InitializationAction::RecordAbsence {
                transaction,
                table,
                row,
            } => {
                let (_, tx) = self.foreground_transaction(client, transaction)?;
                Box::pin(async move {
                    db.prepare_initialization_insert(
                        tx.open_tx_id,
                        &table,
                        RowUuid::from_bytes(row),
                    )
                    .map_err(RelayError::Db)?;
                    Ok(ForegroundOperationResult::Rows(Vec::new()))
                })
            }
            InitializationAction::Status { ids } => {
                if ids.len() > 64 {
                    return Err(RelayError::ForegroundCommand(
                        "initialization status accepts at most 64 identities".into(),
                    ));
                }
                let ids = ids
                    .iter()
                    .map(|id| ReservedTxId::decode(id))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(RelayError::Db)?;
                // A foreground's in-memory receipt is not owner durability. Core also
                // checks this durable owner's admitted relay session and stored author.
                let owner = Rc::clone(&self.persistent);
                Box::pin(async move {
                    let json = owner
                        .binding
                        .initialization_transaction_status(
                            &ids.iter().map(|id| id.encode()).collect::<Vec<_>>(),
                        )
                        .map_err(engine::host_error)?;
                    Ok(ForegroundOperationResult::Rows(json.into_bytes()))
                })
            }
        };
        self.start_foreground_operation(client, None, future)
    }
}

fn unknown_seal() -> RelayError {
    RelayError::ForegroundCommand(
        "initialization seal is unknown, consumed, or belongs to another foreground".into(),
    )
}
