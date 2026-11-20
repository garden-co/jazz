//! Creating a space and settling the creations of this device (protocol
//! §3.2, §5.4; core hooks note H2, H11).
//!
//! A creation is optimistic: the runtime prepares a root and its key `K₀`,
//! keeps the key `unpublished` in the private store, and the next write to
//! the space stages the root in its own transaction (the hooks do that). The
//! authority decides. A creation's outcome settles the key: accepted as
//! the space's root clears the mark, anything else makes the key
//! `refused`, kept for good and never used for a write.
//!
//! The mark names the creation by its root row's ID, which the runtime
//! chose before the transaction existed: the transaction's own number is
//! drawn at commit, when the key has long been stored. The row is found in
//! this device's own store, whose commits it keeps across restarts.

use jazz::db::{Db, ErrorCode, Reason, WireWrite};
use jazz::e2ee::{Provisional, SpaceAddress, TransactionFate};
use jazz::ids::RowUuid;
use jazz::query::{Query, col, eq, lit};
use jazz::tools::OpenTransactionId;
use jazz::tx::DurabilityTier;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::{CreationFate, HeldSpace};
use jazz_e2ee::protocol::error::RefusalCode;
use jazz_e2ee::protocol::records::space::SpaceAddress as ProtocolAddress;
use jazz_e2ee::protocol::space::AccountDirectory;
use jazz_e2ee::protocol::space::build::{Author, SpaceWriter, space_root};

use super::{ATTEMPTS, has_live_key, has_unpublished, offline, protocol_address};
use crate::account::rows::{self, uuid_cell};
use crate::client::{E2eeClient, SharedRandom};
use crate::error::ClientError;
use crate::state::{db_address, not_observed};

/// What [`E2eeClient::create_space`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpaceCreation {
    /// The authority accepted this device's root, and it is the space's
    /// root.
    Created,
    /// The space already had a valid root (or another root won): it was
    /// not created again.
    Existing,
    /// An earlier creation of this device has not been accepted or
    /// refused yet: no second root was staged.
    Pending,
}

impl E2eeClient {
    /// The space whose space row is `row` of table `scope`.
    fn space_address(&self, scope: &str, row: RowUuid) -> Result<SpaceAddress, ClientError> {
        let keys = self.shared.keys.borrow();
        let catalogue = keys.catalogue.as_ref().ok_or_else(not_observed)?;
        Ok(SpaceAddress {
            scope_id: catalogue.table(scope)?.to_owned(),
            identifier: row.0.to_string(),
        })
    }

    /// Prepares a root for `address` from this device's last verified
    /// account: its key is kept `unpublished` (and stored) before anything
    /// stages the root, so a write never rests on a key this device could
    /// lose. The next write to the space stages the root with itself.
    pub(crate) async fn prepare_creation(
        &self,
        address: &ProtocolAddress,
    ) -> Result<(), ClientError> {
        let records = {
            let account = self.account.borrow();
            let account = account.as_ref().ok_or(ClientError::NoAccount)?;
            let ledgers = [account.public.clone()];
            let directory = AccountDirectory::new(
                &ledgers,
                &self.shared.domain,
                &self.shared.registry,
                account.public.through,
            )?;
            let device = self.shared.device()?.clone();
            let author = Author::new(&directory, &self.shared.account_id, &account.key, &device)?;
            let writer = SpaceWriter {
                suite: self.shared.registry.select(&self.shared.selected)?,
                registry: &self.shared.registry,
                domain: &self.shared.domain,
                account_id: &self.shared.account_id,
            };
            space_root(
                &writer,
                address,
                &author,
                &mut SharedRandom(&self.shared.random),
            )?
        };
        let held = HeldSpace::from_creation(
            &self.shared.registry,
            &self.shared.domain,
            &records,
            &records.root.id,
        )?;
        self.keep(held).await?;
        self.shared
            .creations
            .borrow_mut()
            .prepared
            .insert(address.clone(), records.root);
        Ok(())
    }

    /// Creates the space whose space row is `row` of table `scope`
    /// (protocol §3.2, `createSpace`): one transaction holding only the
    /// root, which waits for the authority. It reads the address's roots
    /// first and returns [`SpaceCreation::Existing`] if one is valid; it
    /// attempts even after an earlier refusal on this device, except while
    /// a creation of this device is neither accepted nor refused, which it
    /// reconciles and reports as [`SpaceCreation::Pending`]. A refusal of
    /// the root is `space-create-refused` and stops the automatic paths for
    /// this space while the client runs. Needs the authority and the
    /// account verified in this run.
    pub async fn create_space(
        &self,
        db: &Db,
        scope: &str,
        row: RowUuid,
    ) -> Result<SpaceCreation, ClientError> {
        self.ensure_open()?;
        let space = self.space_address(scope, row)?;
        let address = protocol_address(&space)?;
        self.reconcile_once(db).await?;
        if self.settle_creations(db, &address, None).await?
            || self.shared.keys.borrow().held.holds_unpublished(&address)
        {
            // Accepted but not yet seen as the space's root, or undecided.
            self.acquire_full(db, &space).await?;
            return Ok(
                if self.shared.keys.borrow().held.holds_unpublished(&address) {
                    SpaceCreation::Pending
                } else {
                    SpaceCreation::Existing
                },
            );
        }
        if !self.shared.keys.borrow().online {
            return Err(offline());
        }
        let attempt = crate::account::serializable(db, ATTEMPTS, async |tx| {
            self.create_step(db, tx, &address).await
        })
        .await?;
        match attempt {
            Attempt::Existing => {
                self.acquire_full(db, &space).await?;
                Ok(SpaceCreation::Existing)
            }
            Attempt::Created(root_id) => {
                let acquired = self.acquire_full(db, &space).await?;
                let ours = acquired
                    .chain
                    .is_some_and(|chain| chain.root.row.id == root_id);
                Ok(if ours {
                    SpaceCreation::Created
                } else {
                    SpaceCreation::Existing
                })
            }
            Attempt::Refused => {
                self.shared
                    .keys
                    .borrow_mut()
                    .creation_refused
                    .insert(address.clone());
                Err(ProtocolError::Refused(RefusalCode::SpaceCreateRefused).into())
            }
        }
    }

    /// One attempt of [`E2eeClient::create_space`]: the root alone in the
    /// transaction `tx`, whose read set is the address's roots and the
    /// public rows of the author's account.
    async fn create_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
    ) -> Result<Attempt, ClientError> {
        let read = self.read_space(db, tx, address).await?;
        let records = {
            let chain = self.replay_space(&read).await?.chain;
            if chain.is_some() {
                return Ok(Attempt::Existing);
            }
            // Rows this client cannot decode may be the root it does not
            // see; a key this device holds means a root exists that this
            // read hides: neither creates a second root (protocol §3.3).
            if read.skipped > 0 {
                return Err(ClientError::Unsupported(
                    "the space has rows this client cannot read: it is not created".to_owned(),
                ));
            }
            if self.holds_live_key(address) {
                return Ok(Attempt::Existing);
            }
            let account = self.account.borrow();
            let account = account.as_ref().ok_or(ClientError::NoAccount)?;
            let directory = AccountDirectory::new(
                &read.accounts,
                &self.shared.domain,
                &self.shared.registry,
                read.ledger.through,
            )
            .map_err(ClientError::from)?;
            let device = self.shared.device()?.clone();
            let author = Author::new(&directory, &self.shared.account_id, &account.key, &device)
                .map_err(ClientError::from)?;
            let writer = SpaceWriter {
                suite: self
                    .shared
                    .registry
                    .select(&self.shared.selected)
                    .map_err(ClientError::from)?,
                registry: &self.shared.registry,
                domain: &self.shared.domain,
                account_id: &self.shared.account_id,
            };
            space_root(
                &writer,
                address,
                &author,
                &mut SharedRandom(&self.shared.random),
            )
            .map_err(ClientError::from)?
        };
        let held = HeldSpace::from_creation(
            &self.shared.registry,
            &self.shared.domain,
            &records,
            &records.root.id,
        )
        .map_err(ClientError::from)?;
        let root_id = records.root.id.clone();
        self.keep(held).await?;
        // From here the key exists: whatever happens, it settles it.
        let committed = async {
            self.stage(db, tx, &records.root).await?;
            let receipt = db.commit_serializable_handle(tx).await?;
            db.wait_for_transaction(receipt.0, DurabilityTier::Global)
                .await?;
            Ok::<_, ClientError>(())
        }
        .await;
        match committed {
            Ok(()) => Ok(Attempt::Created(root_id)),
            Err(error) => {
                self.abandon_creation(&root_id).await?;
                match error {
                    ClientError::Database(error)
                        if error.reason == Some(Reason::AuthorizationDenied) =>
                    {
                        Ok(Attempt::Refused)
                    }
                    other => Err(other),
                }
            }
        }
    }

    /// Settles the keys of this device's creations of `address` whose fate
    /// is known (protocol §5.4), given the root of the address's chain at
    /// a read that includes the outcome, if one is readable. Whether a
    /// creation is still undecided.
    pub(crate) async fn settle_creations(
        &self,
        db: &Db,
        address: &ProtocolAddress,
        root: Option<&str>,
    ) -> Result<bool, ClientError> {
        let roots: Vec<String> = self
            .shared
            .keys
            .borrow()
            .held
            .roots_of(address)
            .into_iter()
            .filter(|space| has_unpublished(space))
            .map(|space| space.root_id().to_owned())
            .collect();
        let mut undecided = false;
        for root_id in roots {
            let fate = self.creation_fate(db, address, &root_id).await?;
            let settled = {
                let keys = self.shared.keys.borrow();
                keys.held.space(&root_id).cloned().map(|mut space| {
                    space.settle(fate, root);
                    space
                })
            };
            let Some(settled) = settled else { continue };
            if has_unpublished(&settled) {
                undecided |= fate == CreationFate::Undecided;
                continue;
            }
            {
                let mut creations = self.shared.creations.borrow_mut();
                creations.staged.remove(&root_id);
                creations
                    .prepared
                    .retain(|_, prepared| prepared.id != root_id);
            }
            self.keep(settled).await?;
        }
        Ok(undecided)
    }

    /// Whether this device holds a key of `address` that is not `refused`.
    pub(crate) fn holds_live_key(&self, address: &ProtocolAddress) -> bool {
        self.shared
            .keys
            .borrow()
            .held
            .roots_of(address)
            .iter()
            .any(|space| has_live_key(space))
    }

    /// Stops the automatic paths for `address` when `error` is the
    /// authority refusing a transaction that carried this device's root
    /// (protocol §3.2).
    fn note_refusal(&self, address: &ProtocolAddress, error: Option<&jazz::db::Error>) {
        if error.is_some_and(|error| error.reason == Some(Reason::AuthorizationDenied)) {
            self.shared
                .keys
                .borrow_mut()
                .creation_refused
                .insert(address.clone());
        }
    }

    /// Settles the creations of this run that the authority has since
    /// accepted or refused.
    pub(crate) async fn settle_pending(&self, db: &Db) -> Result<(), ClientError> {
        let addresses: Vec<ProtocolAddress> = self
            .shared
            .keys
            .borrow()
            .held
            .spaces()
            .iter()
            .filter(|space| has_unpublished(space))
            .map(|space| space.address().clone())
            .collect();
        for address in addresses {
            self.settle_creations(db, &address, None).await?;
        }
        Ok(())
    }

    /// Whether the authority shows the root `root_id` of `address`: an
    /// online ledger read, never the local store alone. `Undecided` when
    /// it cannot be read (offline, or rows this client cannot decode may
    /// hide the answer).
    async fn root_at_authority(
        &self,
        db: &Db,
        address: &ProtocolAddress,
        root_id: &str,
    ) -> Result<CreationFate, ClientError> {
        if !self.shared.keys.borrow().online {
            return Ok(CreationFate::Undecided);
        }
        let tx = OpenTransactionId::new();
        db.begin_serializable(tx).await?;
        let read = self.read_space(db, tx, address).await;
        let _ = db.abandon_transaction_handle(tx);
        let read = match read {
            Ok(read) => read,
            Err(ClientError::Database(error)) if error.code == ErrorCode::NotObserved => {
                return Ok(CreationFate::Undecided);
            }
            Err(error) => return Err(error),
        };
        Ok(
            if read.ledger.roots.iter().any(|root| root.row.id == root_id) {
                CreationFate::Accepted
            } else if read.skipped > 0 {
                CreationFate::Undecided
            } else {
                CreationFate::NotAccepted
            },
        )
    }

    /// What became of the transaction that carries the root `root_id`
    /// (protocol §5.4): from this device's own store while the store has
    /// the row, otherwise from the authority.
    async fn creation_fate(
        &self,
        db: &Db,
        address: &ProtocolAddress,
        root_id: &str,
    ) -> Result<CreationFate, ClientError> {
        let in_flight = {
            let creations = self.shared.creations.borrow();
            creations.staged.contains(root_id)
                || creations.prepared.values().any(|root| root.id == root_id)
        };
        let id = uuid_cell(root_id).ok_or(ProtocolError::InvalidRecord(
            "a creation's root ID is not a UUID",
        ))?;
        let id_bytes = *uuid::Uuid::parse_str(root_id)
            .map_err(|_| ProtocolError::InvalidRecord("a creation's root ID is not a UUID"))?
            .as_bytes();
        let query = Query::from(rows::SPACE_ROOTS).filter(eq(col("id"), lit(id)));
        let read = match db.provisional_read(&db.prepare_query(&query)?).await {
            Ok(read) => read,
            // A store that has never had an upstream knows nothing yet.
            Err(error) if error.code == ErrorCode::NotObserved => {
                return Ok(CreationFate::Undecided);
            }
            Err(error) => return Err(error.into()),
        };
        Ok(match read.rows.first() {
            Some((_, Provisional::Accepted(_))) => CreationFate::Accepted,
            Some((_, Provisional::Own(tx))) => match db.transaction_fate(*tx)? {
                TransactionFate::Accepted(_) => CreationFate::Accepted,
                TransactionFate::Refused(error) => {
                    self.note_refusal(address, error.as_ref());
                    CreationFate::NotAccepted
                }
                _ => CreationFate::Undecided,
            },
            None => {
                if let Some(error) = self.refused_root(db, id_bytes)? {
                    self.note_refusal(address, Some(&error));
                    return Ok(CreationFate::NotAccepted);
                }
                if in_flight {
                    return Ok(CreationFate::Undecided);
                }
                // Not in the store, and not a root this run is staging:
                // never committed, refused, or a store that lost the
                // commit. Only the authority says which.
                self.root_at_authority(db, address, root_id).await?
            }
        })
    }

    /// Whether the store keeps a refused commit that inserted the root
    /// `id`: a root refused after the run staged it is gone from the
    /// store's rows, and this is what is left of it.
    fn refused_root(&self, db: &Db, id: [u8; 16]) -> Result<Option<jazz::db::Error>, ClientError> {
        Ok(db
            .rejected_transactions()?
            .into_iter()
            .find(|rejected| {
                rejected.ops.iter().any(|op| {
                    matches!(
                        &op.write,
                        WireWrite::Insert { table, row, .. }
                            if table == rows::SPACE_ROOTS && *row == id
                    )
                })
            })
            .map(|rejected| rejected.error))
    }

    /// Settles the creations this device's store holds from an earlier run
    /// (protocol §5.4): each `unpublished` key by its root's fate; a root
    /// that was accepted is looked up online, to see whether it is the
    /// space's root. Nothing is staged. It runs once per run before any
    /// automatic path, and may be called again.
    pub async fn reconcile(&self, db: &Db) -> Result<(), ClientError> {
        self.ensure_open()?;
        let addresses: Vec<ProtocolAddress> = self
            .shared
            .keys
            .borrow()
            .held
            .spaces()
            .iter()
            .filter(|space| has_unpublished(space))
            .map(|space| space.address().clone())
            .collect();
        let online = self.shared.keys.borrow().online;
        for address in addresses {
            if online {
                // Reads the space too, to learn whether an accepted root
                // is the space's root.
                self.acquire_full(db, &db_address(&address)).await?;
            } else {
                self.settle_creations(db, &address, None).await?;
            }
        }
        self.reconciled.set(true);
        Ok(())
    }

    pub(crate) async fn reconcile_once(&self, db: &Db) -> Result<(), ClientError> {
        if self.reconciled.get() {
            return Ok(());
        }
        self.reconcile(db).await
    }
}

/// What one attempt of `create_space` did.
enum Attempt {
    /// The address already had a valid root.
    Existing,
    /// The authority accepted this root.
    Created(String),
    /// The authority refused the root: the session may not create it.
    Refused,
}
