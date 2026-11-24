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
use jazz::e2ee::{Provisional, TransactionFate};
use jazz::query::{Query, col, eq, lit};
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::{CreationFate, HeldSpace};
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::protocol::space::AccountDirectory;
use jazz_e2ee::protocol::space::build::{Author, SpaceWriter, space_root};

use super::{has_live_key, has_unpublished};
use crate::account::rows::{self, uuid_cell};
use crate::client::{E2eeClient, SharedRandom};
use crate::error::ClientError;

impl E2eeClient {
    /// Prepares a root for `address` from this device's last verified
    /// account: its key is kept `unpublished` (and stored) before anything
    /// stages the root, so a write never rests on a key this device could
    /// lose. The next write to the space stages the root with itself.
    pub(crate) async fn prepare_creation(&self, address: &SpaceAddress) -> Result<(), ClientError> {
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

    /// Settles the keys of this device's creations of `address` whose fate
    /// is known (protocol §5.4), given the root of the address's chain at
    /// a read that includes the outcome, if one is readable. Whether a
    /// creation is still undecided.
    pub(crate) async fn settle_creations(
        &self,
        db: &Db,
        address: &SpaceAddress,
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
    pub(crate) fn holds_live_key(&self, address: &SpaceAddress) -> bool {
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
    fn note_refusal(&self, address: &SpaceAddress, error: Option<&jazz::db::Error>) {
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
        let addresses: Vec<SpaceAddress> = self
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
        address: &SpaceAddress,
        root_id: &str,
    ) -> Result<CreationFate, ClientError> {
        if !self.shared.keys.borrow().online {
            return Ok(CreationFate::Undecided);
        }
        let id = uuid_cell(root_id).ok_or(ProtocolError::InconsistentEvidence)?;
        let query = Query::from(rows::SPACE_ROOTS).filter(eq(col("id"), lit(id)));
        let skipped = std::cell::Cell::new(0);
        let tx = OpenTransactionId::new();
        db.begin_serializable(tx).await?;
        let read = self
            .read_rows::<jazz_e2ee::protocol::records::space::SpaceRoot>(db, tx, &skipped, query)
            .await;
        let _ = db.abandon_transaction_handle(tx);
        match read {
            Ok((roots, _))
                if roots.iter().any(|root| {
                    root.row.scope_id == address.scope_id
                        && root.row.identifier == address.identifier
                }) =>
            {
                Ok(CreationFate::Accepted)
            }
            Ok(_) if skipped.get() > 0 => Ok(CreationFate::Undecided),
            Ok(_) => Ok(CreationFate::NotAccepted),
            Err(ClientError::Database(error)) if error.code == ErrorCode::NotObserved => {
                Ok(CreationFate::Undecided)
            }
            Err(error) => Err(error),
        }
    }

    /// What became of the transaction that carries the root `root_id`
    /// (protocol §5.4): from this device's own store while the store has
    /// the row, otherwise from the authority.
    async fn creation_fate(
        &self,
        db: &Db,
        address: &SpaceAddress,
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
            Some((_, Provisional::Own(tx))) => match db.transaction_fate(*tx).await? {
                TransactionFate::Accepted(_) => CreationFate::Accepted,
                TransactionFate::Refused(error) => {
                    self.note_refusal(address, error.as_ref());
                    CreationFate::NotAccepted
                }
                _ => CreationFate::Undecided,
            },
            None => {
                if let Some(error) = self.refused_root(db, id_bytes).await? {
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
    async fn refused_root(
        &self,
        db: &Db,
        id: [u8; 16],
    ) -> Result<Option<jazz::db::Error>, ClientError> {
        Ok(db
            .rejected_transactions()
            .await?
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
        let addresses: Vec<SpaceAddress> = self
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
                self.acquire_full(db, &address).await?;
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
