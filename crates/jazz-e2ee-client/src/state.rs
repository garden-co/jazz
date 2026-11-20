//! What the runtime and its hooks share on the database's thread: the
//! device, the held keys, the catalogue identities the cell contexts name,
//! and the reports reads leave for the runtime.

use std::cell::{Ref, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use jazz::db::{Db, ErrorCode};
use jazz::e2ee::{EncryptionEntry, SpaceAddress, Unreadable};
use jazz_e2ee::RandomSource;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::account::build::LocalDevice;
use jazz_e2ee::protocol::content::HeldKeys;
use jazz_e2ee::protocol::domain::{AppDomain, SharedDomain};
use jazz_e2ee::registry::Registry;

use crate::error::ClientError;
use crate::values::ColumnCodec;

/// At most this many reports wait for the runtime; older ones are dropped
/// first (protocol §4.3 keeps a bounded set of the same size).
pub const MAX_REPORTS: usize = 10_000;

/// Something a read or a load left for the runtime to act on or pass on.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Report {
    /// A cell or row a read left out, or returned with a notice
    /// (protocol §4.3).
    Unreadable(Unreadable),
    /// A row a read left out because this device does not hold the version
    /// its cell names: the runtime acquires it (protocol §4.2).
    MissingKey {
        table: String,
        space: SpaceAddress,
        epoch_id: String,
    },
    /// A record of the private store that failed its load checks and was
    /// skipped (protocol §6.2, §6.3a).
    SkippedRecord { document: String, reason: String },
}

/// One encrypted table: its entry and how each encrypted column is
/// encoded.
#[derive(Debug, Clone)]
pub(crate) struct TablePlan {
    pub(crate) entry: EncryptionEntry,
    pub(crate) columns: BTreeMap<String, ColumnCodec>,
}

impl TablePlan {
    /// Whether rows of this table are their own spaces.
    pub(crate) fn self_space(&self) -> bool {
        self.entry.space == "id"
    }
}

/// Jazz's catalogue identities of the declared tables and encrypted
/// columns, which every cell context names (protocol §4.1).
#[derive(Debug, Clone, Default)]
pub(crate) struct Catalogue {
    tables: BTreeMap<String, String>,
    columns: BTreeMap<(String, String), String>,
    /// Per declared table, the encrypted columns a migration added to rows
    /// stored before them: those rows hold no equality token for them.
    lens_added: BTreeMap<String, BTreeSet<String>>,
}

/// The error of a read or write before the identities are known.
pub(crate) fn not_observed() -> jazz::db::Error {
    jazz::db::Error::new(
        ErrorCode::NotObserved,
        "encrypted tables cannot be read or written before this client has synced their \
         catalogue identities once",
    )
}

impl Catalogue {
    /// The identities of every declared table, space table and encrypted
    /// column, or `None` while the database has not observed them.
    pub(crate) fn resolve(
        db: &Db,
        plans: &BTreeMap<String, TablePlan>,
    ) -> Result<Option<Self>, ClientError> {
        fn known<T>(
            found: Result<Option<T>, jazz::db::Error>,
            what: &str,
        ) -> Result<Option<T>, ClientError> {
            match found {
                Ok(Some(id)) => Ok(Some(id)),
                Ok(None) => Err(ClientError::Unsupported(format!(
                    "the encryption declarations name `{what}`, which the schema does not have"
                ))),
                Err(error) if error.code == ErrorCode::NotObserved => Ok(None),
                Err(error) => Err(error.into()),
            }
        }
        let mut catalogue = Self::default();
        for (table, plan) in plans {
            for name in [table, &plan.entry.scope] {
                let Some(id) = known(db.table_identity(name), name)? else {
                    return Ok(None);
                };
                catalogue.tables.insert(name.clone(), id.0.to_string());
            }
            let added = db.lens_added_columns(table)?;
            catalogue.lens_added.insert(
                table.clone(),
                plan.columns
                    .keys()
                    .filter(|column| added.contains(*column))
                    .cloned()
                    .collect(),
            );
            for column in plan.columns.keys() {
                let Some(id) = known(
                    db.column_identity(table, column),
                    &format!("{table}.{column}"),
                )?
                else {
                    return Ok(None);
                };
                catalogue
                    .columns
                    .insert((table.clone(), column.clone()), id.0.to_string());
            }
        }
        Ok(Some(catalogue))
    }

    pub(crate) fn table(&self, table: &str) -> Result<&str, jazz::db::Error> {
        self.tables
            .get(table)
            .map(String::as_str)
            .ok_or_else(not_observed)
    }

    /// Whether a migration added encrypted `column` of `table` under rows
    /// stored before it.
    pub(crate) fn lens_added(&self, table: &str, column: &str) -> bool {
        self.lens_added
            .get(table)
            .is_some_and(|columns| columns.contains(column))
    }

    pub(crate) fn column(&self, table: &str, column: &str) -> Result<&str, jazz::db::Error> {
        self.columns
            .get(&(table.to_owned(), column.to_owned()))
            .map(String::as_str)
            .ok_or_else(not_observed)
    }
}

/// The keys and what decides how they are used.
#[derive(Debug, Default)]
pub(crate) struct KeyState {
    pub(crate) held: HeldKeys,
    pub(crate) catalogue: Option<Catalogue>,
    /// Whether the client is online: offline, equality queries compute no
    /// tokens (protocol §4.5).
    pub(crate) online: bool,
    /// Set once the store is wiped: nothing more is sealed or opened.
    pub(crate) closed: bool,
}

/// Everything the runtime and its hooks share.
pub(crate) struct Shared {
    pub(crate) registry: Registry,
    pub(crate) selected: MechanismConfig,
    pub(crate) domain: AppDomain,
    pub(crate) content: SharedDomain,
    pub(crate) account_id: String,
    pub(crate) plans: BTreeMap<String, TablePlan>,
    /// This device; `None` once the store is wiped, its secrets and
    /// approval nonce dropped (and zeroed) with it (§6.4).
    pub(crate) device: RefCell<Option<LocalDevice>>,
    pub(crate) keys: RefCell<KeyState>,
    pub(crate) random: RefCell<Box<dyn RandomSource>>,
    pub(crate) reports: RefCell<VecDeque<Report>>,
}

impl Shared {
    /// This device, or [`ClientError::Closed`] once the store was wiped.
    pub(crate) fn device(&self) -> Result<Ref<'_, LocalDevice>, ClientError> {
        Ref::filter_map(self.device.borrow(), Option::as_ref).map_err(|_| ClientError::Closed)
    }

    /// Drops the device's secrets and approval nonce (each zeroed on drop)
    /// and every held key: the store was wiped (§6.4).
    pub(crate) fn forget_secrets(&self) {
        let mut keys = self.keys.borrow_mut();
        keys.closed = true;
        keys.held = Default::default();
        self.device.borrow_mut().take();
    }

    pub(crate) fn report(&self, report: Report) {
        let mut reports = self.reports.borrow_mut();
        if reports.len() == MAX_REPORTS {
            reports.pop_front();
        }
        reports.push_back(report);
    }
}

/// The core's form of a space address.
pub(crate) fn db_address(
    address: &jazz_e2ee::protocol::records::space::SpaceAddress,
) -> SpaceAddress {
    SpaceAddress {
        scope_id: address.scope_id.clone(),
        identifier: address.identifier.clone(),
    }
}
