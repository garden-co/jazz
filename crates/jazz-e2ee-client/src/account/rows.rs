//! The account and device tables' rows (`E2EE_PROTOCOL.md` §1.2) as the
//! database stores them, and as `jazz-e2ee`'s records hold them.
//!
//! Every ID column is a `uuid`, every number a 64-bit integer, every byte
//! string `bytes`, and optional columns may be null. A row whose cells do
//! not decode into its record (a number out of its range, an unknown
//! `kind` or `action`) is left out of the read and reported: replay treats
//! such a row like any other invalid row, which never counts.

use std::collections::BTreeMap;

use jazz::db::CurrentRow;
use jazz::records::Value;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::records::KeyColumns;
use jazz_e2ee::protocol::records::account::{
    AccountAction, AccountIdentity, AccountKind, AccountRoot, AccountSealingKey, AccountSuccessor,
    AccountSuccessorColumns, DeviceApproval, DeviceChallenge, DeviceDelivery, DeviceKeysRow,
    DeviceProof, DeviceRemovalRequest, DeviceRequest, PublicAccountSuccessor, PublicDeviceApproval,
    RecoveryDelivery, RecoveryRoot,
};
use jazz_e2ee::protocol::records::roster::{Commitment, SubjectKind};
use jazz_e2ee::protocol::records::space::{SpaceDelivery, SpaceMember, SpaceRoot, SpaceSuccessor};

/// The cells of one row, by column.
pub(crate) type Cells = BTreeMap<String, Value>;

/// One table's record.
pub(crate) trait Row: Sized {
    /// The table.
    const TABLE: &'static str;
    /// The record's row ID.
    fn id(&self) -> &str;
    /// The row's cells, its ID aside.
    fn cells(&self) -> Option<Cells>;
    /// The record a stored row holds, or `None` when its cells are not one.
    fn decode(row: &CurrentRow) -> Option<Self>;
    #[cfg(test)]
    fn sample(seed: &mut u8, optionals: bool) -> Self;
}

#[cfg(test)]
fn sample_id(seed: &mut u8) -> String {
    let value = *seed;
    *seed = seed.wrapping_add(1);
    uuid::Uuid::from_bytes([value; 16]).to_string()
}

#[cfg(test)]
fn sample_bytes(seed: &mut u8) -> Vec<u8> {
    let value = *seed;
    *seed = seed.wrapping_add(1);
    vec![value, value.wrapping_add(1), value.wrapping_add(2)]
}

#[cfg(test)]
fn sample_keys(seed: &mut u8) -> KeyColumns {
    let public_key = sample_bytes(seed);
    let signing_public_key = sample_bytes(seed);
    let value = *seed;
    *seed = seed.wrapping_add(1);
    KeyColumns {
        public_key,
        mechanism: format!("sealing-{value}"),
        version: u32::from(value) + 1000,
        signing_public_key,
        signing_mechanism: format!("signing-{value}"),
        signing_version: u32::from(value) + 2000,
    }
}

#[cfg(test)]
macro_rules! sample_column {
    ($seed:ident, $optionals:ident, id) => {
        sample_id($seed)
    };
    ($seed:ident, $optionals:ident, bytes) => {
        sample_bytes($seed)
    };
    ($seed:ident, $optionals:ident, keys) => {
        sample_keys($seed)
    };
    ($seed:ident, $optionals:ident, text) => {{
        let value = *$seed;
        *$seed = $seed.wrapping_add(1);
        format!("value-{value}")
    }};
    ($seed:ident, $optionals:ident, number) => {{
        let value = *$seed;
        *$seed = $seed.wrapping_add(1);
        u64::from(value)
    }};
    ($seed:ident, $optionals:ident, version) => {{
        let value = *$seed;
        *$seed = $seed.wrapping_add(1);
        u32::from(value) + 100
    }};
    ($seed:ident, $optionals:ident, subject_kind) => {
        if $optionals {
            SubjectKind::RecoveryRoot
        } else {
            SubjectKind::Device
        }
    };
    ($seed:ident, $optionals:ident, maybe_id) => {{
        if $optionals {
            Some(sample_id($seed))
        } else {
            None
        }
    }};
    ($seed:ident, $optionals:ident, maybe_bytes) => {{
        if $optionals {
            Some(sample_bytes($seed))
        } else {
            None
        }
    }};
}

/// A UUID cell from its text form; `None` when it is not a UUID.
pub(crate) fn uuid_cell(id: &str) -> Option<Value> {
    uuid::Uuid::parse_str(id).ok().map(Value::Uuid)
}

pub(crate) fn account_id_cell(id: &str) -> Result<Value, ProtocolError> {
    uuid_cell(id).ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))
}

pub(crate) fn row_account_id(row: &CurrentRow) -> Option<String> {
    match row.cell("accountId")? {
        Value::Uuid(id) => Some(id.to_string()),
        _ => None,
    }
}

pub(crate) fn unwrap_optional(value: &Value) -> Option<&Value> {
    match value {
        Value::Nullable(inner) => inner.as_deref(),
        value => Some(value),
    }
}

/// What a decoder reads from a row.
struct Read<'r>(&'r CurrentRow);

impl Read<'_> {
    fn row_id(&self) -> String {
        self.0.row_uuid().0.to_string()
    }

    fn optional(&self, column: &str) -> Option<Value> {
        self.0
            .cells()
            .find(|(name, _)| *name == column)
            .and_then(|(_, value)| value)
            .and_then(unwrap_optional)
            .cloned()
    }

    fn id(&self, column: &str) -> Option<String> {
        match self.optional(column)? {
            Value::Uuid(id) => Some(id.to_string()),
            _ => None,
        }
    }

    fn maybe_id(&self, column: &str) -> Option<Option<String>> {
        match self.optional(column) {
            None => Some(None),
            Some(Value::Uuid(id)) => Some(Some(id.to_string())),
            Some(_) => None,
        }
    }

    fn bytes(&self, column: &str) -> Option<Vec<u8>> {
        match self.optional(column)? {
            Value::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn maybe_bytes(&self, column: &str) -> Option<Option<Vec<u8>>> {
        match self.optional(column) {
            None => Some(None),
            Some(Value::Bytes(bytes)) => Some(Some(bytes)),
            Some(_) => None,
        }
    }

    fn text(&self, column: &str) -> Option<String> {
        match self.optional(column)? {
            Value::String(text) => Some(text),
            _ => None,
        }
    }

    fn number(&self, column: &str) -> Option<u64> {
        match self.optional(column)? {
            Value::I64(number) => u64::try_from(number).ok(),
            _ => None,
        }
    }

    fn version(&self, column: &str) -> Option<u32> {
        u32::try_from(self.number(column)?).ok()
    }

    fn keys(&self) -> Option<KeyColumns> {
        Some(KeyColumns {
            public_key: self.bytes("publicKey")?,
            mechanism: self.text("mechanism")?,
            version: self.version("version")?,
            signing_public_key: self.bytes("signingPublicKey")?,
            signing_mechanism: self.text("signingMechanism")?,
            signing_version: self.version("signingVersion")?,
        })
    }
}

/// Builds a row's cells; any ID that is not a UUID makes it `None`.
struct Write(Option<Cells>);

impl Write {
    fn new() -> Self {
        Self(Some(Cells::new()))
    }

    fn put(mut self, column: &str, value: Option<Value>) -> Self {
        self.0 = match (self.0, value) {
            (Some(mut cells), Some(value)) => {
                cells.insert(column.to_owned(), value);
                Some(cells)
            }
            _ => None,
        };
        self
    }

    fn id(self, column: &str, id: &str) -> Self {
        let value = uuid_cell(id);
        self.put(column, value)
    }

    fn maybe_id(self, column: &str, id: Option<&str>) -> Self {
        match id {
            Some(id) => self.id(column, id),
            None => self.put(column, Some(Value::Nullable(None))),
        }
    }

    fn bytes(self, column: &str, bytes: &[u8]) -> Self {
        self.put(column, Some(Value::Bytes(bytes.to_vec())))
    }

    fn maybe_bytes(self, column: &str, bytes: Option<&[u8]>) -> Self {
        let value = match bytes {
            Some(bytes) => Value::Bytes(bytes.to_vec()),
            None => Value::Nullable(None),
        };
        self.put(column, Some(value))
    }

    fn text(self, column: &str, text: &str) -> Self {
        self.put(column, Some(Value::String(text.to_owned())))
    }

    fn number(self, column: &str, number: u64) -> Self {
        let value = i64::try_from(number).ok().map(Value::I64);
        self.put(column, value)
    }

    fn keys(self, keys: &KeyColumns) -> Self {
        self.bytes("publicKey", &keys.public_key)
            .text("mechanism", &keys.mechanism)
            .number("version", keys.version.into())
            .bytes("signingPublicKey", &keys.signing_public_key)
            .text("signingMechanism", &keys.signing_mechanism)
            .number("signingVersion", keys.signing_version.into())
    }

    fn done(self) -> Option<Cells> {
        self.0
    }
}

pub(crate) use jazz::tools::deployment::artifact::system_tables::{
    ACCOUNT_IDENTITIES, ACCOUNT_KEYS, ACCOUNT_ROOTS, ACCOUNT_SUCCESSORS, COMMITMENTS,
    DEVICE_APPROVALS, DEVICE_CHALLENGES, DEVICE_DELIVERIES, DEVICE_KEYS, DEVICE_PROOFS,
    DEVICE_REMOVAL_REQUESTS, DEVICE_REQUESTS, MEMBERS as SPACE_MEMBERS, PUBLIC_ACCOUNT_SUCCESSORS,
    PUBLIC_DEVICE_APPROVALS, RECOVERY_DELIVERIES, RECOVERY_ROOTS, SPACE_DELIVERIES,
    SPACE_SUCCESSORS, SPACES as SPACE_ROOTS,
};

/// One column of a record's cells, written by its kind.
macro_rules! write_column {
    ($w:expr, $v:expr, keys) => {
        $w.keys(&$v)
    };
    ($w:expr, $v:expr, id, $c:literal) => {
        $w.id($c, &$v)
    };
    ($w:expr, $v:expr, maybe_id, $c:literal) => {
        $w.maybe_id($c, $v.as_deref())
    };
    ($w:expr, $v:expr, bytes, $c:literal) => {
        $w.bytes($c, &$v)
    };
    ($w:expr, $v:expr, maybe_bytes, $c:literal) => {
        $w.maybe_bytes($c, $v.as_deref())
    };
    ($w:expr, $v:expr, text, $c:literal) => {
        $w.text($c, &$v)
    };
    ($w:expr, $v:expr, number, $c:literal) => {
        $w.number($c, $v)
    };
    ($w:expr, $v:expr, version, $c:literal) => {
        $w.number($c, $v.into())
    };
    ($w:expr, $v:expr, action, $c:literal) => {
        $w.text($c, $v.as_str())
    };
    ($w:expr, $v:expr, subject_kind, $c:literal) => {
        $w.text($c, $v.as_str())
    };
}

/// One column of a record, read by its kind.
macro_rules! read_column {
    ($r:expr, keys) => {
        $r.keys()?
    };
    ($r:expr, id, $c:literal) => {
        $r.id($c)?
    };
    ($r:expr, maybe_id, $c:literal) => {
        $r.maybe_id($c)?
    };
    ($r:expr, bytes, $c:literal) => {
        $r.bytes($c)?
    };
    ($r:expr, maybe_bytes, $c:literal) => {
        $r.maybe_bytes($c)?
    };
    ($r:expr, text, $c:literal) => {
        $r.text($c)?
    };
    ($r:expr, number, $c:literal) => {
        $r.number($c)?
    };
    ($r:expr, version, $c:literal) => {
        $r.version($c)?
    };
    ($r:expr, action, $c:literal) => {
        AccountAction::parse(&$r.text($c)?).ok()?
    };
    ($r:expr, subject_kind, $c:literal) => {
        SubjectKind::parse(&$r.text($c)?).ok()?
    };
}

/// One table's [`Row`] from its record's fields, each with the kind of its
/// column and its name: `id` and `bytes` and `text`, `number` (a `u64`),
/// `version` (a `u32`), `action`, the optional `maybe_id` and `maybe_bytes`,
/// and the six `keys` columns as one. The same list writes the cells and
/// reads them back, so a column cannot be written under one name and read
/// under another.
macro_rules! row {
    ($record:ident => $table:ident { $($field:ident : $kind:ident $($column:literal)?),* $(,)? }) => {
        impl Row for $record {
            const TABLE: &'static str = $table;
            fn id(&self) -> &str {
                &self.id
            }
            fn cells(&self) -> Option<Cells> {
                let record = self;
                let w = Write::new();
                    $(let w = write_column!(w, record.$field, $kind $(, $column)?);)*
                w.done()
            }
            fn decode(row: &CurrentRow) -> Option<Self> {
                let r = Read(row);
                Some(Self {
                    id: r.row_id(),
                    $($field: read_column!(r, $kind $(, $column)?)),*
                })
            }
            #[cfg(test)]
            fn sample(seed: &mut u8, optionals: bool) -> Self {
                let _ = optionals;
                Self {
                    id: sample_id(seed),
                    $($field: sample_column!(seed, optionals, $kind)),*
                }
            }
        }
    };
}

row! {
    DeviceRequest => DEVICE_REQUESTS {
        keys: keys,
        nonce_commitment: bytes "nonceCommitment",
    }
}

row! {
    DeviceKeysRow => DEVICE_KEYS {
        device_id: id "deviceId",
        keys: keys,
    }
}

row! {
    AccountIdentity => ACCOUNT_IDENTITIES {
        ledger_version: version "ledgerVersion",
        generation: number "generation",
        device_id: id "deviceId",
        epoch_id: id "epochId",
        envelope: bytes "envelope",
        verification: bytes "verification",
    }
}

impl Row for AccountRoot {
    const TABLE: &'static str = ACCOUNT_ROOTS;
    fn id(&self) -> &str {
        &self.id
    }
    fn cells(&self) -> Option<Cells> {
        Write::new()
            .number("ledgerVersion", self.ledger_version.into())
            .number("generation", self.generation)
            .id("accountId", &self.account_id)
            .id("deviceId", &self.device_id)
            .id("epochId", &self.epoch_id)
            .text("kind", self.kind.as_str())
            .maybe_bytes(
                "deviceAccountPublicKey",
                self.device_account_public_key.as_deref(),
            )
            .maybe_bytes(
                "deviceAccountSignature",
                self.device_account_signature.as_deref(),
            )
            .done()
    }
    /// A root of a kind this client does not know is still a root. Under
    /// another ledger version it is an unsupported capability, valid or
    /// not, which replay reports (§2.4 step 1); under this one it is
    /// malformed, and replay rejects it. Either way it stays in the read,
    /// so the account never looks empty for want of it. It is kept as a
    /// root whose columns do not match its kind, which replay rejects as
    /// malformed, and its ledger version is kept as stored.
    fn decode(row: &CurrentRow) -> Option<Self> {
        let r = Read(row);
        let (kind, device_account_public_key, device_account_signature) =
            match AccountKind::parse(&r.text("kind")?) {
                Ok(kind) => (
                    kind,
                    r.maybe_bytes("deviceAccountPublicKey")?,
                    r.maybe_bytes("deviceAccountSignature")?,
                ),
                Err(_) => (AccountKind::App, Some(Vec::new()), None),
            };
        Some(Self {
            id: r.row_id(),
            ledger_version: r.version("ledgerVersion")?,
            generation: r.number("generation")?,
            account_id: r.id("accountId")?,
            device_id: r.id("deviceId")?,
            epoch_id: r.id("epochId")?,
            kind,
            device_account_public_key,
            device_account_signature,
        })
    }
    #[cfg(test)]
    fn sample(seed: &mut u8, optionals: bool) -> Self {
        let ledger_version = 100 + u32::from(*seed);
        *seed = seed.wrapping_add(1);
        let generation = u64::from(*seed);
        *seed = seed.wrapping_add(1);
        let account_id = sample_id(seed);
        let device_id = sample_id(seed);
        let epoch_id = sample_id(seed);
        let (kind, device_account_public_key, device_account_signature) = if optionals {
            (
                AccountKind::DeviceAccount,
                Some(sample_bytes(seed)),
                Some(sample_bytes(seed)),
            )
        } else {
            (AccountKind::App, None, None)
        };
        Self {
            id: sample_id(seed),
            ledger_version,
            generation,
            account_id,
            device_id,
            epoch_id,
            kind,
            device_account_public_key,
            device_account_signature,
        }
    }
}

row! {
    DeviceChallenge => DEVICE_CHALLENGES {
        device_id: id "deviceId",
        epoch_id: id "epochId",
        envelope: bytes "envelope",
        nonce: bytes "nonce",
        signer_id: id "signerId",
        signature: bytes "signature",
    }
}

row! {
    DeviceProof => DEVICE_PROOFS {
        challenge_id: id "challengeId",
        proof: bytes "proof",
        nonce: bytes "nonce",
        signature: bytes "signature",
    }
}

row! {
    DeviceApproval => DEVICE_APPROVALS {
        challenge_id: id "challengeId",
        signer_id: id "signerId",
        verification: bytes "verification",
        signature: bytes "signature",
    }
}

row! {
    DeviceDelivery => DEVICE_DELIVERIES {
        challenge_id: id "challengeId",
        envelope: bytes "envelope",
        verification: bytes "verification",
    }
}

row! {
    PublicDeviceApproval => PUBLIC_DEVICE_APPROVALS {
        account_id: id "accountId",
        epoch_id: id "epochId",
        device_id: id "deviceId",
        signer_id: id "signerId",
        signature: bytes "signature",
        recovery_root_id: maybe_id "recoveryRootId",
        recovery_signature: maybe_bytes "recoverySignature",
    }
}

macro_rules! successor_row {
    ($record:ident => $table:ident { $($field:ident : $column:literal),* $(,)? }) => {
        impl Row for $record {
            const TABLE: &'static str = $table;

            fn id(&self) -> &str {
                &self.columns.id
            }

            fn cells(&self) -> Option<Cells> {
                let columns = &self.columns;
                Write::new()
                    .id("accountId", &columns.account_id)
                    .id("predecessor", &columns.predecessor)
                    .id("epochId", &columns.epoch_id)
                    .id("signerId", &columns.signer_id)
                    .text("action", columns.action.as_str())
                    .maybe_id("removedDeviceId", columns.removed_device_id.as_deref())
                    .bytes("retiredRecoveryRootIds", &columns.retired_recovery_root_ids)
                    .bytes("membership", &columns.membership)
                    $(.bytes($column, &self.$field))*
                    .done()
            }

            fn decode(row: &CurrentRow) -> Option<Self> {
                let r = Read(row);
                Some(Self {
                    columns: AccountSuccessorColumns {
                        id: r.row_id(),
                        account_id: r.id("accountId")?,
                        predecessor: r.id("predecessor")?,
                        epoch_id: r.id("epochId")?,
                        signer_id: r.id("signerId")?,
                        action: AccountAction::parse(&r.text("action")?).ok()?,
                        removed_device_id: r.maybe_id("removedDeviceId")?,
                        retired_recovery_root_ids: r.bytes("retiredRecoveryRootIds")?,
                        membership: r.bytes("membership")?,
                    },
                    $($field: r.bytes($column)?),*
                })
            }
            #[cfg(test)]
            fn sample(seed: &mut u8, optionals: bool) -> Self {
                let id = sample_id(seed);
                let account_id = sample_id(seed);
                let predecessor = sample_id(seed);
                let epoch_id = sample_id(seed);
                let signer_id = sample_id(seed);
                let removed_device_id = optionals.then(|| sample_id(seed));
                let retired_recovery_root_ids = sample_bytes(seed);
                let membership = sample_bytes(seed);
                let columns = AccountSuccessorColumns {
                    id, account_id, predecessor, epoch_id, signer_id,
                    action: if optionals { AccountAction::RemoveDevice } else { AccountAction::RetireRecoveryRoot },
                    removed_device_id, retired_recovery_root_ids, membership,
                };
                Self { columns, $($field: sample_bytes(seed)),* }
            }
        }
    };
}

successor_row! {
    PublicAccountSuccessor => PUBLIC_ACCOUNT_SUCCESSORS {
        revision: "revision",
        signature: "signature",
    }
}

successor_row! {
    AccountSuccessor => ACCOUNT_SUCCESSORS {
        revision: "revision",
        verification: "verification",
        history: "history",
        deliveries: "deliveries",
        signature: "signature",
    }
}

row! {
    RecoveryRoot => RECOVERY_ROOTS {
        account_id: id "accountId",
        signer_id: id "signerId",
        epoch_id: id "epochId",
        keys: keys,
        signature: bytes "signature",
    }
}

row! {
    RecoveryDelivery => RECOVERY_DELIVERIES {
        root_id: id "rootId",
        epoch_id: id "epochId",
        envelope: bytes "envelope",
    }
}

row! {
    AccountSealingKey => ACCOUNT_KEYS {
        account_id: id "accountId",
        epoch_id: id "epochId",
        signer_id: id "signerId",
        mechanism: text "mechanism",
        version: version "version",
        public_key: bytes "publicKey",
        signature: bytes "signature",
    }
}

row! {
    DeviceRemovalRequest => DEVICE_REMOVAL_REQUESTS {
        account_id: id "accountId",
        epoch_id: id "epochId",
        device_id: id "deviceId",
        signature: bytes "signature",
    }
}

row! {
    SpaceMember => SPACE_MEMBERS {
        scope_id: id "scopeId",
        identifier: id "identifier",
        account_id: id "accountId",
    }
}

row! {
    SpaceRoot => SPACE_ROOTS {
        scope_id: id "scopeId",
        identifier: id "identifier",
        creator_account_id: id "creatorAccountId",
        device_id: id "deviceId",
        account_epoch_id: id "accountEpochId",
        epoch_id: id "epochId",
        mechanism: text "mechanism",
        version: version "version",
        verification: bytes "verification",
        account_envelope: bytes "accountEnvelope",
        signature: bytes "signature",
    }
}

row! {
    SpaceSuccessor => SPACE_SUCCESSORS {
        scope_id: id "scopeId",
        identifier: id "identifier",
        predecessor: id "predecessor",
        history_epoch_id: id "historyEpochId",
        epoch_id: id "epochId",
        author_account_id: id "authorAccountId",
        author_device_id: id "authorDeviceId",
        author_epoch_id: id "authorEpochId",
        verification: bytes "verification",
        history: bytes "history",
        account_envelope: bytes "accountEnvelope",
        signature: bytes "signature",
    }
}

row! {
    Commitment => COMMITMENTS {
        account_id: id "accountId",
        subject_kind: subject_kind "subjectKind",
        subject_id: id "subjectId",
        mechanism: text "mechanism",
        version: version "version",
        commitment: bytes "commitment",
        signature: bytes "signature",
    }
}

row! {
    SpaceDelivery => SPACE_DELIVERIES {
        scope_id: id "scopeId",
        identifier: id "identifier",
        key_epoch_id: id "keyEpochId",
        sender_account_id: id "senderAccountId",
        recipient_account_id: id "recipientAccountId",
        recipient_account_epoch_id: id "recipientAccountEpochId",
        envelope: bytes "envelope",
    }
}

/// The codecs are the tables' column layout, as `E2EE_PROTOCOL.md` §1.2
/// states it: a renamed column, a changed type or a swapped pair of
/// columns would make every stored row of its table unreadable.
#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, block_on};
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::query::Query;
    use jazz::schema::JazzSchema;
    use jazz::tools::OpenTransactionId;
    use jazz::tools::SchemaBuilder;
    use jazz::tools::deployment::artifact::system_tables::with_e2ee_fragment;

    use super::*;

    /// A database with Jazz's system tables only, as its own authority.
    fn database() -> Db {
        let schema = with_e2ee_fragment(&SchemaBuilder::new().build());
        let db = block_on(Db::open(DbConfig::new(
            JazzSchema::new(&schema).unwrap(),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        jazz::db::block_on(db.set_authority()).unwrap();
        db
    }

    /// Writes `record` as its table's cells under its row ID, reads the row
    /// back and decodes it: the result is `record`.
    ///
    /// ```text
    /// record --cells()--> insert(TABLE, id) --commit--> read --decode()--> record
    /// ```
    fn round_trip<R: Row + PartialEq + Debug>(db: &Db, record: R) {
        block_on(async {
            let tx = OpenTransactionId::new();
            db.begin_serializable(tx).await.unwrap();
            db.tx_ref(tx)
                .insert(
                    R::TABLE,
                    record.cells().unwrap(),
                    InsertOptions {
                        row_id: Some(RowUuid(uuid::Uuid::parse_str(record.id()).unwrap())),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            db.commit_serializable_handle(tx).await.unwrap();

            let prepared = db.prepare_query(&Query::from(R::TABLE)).unwrap();
            let read = db
                .all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))
                .await
                .unwrap();
            let rows: Vec<R> = read
                .iter()
                .filter(|row| row.row_uuid().0.to_string() == record.id())
                .map(|row| R::decode(row).expect("the stored row decodes"))
                .collect();
            assert_eq!(rows, vec![record], "{}", R::TABLE);
        });
    }

    /// Samples generated by each table mapping round-trip with optional
    /// columns both present and absent.
    #[test]
    fn every_table_round_trips() {
        let db = database();
        macro_rules! sample {
            ($record:ty) => {
                let mut seed = 1;
                for optionals in [false, true] {
                    round_trip(&db, <$record as Row>::sample(&mut seed, optionals));
                }
            };
        }
        sample!(DeviceRequest);
        sample!(DeviceKeysRow);
        sample!(AccountIdentity);
        sample!(AccountRoot);
        sample!(DeviceChallenge);
        sample!(DeviceProof);
        sample!(DeviceApproval);
        sample!(DeviceDelivery);
        sample!(PublicDeviceApproval);
        sample!(PublicAccountSuccessor);
        sample!(AccountSuccessor);
        sample!(RecoveryRoot);
        sample!(RecoveryDelivery);
        sample!(AccountSealingKey);
        sample!(DeviceRemovalRequest);
        sample!(Commitment);
        sample!(SpaceMember);
        sample!(SpaceRoot);
        sample!(SpaceSuccessor);
        sample!(SpaceDelivery);
    }
}
