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
use jazz_e2ee::protocol::records::KeyColumns;
use jazz_e2ee::protocol::records::account::{
    AccountAction, AccountIdentity, AccountKind, AccountRoot, AccountSealingKey, AccountSuccessor,
    AccountSuccessorColumns, DeviceApproval, DeviceChallenge, DeviceDelivery, DeviceKeysRow,
    DeviceProof, DeviceRemovalRequest, DeviceRequest, PublicAccountSuccessor, PublicDeviceApproval,
    RecoveryDelivery, RecoveryRoot,
};
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
}

/// A UUID cell from its text form; `None` when it is not a UUID.
pub(crate) fn uuid_cell(id: &str) -> Option<Value> {
    uuid::Uuid::parse_str(id).ok().map(Value::Uuid)
}

/// What a decoder reads from a row.
struct Read<'r>(&'r CurrentRow);

fn unwrap_optional(value: Value) -> Option<Value> {
    match value {
        Value::Nullable(inner) => inner.map(|inner| *inner),
        value => Some(value),
    }
}

impl Read<'_> {
    fn row_id(&self) -> String {
        self.0.row_uuid().0.to_string()
    }

    fn optional(&self, column: &str) -> Option<Value> {
        self.0.cell(column).and_then(unwrap_optional)
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
#[derive(Default)]
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
    ACCOUNT_IDENTITIES, ACCOUNT_KEYS, ACCOUNT_ROOTS, ACCOUNT_SUCCESSORS, DEVICE_APPROVALS,
    DEVICE_CHALLENGES, DEVICE_DELIVERIES, DEVICE_KEYS, DEVICE_PROOFS, DEVICE_REMOVAL_REQUESTS,
    DEVICE_REQUESTS, MEMBERS as SPACE_MEMBERS, PUBLIC_ACCOUNT_SUCCESSORS, PUBLIC_DEVICE_APPROVALS,
    RECOVERY_DELIVERIES, RECOVERY_ROOTS, SPACE_DELIVERIES, SPACE_SUCCESSORS, SPACES as SPACE_ROOTS,
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

    /// A distinct UUID for each `n`.
    fn id(n: u8) -> String {
        uuid::Uuid::from_bytes([n; 16]).to_string()
    }

    /// Distinct bytes for each `n`.
    fn bytes(n: u8) -> Vec<u8> {
        vec![n, n.wrapping_add(1), n.wrapping_add(2)]
    }

    fn keys(n: u8) -> KeyColumns {
        KeyColumns {
            public_key: bytes(n),
            mechanism: format!("sealing-{n}"),
            version: u32::from(n) + 1000,
            signing_public_key: bytes(n + 1),
            signing_mechanism: format!("signing-{n}"),
            signing_version: u32::from(n) + 2000,
        }
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

    /// Every table's record, with every column set to a value no other
    /// column of the record holds, and each optional column both present
    /// and absent, survives being stored and read back.
    #[test]
    fn every_account_table_round_trips() {
        let db = database();
        round_trip(
            &db,
            DeviceRequest {
                id: id(1),
                keys: keys(2),
                nonce_commitment: bytes(4),
            },
        );
        round_trip(
            &db,
            DeviceKeysRow {
                id: id(5),
                device_id: id(6),
                keys: keys(7),
            },
        );
        round_trip(
            &db,
            AccountIdentity {
                id: id(10),
                ledger_version: 11,
                generation: 12,
                device_id: id(13),
                epoch_id: id(14),
                envelope: bytes(15),
                verification: bytes(16),
            },
        );
        for (row, kind, key, signature) in [
            (
                20,
                AccountKind::DeviceAccount,
                Some(bytes(21)),
                Some(bytes(22)),
            ),
            (23, AccountKind::App, None, None),
        ] {
            round_trip(
                &db,
                AccountRoot {
                    id: id(row),
                    ledger_version: 24,
                    generation: 25,
                    account_id: id(26),
                    device_id: id(27),
                    epoch_id: id(28),
                    kind,
                    device_account_public_key: key,
                    device_account_signature: signature,
                },
            );
        }
        round_trip(
            &db,
            DeviceChallenge {
                id: id(30),
                device_id: id(31),
                epoch_id: id(32),
                envelope: bytes(33),
                nonce: bytes(34),
                signer_id: id(35),
                signature: bytes(36),
            },
        );
        round_trip(
            &db,
            DeviceProof {
                id: id(40),
                challenge_id: id(41),
                proof: bytes(42),
                nonce: bytes(43),
                signature: bytes(44),
            },
        );
        round_trip(
            &db,
            DeviceApproval {
                id: id(50),
                challenge_id: id(51),
                signer_id: id(52),
                verification: bytes(53),
                signature: bytes(54),
            },
        );
        round_trip(
            &db,
            DeviceDelivery {
                id: id(60),
                challenge_id: id(61),
                envelope: bytes(62),
                verification: bytes(63),
            },
        );
        for (row, root, signature) in [(70, Some(id(71)), Some(bytes(72))), (73, None, None)] {
            round_trip(
                &db,
                PublicDeviceApproval {
                    id: id(row),
                    account_id: id(74),
                    epoch_id: id(75),
                    device_id: id(76),
                    signer_id: id(77),
                    signature: bytes(78),
                    recovery_root_id: root,
                    recovery_signature: signature,
                },
            );
        }
        for (row, action, removed) in [
            (80, AccountAction::RemoveDevice, Some(id(81))),
            (82, AccountAction::RetireRecoveryRoot, None),
        ] {
            round_trip(
                &db,
                PublicAccountSuccessor {
                    columns: AccountSuccessorColumns {
                        id: id(row),
                        account_id: id(83),
                        predecessor: id(84),
                        epoch_id: id(85),
                        signer_id: id(86),
                        action,
                        removed_device_id: removed.clone(),
                        retired_recovery_root_ids: bytes(87),
                        membership: bytes(88),
                    },
                    revision: bytes(89),
                    signature: bytes(90),
                },
            );
            round_trip(
                &db,
                AccountSuccessor {
                    columns: AccountSuccessorColumns {
                        id: id(row + 100),
                        account_id: id(91),
                        predecessor: id(92),
                        epoch_id: id(93),
                        signer_id: id(94),
                        action,
                        removed_device_id: removed,
                        retired_recovery_root_ids: bytes(95),
                        membership: bytes(96),
                    },
                    revision: bytes(97),
                    verification: bytes(98),
                    history: bytes(99),
                    deliveries: bytes(100),
                    signature: bytes(101),
                },
            );
        }
        round_trip(
            &db,
            RecoveryRoot {
                id: id(110),
                account_id: id(111),
                signer_id: id(112),
                epoch_id: id(113),
                keys: keys(114),
                signature: bytes(116),
            },
        );
        round_trip(
            &db,
            RecoveryDelivery {
                id: id(120),
                root_id: id(121),
                epoch_id: id(122),
                envelope: bytes(123),
            },
        );
        round_trip(
            &db,
            AccountSealingKey {
                id: id(130),
                account_id: id(131),
                epoch_id: id(132),
                signer_id: id(133),
                mechanism: "sealing-134".to_owned(),
                version: 135,
                public_key: bytes(136),
                signature: bytes(137),
            },
        );
        round_trip(
            &db,
            DeviceRemovalRequest {
                id: id(140),
                account_id: id(141),
                epoch_id: id(142),
                device_id: id(143),
                signature: bytes(144),
            },
        );
    }

    /// The space tables' records, as above.
    #[test]
    fn every_space_table_round_trips() {
        let db = database();
        round_trip(
            &db,
            SpaceMember {
                id: id(150),
                scope_id: id(151),
                identifier: id(152),
                account_id: id(153),
            },
        );
        round_trip(
            &db,
            SpaceRoot {
                id: id(160),
                scope_id: id(161),
                identifier: id(162),
                creator_account_id: id(163),
                device_id: id(164),
                account_epoch_id: id(165),
                epoch_id: id(166),
                mechanism: "wrap-167".to_owned(),
                version: 168,
                verification: bytes(169),
                account_envelope: bytes(170),
                signature: bytes(171),
            },
        );
        round_trip(
            &db,
            SpaceSuccessor {
                id: id(180),
                scope_id: id(181),
                identifier: id(182),
                predecessor: id(183),
                history_epoch_id: id(184),
                epoch_id: id(185),
                author_account_id: id(186),
                author_device_id: id(187),
                author_epoch_id: id(188),
                verification: bytes(189),
                history: bytes(190),
                account_envelope: bytes(191),
                signature: bytes(192),
            },
        );
        round_trip(
            &db,
            SpaceDelivery {
                id: id(200),
                scope_id: id(201),
                identifier: id(202),
                key_epoch_id: id(203),
                sender_account_id: id(204),
                recipient_account_id: id(205),
                recipient_account_epoch_id: id(206),
                envelope: bytes(207),
            },
        );
    }
}
