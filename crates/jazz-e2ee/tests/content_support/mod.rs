//! Shared pieces of the content tests: a world's held keys, column
//! declarations, a plaintext decoder and a cell cipher that can be switched
//! to fail.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jazz_e2ee::agility::{CellCipher, MechanismConfig, MechanismImpl};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::cell;
use jazz_e2ee::protocol::content::cells::{CellInput, SealedCell, seal_cells};
use jazz_e2ee::protocol::content::cells::{EqualityValueBuf, PlaintextValues};
use jazz_e2ee::protocol::content::{
    ContentResult, EncryptedColumn, HeldKeys, HeldSpace, RowTarget, WriteChoice, WriteKey,
};
use jazz_e2ee::protocol::domain::SharedDomain;
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::protocol::space::build::{self, Author};
use jazz_e2ee::protocol::space::{SpaceChain, acquire};
use jazz_e2ee::records::EqualityValue;
use jazz_e2ee::records::{EqualityColumn, EqualityColumnType};
use jazz_e2ee::registry::Registry;
use jazz_e2ee::{Error, RandomSource, Result, Zeroizing};

use crate::common::space::{SCOPE, World};

/// The encrypted table's catalogue identity and two of its rows.
pub const TABLE: &str = "7ab1e000-0000-5000-8000-0000000ab1e0";
pub const ROW_1: &str = "0a000000-0000-4000-8000-000000000001";
pub const ROW_2: &str = "0a000000-0000-4000-8000-000000000002";

pub const TITLE_TYPE: &[u8] = br#"{"column_type":"Text","nullable":false}"#;
pub const BLOB_TYPE: &[u8] = br#"{"column_type":"Bytea","nullable":false}"#;

/// An equality-indexed text column.
pub fn title() -> EncryptedColumn<'static> {
    EncryptedColumn {
        column_id: "c0000000-0000-5000-8000-00000000000a",
        cell_type: TITLE_TYPE,
        equality: Some(EqualityColumn::declare(EqualityColumnType::Text).unwrap()),
        is_bytes: false,
    }
}

/// A text column without an index.
pub fn notes() -> EncryptedColumn<'static> {
    EncryptedColumn {
        column_id: "c0000000-0000-5000-8000-00000000000b",
        cell_type: TITLE_TYPE,
        equality: None,
        is_bytes: false,
    }
}

/// A non-indexed bytes column: it may stream.
pub fn blob() -> EncryptedColumn<'static> {
    EncryptedColumn {
        column_id: "c0000000-0000-5000-8000-00000000000c",
        cell_type: BLOB_TYPE,
        equality: None,
        is_bytes: true,
    }
}

pub fn space(identifier: &str) -> SpaceAddress {
    SpaceAddress::new(SCOPE, identifier).unwrap()
}

pub fn shared(world: &World) -> SharedDomain {
    world.shared.domain.shared()
}

/// The chain of the space at `identifier` at the current cut.
pub fn chain_of(world: &World, identifier: &str) -> SpaceChain {
    let read = world.read_of(space(identifier));
    SpaceChain::replay(&read.space, &read.directory(world))
        .unwrap()
        .expect("a space root")
}

/// What `account`'s default device acquires of the space at `identifier`,
/// as held keys.
pub fn acquire_held(world: &World, account: &str, identifier: &str) -> HeldSpace {
    let read = world.read_of(space(identifier));
    let directory = read.directory(world);
    let chain = SpaceChain::replay(&read.space, &directory)
        .unwrap()
        .expect("a space root");
    let own = world.own(account);
    let acquisition = acquire(&chain, &read.space, &directory, &own.epoch_keys).unwrap();
    HeldSpace::from_acquisition(&chain, &acquisition.keys)
        .unwrap()
        .expect("a key")
}

/// Plaintext is the value's UTF-8 text for text columns and its bytes
/// otherwise; a stand-in for Jazz's native encoding.
pub struct Values;

impl PlaintextValues for Values {
    fn equality_value(
        &self,
        column: &EncryptedColumn<'_>,
        plaintext: &[u8],
    ) -> ContentResult<EqualityValueBuf> {
        Ok(if column.is_bytes {
            EqualityValueBuf::Bytes(plaintext.to_vec())
        } else {
            EqualityValueBuf::Text(
                String::from_utf8(plaintext.to_vec()).map_err(|_| Error::InvalidInput)?,
            )
        })
    }
}

pub const FLAKY_CELL: Mechanism<'static> = Mechanism::new("com.example.flaky-cell", 1);

/// The built-in cell cipher under [`FLAKY_CELL`]. With `fails` set,
/// decryption is a provider failure, as a host-provided mechanism's might
/// be.
pub struct FlakyCell {
    pub fails: Arc<AtomicBool>,
}

impl MechanismImpl for FlakyCell {
    fn mechanism(&self) -> Mechanism<'static> {
        FLAKY_CELL
    }
}

impl CellCipher for FlakyCell {
    fn encrypt(
        &self,
        key: &[u8; 32],
        context: &[u8],
        plaintext: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        let sealed = cell::Builtin.encrypt(key, context, plaintext, rng)?;
        envelope::encode(FLAKY_CELL, envelope::decode(cell::MECHANISM, &sealed)?)
    }

    fn decrypt(&self, key: &[u8; 32], context: &[u8], sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        if self.fails.load(Ordering::SeqCst) {
            return Err(Error::CryptoFailure);
        }
        let builtin = envelope::encode(cell::MECHANISM, envelope::decode(FLAKY_CELL, sealed)?)?;
        cell::Builtin.decrypt(key, context, &builtin)
    }
}

/// The built-ins plus [`FlakyCell`], its failure switch, and the
/// configuration that writes cells with it.
pub fn flaky_cells() -> (Registry, Arc<AtomicBool>, MechanismConfig) {
    let fails = Arc::new(AtomicBool::new(false));
    let mut registry = Registry::builtin();
    registry
        .register_cell_cipher(Arc::new(FlakyCell {
            fails: fails.clone(),
        }))
        .unwrap();
    let config = MechanismConfig {
        cell: FLAKY_CELL.into(),
        ..MechanismConfig::default()
    };
    (registry, fails, config)
}

/// Distinct randomness for every seal of a test binary.
static SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1000);

pub fn fresh_rng() -> crate::common::TestRandom {
    crate::common::TestRandom(SEED.fetch_add(1, Ordering::SeqCst))
}

/// The held keys `account`'s default device acquires of `identifier`.
pub fn held_of(world: &World, account: &str, identifier: &str) -> HeldKeys {
    let mut held = HeldKeys::new();
    held.hold(acquire_held(world, account, identifier)).unwrap();
    held
}

/// The key a write to `address` uses; panics without one.
pub fn write_key<'k>(held: &'k HeldKeys, address: &SpaceAddress) -> WriteKey<'k> {
    match held.write_key(address, false) {
        WriteChoice::Key(key) => key,
        other => panic!("expected a key, got {other:?}"),
    }
}

/// Seals text values of the given columns (an indexed column is tagged
/// with the text) in `target` under `key`.
pub fn seal_texts(
    world: &World,
    key: &WriteKey<'_>,
    target: &RowTarget<'_>,
    cells: &[(EncryptedColumn<'_>, &str)],
) -> Vec<SealedCell> {
    let suite = world
        .shared
        .registry
        .select(&MechanismConfig::default())
        .unwrap();
    let inputs: Vec<_> = cells
        .iter()
        .map(|(column, text)| CellInput {
            column: *column,
            value: text.as_bytes(),
            equality: column.equality.map(|_| EqualityValue::Text(text)),
        })
        .collect();
    seal_cells(&suite, key, target, &inputs, &mut fresh_rng())
        .unwrap()
        .cells
}

/// A root of the space at `identifier` by `account`'s default device, not
/// accepted, held as an unpublished creation.
pub fn own_creation(
    world: &World,
    account: &str,
    identifier: &str,
    transaction: &str,
) -> HeldSpace {
    let own = world.own(account);
    let read = world.read();
    let directory = read.directory(world);
    let device = world.accounts[account].device.clone();
    let author = Author::new(&directory, account, &own.key, &device).unwrap();
    let records = build::space_root(
        &world.writer(account),
        &space(identifier),
        &author,
        &mut fresh_rng(),
    )
    .unwrap();
    HeldSpace::from_creation(
        &world.shared.registry,
        &world.shared.domain,
        &records,
        transaction,
    )
    .unwrap()
}
