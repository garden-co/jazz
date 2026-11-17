//! Content under held keys (`E2EE_PROTOCOL.md` §4.1–§4.4, §5.1–§5.5):
//! sealing and opening cells and streams, which held key a write and a read
//! use, and how a read settles a version it does not hold.

mod common;
mod content_support;

use std::sync::atomic::Ordering;

use common::FixedRandom;
use common::TestRandom;
use common::space::*;
use content_support::*;
use jazz_e2ee::agility::MechanismAllowList;
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::{cell, equality};
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::cells::{
    CellInput, EqualityValueBuf, Opened, StreamOpen, StreamResealer, StreamValueReader,
    begin_stream, min_cell_record_len, open_cell, seal_cells, stream_routing,
};
use jazz_e2ee::protocol::content::convert::SourceIndex;
use jazz_e2ee::protocol::content::{
    CLEARED_MARKER, ContentError, CreationFate, DataError, HeldKeys, HeldSpace, KeyMark,
    MissingKey, RowTarget, TableTarget, WriteChoice, WriteKey,
};
use jazz_e2ee::protocol::domain::{AppDomain, SharedDomain};
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::protocol::space::build::{self, Author, RootRecords};
use jazz_e2ee::records::{CELL_RECORD, EqualityValue};
use jazz_e2ee::registry::Registry;
use jazz_e2ee::{Error, Zeroizing};

/// Alice's space at `ROW`, created and accepted, with Alice and Bob as
/// members; Bob holds no key yet.
fn world(seed: u64) -> World {
    let mut world = World::new(seed);
    world.account(ALICE);
    world.account(BOB);
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    world
}

fn held_of(world: &World, account: &str, identifier: &str) -> HeldKeys {
    let mut held = HeldKeys::new();
    held.hold(acquire_held(world, account, identifier)).unwrap();
    held
}

fn row<'a>(domain: &'a SharedDomain, address: &'a SpaceAddress, row_id: &'a str) -> RowTarget<'a> {
    RowTarget {
        domain,
        space: address,
        table_id: TABLE,
        row_id,
    }
}

fn write_key<'k>(held: &'k HeldKeys, address: &SpaceAddress) -> WriteKey<'k> {
    match held.write_key(address, false) {
        WriteChoice::Key(key) => key,
        other => panic!("expected a key, got {other:?}"),
    }
}

/// Distinct randomness for every seal.
static SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Seals `title = text` in `target` under `key`; returns the stored cell
/// and its token.
fn seal_title(
    world: &World,
    key: &WriteKey<'_>,
    target: &RowTarget<'_>,
    text: &str,
) -> (Vec<u8>, Vec<u8>) {
    let suite = world.shared.registry.select(&Default::default()).unwrap();
    let column = title();
    let sealed = seal_cells(
        &suite,
        key,
        target,
        &[CellInput {
            column,
            value: text.as_bytes(),
            equality: Some(EqualityValue::Text(text)),
        }],
        &mut TestRandom(SEED.fetch_add(1, Ordering::SeqCst)),
    )
    .unwrap();
    assert_eq!(sealed.epoch_id, key.epoch_id());
    let cell = sealed.cells.into_iter().next().unwrap();
    (cell.stored, cell.equality_token.unwrap())
}

fn value(opened: Opened) -> Vec<u8> {
    match opened {
        Opened::Value(value) => value.to_vec(),
        other => panic!("expected a value, got {other:?}"),
    }
}

/// A root of the space at `identifier` by Alice's default device, drawn
/// from `random` (its key, row ID and epoch come first), not accepted.
fn root_from(world: &World, identifier: &str, random: Vec<u8>) -> RootRecords {
    let own = world.own(ALICE);
    let read = world.read();
    let directory = read.directory(world);
    let device = world.accounts[ALICE].device.clone();
    let author = Author::new(&directory, ALICE, &own.key, &device).unwrap();
    build::space_root(
        &world.writer(ALICE),
        &space(identifier),
        &author,
        &mut FixedRandom(random),
    )
    .unwrap()
}

/// Random bytes for a root: its key, its row ID and its epoch, then
/// whatever the marker and the account envelope draw.
fn root_random(key: u8, id: u8, epoch: u8) -> Vec<u8> {
    let mut random = vec![key; 32];
    random.extend([id; 16]);
    random.extend([epoch; 16]);
    random.extend(TestRandom(u64::from(key) + 1).bytes(512));
    random
}

fn creation(world: &World, records: &RootRecords, transaction: &str) -> HeldSpace {
    HeldSpace::from_creation(
        &world.shared.registry,
        &world.shared.domain,
        records,
        transaction,
    )
    .unwrap()
}

#[test]
fn sealed_cells_open_with_the_held_key_of_their_version() {
    let world = world(1);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let key = write_key(&held, &address);
    let (stored, token) = seal_title(&world, &key, &target, "hello");
    // The equality envelope's header and a 32-byte token.
    assert_eq!(token.len(), 30 + 32);
    let registry = &world.shared.registry;
    assert_eq!(
        value(open_cell(registry, &held, &target, &title(), &stored, false).unwrap()),
        b"hello"
    );
    // Sealing twice gives different ciphertext of the same value.
    let (again, token_again) = seal_title(&world, &key, &target, "hello");
    assert_ne!(again, stored);
    assert_eq!(token_again, token);
}

#[test]
fn a_cell_record_frames_its_epoch_and_binds_the_cell_context() {
    // The context of §4.1 built by hand from the formula: ctx{application:
    // Dₛ, policy, scope, identifier, table, row, column, epoch} framed with
    // the cell type; the equality context without the row.
    fn context(fields: [&str; 9]) -> Vec<u8> {
        let mut out = b"JE2C\x01".to_vec();
        for field in fields {
            out.extend((field.len() as u32).to_be_bytes());
            out.extend(field.as_bytes());
        }
        out
    }
    fn frame(parts: &[&[u8]]) -> Vec<u8> {
        parts
            .iter()
            .flat_map(|part| [(part.len() as u32).to_be_bytes().to_vec(), part.to_vec()])
            .flatten()
            .collect()
    }
    let world = world(2);
    let records = root_from(&world, ROW, root_random(1, 2, 3));
    let mut held = HeldKeys::new();
    held.hold(creation(&world, &records, "tx-1")).unwrap();
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let key = write_key(&held, &address);
    let (stored, token) = seal_title(&world, &key, &target, "hello");

    let epoch = records.root.epoch_id.as_str();
    let header = CELL_RECORD.header().unwrap();
    assert_eq!(&stored[..header.len()], header.as_slice());
    assert_eq!(&stored[header.len()..header.len() + 36], epoch.as_bytes());
    let inner = &stored[header.len() + 36..];
    let column = title();
    let cell_context = frame(&[
        &context([
            domain.as_str(),
            "jazz.e2ee.cell-record.v1",
            SCOPE,
            ROW,
            TABLE,
            ROW_1,
            column.column_id,
            epoch,
            "",
        ]),
        TITLE_TYPE,
    ]);
    assert_eq!(
        cell::decrypt(&records.key, &cell_context, inner)
            .unwrap()
            .as_slice(),
        b"hello"
    );
    let equality_context = frame(&[
        &context([
            domain.as_str(),
            "jazz.e2ee.equality.v1",
            SCOPE,
            ROW,
            TABLE,
            "",
            column.column_id,
            epoch,
            "",
        ]),
        TITLE_TYPE,
    ]);
    assert_eq!(
        token,
        equality::token(&records.key, &equality_context, b"\x01hello").unwrap()
    );
    assert_eq!(min_cell_record_len(), header.len() + 47);
    assert_eq!(min_cell_record_len(), 78);
    assert_eq!(CLEARED_MARKER, [0x4a, 0x45, 0x32, 0x58, 0x01]);
}

#[test]
fn a_cell_opens_only_at_its_own_coordinates() {
    let world = world(3);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let (stored, _) = seal_title(&world, &write_key(&held, &address), &target, "hello");
    let registry = &world.shared.registry;
    let invalid = Opened::Unreadable(DataError::InvalidCiphertext);
    let open = |target: &RowTarget<'_>, column| {
        open_cell(registry, &held, target, &column, &stored, false).unwrap()
    };
    assert_eq!(open(&row(&domain, &address, ROW_2), title()), invalid);
    let other_table = RowTarget {
        table_id: "7ab1e000-0000-5000-8000-0000000ab1e1",
        ..target
    };
    assert_eq!(open(&other_table, title()), invalid);
    assert_eq!(open(&target, notes()), invalid, "another column");
    let mut retyped = title();
    retyped.cell_type = br#"{"column_type":"Text","nullable":true}"#;
    assert_eq!(open(&target, retyped), invalid, "another sealed type");
    let other_app = AppDomain::new("0b000000-0000-4000-8000-000000000000", None)
        .unwrap()
        .shared();
    assert_eq!(
        open(&row(&other_app, &address, ROW_1), title()),
        invalid,
        "another application"
    );
    let production = AppDomain::new(world.shared.domain.app_id(), Some("prod"))
        .unwrap()
        .shared();
    assert_eq!(
        open(&row(&production, &address, ROW_1), title()),
        invalid,
        "another environment"
    );
    // A flipped bit anywhere after the epoch fails authentication.
    let mut rng = TestRandom(3);
    for _ in 0..32 {
        let mut damaged = stored.clone();
        let at = 31 + 36 + rng.below(stored.len() - 31 - 36);
        damaged[at] ^= 1 << rng.below(8);
        let opened = open_cell(registry, &held, &target, &title(), &damaged, false).unwrap();
        assert!(
            matches!(opened, Opened::Unreadable(_)),
            "{opened:?} at byte {at}"
        );
    }
}

#[test]
fn short_values_have_fixed_meanings() {
    let world = world(4);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let registry = &world.shared.registry;
    let open = |stored: &[u8], converting| {
        open_cell(registry, &held, &target, &title(), stored, converting).unwrap()
    };
    assert_eq!(open(b"", false), Opened::Empty);
    assert_eq!(open(b"", false).report(), Some(DataError::EmptyCell));
    assert!(!open(b"", false).leaves_row_out());
    assert_eq!(open(&CLEARED_MARKER, true), Opened::Cleared);
    assert_eq!(open(&CLEARED_MARKER, true).report(), None);
    assert_eq!(
        open(&CLEARED_MARKER, false),
        Opened::Unreadable(DataError::Superseded)
    );
    // Every other value shorter than a cell record is invalid, even a
    // well-formed envelope of a mechanism nobody routes.
    let unknown = envelope::encode(Mechanism::new("com.example.short", 1), &[0; 30]).unwrap();
    assert!(unknown.len() < 78);
    let short_cell = envelope::encode(CELL_RECORD, &[0x30; 46]).unwrap();
    assert_eq!(short_cell.len(), 77);
    for stored in [
        &b"\x00"[..],
        &CLEARED_MARKER[..4],
        b"JE2X\x02",
        &unknown,
        &short_cell,
    ] {
        assert_eq!(
            open(stored, true),
            Opened::Unreadable(DataError::InvalidCiphertext),
            "{stored:02x?}"
        );
    }
    // From the shortest record length on, a value of an unknown mechanism
    // is unsupported.
    let long_unknown = envelope::encode(Mechanism::new("com.example.short", 1), &[0; 60]).unwrap();
    assert!(long_unknown.len() >= 78);
    assert_eq!(
        open(&long_unknown, false),
        Opened::Unreadable(DataError::UnsupportedFormat)
    );
}

/// The shortest record length is the boundary itself: one byte short of
/// it, even a well-formed envelope of a mechanism nobody routes is
/// invalid; at it, the same envelope is unsupported.
#[test]
fn the_shortest_record_length_is_the_boundary_for_any_mechanism() {
    let world = world(4);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let registry = &world.shared.registry;
    let unknown = Mechanism::new("com.example.short", 1);
    let header = envelope::encode(unknown, &[]).unwrap().len();
    let below = envelope::encode(unknown, &vec![0; 77 - header]).unwrap();
    let at = envelope::encode(unknown, &vec![0; 78 - header]).unwrap();
    assert_eq!((below.len(), at.len()), (77, 78));
    assert_eq!(
        open_cell(registry, &held, &target, &title(), &below, false).unwrap(),
        Opened::Unreadable(DataError::InvalidCiphertext)
    );
    assert_eq!(
        open_cell(registry, &held, &target, &title(), &at, false).unwrap(),
        Opened::Unreadable(DataError::UnsupportedFormat)
    );
}

#[test]
fn an_unrouted_cell_mechanism_is_unsupported_not_invalid() {
    let world = world(5);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let (stored, _) = seal_title(&world, &write_key(&held, &address), &target, "hello");
    let header = CELL_RECORD.header().unwrap().len() + 36;
    // The inner envelope relabelled to a mechanism nobody registers.
    let inner = envelope::decode(cell::MECHANISM, &stored[header..]).unwrap();
    let relabelled = [
        &stored[..header],
        &envelope::encode(Mechanism::new("com.example.cell", 1), inner).unwrap(),
    ]
    .concat();
    let relabelled = envelope::encode(
        CELL_RECORD,
        &relabelled[CELL_RECORD.header().unwrap().len()..],
    )
    .unwrap();
    let registry = &world.shared.registry;
    assert_eq!(
        open_cell(registry, &held, &target, &title(), &relabelled, false).unwrap(),
        Opened::Unreadable(DataError::UnsupportedFormat)
    );
    // A built-in mechanism the application's allow-list leaves out.
    let retired = Registry::builtin()
        .allowing(&MechanismAllowList {
            cell: Vec::new(),
            ..MechanismAllowList::default()
        })
        .unwrap();
    assert_eq!(
        open_cell(&retired, &held, &target, &title(), &stored, false).unwrap(),
        Opened::Unreadable(DataError::UnsupportedFormat)
    );
    // A strict read fails on it.
    assert_eq!(
        Opened::Unreadable(DataError::UnsupportedFormat).strict(),
        Err(ContentError::Data(DataError::UnsupportedFormat))
    );
}

#[test]
fn a_provider_failure_fails_the_read() {
    let (registry, fails, config) = flaky_cells();
    let mut world = World::with_registry(6, registry);
    world.account(ALICE);
    world.add_member(ALICE);
    world.create(ALICE);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let registry = &world.shared.registry;
    let suite = registry.select(&config).unwrap();
    let stored = seal_cells(
        &suite,
        &write_key(&held, &address),
        &target,
        &[CellInput {
            column: notes(),
            value: b"hello",
            equality: None,
        }],
        &mut TestRandom(6),
    )
    .unwrap()
    .cells
    .remove(0)
    .stored;
    assert_eq!(
        value(open_cell(registry, &held, &target, &notes(), &stored, false).unwrap()),
        b"hello"
    );
    fails.store(true, Ordering::SeqCst);
    let error = open_cell(registry, &held, &target, &notes(), &stored, false).unwrap_err();
    assert!(error.is_provider_failure(), "{error:?}");
    // Also when the key is another space's (a stale-space candidate).
    let elsewhere_address = space(OTHER_ROW);
    let error = open_cell(
        registry,
        &held,
        &row(&domain, &elsewhere_address, ROW_1),
        &notes(),
        &stored,
        false,
    )
    .unwrap_err();
    assert!(error.is_provider_failure(), "{error:?}");
}

#[test]
fn a_missing_version_settles_by_membership_and_chain() {
    let mut world = world(7);
    let alice = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let (stored, _) = seal_title(&world, &write_key(&alice, &address), &target, "hello");
    let registry = &world.shared.registry.clone();
    let epoch = world.current();

    // Bob holds nothing: the version is missing until he acquires it.
    let mut bob = HeldKeys::new();
    let opened = open_cell(registry, &bob, &target, &title(), &stored, false).unwrap();
    assert_eq!(
        opened,
        Opened::Missing {
            epoch_id: epoch.clone()
        }
    );
    assert!(opened.leaves_row_out());
    assert_eq!(opened.clone().strict(), Ok(opened));
    let chain = chain_of(&world, ROW);
    assert_eq!(
        bob.settle_missing(&address, &epoch, Some(&chain), true),
        Ok(MissingKey::Report(DataError::KeyPending))
    );
    assert_eq!(
        bob.settle_missing(&address, &epoch, Some(&chain), false),
        Ok(MissingKey::Report(DataError::KeyUnavailable))
    );
    assert_eq!(
        bob.settle_missing(&address, &epoch, None, true),
        Ok(MissingKey::Report(DataError::SpacePending))
    );
    assert_eq!(
        bob.settle_missing(&address, &epoch, None, false),
        Ok(MissingKey::Report(DataError::KeyUnavailable))
    );
    assert!(DataError::KeyPending.is_per_space() && DataError::SpacePending.is_per_space());
    assert!(!DataError::KeyUnavailable.is_per_space());
    // A chain of another space is not evidence for this one.
    let other_world = {
        let mut other = World::new(8);
        other.account(ALICE);
        other.add_member_of(OTHER_ROW, ALICE);
        other.create_of(OTHER_ROW, ALICE);
        other
    };
    assert_eq!(
        bob.settle_missing(
            &address,
            &epoch,
            Some(&chain_of(&other_world, OTHER_ROW)),
            true
        ),
        Err(ProtocolError::InconsistentEvidence)
    );

    // After a delivery and an acquisition the version is held.
    world.deliver(ALICE, BOB, &epoch);
    bob.hold(acquire_held(&world, BOB, ROW)).unwrap();
    assert_eq!(
        bob.settle_missing(&address, &epoch, Some(&chain_of(&world, ROW)), true),
        Ok(MissingKey::Held)
    );
    assert_eq!(
        value(open_cell(registry, &bob, &target, &title(), &stored, false).unwrap()),
        b"hello"
    );
}

#[test]
fn an_epoch_outside_the_chain_is_invalid_until_the_chain_advances() {
    let mut world = world(9);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    // Alice's second root of the address is never accepted: cells under its
    // key name an epoch outside the chain.
    let lost = root_from(&world, ROW, root_random(4, 5, 6));
    let mut alice = HeldKeys::new();
    alice.hold(creation(&world, &lost, "tx-lost")).unwrap();
    let (stored, _) = seal_title(&world, &write_key(&alice, &address), &target, "lost");
    let registry = &world.shared.registry.clone();

    let mut bob = HeldKeys::new();
    let lost_epoch = lost.root.epoch_id.clone();
    let opened = open_cell(registry, &bob, &target, &title(), &stored, false).unwrap();
    assert_eq!(
        opened,
        Opened::Missing {
            epoch_id: lost_epoch.clone()
        }
    );
    let chain = chain_of(&world, ROW);
    assert_eq!(
        bob.settle_missing(&address, &lost_epoch, Some(&chain), true),
        Ok(MissingKey::Report(DataError::InvalidCiphertext))
    );
    assert!(bob.known(&address).unwrap().is_absent(&lost_epoch));
    // Remembered: the next read reports it without acquiring again.
    assert_eq!(
        open_cell(registry, &bob, &target, &title(), &stored, false).unwrap(),
        Opened::Unreadable(DataError::InvalidCiphertext)
    );
    // The same chain read again changes nothing.
    bob.observe_chain(&chain);
    assert!(bob.known(&address).unwrap().is_absent(&lost_epoch));
    // Once the chain advances, the epoch is asked about again.
    world.rotate(ALICE);
    bob.observe_chain(&chain_of(&world, ROW));
    assert!(!bob.known(&address).unwrap().is_absent(&lost_epoch));
    assert!(matches!(
        open_cell(registry, &bob, &target, &title(), &stored, false).unwrap(),
        Opened::Missing { .. }
    ));
    // Alice's own key, refused or not, still opens what she wrote under it.
    alice
        .space_mut(&lost.root.id)
        .unwrap()
        .settle(CreationFate::NotAccepted, None);
    assert_eq!(
        value(open_cell(registry, &alice, &target, &title(), &stored, false).unwrap()),
        b"lost"
    );
}

#[test]
fn a_cell_of_another_space_this_device_holds_is_stale() {
    let mut world = world(10);
    world.add_member_of(OTHER_ROW, ALICE);
    world.create_of(OTHER_ROW, ALICE);
    let mut held = held_of(&world, ALICE, ROW);
    held.hold(acquire_held(&world, ALICE, OTHER_ROW)).unwrap();
    let domain = shared(&world);
    let old = space(ROW);
    let new = space(OTHER_ROW);
    let (stored, _) = seal_title(
        &world,
        &write_key(&held, &old),
        &row(&domain, &old, ROW_1),
        "moved",
    );
    let opened = open_cell(
        &world.shared.registry,
        &held,
        &row(&domain, &new, ROW_1),
        &title(),
        &stored,
        false,
    )
    .unwrap();
    assert_eq!(opened.report(), Some(DataError::StaleSpace));
    assert!(!opened.leaves_row_out());
    match opened {
        Opened::StaleSpace { value, space } => {
            assert_eq!(value.as_slice(), b"moved");
            assert_eq!(space, old);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn another_spaces_key_of_the_same_epoch_never_decides_a_cell() {
    let world = world(11);
    // Two roots, of two addresses, with one epoch and different keys.
    let ours = root_from(&world, ROW, root_random(1, 2, 9));
    let theirs = root_from(&world, OTHER_ROW, root_random(3, 4, 9));
    assert_eq!(ours.root.epoch_id, theirs.root.epoch_id);
    let domain = shared(&world);
    let address = space(OTHER_ROW);
    let target = row(&domain, &address, ROW_1);
    let mut writer = HeldKeys::new();
    writer.hold(creation(&world, &theirs, "tx-theirs")).unwrap();
    let (stored, _) = seal_title(&world, &write_key(&writer, &address), &target, "theirs");
    // A reader holding only the other address's key: the cell is missing,
    // not invalid and not stale.
    let mut reader = HeldKeys::new();
    reader.hold(creation(&world, &ours, "tx-ours")).unwrap();
    assert_eq!(
        open_cell(
            &world.shared.registry,
            &reader,
            &target,
            &title(),
            &stored,
            false
        )
        .unwrap(),
        Opened::Missing {
            epoch_id: theirs.root.epoch_id.clone()
        }
    );
}

#[test]
fn a_wrong_key_of_the_cells_epoch_does_not_block_the_right_one() {
    let world = world(12);
    // Two roots of one address with one epoch: the one with the smaller
    // row ID, tried first, does not authenticate the cell.
    let first = root_from(&world, ROW, root_random(1, 0x11, 9));
    let second = root_from(&world, ROW, root_random(2, 0x22, 9));
    assert!(first.root.id < second.root.id);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let mut writer = HeldKeys::new();
    writer.hold(creation(&world, &second, "tx-2")).unwrap();
    let (stored, _) = seal_title(&world, &write_key(&writer, &address), &target, "right");
    let mut reader = HeldKeys::new();
    reader.hold(creation(&world, &first, "tx-1")).unwrap();
    reader.hold(creation(&world, &second, "tx-2")).unwrap();
    assert_eq!(
        value(
            open_cell(
                &world.shared.registry,
                &reader,
                &target,
                &title(),
                &stored,
                false
            )
            .unwrap()
        ),
        b"right"
    );
    // With only the wrong key the cell does not open.
    let mut wrong = HeldKeys::new();
    wrong.hold(creation(&world, &first, "tx-1")).unwrap();
    assert_eq!(
        open_cell(
            &world.shared.registry,
            &wrong,
            &target,
            &title(),
            &stored,
            false
        )
        .unwrap(),
        Opened::Unreadable(DataError::InvalidCiphertext)
    );
}

/// Random bytes for a root with the given epoch, as [`root_random`].
fn root_random_with_epoch(key: u8, id: u8, epoch_id: &str) -> Vec<u8> {
    let hex: String = epoch_id.chars().filter(|c| *c != '-').collect();
    let epoch = (0..16).map(|at| u8::from_str_radix(&hex[2 * at..2 * at + 2], 16).unwrap());
    let mut random = vec![key; 32];
    random.extend([id; 16]);
    random.extend(epoch);
    random.extend(TestRandom(u64::from(key) + 1).bytes(512));
    random
}

/// A refused creation whose epoch the space's real root also names (an
/// updater may reuse a public epoch ID): the refused key still opens this
/// device's own cells, but decides nothing about the space's: their key is
/// missing, not held, and is acquired; once held, it decides.
#[test]
fn a_refused_roots_key_never_decides_the_spaces_cells() {
    let world = world(25);
    let address = space(ROW);
    let chain = chain_of(&world, ROW);
    let epoch = chain.root.row.epoch_id.clone();
    let lost = root_from(&world, ROW, root_random_with_epoch(7, 8, &epoch));
    assert_eq!(lost.root.epoch_id, epoch);
    assert_ne!(lost.root.id, chain.root.row.id);
    let domain = shared(&world);
    let target = row(&domain, &address, ROW_1);
    let real = held_of(&world, ALICE, ROW);
    let (theirs, _) = seal_title(&world, &write_key(&real, &address), &target, "theirs");
    let stream = seal_stream(
        &world,
        &write_key(&real, &address),
        &target,
        b"a streamed value",
        64,
    );
    let mut device = HeldKeys::new();
    device.hold(creation(&world, &lost, "tx-lost")).unwrap();
    let (mine, _) = seal_title(&world, &write_key(&device, &address), &target, "mine");
    device
        .space_mut(&lost.root.id)
        .unwrap()
        .settle(CreationFate::NotAccepted, None);
    let registry = &world.shared.registry;
    let open = |held: &HeldKeys, stored: &[u8]| {
        open_cell(registry, held, &target, &title(), stored, false).unwrap()
    };
    let missing = Opened::Missing {
        epoch_id: epoch.clone(),
    };
    assert_eq!(value(open(&device, &mine)), b"mine");
    assert_eq!(open(&device, &theirs), missing);
    assert!(!device.holds(&address, &epoch));
    assert!(matches!(
        StreamValueReader::open(registry, &device, &target, &blob(), &stream).unwrap(),
        StreamOpen::Unreadable(Opened::Missing { epoch_id }) if epoch_id == epoch
    ));
    // The acquisition's read names the real root: the key is pending, not
    // held, and the cell stays missing.
    assert_eq!(
        device
            .settle_missing(&address, &epoch, Some(&chain), true)
            .unwrap(),
        MissingKey::Report(DataError::KeyPending)
    );
    assert_eq!(open(&device, &theirs), missing);
    // Once the real key is held it decides, and the refused key still
    // opens this device's own cells.
    device.hold(acquire_held(&world, ALICE, ROW)).unwrap();
    assert!(device.holds(&address, &epoch));
    assert_eq!(value(open(&device, &theirs)), b"theirs");
    assert_eq!(value(open(&device, &mine)), b"mine");
    let mut damaged = theirs.clone();
    *damaged.last_mut().unwrap() ^= 1;
    assert_eq!(
        open(&device, &damaged),
        Opened::Unreadable(DataError::InvalidCiphertext)
    );
}

#[test]
fn a_write_uses_the_deepest_key_and_old_cells_stay_readable() {
    let mut world = world(13);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let before = held_of(&world, ALICE, ROW);
    let (old, _) = seal_title(&world, &write_key(&before, &address), &target, "old");
    let first = world.current();
    let second = world.rotate(ALICE);
    let after = held_of(&world, ALICE, ROW);
    let key = write_key(&after, &address);
    assert_eq!((key.epoch_id(), key.depth()), (second.as_str(), 1));
    assert_eq!(key.mark(), &KeyMark::Published);
    let (new, _) = seal_title(&world, &key, &target, "new");
    let registry = &world.shared.registry;
    assert_eq!(
        value(open_cell(registry, &after, &target, &title(), &old, false).unwrap()),
        b"old"
    );
    assert_eq!(
        value(open_cell(registry, &after, &target, &title(), &new, false).unwrap()),
        b"new"
    );
    // A device that has not obtained the newest version writes with the
    // newest it holds.
    assert_eq!(write_key(&before, &address).epoch_id(), first);
}

#[test]
fn an_unpublished_key_never_wins_a_tie_and_a_refused_key_never_writes() {
    let world = world(14);
    let address = space(ROW);
    let lost = root_from(&world, ROW, root_random(4, 5, 6));
    let mut held = HeldKeys::new();
    held.hold(creation(&world, &lost, "tx-lost")).unwrap();
    assert!(held.holds_unpublished(&address));
    // Alone, the unpublished key writes.
    assert_eq!(write_key(&held, &address).root_id(), lost.root.id);
    // The space's root at the same depth wins, whichever root ID sorts
    // first.
    held.hold(acquire_held(&world, ALICE, ROW)).unwrap();
    let space_root = chain_of(&world, ROW).root.row.id.clone();
    assert_eq!(write_key(&held, &address).root_id(), space_root);
    // Refused: never chosen, alone or not.
    held.space_mut(&lost.root.id)
        .unwrap()
        .settle(CreationFate::Accepted, Some(&space_root));
    assert!(!held.holds_unpublished(&address));
    assert_eq!(
        held.space(&lost.root.id).unwrap().mark(0),
        Some(&KeyMark::Refused)
    );
    let mut only_refused = HeldKeys::new();
    let mut refused = creation(&world, &lost, "tx-lost");
    refused.settle(CreationFate::NotAccepted, None);
    only_refused.hold(refused).unwrap();
    assert!(matches!(
        only_refused.write_key(&address, false),
        WriteChoice::NeedsKey
    ));
    // After this device's creation was refused in this run, and while no
    // valid root is known, the write is space-pending.
    assert!(matches!(
        only_refused.write_key(&address, true),
        WriteChoice::SpacePending
    ));
    only_refused.observe_chain(&chain_of(&world, ROW));
    assert!(matches!(
        only_refused.write_key(&address, true),
        WriteChoice::NeedsKey
    ));
}

#[test]
fn a_creation_settles_by_its_fate_and_the_spaces_root() {
    let world = world(15);
    let records = root_from(&world, ROW, root_random(4, 5, 6));
    let own = records.root.id.clone();
    let other = chain_of(&world, ROW).root.row.id.clone();
    let unpublished = KeyMark::Unpublished {
        transaction: "tx".into(),
    };
    let cases = [
        (
            CreationFate::Accepted,
            Some(own.as_str()),
            KeyMark::Published,
        ),
        (
            CreationFate::Accepted,
            Some(other.as_str()),
            KeyMark::Refused,
        ),
        (CreationFate::Accepted, None, unpublished.clone()),
        (CreationFate::NotAccepted, None, KeyMark::Refused),
        (
            CreationFate::NotAccepted,
            Some(own.as_str()),
            KeyMark::Refused,
        ),
        (
            CreationFate::Undecided,
            Some(own.as_str()),
            unpublished.clone(),
        ),
        (
            CreationFate::Undecided,
            Some(other.as_str()),
            unpublished.clone(),
        ),
        (
            CreationFate::UnknownToStore,
            Some(own.as_str()),
            KeyMark::Published,
        ),
        (
            CreationFate::UnknownToStore,
            Some(other.as_str()),
            KeyMark::Refused,
        ),
        (CreationFate::UnknownToStore, None, unpublished.clone()),
    ];
    for (fate, root, expected) in cases {
        let mut space = creation(&world, &records, "tx");
        space.settle(fate, root);
        assert_eq!(space.mark(0), Some(&expected), "{fate:?} {root:?}");
    }
    // Refused and published marks are final.
    let mut space = creation(&world, &records, "tx");
    space.settle(CreationFate::NotAccepted, None);
    space.settle(CreationFate::Accepted, Some(&own));
    assert_eq!(space.mark(0), Some(&KeyMark::Refused));
    let mut space = creation(&world, &records, "tx");
    space.settle(CreationFate::Accepted, Some(&own));
    space.settle(CreationFate::NotAccepted, None);
    assert_eq!(space.mark(0), Some(&KeyMark::Published));
}

#[test]
fn keeping_a_key_replaces_its_record_but_a_refusal_is_final() {
    let world = world(16);
    let records = root_from(&world, ROW, root_random(4, 5, 6));
    let mut held = creation(&world, &records, "tx");
    held.settle(CreationFate::NotAccepted, None);
    // Holding the same key again, unpublished, leaves it refused.
    held.keep(creation(&world, &records, "tx-again")).unwrap();
    assert_eq!(held.mark(0), Some(&KeyMark::Refused));
    // An unpublished key is replaced by a later record of it.
    let mut pending = creation(&world, &records, "tx");
    pending.keep(creation(&world, &records, "tx-2")).unwrap();
    assert_eq!(
        pending.mark(0),
        Some(&KeyMark::Unpublished {
            transaction: "tx-2".into()
        })
    );
    // Another root is never kept with this one.
    let other = root_from(&world, ROW, root_random(7, 8, 9));
    assert_eq!(
        pending.keep(creation(&world, &other, "tx")),
        Err(ProtocolError::InconsistentEvidence)
    );
}

#[test]
fn keeping_a_deeper_holding_extends_the_lineage() {
    let mut world = world(17);
    let address = space(ROW);
    let mut held = held_of(&world, ALICE, ROW);
    let root = chain_of(&world, ROW).root.row.id.clone();
    world.rotate(ALICE);
    world.rotate(ALICE);
    held.hold(acquire_held(&world, ALICE, ROW)).unwrap();
    let space = held.space(&root).unwrap();
    assert_eq!(space.successors().len(), 2);
    assert_eq!(space.held().count(), 3);
    assert_eq!(write_key(&held, &address).depth(), 2);
    // A shorter holding of the same root keeps the longer lineage.
    let mut shorter = World::new(17);
    shorter.account(ALICE);
    shorter.account(BOB);
    shorter.add_member(ALICE);
    shorter.add_member(BOB);
    shorter.create(ALICE);
    held.hold(acquire_held(&shorter, ALICE, ROW)).unwrap();
    assert_eq!(held.space(&root).unwrap().successors().len(), 2);
}

#[test]
fn a_creation_key_must_pass_its_roots_marker() {
    let world = world(18);
    let mut records = root_from(&world, ROW, root_random(4, 5, 6));
    assert!(matches!(
        HeldSpace::from_creation(&world.shared.registry, &world.shared.domain, &records, ""),
        Err(ProtocolError::InvalidRecord(_))
    ));
    records.key = Zeroizing::new([9; 32]);
    assert!(matches!(
        HeldSpace::from_creation(&world.shared.registry, &world.shared.domain, &records, "tx"),
        Err(ProtocolError::InvalidRecord(_))
    ));
}

#[test]
fn sealing_refuses_mixed_or_incomplete_inputs() {
    let mut world = world(19);
    world.add_member_of(OTHER_ROW, ALICE);
    world.create_of(OTHER_ROW, ALICE);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let other = space(OTHER_ROW);
    let key = write_key(&held, &address);
    let suite = world.shared.registry.select(&Default::default()).unwrap();
    let seal = |target: &RowTarget<'_>, cells: &[CellInput<'_>]| {
        seal_cells(&suite, &key, target, cells, &mut TestRandom(1))
    };
    let cell = |column, equality| CellInput {
        column,
        value: b"v",
        equality,
    };
    let target = row(&domain, &address, ROW_1);
    assert_eq!(
        seal(&row(&domain, &other, ROW_1), &[cell(notes(), None)]),
        Err(ContentError::Protocol(ProtocolError::InconsistentEvidence))
    );
    let invalid = Err(ContentError::Protocol(ProtocolError::Crypto(
        Error::InvalidInput,
    )));
    assert_eq!(seal(&target, &[cell(title(), None)]), invalid);
    assert_eq!(
        seal(&target, &[cell(notes(), Some(EqualityValue::Text("v")))]),
        invalid
    );
    assert_eq!(
        seal(&target, &[cell(title(), Some(EqualityValue::Int(1)))]),
        invalid
    );
    assert_eq!(
        seal(&target, &[cell(notes(), None), cell(notes(), None)]),
        invalid
    );
    let bad_row = row(&domain, &address, "not-a-uuid");
    assert_eq!(seal(&bad_row, &[cell(notes(), None)]), invalid);
    assert!(seal(&target, &[cell(notes(), None), cell(blob(), None)]).is_ok());
}

/// A stream of `plaintext` for `column` in `target` under `key`, fed in
/// chunks of `chunk` bytes.
fn seal_stream(
    world: &World,
    key: &WriteKey<'_>,
    target: &RowTarget<'_>,
    plaintext: &[u8],
    chunk: usize,
) -> Vec<u8> {
    let suite = world.shared.registry.select(&Default::default()).unwrap();
    let (mut writer, mut out) =
        begin_stream(&suite, key, target, &blob(), &mut TestRandom(3)).unwrap();
    for part in plaintext.chunks(chunk.max(1)) {
        writer.update(part, &mut out).unwrap();
    }
    writer.finish(&mut out).unwrap();
    out
}

/// Reads a stored stream through [`StreamValueReader`] in chunks.
fn read_stream(
    registry: &Registry,
    held: &HeldKeys,
    target: &RowTarget<'_>,
    stored: &[u8],
    chunk: usize,
) -> Result<Vec<u8>, ContentError> {
    let StreamOpen::Reader {
        mut reader,
        initial,
    } = StreamValueReader::open(registry, held, target, &blob(), &stored[..69])?
    else {
        panic!("a reader");
    };
    assert!(initial.is_empty(), "the routing alone holds no record");
    let mut out = Vec::new();
    for part in stored[69..].chunks(chunk) {
        reader.update(part, &mut out)?;
    }
    reader.finish()?;
    Ok(out)
}

#[test]
fn large_values_stream_in_bytes_columns_only() {
    let world = world(20);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let key = write_key(&held, &address);
    let suite = world.shared.registry.select(&Default::default()).unwrap();
    for column in [title(), notes()] {
        assert!(matches!(
            begin_stream(&suite, &key, &target, &column, &mut TestRandom(1)),
            Err(ContentError::NotStreamable)
        ));
    }
    let registry = &world.shared.registry;
    let plaintext = TestRandom(20).bytes(200_000);
    let stored = seal_stream(&world, &key, &target, &plaintext, 10_000);
    assert_eq!(
        stream_routing(&stored).unwrap().epoch_id,
        key.epoch_id().to_owned()
    );
    assert_eq!(
        value(open_cell(registry, &held, &target, &blob(), &stored, false).unwrap()),
        plaintext
    );
    // A stream in a column that may not stream is unsupported.
    for column in [title(), notes()] {
        assert_eq!(
            open_cell(registry, &held, &target, &column, &stored, false).unwrap(),
            Opened::Unreadable(DataError::UnsupportedFormat)
        );
    }
    let mut indexed_bytes = blob();
    indexed_bytes.equality = Some(
        jazz_e2ee::records::EqualityColumn::declare(jazz_e2ee::records::EqualityColumnType::Bytes)
            .unwrap(),
    );
    assert!(!indexed_bytes.may_stream());
    for chunk in [1, 7, 69, 65_552, 300_000] {
        assert_eq!(
            read_stream(registry, &held, &target, &stored, chunk).unwrap(),
            plaintext,
            "chunks of {chunk}"
        );
    }
    let empty = seal_stream(&world, &key, &target, b"", 1);
    assert_eq!(
        read_stream(registry, &held, &target, &empty, 3).unwrap(),
        b""
    );
}

#[test]
fn a_damaged_stream_never_completes() {
    let world = world(21);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let key = write_key(&held, &address);
    let registry = &world.shared.registry;
    let plaintext = TestRandom(21).bytes(70_000);
    let stored = seal_stream(&world, &key, &target, &plaintext, 70_000);
    let invalid = Err(ContentError::Data(DataError::InvalidCiphertext));
    // Truncated before the last record, or with trailing bytes.
    assert_eq!(
        read_stream(registry, &held, &target, &stored[..stored.len() - 20], 4096),
        invalid
    );
    let trailing = [&stored[..], b"x"].concat();
    assert_eq!(
        read_stream(registry, &held, &target, &trailing, 4096),
        invalid
    );
    let mut damaged = stored.clone();
    let at = damaged.len() - 30_000;
    damaged[at] ^= 1;
    assert_eq!(
        read_stream(registry, &held, &target, &damaged, 4096),
        invalid
    );
    // Another row's context.
    assert_eq!(
        read_stream(
            registry,
            &held,
            &row(&domain, &address, ROW_2),
            &stored,
            4096
        ),
        invalid
    );
    // Routing: a cell record is another record mechanism; garbage is
    // malformed; a version nobody holds is missing.
    assert_eq!(
        stream_routing(&envelope::encode(CELL_RECORD, &[0x30; 60]).unwrap()),
        Err(DataError::UnsupportedFormat)
    );
    assert_eq!(stream_routing(&[0; 80]), Err(DataError::InvalidCiphertext));
    assert!(matches!(
        StreamValueReader::open(registry, &HeldKeys::new(), &target, &blob(), &stored),
        Ok(StreamOpen::Unreadable(Opened::Missing { .. }))
    ));
    assert!(matches!(
        StreamValueReader::open(registry, &held, &target, &notes(), &stored),
        Ok(StreamOpen::Unreadable(Opened::Unreadable(
            DataError::UnsupportedFormat
        )))
    ));
}

#[test]
fn a_stream_reseals_under_another_version_in_bounded_memory() {
    let mut world = world(22);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let before = held_of(&world, ALICE, ROW);
    let plaintext = TestRandom(22).bytes(150_000);
    let stored = seal_stream(
        &world,
        &write_key(&before, &address),
        &target,
        &plaintext,
        150_000,
    );
    world.rotate(ALICE);
    let after = held_of(&world, ALICE, ROW);
    let key = write_key(&after, &address);
    let registry = &world.shared.registry;
    let suite = registry.select(&Default::default()).unwrap();
    let reseal = |source: &[u8]| -> Result<Vec<u8>, ContentError> {
        let StreamOpen::Reader { reader, initial } =
            StreamValueReader::open(registry, &after, &target, &blob(), &source[..69])?
        else {
            panic!("a reader");
        };
        let (writer, mut out) = begin_stream(&suite, &key, &target, &blob(), &mut TestRandom(5))?;
        assert!(initial.is_empty(), "the routing alone holds no record");
        let mut resealer = StreamResealer::new(reader, writer);
        for part in source[69..].chunks(10_000) {
            resealer.update(part, &mut out)?;
        }
        resealer.finish(&mut out)?;
        Ok(out)
    };
    let resealed = reseal(&stored).unwrap();
    assert_eq!(stream_routing(&resealed).unwrap().epoch_id, key.epoch_id());
    assert_eq!(
        read_stream(registry, &after, &target, &resealed, 4096).unwrap(),
        plaintext
    );
    // A truncated source never yields a complete new stream.
    assert_eq!(
        reseal(&stored[..stored.len() - 20]),
        Err(ContentError::Data(DataError::InvalidCiphertext))
    );
}

/// A first chunk may carry ciphertext beyond the routing: the records that
/// complete inside it come back with the reader, never lost, whether the
/// chunk is the whole value or ends inside a later record.
#[test]
fn plaintext_inside_the_first_chunk_is_returned() {
    let world = world(21);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let key = write_key(&held, &address);
    let registry = &world.shared.registry;
    let plaintext = TestRandom(21).bytes(200_000);
    let stored = seal_stream(&world, &key, &target, &plaintext, 10_000);
    for split in [stored.len(), 69 + 70_000, 69 + 1] {
        let StreamOpen::Reader {
            mut reader,
            initial,
        } = StreamValueReader::open(registry, &held, &target, &blob(), &stored[..split]).unwrap()
        else {
            panic!("a reader");
        };
        let mut out = initial.to_vec();
        reader.update(&stored[split..], &mut out).unwrap();
        reader.finish().unwrap();
        assert_eq!(out, plaintext, "first chunk of {split} bytes");
    }
    // A small value given whole: all of it comes back from `open`.
    let small = TestRandom(22).bytes(300);
    let stored = seal_stream(&world, &key, &target, &small, 300);
    let StreamOpen::Reader { reader, initial } =
        StreamValueReader::open(registry, &held, &target, &blob(), &stored).unwrap()
    else {
        panic!("a reader");
    };
    reader.finish().unwrap();
    assert_eq!(&initial[..], &small[..]);
}

/// The stream counterpart of
/// `another_spaces_key_of_the_same_epoch_never_decides_a_cell`: keys of
/// another space with the value's epoch that do not open it leave the row's
/// own key missing, never the value invalid.
#[test]
fn another_spaces_key_of_the_same_epoch_never_decides_a_stream() {
    let world = world(23);
    let ours = root_from(&world, ROW, root_random(1, 2, 9));
    let theirs = root_from(&world, OTHER_ROW, root_random(3, 4, 9));
    assert_eq!(ours.root.epoch_id, theirs.root.epoch_id);
    let domain = shared(&world);
    let address = space(OTHER_ROW);
    let target = row(&domain, &address, ROW_1);
    let mut writer = HeldKeys::new();
    writer.hold(creation(&world, &theirs, "tx-theirs")).unwrap();
    let plaintext = TestRandom(23).bytes(50_000);
    let stored = seal_stream(
        &world,
        &write_key(&writer, &address),
        &target,
        &plaintext,
        10_000,
    );
    let mut reader = HeldKeys::new();
    reader.hold(creation(&world, &ours, "tx-ours")).unwrap();
    let registry = &world.shared.registry;
    let missing = theirs.root.epoch_id.clone();
    // Given whole, the first record already fails under the other key.
    assert!(matches!(
        StreamValueReader::open(registry, &reader, &target, &blob(), &stored).unwrap(),
        StreamOpen::Unreadable(Opened::Missing { epoch_id }) if epoch_id == missing
    ));
    // Given in chunks, the failure surfaces as missing from `update`.
    assert_eq!(
        read_stream(registry, &reader, &target, &stored, 10_000).err(),
        Some(ContentError::Missing { epoch_id: missing })
    );
}

/// Fails if `debug` shows `secret` as text, as a byte list or as hex.
fn assert_redacted(debug: &str, secret: &[u8]) {
    let text = String::from_utf8_lossy(secret);
    let list = format!("{secret:?}");
    let list = &list[1..list.len() - 1];
    let hex: String = secret.iter().map(|byte| format!("{byte:02x}")).collect();
    for shown in [&text[..], list, &hex] {
        assert!(!debug.contains(shown), "{debug} shows {shown}");
    }
}

/// Decrypted values, plaintext about to be sealed and equality values are
/// never printed, wherever they are carried.
#[test]
fn plaintext_and_equality_values_never_print() {
    let world = world(24);
    let held = held_of(&world, ALICE, ROW);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let registry = &world.shared.registry;
    let secret = b"very secret plaintext";
    let text = core::str::from_utf8(secret).unwrap();
    let opened = Opened::Value(Zeroizing::new(secret.to_vec()));
    let stale = Opened::StaleSpace {
        value: Zeroizing::new(secret.to_vec()),
        space: space(OTHER_ROW),
    };
    let input = CellInput {
        column: title(),
        value: secret,
        equality: Some(EqualityValue::Text(text)),
    };
    let stored = seal_stream(&world, &write_key(&held, &address), &target, secret, 64);
    let stream = StreamValueReader::open(registry, &held, &target, &blob(), &stored).unwrap();
    let StreamOpen::Reader { initial, .. } = &stream else {
        panic!("a reader");
    };
    assert_eq!(&initial[..], secret, "the whole value opened");
    let values = [EqualityValue::Text(text)];
    let index = SourceIndex {
        table: TableTarget {
            domain: &domain,
            space: &address,
            table_id: TABLE,
        },
        column: title(),
        values: &values,
    };
    for debug in [
        format!("{opened:?}"),
        format!("{stale:?}"),
        format!("{input:?}"),
        format!("{stream:?}"),
        format!("{index:?}"),
        format!("{:?}", EqualityValueBuf::Text(text.to_owned())),
        format!("{:?}", EqualityValueBuf::Bytes(secret.to_vec())),
        format!("{:?}", EqualityValueBuf::Enum(text.to_owned())),
    ] {
        assert_redacted(&debug, secret);
    }
}
