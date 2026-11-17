//! The runtime's sweeps as plans (`E2EE_PROTOCOL.md` §3.2; migrations
//! §3–§4): repairing a refused or lost creation's cells, tag backfill under
//! each cell's own epoch, and re-homing or moving rows to another space.

mod common;
mod content_support;

use std::sync::atomic::Ordering;

use common::TestRandom;
use common::space::*;
use content_support::*;
use jazz_e2ee::Error;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::cells::{
    CellInput, Opened, SealedCell, begin_stream, open_cell, seal_cells,
};
use jazz_e2ee::protocol::content::convert::{
    Backfill, Converted, Rehome, RehomeCell, backfill_token, move_row, rehome_row,
};
use jazz_e2ee::protocol::content::repair::{
    REPAIR_BATCH, RepairPlan, RepairRow, RepairTarget, Rewrite, plan_repair,
};
use jazz_e2ee::protocol::content::store::{StoreOwner, decode_document};
use jazz_e2ee::protocol::content::{
    CLEARED_MARKER, ContentError, CreationFate, DataError, EncryptedColumn, HeldKeys, KeyMark,
    MissingKey, QueryPlan, RowTarget, TableTarget, UnsupportedQuery, plan_equality,
};
use jazz_e2ee::protocol::domain::SharedDomain;
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::records::EqualityValue;

fn row<'a>(domain: &'a SharedDomain, address: &'a SpaceAddress, row_id: &'a str) -> RowTarget<'a> {
    RowTarget {
        domain,
        space: address,
        table_id: TABLE,
        row_id,
    }
}

fn columns() -> [EncryptedColumn<'static>; 3] {
    [title(), notes(), blob()]
}

/// Bob's space at `ROW`, of which Alice and Bob are members, and Alice's
/// own creation of the same address, which lost: Alice holds its key,
/// `unpublished` until [`Repairing::settle`].
struct Repairing {
    world: World,
    alice: HeldKeys,
    losing: String,
    space_root: String,
}

impl Repairing {
    fn new(seed: u64) -> Self {
        Self::with(World::new(seed))
    }

    fn with(mut world: World) -> Self {
        world.account(ALICE);
        world.account(BOB);
        world.add_member(ALICE);
        world.add_member(BOB);
        let lost = own_creation(&world, ALICE, ROW, "tx-lost");
        let losing = lost.root_id().to_owned();
        world.create(BOB);
        let space_root = chain_of(&world, ROW).root.row.id.clone();
        let mut alice = HeldKeys::new();
        alice.hold(lost).unwrap();
        Self {
            world,
            alice,
            losing,
            space_root,
        }
    }

    /// Alice learns that another root won, and acquires the real key.
    fn settle(&mut self) {
        self.alice
            .space_mut(&self.losing)
            .unwrap()
            .settle(CreationFate::Accepted, Some(&self.space_root));
        let epoch = self.world.current();
        self.world.deliver(BOB, ALICE, &epoch);
        self.alice
            .hold(acquire_held(&self.world, ALICE, ROW))
            .unwrap();
    }

    /// Seals cells in `row_id` under Alice's current write key.
    fn alice_writes(&self, row_id: &str, cells: &[(EncryptedColumn<'_>, &str)]) -> Vec<SealedCell> {
        let domain = shared(&self.world);
        let address = space(ROW);
        seal_texts(
            &self.world,
            &write_key(&self.alice, &address),
            &row(&domain, &address, row_id),
            cells,
        )
    }

    fn plan(
        &self,
        held: &HeldKeys,
        rows: &[RepairRow<'_>],
    ) -> Result<Option<RepairPlan>, ContentError> {
        let domain = shared(&self.world);
        let columns = columns();
        let target = RepairTarget {
            registry: &self.world.shared.registry,
            suite: self
                .world
                .shared
                .registry
                .select(&MechanismConfig::default())
                .unwrap(),
            held,
            domain: &domain,
            table_id: TABLE,
            columns: &columns,
            values: &Values,
        };
        plan_repair(
            &target,
            &self.losing,
            &self.space_root,
            rows,
            &mut TestRandom(9),
        )
    }
}

fn stored_of<'a>(cells: &'a [SealedCell], column: &EncryptedColumn<'_>) -> &'a [u8] {
    &cells
        .iter()
        .find(|cell| cell.column_id == column.column_id)
        .unwrap()
        .stored
}

fn rewritten(plan: &RepairPlan, row_id: &str) -> Vec<SealedCell> {
    plan.rows
        .iter()
        .find(|row| row.row_id == row_id)
        .unwrap()
        .rewrites
        .iter()
        .map(|rewrite| match rewrite {
            Rewrite::Cell(cell) => cell.clone(),
            Rewrite::Stream { .. } => panic!("a cell"),
        })
        .collect()
}

fn value(opened: Opened) -> Vec<u8> {
    match opened {
        Opened::Value(value) => value.to_vec(),
        other => panic!("expected a value, got {other:?}"),
    }
}

#[test]
fn repair_rewrites_only_cells_naming_the_losing_epoch() {
    let mut repairing = Repairing::new(1);
    let r1 = repairing.alice_writes(ROW_1, &[(title(), "old"), (notes(), "n1")]);
    let r2_notes = repairing.alice_writes(ROW_2, &[(notes(), "n2")]);
    repairing.settle();
    let world = &repairing.world;
    let domain = shared(world);
    let address = space(ROW);
    // Bob writes R2's title under the real key afterwards.
    let bob = held_of(world, BOB, ROW);
    let r2_title = seal_texts(
        world,
        &write_key(&bob, &address),
        &row(&domain, &address, ROW_2),
        &[(title(), "bob")],
    );
    let r1_cells = [
        (title().column_id, stored_of(&r1, &title())),
        (notes().column_id, stored_of(&r1, &notes())),
    ];
    let r2_cells = [
        (title().column_id, stored_of(&r2_title, &title())),
        (notes().column_id, stored_of(&r2_notes, &notes())),
    ];
    let short_cells = [
        (title().column_id, &b""[..]),
        (notes().column_id, &CLEARED_MARKER[..]),
    ];
    let elsewhere = "0a000000-0000-4000-8000-000000000003";
    let rows = [
        RepairRow {
            row_id: ROW_1,
            space_identifier: ROW,
            cells: &r1_cells,
        },
        RepairRow {
            row_id: ROW_2,
            space_identifier: ROW,
            cells: &r2_cells,
        },
        RepairRow {
            row_id: elsewhere,
            space_identifier: OTHER_ROW,
            cells: &r1_cells,
        },
        RepairRow {
            row_id: "0a000000-0000-4000-8000-000000000004",
            space_identifier: ROW,
            cells: &short_cells,
        },
    ];
    let plan = repairing.plan(&repairing.alice, &rows).unwrap().unwrap();
    assert_eq!(
        plan.losing_epoch_id,
        repairing
            .alice
            .space(&repairing.losing)
            .unwrap()
            .root()
            .epoch_id
    );
    assert_eq!(plan.epoch_id, world.current());
    assert_eq!(rewritten(&plan, ROW_1).len(), 2);
    let r2 = rewritten(&plan, ROW_2);
    assert_eq!(r2.len(), 1, "the title Bob wrote is left as it is");
    assert_eq!(r2[0].column_id, notes().column_id);
    assert!(
        plan.rows[1].skipped.is_empty(),
        "the title Bob wrote is not even opened"
    );
    let moved = plan
        .rows
        .iter()
        .find(|row| row.row_id == elsewhere)
        .unwrap();
    assert!(moved.other_space && moved.rewrites.is_empty());
    assert!(plan.rows[3].rewrites.is_empty() && plan.rows[3].skipped.is_empty());
    // A row that has since moved to another space is left as it is, even
    // though its cell still opens under the losing key.
    let moved = repairing
        .plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: OTHER_ROW,
                cells: &r1_cells,
            }],
        )
        .unwrap()
        .unwrap();
    assert!(moved.rows[0].other_space);
    assert!(moved.rows[0].rewrites.is_empty() && moved.rows[0].skipped.is_empty());

    // Every member now reads the rewritten cells, and the title has a fresh
    // tag under the real version.
    let registry = &world.shared.registry;
    let target = row(&domain, &address, ROW_1);
    let repaired = rewritten(&plan, ROW_1);
    let title_cell = repaired
        .iter()
        .find(|cell| cell.column_id == title().column_id)
        .unwrap();
    assert_eq!(
        value(open_cell(registry, &bob, &target, &title(), &title_cell.stored, false).unwrap()),
        b"old"
    );
    let tagger = registry
        .select(&MechanismConfig::default())
        .unwrap()
        .equality;
    let QueryPlan::Tokens(query) = plan_equality(
        tagger,
        &bob,
        &TableTarget {
            domain: &domain,
            space: &address,
            table_id: TABLE,
        },
        &title(),
        &EqualityValue::Text("old"),
        true,
    )
    .unwrap() else {
        panic!("tokens");
    };
    assert_eq!(
        query.tokens,
        vec![title_cell.equality_token.clone().unwrap()]
    );
    let notes_cell = repaired
        .iter()
        .find(|cell| cell.column_id == notes().column_id)
        .unwrap();
    assert_eq!(notes_cell.equality_token, None);
    assert_eq!(
        value(open_cell(registry, &bob, &target, &notes(), &notes_cell.stored, false).unwrap()),
        b"n1"
    );
    const { assert!(REPAIR_BATCH == 100) };
}

#[test]
fn a_repair_that_read_an_old_value_leaves_a_later_write_untouched() {
    let mut repairing = Repairing::new(2);
    let old = repairing.alice_writes(ROW_1, &[(title(), "old")]);
    repairing.settle();
    let world = &repairing.world;
    let domain = shared(world);
    let address = space(ROW);
    let read = [(title().column_id, stored_of(&old, &title()))];
    let first = repairing
        .plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: ROW,
                cells: &read,
            }],
        )
        .unwrap()
        .unwrap();
    assert_eq!(rewritten(&first, ROW_1).len(), 1);
    // Another member writes `new` under the real key before the repair
    // commits: its transaction conflicts, and the plan from the re-read
    // rewrites nothing.
    let bob = held_of(world, BOB, ROW);
    let new = seal_texts(
        world,
        &write_key(&bob, &address),
        &row(&domain, &address, ROW_1),
        &[(title(), "new")],
    );
    let reread = [(title().column_id, stored_of(&new, &title()))];
    let second = repairing
        .plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: ROW,
                cells: &reread,
            }],
        )
        .unwrap()
        .unwrap();
    assert!(rewritten(&second, ROW_1).is_empty());
}

#[test]
fn an_unrepaired_row_stays_unreadable_to_others_and_readable_to_its_author() {
    let mut repairing = Repairing::new(3);
    let cells = repairing.alice_writes(ROW_1, &[(title(), "mine")]);
    repairing.settle();
    let world = &repairing.world;
    let domain = shared(world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let registry = &world.shared.registry.clone();
    let stored = stored_of(&cells, &title());
    assert_eq!(
        value(open_cell(registry, &repairing.alice, &target, &title(), stored, false).unwrap()),
        b"mine"
    );
    let mut bob = held_of(world, BOB, ROW);
    let Opened::Missing { epoch_id } =
        open_cell(registry, &bob, &target, &title(), stored, false).unwrap()
    else {
        panic!("missing");
    };
    assert_eq!(
        bob.settle_missing(&address, &epoch_id, Some(&chain_of(world, ROW)), true),
        Ok(MissingKey::Report(DataError::InvalidCiphertext))
    );
}

#[test]
fn an_interrupted_repair_keeps_the_key_and_finishes_after_a_restart() {
    let mut repairing = Repairing::new(4);
    let first = repairing.alice_writes(ROW_1, &[(notes(), "one")]);
    let second = repairing.alice_writes(ROW_2, &[(notes(), "two")]);
    repairing.settle();
    let world = &repairing.world;
    let first_cells = [(notes().column_id, stored_of(&first, &notes()))];
    let batch = repairing
        .plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: ROW,
                cells: &first_cells,
            }],
        )
        .unwrap()
        .unwrap();
    assert_eq!(rewritten(&batch, ROW_1).len(), 1);
    // Restart: the held keys come back from the store, the losing key
    // still `refused`.
    let owner = StoreOwner {
        registry: &world.shared.registry,
        domain: &world.shared.domain,
        account_id: ALICE,
        device: &world.accounts[ALICE].device,
    };
    let mut restarted = HeldKeys::new();
    for space in repairing.alice.spaces() {
        let bytes = owner
            .seal_document(space, 0, &mut TestRandom(1))
            .unwrap()
            .encode()
            .unwrap();
        let loaded = owner.load(&decode_document(&bytes).unwrap()).unwrap();
        restarted.hold(loaded.space.unwrap()).unwrap();
    }
    assert_eq!(
        restarted.space(&repairing.losing).unwrap().mark(0),
        Some(&KeyMark::Refused)
    );
    let second_cells = [(notes().column_id, stored_of(&second, &notes()))];
    let rest = repairing
        .plan(
            &restarted,
            &[RepairRow {
                row_id: ROW_2,
                space_identifier: ROW,
                cells: &second_cells,
            }],
        )
        .unwrap()
        .unwrap();
    assert_eq!(rewritten(&rest, ROW_2).len(), 1);
}

#[test]
fn a_cell_sealed_before_the_refusal_and_committed_after_a_pass_is_repaired_by_a_later_pass() {
    let mut repairing = Repairing::new(5);
    let early = repairing.alice_writes(ROW_1, &[(notes(), "early")]);
    // A handle transaction seals under the losing key, and commits only
    // after the first pass.
    let late = repairing.alice_writes(ROW_2, &[(notes(), "late")]);
    repairing.settle();
    let early_cells = [(notes().column_id, stored_of(&early, &notes()))];
    let late_cells = [(notes().column_id, stored_of(&late, &notes()))];
    let first = repairing
        .plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: ROW,
                cells: &early_cells,
            }],
        )
        .unwrap()
        .unwrap();
    assert_eq!(first.rows.len(), 1);
    let later = repairing
        .plan(
            &repairing.alice,
            &[
                RepairRow {
                    row_id: ROW_1,
                    space_identifier: ROW,
                    cells: &[(
                        notes().column_id,
                        stored_of(&rewritten(&first, ROW_1), &notes()),
                    )],
                },
                RepairRow {
                    row_id: ROW_2,
                    space_identifier: ROW,
                    cells: &late_cells,
                },
            ],
        )
        .unwrap()
        .unwrap();
    assert!(rewritten(&later, ROW_1).is_empty());
    assert_eq!(rewritten(&later, ROW_2).len(), 1);
}

#[test]
fn one_bad_cell_never_stops_the_repair_of_the_rest() {
    let mut repairing = Repairing::new(6);
    let cells = repairing.alice_writes(ROW_1, &[(title(), "t"), (notes(), "n")]);
    // A stream under the losing key, in a column that may stream and in
    // one that may not.
    let stream = {
        let domain = shared(&repairing.world);
        let address = space(ROW);
        let suite = repairing
            .world
            .shared
            .registry
            .select(&MechanismConfig::default())
            .unwrap();
        let (mut writer, mut out) = begin_stream(
            &suite,
            &write_key(&repairing.alice, &address),
            &row(&domain, &address, ROW_2),
            &blob(),
            &mut TestRandom(2),
        )
        .unwrap();
        writer.update(b"stream", &mut out).unwrap();
        writer.finish(&mut out).unwrap();
        out
    };
    repairing.settle();
    let mut damaged = stored_of(&cells, &notes()).to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    let r1 = [
        (title().column_id, stored_of(&cells, &title())),
        (notes().column_id, &damaged[..]),
    ];
    let r2 = [
        (blob().column_id, &stream[..]),
        (notes().column_id, &stream[..]),
    ];
    let plan = repairing
        .plan(
            &repairing.alice,
            &[
                RepairRow {
                    row_id: ROW_1,
                    space_identifier: ROW,
                    cells: &r1,
                },
                RepairRow {
                    row_id: ROW_2,
                    space_identifier: ROW,
                    cells: &r2,
                },
            ],
        )
        .unwrap()
        .unwrap();
    assert_eq!(plan.rows[0].rewrites.len(), 1);
    assert_eq!(
        plan.rows[0].skipped,
        vec![(notes().column_id.to_owned(), DataError::InvalidCiphertext)]
    );
    assert_eq!(
        plan.rows[1].rewrites,
        vec![Rewrite::Stream {
            column_id: blob().column_id.to_owned()
        }]
    );
    assert_eq!(
        plan.rows[1].skipped,
        vec![(notes().column_id.to_owned(), DataError::UnsupportedFormat)]
    );
}

#[test]
fn repair_needs_a_settled_losing_key_and_the_real_key() {
    let mut repairing = Repairing::new(7);
    let cells = repairing.alice_writes(ROW_1, &[(notes(), "n")]);
    let read = [(notes().column_id, stored_of(&cells, &notes()))];
    let rows = [RepairRow {
        row_id: ROW_1,
        space_identifier: ROW,
        cells: &read,
    }];
    let invalid = Err(ContentError::Protocol(ProtocolError::Crypto(
        Error::InvalidInput,
    )));
    // Unsettled: the losing key is still `unpublished`.
    assert_eq!(repairing.plan(&repairing.alice, &rows), invalid);
    repairing
        .alice
        .space_mut(&repairing.losing)
        .unwrap()
        .settle(CreationFate::Accepted, Some(&repairing.space_root));
    // The real key is not held yet: repair waits.
    assert_eq!(repairing.plan(&repairing.alice, &rows), Ok(None));
    repairing.settle();
    assert!(repairing.plan(&repairing.alice, &rows).unwrap().is_some());
    // A column the table does not declare, or one given twice, is a caller
    // error.
    let unknown = [("c9000000-0000-5000-8000-000000000000", read[0].1)];
    assert_eq!(
        repairing.plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: ROW,
                cells: &unknown,
            }]
        ),
        invalid
    );
    let twice = [read[0], read[0]];
    assert_eq!(
        repairing.plan(
            &repairing.alice,
            &[RepairRow {
                row_id: ROW_1,
                space_identifier: ROW,
                cells: &twice,
            }]
        ),
        invalid
    );
    // The losing root is never its own repair target.
    let same = Repairing {
        space_root: repairing.losing.clone(),
        ..repairing
    };
    assert_eq!(
        same.plan(&same.alice, &rows),
        Err(ContentError::Protocol(ProtocolError::InconsistentEvidence))
    );
}

#[test]
fn a_provider_failure_fails_the_repair() {
    let (registry, fails, config) = flaky_cells();
    let mut repairing = Repairing::with(World::with_registry(8, registry));
    let domain = shared(&repairing.world);
    let address = space(ROW);
    let suite = repairing.world.shared.registry.select(&config).unwrap();
    let stored = seal_cells(
        &suite,
        &write_key(&repairing.alice, &address),
        &row(&domain, &address, ROW_1),
        &[CellInput {
            column: notes(),
            value: b"n",
            equality: None,
        }],
        &mut TestRandom(1),
    )
    .unwrap()
    .cells
    .remove(0)
    .stored;
    repairing.settle();
    let read = [(notes().column_id, &stored[..])];
    let rows = [RepairRow {
        row_id: ROW_1,
        space_identifier: ROW,
        cells: &read,
    }];
    assert!(repairing.plan(&repairing.alice, &rows).unwrap().is_some());
    fails.store(true, Ordering::SeqCst);
    assert!(
        repairing
            .plan(&repairing.alice, &rows)
            .unwrap_err()
            .is_provider_failure()
    );
}

/// Alice's two spaces, `ROW` and `OTHER_ROW`, both held.
fn two_spaces(seed: u64) -> (World, HeldKeys) {
    let mut world = World::new(seed);
    world.account(ALICE);
    world.account(BOB);
    world.add_member(ALICE);
    world.add_member_of(OTHER_ROW, ALICE);
    world.create(ALICE);
    world.create_of(OTHER_ROW, ALICE);
    let mut held = held_of(&world, ALICE, ROW);
    held.hold(acquire_held(&world, ALICE, OTHER_ROW)).unwrap();
    (world, held)
}

#[test]
fn tag_backfill_uses_each_cells_own_epoch() {
    let mut world = World::new(9);
    world.account(ALICE);
    world.add_member(ALICE);
    world.create(ALICE);
    let domain = shared(&world);
    let address = space(ROW);
    let target = row(&domain, &address, ROW_1);
    let before = held_of(&world, ALICE, ROW);
    let cells = seal_texts(
        &world,
        &write_key(&before, &address),
        &target,
        &[(title(), "x")],
    );
    let first_epoch = world.current();
    world.rotate(ALICE);
    let held = held_of(&world, ALICE, ROW);
    let registry = &world.shared.registry;
    let tagger = registry
        .select(&MechanismConfig::default())
        .unwrap()
        .equality;
    let backfill = |held: &HeldKeys, column: &EncryptedColumn<'_>, stored: &[u8]| {
        backfill_token(registry, tagger, held, &target, column, stored, &Values)
    };
    assert_eq!(
        backfill(&held, &title(), &cells[0].stored),
        Ok(Backfill::Token {
            epoch_id: first_epoch,
            token: cells[0].equality_token.clone().unwrap()
        })
    );
    assert!(matches!(
        backfill(&HeldKeys::new(), &title(), &cells[0].stored),
        Ok(Backfill::Skip(Opened::Missing { .. }))
    ));
    assert_eq!(
        backfill(&held, &title(), b""),
        Ok(Backfill::Skip(Opened::Empty))
    );
    assert_eq!(
        backfill(&held, &notes(), &cells[0].stored),
        Err(ContentError::UnsupportedQuery(
            UnsupportedQuery::UnindexedEquality
        ))
    );
}

#[test]
fn a_rehomed_row_converts_whole_under_the_new_space() {
    let (world, held) = two_spaces(10);
    let domain = shared(&world);
    let old = space(ROW);
    let new = space(OTHER_ROW);
    let from = row(&domain, &old, ROW_1);
    let to = row(&domain, &new, ROW_1);
    let cells = seal_texts(
        &world,
        &write_key(&held, &old),
        &from,
        &[(title(), "x"), (notes(), "n")],
    );
    let mut new_title = title();
    new_title.column_id = "c1000000-0000-5000-8000-00000000000a";
    let mut new_notes = notes();
    new_notes.column_id = "c1000000-0000-5000-8000-00000000000b";
    let registry = &world.shared.registry;
    let suite = registry.select(&MechanismConfig::default()).unwrap();
    let key = write_key(&held, &new);
    let rehome = |cells: &[RehomeCell<'_>]| {
        rehome_row(
            registry,
            &suite,
            &held,
            &from,
            &key,
            &to,
            cells,
            &Values,
            &mut TestRandom(3),
        )
    };
    let pairs = [
        RehomeCell {
            source: title(),
            target: new_title,
            stored: &cells[0].stored,
        },
        RehomeCell {
            source: notes(),
            target: new_notes,
            stored: &cells[1].stored,
        },
        RehomeCell {
            source: notes(),
            target: new_notes,
            stored: b"",
        },
        RehomeCell {
            source: notes(),
            target: new_notes,
            stored: &CLEARED_MARKER,
        },
    ];
    let Rehome::Converted(converted) = rehome(&pairs).unwrap() else {
        panic!("converted");
    };
    let Converted::Cell(title_cell) = &converted[0] else {
        panic!("a cell");
    };
    assert_eq!(title_cell.column_id, new_title.column_id);
    assert_eq!(
        value(open_cell(registry, &held, &to, &new_title, &title_cell.stored, false).unwrap()),
        b"x"
    );
    let QueryPlan::Tokens(query) = plan_equality(
        suite.equality,
        &held,
        &TableTarget {
            domain: &domain,
            space: &new,
            table_id: TABLE,
        },
        &new_title,
        &EqualityValue::Text("x"),
        true,
    )
    .unwrap() else {
        panic!("tokens");
    };
    assert_eq!(
        query.tokens,
        vec![title_cell.equality_token.clone().unwrap()]
    );
    assert!(matches!(&converted[1], Converted::Cell(cell) if cell.equality_token.is_none()));
    assert_eq!(
        converted[2],
        Converted::Empty {
            target_column: new_notes.column_id.into()
        }
    );
    assert_eq!(
        converted[3],
        Converted::Cleared {
            target_column: new_notes.column_id.into()
        }
    );
    // A source that does not open stops this row only.
    let mut damaged = cells[1].stored.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert_eq!(
        rehome(&[
            pairs[0],
            RehomeCell {
                stored: &damaged,
                ..pairs[1]
            }
        ]),
        Ok(Rehome::Unconvertible {
            column_id: notes().column_id.into(),
            opened: Opened::Unreadable(DataError::InvalidCiphertext)
        })
    );
    // Keys and spaces must line up.
    let old_key = write_key(&held, &old);
    assert_eq!(
        rehome_row(
            registry,
            &suite,
            &held,
            &from,
            &old_key,
            &to,
            &pairs,
            &Values,
            &mut TestRandom(3)
        ),
        Err(ContentError::Protocol(ProtocolError::InconsistentEvidence))
    );
    assert_eq!(
        rehome_row(
            registry,
            &suite,
            &held,
            &from,
            &old_key,
            &from,
            &pairs,
            &Values,
            &mut TestRandom(3)
        ),
        Err(ContentError::Protocol(ProtocolError::Crypto(
            Error::InvalidInput
        )))
    );
    // A streamed source is resealed by the runtime in bounded memory.
    let (mut writer, mut stream) = begin_stream(
        &suite,
        &write_key(&held, &old),
        &from,
        &blob(),
        &mut TestRandom(4),
    )
    .unwrap();
    writer.update(b"large", &mut stream).unwrap();
    writer.finish(&mut stream).unwrap();
    assert_eq!(
        rehome(&[RehomeCell {
            source: blob(),
            target: blob(),
            stored: &stream
        }]),
        Ok(Rehome::Converted(vec![Converted::Stream {
            source_column: blob().column_id.into(),
            target_column: blob().column_id.into()
        }]))
    );
}

#[test]
fn a_move_reencrypts_every_encrypted_column() {
    let (world, held) = two_spaces(11);
    let domain = shared(&world);
    let old = space(ROW);
    let new = space(OTHER_ROW);
    let from = row(&domain, &old, ROW_1);
    let to = row(&domain, &new, ROW_1);
    let cells = seal_texts(
        &world,
        &write_key(&held, &old),
        &from,
        &[(title(), "x"), (notes(), "n")],
    );
    let registry = &world.shared.registry;
    let suite = registry.select(&MechanismConfig::default()).unwrap();
    let key = write_key(&held, &new);
    let declared = [title(), notes()];
    let movement = |stored: &[(&str, &[u8])]| {
        move_row(
            registry,
            &suite,
            &held,
            &from,
            &key,
            &to,
            &declared,
            stored,
            &Values,
            &mut TestRandom(5),
        )
    };
    let title_stored = (title().column_id, &cells[0].stored[..]);
    let notes_stored = (notes().column_id, &cells[1].stored[..]);
    assert_eq!(movement(&[title_stored]), Err(ContentError::Partial));
    assert_eq!(
        movement(&[title_stored, title_stored]),
        Err(ContentError::Partial)
    );
    let Rehome::Converted(converted) = movement(&[notes_stored, title_stored]).unwrap() else {
        panic!("converted");
    };
    for (column, (cell, expected)) in declared.iter().zip(converted.iter().zip(["x", "n"])) {
        let Converted::Cell(cell) = cell else {
            panic!("a cell");
        };
        assert_eq!(cell.column_id, column.column_id);
        assert_eq!(
            value(open_cell(registry, &held, &to, column, &cell.stored, false).unwrap()),
            expected.as_bytes()
        );
    }
    // An update sealed under the old space after the move is read as
    // stale, not lost.
    let stale = open_cell(registry, &held, &to, &title(), &cells[0].stored, false).unwrap();
    assert_eq!(stale.report(), Some(DataError::StaleSpace));
}
