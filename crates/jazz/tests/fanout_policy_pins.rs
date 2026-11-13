//! Evidence watches (`fanout.rs`, `Occurrence`): a table that read
//! policies read as evidence (a membership `EXISTS`, an `INHERITS` of a
//! referenced row) wakes a live result only on writes those reads can see
//! change: a row meeting the `EXISTS`'s session pin in its old or new
//! image, inserted, deleted, or changing a column the read uses.
//!
//! Every test compares each maintained result with a fresh nested one-shot
//! read (the full re-run path) after every write, on SQLite file storage.
//! The gates name the cases a filter on what the result holds gets wrong:
//! a grant shows a row the result never held, a revocation hides one, a
//! claims change moves the pin. The engagement gates fail while the
//! evidence tables are watched whole; the randomized differential at the
//! end compares runs with the watches on and off.

use std::collections::BTreeMap;

use jazz::db::{Db, InsertOptions, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::member_chat::{claims, room_list, schema};
use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use crate::fanout_correlation::{
    Live, Snapshot, check, delete, id, insert, open, subscribe, text, update, user, uuid,
};

const PERSON: u8 = 0x11;
const HANDLE: u8 = 0x12;
const ROOM: u8 = 0x13;
const MEMBER: u8 = 0x14;
const CALL: u8 = 0x15;
const SEEN: u8 = 0x16;
const BOT: u8 = 0x17;

const PEOPLE: usize = 6;
const ROOMS: usize = 6;

fn person(n: usize) -> RowUuid {
    id(PERSON, n)
}

fn room(n: usize) -> RowUuid {
    id(ROOM, n)
}

/// What the store holds, for the random writes.
struct World {
    /// Membership row → (room, person).
    members: BTreeMap<usize, (usize, usize)>,
    next_member: usize,
    next_seen: usize,
    /// The person each user's claim names.
    me: Vec<usize>,
}

fn join(db: &Db, world: &mut World, r: usize, p: usize) -> usize {
    let n = world.next_member;
    world.next_member += 1;
    insert(
        db,
        "members",
        id(MEMBER, n),
        &[
            ("room", uuid(room(r))),
            ("person", uuid(person(p))),
            ("banned", Value::Bool(false)),
            ("left_at", Value::Nullable(None)),
            ("pinned_at", Value::Nullable(None)),
        ],
    );
    world.members.insert(n, (r, p));
    n
}

/// People 0..6 (even ones discoverable), each with a handle; rooms 0..6,
/// each with a group call (open in even rooms) and a bot; person `p` is a
/// member of rooms `p` and `p + 1`, with a seen mark in each.
fn seed(db: &Db) -> World {
    let mut world = World {
        members: BTreeMap::new(),
        next_member: 0,
        next_seen: 0,
        me: (0..PEOPLE).collect(),
    };
    for p in 0..PEOPLE {
        insert(
            db,
            "people",
            person(p),
            &[
                ("name", text(&format!("p{p}"))),
                ("discoverable", Value::Bool(p % 2 == 0)),
            ],
        );
        insert(
            db,
            "handles",
            id(HANDLE, p),
            &[
                ("person", uuid(person(p))),
                ("handle", text(&format!("h{p}"))),
            ],
        );
        db.set_identity_claims(user(p), claims(person(p)));
    }
    for r in 0..ROOMS {
        insert(db, "rooms", room(r), &[("title", text(&format!("r{r}")))]);
        insert(
            db,
            "calls",
            id(CALL, r),
            &[
                ("room", uuid(room(r))),
                ("kind", text("group")),
                ("ended", Value::Bool(r % 2 == 1)),
            ],
        );
        insert(
            db,
            "bots",
            id(BOT, r),
            &[("room", uuid(room(r))), ("name", text("bot"))],
        );
    }
    for p in 0..PEOPLE {
        for r in [p, (p + 1) % ROOMS] {
            join(db, &mut world, r, p);
            seen(db, &mut world, r, p, p as i64);
        }
    }
    world
}

fn seen(db: &Db, world: &mut World, r: usize, p: usize, at: i64) {
    let n = world.next_seen;
    world.next_seen += 1;
    insert(
        db,
        "seen",
        id(SEEN, n),
        &[
            ("room", uuid(room(r))),
            ("person", uuid(person(p))),
            ("at", Value::I64(at)),
        ],
    );
}

/// Every room the reader can read, with its roster (each member's
/// profile and handles): the reader's rooms come only from the policy, so
/// a grant shows a room the result never held.
fn readable_rooms() -> Query {
    Query::from("rooms")
        .order_by("title", OrderDirection::Asc)
        .array_subquery(
            ArraySubquery::new("roster", "members", "room", "id").nested(
                ArraySubquery::new("profile", "people", "id", "person")
                    .nested(ArraySubquery::new("handles", "handles", "person", "id")),
            ),
        )
}

fn lives_of(db: &Db, readers: &[usize]) -> Vec<Live> {
    readers
        .iter()
        .flat_map(|&u| {
            [
                subscribe(
                    db,
                    &format!("user {u}: room list"),
                    room_list(person(u)),
                    Some(user(u)),
                ),
                subscribe(
                    db,
                    &format!("user {u}: readable rooms"),
                    readable_rooms(),
                    Some(user(u)),
                ),
            ]
        })
        .collect()
}

fn reran(db: &Db) -> u64 {
    db.fanout_stats().reran
}

/// Re-runs during `write`, with every result checked after it.
fn reruns(db: &Db, lives: &mut [Live], what: &str, write: impl FnOnce()) -> (u64, Vec<Snapshot>) {
    let before = reran(db);
    write();
    let now = check(db, lives, what);
    (reran(db) - before, now)
}

// ---- grants, revocations, claims ---------------------------------------

/// A grant: alice joins room 4. Her readable rooms never held room 4 (nor
/// any row naming it), so a filter on what the result holds would skip
/// the write; her membership `EXISTS` is pinned to her, and the new row
/// is hers, so it wakes the result and room 4 appears.
///
/// ```
/// alice: rooms (readable WHERE EXISTS members(room = id, person = me))
/// alice ──insert members(room 4, alice)──► room 4 appears
/// ```
#[test]
fn a_grant_shows_a_room_the_result_never_held() {
    let db = open(schema(), 1);
    let mut world = seed(&db);
    let mut lives = lives_of(&db, &[0]);
    let opened = check(&db, &mut lives, "subscribe");
    join(&db, &mut world, 4, 0);
    let now = check(&db, &mut lives, "alice joins room 4");
    assert_ne!(opened[1], now[1], "room 4 is now readable");
    assert!(
        now[1].iter().any(|(row, _)| *row == room(4)),
        "the readable rooms include room 4"
    );
}

/// Revocations: alice's membership of room 1 moves to room 5 (its old
/// image is hers in room 1, its new one in room 5), then her membership of
/// room 0 is deleted. Each hides a room, and each is her own row.
///
/// ```
/// alice ──move members(room 1 → room 5)──► room 1 hidden, room 5 shown
/// alice ──delete members(room 0)─────────► room 0 hidden
/// ```
#[test]
fn a_revocation_hides_its_room() {
    let db = open(schema(), 1);
    let world = seed(&db);
    let mut lives = lives_of(&db, &[0, 1]);
    check(&db, &mut lives, "subscribe");
    let of = |r: usize, p: usize| {
        world
            .members
            .iter()
            .find(|(_, (room, person))| *room == r && *person == p)
            .map(|(n, _)| *n)
            .unwrap()
    };
    update(
        &db,
        "members",
        id(MEMBER, of(1, 0)),
        &[("room", uuid(room(5)))],
    );
    let now = check(&db, &mut lives, "alice's room-1 membership moves to room 5");
    assert!(!now[1].iter().any(|(row, _)| *row == room(1)));
    assert!(now[1].iter().any(|(row, _)| *row == room(5)));
    delete(&db, "members", id(MEMBER, of(0, 0)));
    let now = check(&db, &mut lives, "alice's room-0 membership is deleted");
    assert!(!now[1].iter().any(|(row, _)| *row == room(0)));
}

/// A revocation the result holds no row of: alice reads bare rooms, with
/// no roster, so no membership row is held anywhere. Her membership of
/// room 1 is handed to person 2: only its old image is hers, and that
/// alone must wake the result.
///
/// ```
/// alice: rooms (readable WHERE EXISTS members(room = id, person = me))
/// writer ──members(room 1, alice).person = person 2──► room 1 hidden
/// ```
#[test]
fn a_membership_leaving_the_pin_wakes_by_its_old_image() {
    let db = open(schema(), 1);
    let world = seed(&db);
    let mut lives = vec![subscribe(
        &db,
        "alice: bare rooms",
        Query::from("rooms").order_by("title", OrderDirection::Asc),
        Some(user(0)),
    )];
    let opened = check(&db, &mut lives, "subscribe");
    assert!(opened[0].iter().any(|(row, _)| *row == room(1)));
    let mine = *world
        .members
        .iter()
        .find(|(_, (r, p))| *r == 1 && *p == 0)
        .unwrap()
        .0;
    update(
        &db,
        "members",
        id(MEMBER, mine),
        &[("person", uuid(person(2)))],
    );
    let now = check(
        &db,
        &mut lives,
        "alice's room-1 membership goes to person 2",
    );
    assert!(!now[0].iter().any(|(row, _)| *row == room(1)));
}

/// A claims change moves the pin: alice's claim now names dave, so his
/// rooms are hers. A later membership of dave's wakes her results, and
/// one of the person she was before skips them.
///
/// ```
/// claims ──alice: person = dave──► alice reads dave's rooms
/// writer ──insert members(room 0, dave)──► room 0 appears
/// writer ──insert members(room 1, alice's old person)──► skipped
/// ```
#[test]
fn a_claims_change_moves_the_pin() {
    let db = open(schema(), 1);
    let mut world = seed(&db);
    let mut lives = vec![subscribe(
        &db,
        "alice: readable rooms",
        readable_rooms(),
        Some(user(0)),
    )];
    let opened = check(&db, &mut lives, "subscribe");
    db.set_identity_claims(user(0), claims(person(3)));
    let now = check(&db, &mut lives, "alice's claim names dave");
    assert_ne!(opened, now, "dave's rooms are readable");
    let (woke, now) = reruns(&db, &mut lives, "dave joins room 0", || {
        join(&db, &mut world, 0, 3);
    });
    assert_eq!(woke, 1, "dave's membership wakes alice's result");
    assert!(now[0].iter().any(|(row, _)| *row == room(0)));
    // Alice now holds rooms 0, 3 and 4.
    let (woke, _) = reruns(&db, &mut lives, "person 0 joins room 1", || {
        join(&db, &mut world, 1, 0);
    });
    assert_eq!(woke, 0, "person 0 is no longer alice's pin");
}

/// An inherited policy: a handle is readable when its person is. Carol
/// (discoverable) stops being discoverable, which hides her profile and
/// handle from the rooms that hold her; renaming a person no roster holds
/// changes nothing a policy reads, and skips.
///
/// ```
/// handles: readable INHERITS SELECT VIA person
/// people:  readable WHERE id = me OR discoverable
/// writer ──people(carol).discoverable = false──► carol's profile hidden
/// writer ──rename person 4 (not in alice's rooms)──► skipped
/// ```
#[test]
fn an_inherited_policy_follows_the_columns_it_reads() {
    let db = open(schema(), 1);
    seed(&db);
    // Alice (0) shares room 1 with person 1 and room 0 with person 5.
    let mut lives = lives_of(&db, &[0]);
    check(&db, &mut lives, "subscribe");
    let (woke, _) = reruns(&db, &mut lives, "person 4 renamed", || {
        update(&db, "people", person(4), &[("name", text("four"))]);
    });
    assert_eq!(
        woke, 0,
        "no result holds person 4, and no policy reads names"
    );
    let (woke, _) = reruns(&db, &mut lives, "person 4 stops being discoverable", || {
        update(
            &db,
            "people",
            person(4),
            &[("discoverable", Value::Bool(false))],
        );
    });
    assert_eq!(woke, 2, "the people policy reads discoverable");
    // Person 0's rooms 0 and 1 hold persons 0, 1 and 5; 0 is alice.
    let (woke, _) = reruns(&db, &mut lives, "person 1 becomes discoverable", || {
        update(
            &db,
            "people",
            person(1),
            &[("discoverable", Value::Bool(true))],
        );
    });
    assert_eq!(woke, 2);
    let shown = check(&db, &mut lives, "person 1 is discoverable");
    let (_, now) = reruns(&db, &mut lives, "person 1 stops being discoverable", || {
        update(
            &db,
            "people",
            person(1),
            &[("discoverable", Value::Bool(false))],
        );
    });
    assert_ne!(shown[1], now[1], "person 1's profile is hidden again");
}

// ---- the filter engages ------------------------------------------------

/// Other people's memberships outside alice's rooms skip her results; a
/// co-member joining one of her rooms changes her rosters (by
/// correlation); her own membership changing re-runs both (the room
/// list's root key, and the rosters that hold the row).
///
/// ```
/// bob ──join room 3 (not alice's)──► skipped
/// bob ──join room 0 (alice's)──────► both results re-run
/// alice ──pin her room-0 membership──► both re-run
/// ```
#[test]
fn another_persons_membership_skips_unless_it_shares_a_room() {
    let db = open(schema(), 1);
    let mut world = seed(&db);
    let mut lives = lives_of(&db, &[0]);
    let opened = check(&db, &mut lives, "subscribe");
    let (woke, _) = reruns(&db, &mut lives, "person 2 joins room 3", || {
        join(&db, &mut world, 3, 2);
    });
    assert_eq!(woke, 0, "room 3 is not alice's: her results skip it");
    let (woke, now) = reruns(&db, &mut lives, "person 2 joins room 0", || {
        join(&db, &mut world, 0, 2);
    });
    assert_eq!(woke, 2, "room 0 is alice's: both rosters change");
    assert_ne!(opened[1], now[1], "room 0's roster gains person 2");
    let mine = *world
        .members
        .iter()
        .find(|(_, (r, p))| *r == 0 && *p == 0)
        .unwrap()
        .0;
    let (woke, _) = reruns(&db, &mut lives, "alice pins room 0", || {
        update(
            &db,
            "members",
            id(MEMBER, mine),
            &[("pinned_at", Value::I64(9))],
        );
    });
    assert_eq!(woke, 2, "the root key and the held roster row wake");
}

// ---- $createdBy ---------------------------------------------------------

/// A policy `EXISTS` on `$createdBy` has no pin a changed row's image
/// shows (the stamp is not in it): every membership write wakes, and a
/// membership bob created for alice does not make its room readable.
///
/// ```
/// rooms: readable WHERE EXISTS members(room = id, $createdBy = me)
/// bob ──insert members(room 2) as bob──► alice's rooms unchanged
/// alice ──insert members(room 2) as alice──► room 2 appears
/// ```
#[test]
fn a_created_by_pin_watches_every_membership() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "members",
                        vec![
                            outer_eq("room", "id"),
                            session_eq("$createdBy.identity.subject", &["identity", "subject"]),
                        ],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("room", "rooms")
                    .column("note", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = open(schema, 1);
    for r in 0..3 {
        insert(&db, "rooms", room(r), &[("title", text(&format!("r{r}")))]);
    }
    let by = |n: usize, r: usize, author: AuthorSubject| {
        block_on(db.insert(
            "members",
            BTreeMap::from([
                ("room".to_owned(), uuid(room(r))),
                ("note".to_owned(), text("x")),
            ]),
            InsertOptions {
                row_id: Some(id(MEMBER, n)),
                identity: WriteIdentity::Attribution(author),
                ..Default::default()
            },
        ))
        .unwrap();
    };
    by(0, 0, user(0));
    let mut lives = vec![subscribe(
        &db,
        "alice: readable rooms",
        Query::from("rooms").order_by("title", OrderDirection::Asc),
        Some(user(0)),
    )];
    let opened = check(&db, &mut lives, "subscribe");
    assert_eq!(opened[0].len(), 1);
    by(1, 2, user(1));
    let now = check(&db, &mut lives, "bob's membership of room 2");
    assert_eq!(now[0].len(), 1, "bob's membership grants alice nothing");
    by(2, 2, user(0));
    let now = check(&db, &mut lives, "alice's membership of room 2");
    assert_eq!(now[0].len(), 2);
    update(&db, "members", id(MEMBER, 2), &[("room", uuid(room(1)))]);
    check(&db, &mut lives, "alice's membership moves to room 1");
}

// ---- randomized differential ------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n.max(1)
    }
}

/// One random write; what it was.
fn random_write(db: &Db, rng: &mut Rng, world: &mut World, step: usize) -> String {
    let r = rng.next(ROOMS);
    let p = rng.next(PEOPLE);
    let any_member = |rng: &mut Rng, world: &World| {
        let keys: Vec<usize> = world.members.keys().copied().collect();
        (!keys.is_empty()).then(|| keys[rng.next(keys.len())])
    };
    match rng.next(16) {
        0 | 1 => {
            let n = join(db, world, r, p);
            format!("member {n}: person {p} joins room {r}")
        }
        2 => match any_member(rng, world) {
            Some(n) => {
                delete(db, "members", id(MEMBER, n));
                world.members.remove(&n);
                format!("member {n} deleted")
            }
            None => "nothing".into(),
        },
        3 => match any_member(rng, world) {
            Some(n) => {
                update(db, "members", id(MEMBER, n), &[("room", uuid(room(r)))]);
                world.members.get_mut(&n).unwrap().0 = r;
                format!("member {n} moves to room {r}")
            }
            None => "nothing".into(),
        },
        4 => match any_member(rng, world) {
            Some(n) => {
                update(db, "members", id(MEMBER, n), &[("person", uuid(person(p)))]);
                world.members.get_mut(&n).unwrap().1 = p;
                format!("member {n} becomes person {p}")
            }
            None => "nothing".into(),
        },
        5 => match any_member(rng, world) {
            Some(n) => {
                let column = ["banned", "left_at", "pinned_at"][rng.next(3)];
                let value = match column {
                    "banned" => Value::Bool(rng.next(2) == 0),
                    _ if rng.next(2) == 0 => Value::Nullable(None),
                    _ => Value::I64(step as i64),
                };
                update(db, "members", id(MEMBER, n), &[(column, value)]);
                format!("member {n}: {column} changed")
            }
            None => "nothing".into(),
        },
        6 => {
            update(
                db,
                "people",
                person(p),
                &[("name", text(&format!("p{p}.{step}")))],
            );
            format!("person {p} renamed")
        }
        7 => {
            let on = rng.next(2) == 0;
            update(
                db,
                "people",
                person(p),
                &[("discoverable", Value::Bool(on))],
            );
            format!("person {p} discoverable = {on}")
        }
        8 => {
            let to = rng.next(PEOPLE);
            update(
                db,
                "handles",
                id(HANDLE, p),
                &[
                    ("person", uuid(person(to))),
                    ("handle", text(&format!("h{step}"))),
                ],
            );
            format!("handle {p} moves to person {to}")
        }
        9 => {
            update(
                db,
                "calls",
                id(CALL, r),
                &[("ended", Value::Bool(rng.next(2) == 0))],
            );
            format!("call of room {r} toggled")
        }
        10 => {
            update(
                db,
                "calls",
                id(CALL, r),
                &[("room", uuid(room(rng.next(ROOMS))))],
            );
            format!("call {r} moves")
        }
        11 => {
            seen(db, world, r, p, step as i64);
            format!("person {p} saw room {r}")
        }
        12 => {
            update(
                db,
                "bots",
                id(BOT, r),
                &[("room", uuid(room(rng.next(ROOMS))))],
            );
            format!("bot {r} moves")
        }
        13 => {
            update(
                db,
                "rooms",
                room(r),
                &[("title", text(&format!("r{r}.{step}")))],
            );
            format!("room {r} renamed")
        }
        14 => {
            let u = rng.next(4);
            world.me[u] = p;
            db.set_identity_claims(user(u), claims(person(p)));
            format!("user {u}'s claim names person {p}")
        }
        _ => {
            let to = rng.next(PEOPLE);
            update(
                db,
                "seen",
                id(SEEN, rng.next(world.next_seen)),
                &[("person", uuid(person(to)))],
            );
            format!("a seen mark moves to person {to}")
        }
    }
}

/// One seed's random writes, with evidence watches on or off; the final
/// results and the fan-out counters.
fn differential_run(seed_of: u64, pins: bool) -> (Vec<Snapshot>, jazz::db::FanoutStats) {
    jazz::db::set_policy_pins_for_test(pins);
    let db = open(schema(), 1);
    let mut world = seed(&db);
    let mut lives = lives_of(&db, &[0, 1, 2, 3]);
    let mode = if pins { "pins" } else { "whole" };
    check(&db, &mut lives, &format!("{mode}: subscribe"));
    let mut rng = Rng(seed_of);
    for step in 0..150 {
        let what = random_write(&db, &mut rng, &mut world, step);
        check(
            &db,
            &mut lives,
            &format!("{mode}: seed {seed_of} step {step}: {what}"),
        );
    }
    jazz::db::set_policy_pins_for_test(true);
    let rows = lives.into_iter().map(|live| live.rows).collect();
    (rows, db.fanout_stats())
}

/// Random membership, people, handle, call, seen, bot and room writes and
/// claims changes, for four readers' room lists and readable rooms: with
/// evidence watches on and off, every result equals a fresh read after
/// every write, both runs end equal, and the watched run re-runs less.
#[test]
fn evidence_watched_results_match_one_shot_reads_under_random_writes() {
    let mut totals = [(0u64, 0u64); 2];
    for seed in [5, 23, 71, 202] {
        let on = differential_run(seed, true);
        let off = differential_run(seed, false);
        assert_eq!(on.0, off.0, "seed {seed}: watched and whole runs differ");
        for (total, (_, stats)) in totals.iter_mut().zip([&on, &off]) {
            total.0 += stats.reran;
            total.1 += stats.unevidenced;
        }
    }
    let [(watched, skipped), (whole, none)] = totals;
    assert_eq!(none, 0, "pins off: no evidence skips");
    assert!(skipped > 0, "evidence watches skipped changes: {skipped}");
    assert!(
        watched < whole,
        "evidence watches re-run less: {watched} re-runs against {whole}"
    );
}
