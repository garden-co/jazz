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

use jazz::db::{Db, InsertOptions, ReadOpts, ReadTier, WriteIdentity, block_on, duplex};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::member_chat::{claims, room_list, schema};
use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use crate::fanout_correlation::{
    Live, Snapshot, check, check_with, delete, id, insert, open, subscribe, subscribe_with, text,
    update, user, uuid,
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

/// Two writes of one membership in one transaction: the first hands
/// alice's membership of room 1 to person 2, the second bans it. Only the
/// version before the transaction is alice's.
///
/// ```
/// tx { members(room 1, alice).person = person 2; .banned = true }
///   ──► alice's bare rooms hide room 1
/// ```
#[test]
fn two_writes_of_a_membership_in_one_transaction() {
    let db = open(schema(), 1);
    let world = seed(&db);
    let mut lives = vec![subscribe(
        &db,
        "alice: bare rooms",
        Query::from("rooms").order_by("title", OrderDirection::Asc),
        Some(user(0)),
    )];
    check(&db, &mut lives, "subscribe");
    let mine = *world
        .members
        .iter()
        .find(|(_, (r, p))| *r == 1 && *p == 0)
        .unwrap()
        .0;
    let cells = |pairs: &[(&str, Value)]| -> BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    };
    block_on(db.transaction(async |tx| {
        tx.update(
            "members",
            id(MEMBER, mine),
            cells(&[("person", uuid(person(2)))]),
            Default::default(),
        )
        .await?;
        tx.update(
            "members",
            id(MEMBER, mine),
            cells(&[("banned", Value::Bool(true))]),
            Default::default(),
        )
        .await?;
        Ok(())
    }))
    .unwrap();
    let now = check(
        &db,
        &mut lives,
        "two writes of alice's membership in one tx",
    );
    assert!(!now[0].iter().any(|(row, _)| *row == room(1)));
}

/// Several versions of one row in one dispatch: a replica applies two
/// server pushes of alice's membership in one tick. The first hands it to
/// person 2, the second bans it. Neither the current version nor the one
/// before it in history is alice's; only the version her result last saw
/// is, and that alone must wake it.
///
/// ```
/// server ──members(room 1, alice).person = person 2──┐
/// server ──members(room 1, person 2).banned = true───┴─► replica tick:
///                                     alice's bare rooms hide room 1
/// ```
#[test]
fn two_versions_of_a_membership_in_one_replica_tick() {
    let server = open(schema(), 1);
    let client = open(schema(), 2);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let pump = || {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    let world = seed(&server);
    for p in 0..PEOPLE {
        client.set_identity_claims(user(p), claims(person(p)));
    }
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    // Synced in full, so the replica holds every row the policies read.
    let mut lives: Vec<Live> = ["rooms", "members", "people"]
        .into_iter()
        .map(|table| subscribe(&client, &format!("all {table}"), Query::from(table), None))
        .collect();
    lives.push(subscribe_with(
        &client,
        "alice: bare rooms",
        Query::from("rooms").order_by("title", OrderDirection::Asc),
        Some(user(0)),
        local.clone(),
    ));
    pump();
    let opened = check_with(&client, &mut lives, "sync", local.clone());
    assert!(opened[3].iter().any(|(row, _)| *row == room(1)));
    let mine = *world
        .members
        .iter()
        .find(|(_, (r, p))| *r == 1 && *p == 0)
        .unwrap()
        .0;
    update(
        &server,
        "members",
        id(MEMBER, mine),
        &[("person", uuid(person(2)))],
    );
    block_on(server.tick()).unwrap();
    update(
        &server,
        "members",
        id(MEMBER, mine),
        &[("banned", Value::Bool(true))],
    );
    block_on(server.tick()).unwrap();
    block_on(client.tick()).unwrap();
    let now = check_with(
        &client,
        &mut lives,
        "two versions of alice's membership in one tick",
        local.clone(),
    );
    assert!(!now[3].iter().any(|(row, _)| *row == room(1)));
    pump();
    check_with(&client, &mut lives, "settled", local);
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

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

/// Two updates of one row in one transaction.
fn update_twice(db: &Db, table: &str, row: RowUuid, a: &[(&str, Value)], b: &[(&str, Value)]) {
    let (a, b) = (cells(a), cells(b));
    block_on(db.transaction(async |tx| {
        tx.update(table, row, a, Default::default()).await?;
        tx.update(table, row, b, Default::default()).await?;
        Ok(())
    }))
    .unwrap();
}

/// One random write; what it was. On a replica's authority (`replica`)
/// claims stay put: a relay serving with its own copy would not follow.
fn random_write(db: &Db, rng: &mut Rng, world: &mut World, step: usize, replica: bool) -> String {
    let r = rng.next(ROOMS);
    let p = rng.next(PEOPLE);
    let any_member = |rng: &mut Rng, world: &World| {
        let keys: Vec<usize> = world.members.keys().copied().collect();
        (!keys.is_empty()).then(|| keys[rng.next(keys.len())])
    };
    match rng.next(17) {
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
        14 if !replica => {
            let u = rng.next(4);
            world.me[u] = p;
            db.set_identity_claims(user(u), claims(person(p)));
            format!("user {u}'s claim names person {p}")
        }
        14 | 16 => match any_member(rng, world) {
            Some(n) => {
                update_twice(
                    db,
                    "members",
                    id(MEMBER, n),
                    &[("person", uuid(person(p)))],
                    &[("banned", Value::Bool(rng.next(2) == 0))],
                );
                world.members.get_mut(&n).unwrap().1 = p;
                format!("member {n}: tx moves it to person {p} and bans or unbans it")
            }
            None => "nothing".into(),
        },
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
        let what = random_write(&db, &mut rng, &mut world, step, false);
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

// ---- a second schema: text, integer and enum pins, NOT EXISTS,
// ---- INHERITS_REFERENCING, a policy reading its own table ---------------

const TEAM: u8 = 0x21;
const SEAT: u8 = 0x22;
const PASS: u8 = 0x23;
const DOC: u8 = 0x24;
const NOTE: u8 = 0x25;

const TEAMS: usize = 4;
const ROLES: [&str; 3] = ["admin", "member", "guest"];

fn team(n: usize) -> RowUuid {
    id(TEAM, n)
}

fn name(u: usize) -> String {
    format!("n{u}")
}

/// User `u`'s claims: a name (text), a role (an enum variant) and a level
/// (an integer).
fn team_claims(u: usize, role: &str, level: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("\0claims:name".to_owned(), text(&name(u))),
        ("\0claims:role".to_owned(), text(role)),
        ("\0claims:level".to_owned(), Value::I64(level)),
    ])
}

/// ```text
/// teams ◄── seats(team, holder text, role enum, level int)
///   ▲   ◄── passes(team, level int)
///   └────── docs(team, title, tier) ◄── notes(doc, body)
/// ```
/// - seats: mine (`holder = claims.name`), or of a team I hold a seat in;
/// - docs: I hold a seat in the team, the team has a seat of my role, or
///   a pass of the team is at my level (`NOT EXISTS` is not supported by
///   the schema yet, so it cannot be gated here);
/// - notes: `INHERITS SELECT VIA doc`;
/// - teams: some doc of the team is readable (`INHERITS_REFERENCING`).
fn teams_schema() -> jazz::schema::JazzSchema {
    use jazz::tools::{Operation, PolicyExpr};
    let seated = |column: &str| {
        exists(
            "seats",
            vec![
                outer_eq("team", column),
                session_eq("holder", &["claims", "name"]),
            ],
        )
    };
    let read = read_and_allow_all_writes;
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .policies(read(PolicyExpr::inherits_referencing(
                        Operation::Select,
                        "docs",
                        "team",
                    ))),
            )
            .table(
                TableSchemaBuilder::new("seats")
                    .fk_column("team", "teams")
                    .column("holder", ColumnType::Text)
                    .column(
                        "role",
                        ColumnType::Enum {
                            variants: ROLES.iter().map(|r| (*r).to_owned()).collect(),
                        },
                    )
                    .column("level", ColumnType::Integer)
                    .policies(read(PolicyExpr::or(vec![
                        session_eq("holder", &["claims", "name"]),
                        seated("team"),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("passes")
                    .fk_column("team", "teams")
                    .column("level", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .fk_column("team", "teams")
                    .column("title", ColumnType::Text)
                    .column("tier", ColumnType::Integer)
                    .policies(read(PolicyExpr::or(vec![
                        seated("team"),
                        exists(
                            "seats",
                            vec![
                                outer_eq("team", "team"),
                                session_eq("role", &["claims", "role"]),
                            ],
                        ),
                        exists(
                            "passes",
                            vec![
                                outer_eq("team", "team"),
                                session_eq("level", &["claims", "level"]),
                            ],
                        ),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("doc", "docs")
                    .column("body", ColumnType::Text)
                    .policies(read(PolicyExpr::inherits(Operation::Select, "doc"))),
            )
            .build(),
    )
}

/// What the teams store holds: per kind, the live rows and the next id.
#[derive(Default)]
struct Rows {
    live: BTreeMap<u8, Vec<usize>>,
    next: BTreeMap<u8, usize>,
}

impl Rows {
    fn add(&mut self, kind: u8) -> usize {
        let n = *self.next.entry(kind).or_default();
        self.next.insert(kind, n + 1);
        self.live.entry(kind).or_default().push(n);
        n
    }

    fn any(&self, kind: u8, rng: &mut Rng) -> Option<usize> {
        let live = self.live.get(&kind)?;
        (!live.is_empty()).then(|| live[rng.next(live.len())])
    }

    fn remove(&mut self, kind: u8, n: usize) {
        if let Some(live) = self.live.get_mut(&kind) {
            live.retain(|m| *m != n);
        }
    }
}

fn seat(db: &Db, rows: &mut Rows, t: usize, u: usize, role: &str, level: i64) {
    let n = rows.add(SEAT);
    insert(
        db,
        "seats",
        id(SEAT, n),
        &[
            ("team", uuid(team(t))),
            ("holder", text(&name(u))),
            ("role", text(role)),
            ("level", Value::I32(level as i32)),
        ],
    );
}

fn doc(db: &Db, rows: &mut Rows, t: usize, tier: i64) {
    let n = rows.add(DOC);
    insert(
        db,
        "docs",
        id(DOC, n),
        &[
            ("team", uuid(team(t))),
            ("title", text(&format!("d{n}"))),
            ("tier", Value::I32(tier as i32)),
        ],
    );
}

fn note(db: &Db, rows: &mut Rows, d: usize) {
    let n = rows.add(NOTE);
    insert(
        db,
        "notes",
        id(NOTE, n),
        &[("doc", uuid(id(DOC, d))), ("body", text(&format!("b{n}")))],
    );
}

/// Teams 0..4, each with two docs and a note on each; user `u` holds a
/// seat in teams `u` and `u + 1`, with role and level by `u`; one pass.
fn seed_teams(db: &Db) -> Rows {
    let mut rows = Rows::default();
    for t in 0..TEAMS {
        insert(db, "teams", team(t), &[("name", text(&format!("t{t}")))]);
        for k in 0..2 {
            doc(db, &mut rows, t, k);
            let d = rows.next[&DOC] - 1;
            note(db, &mut rows, d);
        }
    }
    for u in 0..PEOPLE {
        set_team_claims(db, u);
        for t in [u % TEAMS, (u + 1) % TEAMS] {
            seat(db, &mut rows, t, u, ROLES[u % 3], (u % 3) as i64);
        }
    }
    let n = rows.add(PASS);
    insert(
        db,
        "passes",
        id(PASS, n),
        &[("team", uuid(team(2))), ("level", Value::I32(1))],
    );
    rows
}

fn set_team_claims(db: &Db, u: usize) {
    db.set_identity_claims(user(u), team_claims(u, ROLES[(u + 1) % 3], (u % 3) as i64));
}

fn team_queries(u: usize) -> Vec<(String, Query)> {
    use jazz::query::{col, eq, lit};
    vec![
        (
            format!("user {u}: teams with docs and notes"),
            Query::from("teams")
                .order_by("name", OrderDirection::Asc)
                .array_subquery(
                    ArraySubquery::new("docs_of", "docs", "team", "id")
                        .order_by("title", OrderDirection::Asc)
                        .nested(ArraySubquery::new("notes_of", "notes", "doc", "id")),
                ),
        ),
        (
            format!("user {u}: docs with notes and seats"),
            Query::from("docs")
                .order_by("title", OrderDirection::Asc)
                .array_subquery(ArraySubquery::new("notes_of", "notes", "doc", "id"))
                .array_subquery(ArraySubquery::new("seats_of", "seats", "team", "team")),
        ),
        (
            format!("user {u}: my seats"),
            Query::from("seats").filter(eq(col("holder"), lit(text(&name(u))))),
        ),
        (format!("user {u}: notes"), Query::from("notes")),
    ]
}

fn random_team_write(
    db: &Db,
    rng: &mut Rng,
    rows: &mut Rows,
    step: usize,
    replica: bool,
) -> String {
    let t = rng.next(TEAMS);
    let u = rng.next(PEOPLE);
    let role = ROLES[rng.next(3)];
    let level = rng.next(3) as i32;
    match rng.next(15) {
        0 | 1 => {
            seat(db, rows, t, u, role, level as i64);
            format!("seat: {} in team {t} as {role} at {level}", name(u))
        }
        2 => match rows.any(SEAT, rng) {
            Some(n) => {
                delete(db, "seats", id(SEAT, n));
                rows.remove(SEAT, n);
                format!("seat {n} deleted")
            }
            None => "nothing".into(),
        },
        3 => match rows.any(SEAT, rng) {
            Some(n) => {
                let (column, value) = match rng.next(4) {
                    0 => ("holder", text(&name(u))),
                    1 => ("role", text(role)),
                    2 => ("level", Value::I32(level)),
                    _ => ("team", uuid(team(t))),
                };
                update(db, "seats", id(SEAT, n), &[(column, value)]);
                format!("seat {n}: {column} changed")
            }
            None => "nothing".into(),
        },
        4 => match rows.any(SEAT, rng) {
            Some(n) => {
                update_twice(
                    db,
                    "seats",
                    id(SEAT, n),
                    &[("holder", text(&name(u)))],
                    &[("role", text(role))],
                );
                format!("seat {n}: tx to {} then {role}", name(u))
            }
            None => "nothing".into(),
        },
        5 => {
            let n = rows.add(PASS);
            insert(
                db,
                "passes",
                id(PASS, n),
                &[("team", uuid(team(t))), ("level", Value::I32(level))],
            );
            format!("pass {n}: team {t} at {level}")
        }
        6 => match rows.any(PASS, rng) {
            Some(n) if rng.next(2) == 0 => {
                delete(db, "passes", id(PASS, n));
                rows.remove(PASS, n);
                format!("pass {n} revoked")
            }
            Some(n) => {
                update(
                    db,
                    "passes",
                    id(PASS, n),
                    &[("level", Value::I32(level)), ("team", uuid(team(t)))],
                );
                format!("pass {n} moves to team {t} at {level}")
            }
            None => "nothing".into(),
        },
        7 => {
            doc(db, rows, t, level as i64);
            format!("doc in team {t}")
        }
        8 => match rows.any(DOC, rng) {
            Some(n) => {
                update(db, "docs", id(DOC, n), &[("team", uuid(team(t)))]);
                format!("doc {n} moves to team {t}")
            }
            None => "nothing".into(),
        },
        9 => match rows.any(DOC, rng) {
            Some(n) => {
                update(db, "docs", id(DOC, n), &[("tier", Value::I32(step as i32))]);
                format!("doc {n}: tier changed")
            }
            None => "nothing".into(),
        },
        10 => match rows.any(DOC, rng) {
            Some(d) => {
                note(db, rows, d);
                format!("note on doc {d}")
            }
            None => "nothing".into(),
        },
        11 => match (rows.any(NOTE, rng), rows.any(DOC, rng)) {
            (Some(n), Some(d)) => {
                update(db, "notes", id(NOTE, n), &[("doc", uuid(id(DOC, d)))]);
                format!("note {n} moves to doc {d}")
            }
            _ => "nothing".into(),
        },
        12 => {
            update(
                db,
                "teams",
                team(t),
                &[("name", text(&format!("t{t}.{step}")))],
            );
            format!("team {t} renamed")
        }
        13 if !replica => {
            let reader = rng.next(4);
            db.set_identity_claims(user(reader), team_claims(reader, role, level as i64));
            format!("user {reader}'s claims: {role} at {level}")
        }
        _ => match rows.any(NOTE, rng) {
            Some(n) => {
                update(
                    db,
                    "notes",
                    id(NOTE, n),
                    &[("body", text(&format!("b{step}")))],
                );
                format!("note {n} edited")
            }
            None => "nothing".into(),
        },
    }
}

/// A schema under test: its stores, the readers' queries, random writes.
struct Fixture {
    name: &'static str,
    schema: fn() -> jazz::schema::JazzSchema,
    /// Tables a replica syncs in full, so it holds every row its
    /// policies read.
    tables: &'static [&'static str],
    queries: fn(usize) -> Vec<(String, Query)>,
    /// Seeds a store; returns its random writer (`replica`: no claims
    /// changes).
    seed: fn(&Db) -> Writer,
    /// Sets every user's claims on a store that was not seeded.
    claims: fn(&Db),
}

type Writer = Box<dyn FnMut(&Db, &mut Rng, usize, bool) -> String>;

fn rooms_fixture() -> Fixture {
    Fixture {
        name: "rooms",
        schema,
        tables: &[
            "people", "handles", "rooms", "members", "calls", "seen", "bots",
        ],
        queries: |u| {
            vec![
                (format!("user {u}: room list"), room_list(person(u))),
                (format!("user {u}: readable rooms"), readable_rooms()),
            ]
        },
        seed: |db| {
            let mut world = seed(db);
            Box::new(move |db, rng, step, replica| random_write(db, rng, &mut world, step, replica))
        },
        claims: |db| {
            for p in 0..PEOPLE {
                db.set_identity_claims(user(p), claims(person(p)));
            }
        },
    }
}

fn teams_fixture() -> Fixture {
    Fixture {
        name: "teams",
        schema: teams_schema,
        tables: &["teams", "seats", "passes", "docs", "notes"],
        queries: team_queries,
        seed: |db| {
            let mut rows = seed_teams(db);
            Box::new(move |db, rng, step, replica| {
                random_team_write(db, rng, &mut rows, step, replica)
            })
        },
        claims: |db| {
            for u in 0..PEOPLE {
                set_team_claims(db, u);
            }
        },
    }
}

/// One seed's random writes on one store, evidence watches on or off.
fn store_run(
    fixture: &Fixture,
    seed_of: u64,
    pins: bool,
) -> (Vec<Snapshot>, jazz::db::FanoutStats) {
    jazz::db::set_policy_pins_for_test(pins);
    let db = open((fixture.schema)(), 1);
    let mut write = (fixture.seed)(&db);
    let mut lives: Vec<Live> = (0..4)
        .flat_map(|u| {
            (fixture.queries)(u)
                .into_iter()
                .map(move |(name, query)| (u, name, query))
        })
        .map(|(u, name, query)| subscribe(&db, &name, query, Some(user(u))))
        .collect();
    let mode = if pins { "pins" } else { "whole" };
    check(
        &db,
        &mut lives,
        &format!("{}: {mode}: subscribe", fixture.name),
    );
    let mut rng = Rng(seed_of);
    for step in 0..150 {
        let what = write(&db, &mut rng, step, false);
        check(
            &db,
            &mut lives,
            &format!(
                "{}: {mode}: seed {seed_of} step {step}: {what}",
                fixture.name
            ),
        );
    }
    jazz::db::set_policy_pins_for_test(true);
    let rows = lives.into_iter().map(|live| live.rows).collect();
    (rows, db.fanout_stats())
}

/// The second schema's random writes (text, integer and enum pins, a
/// `INHERITS` and `INHERITS_REFERENCING`, a policy reading
/// its own table, transactions writing one row twice, claims changes):
/// with evidence watches on and off, every result equals a fresh read
/// after every write, both runs end equal, and the watched run re-runs
/// less.
#[test]
fn a_second_schema_matches_one_shot_reads_under_random_writes() {
    let fixture = teams_fixture();
    let mut totals = [(0u64, 0u64); 2];
    for seed_of in [7, 41, 88, 133] {
        let on = store_run(&fixture, seed_of, true);
        let off = store_run(&fixture, seed_of, false);
        assert_eq!(on.0, off.0, "seed {seed_of}: watched and whole runs differ");
        for (total, (_, stats)) in totals.iter_mut().zip([&on, &off]) {
            total.0 += stats.reran;
            total.1 += stats.unevidenced;
        }
    }
    let [(watched, skipped), (whole, none)] = totals;
    assert_eq!(none, 0);
    assert!(skipped > 0, "evidence watches skipped changes: {skipped}");
    assert!(watched < whole, "{watched} re-runs against {whole}");
}

#[derive(Clone, Copy, Debug)]
enum Topology {
    /// Connected to the authority, served as user 0: the authority's
    /// results for it are identity-bound, policy-checked groups.
    DirectAs,
    /// Through a relay served as SYSTEM that serves the leaf as user 0.
    RelayAs,
    /// Connected to the authority as SYSTEM, every table synced; the
    /// leaf's own results are read as users, with policies checked
    /// locally, over rows the authority pushes.
    Local,
}

/// Batches of one to four authority writes, each shipped in a tick of
/// its own and applied by the leaf in one tick, so the leaf (and a relay)
/// dispatch several versions of a row at once. After each batch the
/// leaf's maintained results equal fresh local reads, and (served as a
/// user) the authority's read for the same identity.
fn replica_run(fixture: &Fixture, topology: Topology, seed_of: u64) {
    use jazz::db::PreparedQuery;
    let served_as = match topology {
        Topology::Local => AuthorSubject::SYSTEM,
        _ => user(0),
    };
    let server = open((fixture.schema)(), 1);
    let relay = matches!(topology, Topology::RelayAs).then(|| open((fixture.schema)(), 3));
    let client = crate::fanout_correlation::open_as((fixture.schema)(), 2, served_as);
    let mut links = Vec::new();
    match &relay {
        None => {
            let (client_side, server_side) = duplex();
            links.push(block_on(client.connect_upstream(client_side)));
            server.accept_subscriber(server_side, served_as).unwrap();
        }
        Some(relay) => {
            let (relay_side, server_side) = duplex();
            links.push(block_on(relay.connect_upstream(relay_side)));
            server
                .accept_subscriber(server_side, AuthorSubject::SYSTEM)
                .unwrap();
            relay.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
            let (client_side, relay_down) = duplex();
            links.push(block_on(client.connect_upstream(client_side)));
            relay.accept_subscriber(relay_down, served_as).unwrap();
            (fixture.claims)(relay);
        }
    }
    (fixture.claims)(&client);
    let pump = || {
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            if let Some(relay) = &relay {
                block_on(relay.tick()).unwrap();
            }
            block_on(server.tick()).unwrap();
            if let Some(relay) = &relay {
                block_on(relay.tick()).unwrap();
            }
        }
        block_on(client.tick()).unwrap();
    };
    let mut write = (fixture.seed)(&server);
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let queries = (fixture.queries)(0);
    let mut lives: Vec<Live> = match topology {
        Topology::Local => (0..4)
            .flat_map(|u| (fixture.queries)(u).into_iter().map(move |q| (u, q)))
            .map(|(u, (name, query))| {
                subscribe_with(&client, &name, query, Some(user(u)), local.clone())
            })
            .collect(),
        _ => queries
            .iter()
            .map(|(name, query)| subscribe(&client, name, query.clone(), None))
            .collect(),
    };
    let watched = lives.len();
    for table in fixture.tables {
        lives.push(subscribe(&client, table, Query::from(*table), None));
    }
    let authority: Vec<PreparedQuery> = queries
        .iter()
        .map(|(_, query)| server.prepare_query(query).unwrap())
        .collect();
    let converged = |lives: &[Live], after: &str| {
        if matches!(topology, Topology::Local) {
            return;
        }
        for (live, query) in lives[..watched].iter().zip(&authority) {
            let truth = crate::fanout_correlation::snapshot(
                &block_on(server.all_nested_for_test(query, ReadOpts::default(), Some(user(0))))
                    .unwrap(),
            );
            assert_eq!(
                live.rows, truth,
                "{} {topology:?} {}: the leaf differs from the authority after {after}",
                fixture.name, live.name
            );
        }
    };
    pump();
    check_with(&client, &mut lives, "sync", local.clone());
    converged(&lives, "sync");
    let mut rng = Rng(seed_of);
    for step in 0..60 {
        let mut what = Vec::new();
        for _ in 0..1 + rng.next(4) {
            what.push(write(&server, &mut rng, step, true));
            block_on(server.tick()).unwrap();
            if let Some(relay) = &relay {
                block_on(relay.tick()).unwrap();
            }
        }
        block_on(client.tick()).unwrap();
        let after = format!(
            "{} {topology:?} seed {seed_of} step {step}: {what:?}",
            fixture.name
        );
        check_with(&client, &mut lives, &after, local.clone());
        pump();
        check_with(&client, &mut lives, &after, local.clone());
        converged(&lives, &after);
    }
}

/// Both schemas, through each topology, with batched pushes.
#[test]
fn evidence_watched_results_match_on_replicas_under_batched_pushes() {
    for fixture in [rooms_fixture(), teams_fixture()] {
        for (topology, seed_of) in [
            (Topology::DirectAs, 3),
            (Topology::RelayAs, 19),
            (Topology::Local, 37),
        ] {
            replica_run(&fixture, topology, seed_of);
        }
    }
}
