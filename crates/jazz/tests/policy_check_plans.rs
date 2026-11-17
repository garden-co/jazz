//! Kept policy checks (`Inner::check_in`): a store keeps each policy
//! check's compiled statement per (schema, table, operation, author) and
//! runs it again with only the row and branch parameters changed. These
//! gates compare every kept check with one compiled afresh
//! (`Db::policy_check_for_test`), on SQLite file storage: after claims
//! change, after an enum variant gets its first tag, across a schema
//! change, and under a random mix of all of them.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{Db, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::protocol::{MigrationLens, TableLens};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder, TableSchemaBuilder,
    Value as Literal,
};

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use crate::fanout_correlation::{delete, id, insert, open, text, update, user, uuid};

const DOC: u8 = 0x41;
const TEAM: u8 = 0x42;
const MEMBER: u8 = 0x43;

fn enumeration(variants: &[&str]) -> ColumnType {
    ColumnType::Enum {
        variants: variants.iter().map(|v| (*v).to_owned()).collect(),
    }
}

/// `teams`, `members(team, user)`, `docs(team, owner, level, status)` with
/// `read` as the docs' read policy.
fn docs_schema(variants: &[&str], read: PolicyExpr) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("team", "teams")
                    .column("user", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .fk_column("team", "teams")
                    .column("owner", ColumnType::Text)
                    .column("level", ColumnType::Integer)
                    .column("status", enumeration(variants))
                    .policies(read_and_allow_all_writes(read)),
            )
            .build(),
    )
}

fn lens(source: &JazzSchema, target: &JazzSchema) -> MigrationLens {
    MigrationLens::new(
        source.version_id(),
        target.version_id(),
        ["teams", "members", "docs"]
            .into_iter()
            .map(|table| TableLens {
                source_table: table.into(),
                target_table: table.into(),
                ops: Vec::new(),
            })
            .collect(),
    )
    .unwrap()
}

fn claims(name: &str, role: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("\0claims:name".to_owned(), text(name)),
        ("\0claims:role".to_owned(), text(role)),
    ])
}

fn try_insert(
    db: &Db,
    table: &str,
    row: RowUuid,
    pairs: &[(&str, Value)],
) -> Result<(), jazz::db::Error> {
    block_on(
        db.insert(
            table,
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ),
    )
    .map(|_| ())
}

fn doc(db: &Db, n: usize, owner: &str, level: i32, status: &str) -> Result<(), jazz::db::Error> {
    try_insert(
        db,
        "docs",
        id(DOC, n),
        &[
            ("team", uuid(id(TEAM, n % 2))),
            ("owner", text(owner)),
            ("level", Value::I32(level)),
            ("status", text(status)),
        ],
    )
}

/// Every branch copy of `row`: the kept check equals a fresh one, as a
/// plain check and as one for a row just read. Returns the fresh answers.
fn same(db: &Db, row: RowUuid, author: AuthorSubject, what: &str) -> Vec<bool> {
    let mut answers = Vec::new();
    for operation in [Operation::Select, Operation::Update] {
        for existing in [false, true] {
            for (kept, fresh) in db
                .policy_check_for_test("docs", row, author, operation, existing)
                .unwrap()
            {
                assert_eq!(
                    kept, fresh,
                    "{what}: {operation:?} (existing {existing}) of {row:?} as {author:?}: the kept check differs from a fresh one"
                );
                answers.push(fresh);
            }
        }
    }
    answers
}

/// A variant with no tag yet: a policy comparing the enum column with it
/// binds the name as text and matches no cell. Then a write keeps that
/// variant (as an import may) and it gets its first tag, so a fresh
/// compile binds the tag and matches the new row. A check kept from before
/// must be compiled again, not keep binding the text.
#[test]
fn a_variant_tagged_after_a_check_was_kept_is_matched_by_its_tag() {
    let schema = || docs_schema(&["todo", "done"], session_eq("status", &["claims", "role"]));
    let db = open(schema(), 1);
    db.set_authority();
    db.set_identity_claims(user(0), claims("n0", "ghost"));
    insert(&db, "teams", id(TEAM, 0), &[("name", text("t0"))]);
    doc(&db, 0, "n0", 0, "todo").unwrap();
    // Select (plain, existing), then Update, which every row passes.
    assert_eq!(
        same(&db, id(DOC, 0), user(0), "a todo doc"),
        [false, false, true, true]
    );
    db.keep_variants_for_import(
        [(
            ("docs".into(), "status".into()),
            BTreeSet::from(["ghost".into()]),
        )]
        .into(),
    );
    doc(&db, 2, "n0", 0, "ghost").unwrap();
    assert_eq!(
        same(
            &db,
            id(DOC, 2),
            user(0),
            "a doc keeping the variant 'ghost'"
        ),
        [true; 4],
        "the ghost doc is readable to the ghost role"
    );
}

/// A check that always passes is not run for a row just read; a plain
/// check of a row the store does not hold still runs, and fails as a fresh
/// one does.
#[test]
fn an_always_passing_check_of_an_absent_row_is_still_run() {
    let db = open(docs_schema(&["todo", "done"], PolicyExpr::True), 1);
    insert(&db, "teams", id(TEAM, 0), &[("name", text("t0"))]);
    doc(&db, 0, "n0", 0, "todo").unwrap();
    assert_eq!(same(&db, id(DOC, 0), user(0), "a stored doc"), [true; 4]);
    for operation in [Operation::Select, Operation::Update] {
        let (kept, fresh) = db
            .absent_policy_check_for_test("docs", id(DOC, 9), id(DOC, 0), user(0), operation)
            .unwrap();
        assert_eq!(kept, fresh, "{operation:?} of an absent doc");
    }
}

/// A permissions-only change keeps the schema version (and the new schema
/// may take the old one's address): a check kept under the old policy is
/// not run under the new one.
#[test]
fn a_policy_change_of_one_version_drops_the_kept_checks() {
    let policy = |read| {
        docs_schema(
            &["todo", "done"],
            if read {
                PolicyExpr::True
            } else {
                PolicyExpr::False
            },
        )
    };
    assert_eq!(policy(true).version_id(), policy(false).version_id());
    let db = open(policy(true), 1);
    insert(&db, "teams", id(TEAM, 0), &[("name", text("t0"))]);
    doc(&db, 0, "n0", 0, "todo").unwrap();
    db.reuse_check_key_addresses_for_test();
    for round in 0..4 {
        let read = round % 2 == 0;
        if round > 0 {
            block_on(db.activate_schema(policy(read))).unwrap();
        }
        assert_eq!(
            same(&db, id(DOC, 0), user(0), &format!("round {round}"))[..2],
            [read; 2],
            "round {round}"
        );
    }
}

/// An author whose claims switch back and forth: every check under either
/// set equals a fresh one.
#[test]
fn alternating_claims_each_check_under_their_own() {
    let db = open(
        docs_schema(&["todo", "done"], session_eq("owner", &["claims", "name"])),
        1,
    );
    insert(&db, "teams", id(TEAM, 0), &[("name", text("t0"))]);
    doc(&db, 0, "n0", 0, "todo").unwrap();
    // Two sets alternate (both kept), then a third displaces one of them.
    for (round, name) in ["n0", "n1", "n0", "n1", "n2", "n0", "n1"]
        .into_iter()
        .enumerate()
    {
        db.set_identity_claims(user(0), claims(name, "todo"));
        let readable = same(
            &db,
            id(DOC, 0),
            user(0),
            &format!("round {round} as {name}"),
        );
        assert_eq!(readable[..2], [name == "n0"; 2], "round {round} as {name}");
    }
}

/// Kept checks are bounded: past the cap the store forgets them and
/// compiles again, and every check still equals a fresh one.
#[test]
fn kept_checks_stay_bounded_by_the_cap() {
    let db = open(
        docs_schema(&["todo", "done"], session_eq("owner", &["claims", "name"])),
        1,
    );
    db.set_check_plan_cap_for_test(8);
    insert(&db, "teams", id(TEAM, 0), &[("name", text("t0"))]);
    doc(&db, 0, "n3", 0, "todo").unwrap();
    for u in 0..40 {
        db.set_identity_claims(user(u), claims(&format!("n{u}"), "todo"));
        let answers = same(&db, id(DOC, 0), user(u), &format!("author {u}"));
        assert_eq!(answers.iter().all(|a| *a), u == 3);
        assert!(
            db.check_plan_keys_for_test() <= 8,
            "{} kept keys after {} authors",
            db.check_plan_keys_for_test(),
            u + 1
        );
    }
}

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

const NAMES: [&str; 4] = ["n0", "n1", "n2", "n3"];
const ROLES: [&str; 5] = ["todo", "doing", "done", "later", "ghost"];

/// A random read policy over docs: claims pins and literals (an enum one
/// may name a variant with no tag) under `NOT`/`AND`/`OR`, with or without
/// a membership `EXISTS` beside them (the core takes `EXISTS` only outside
/// a `NOT`).
fn policy(rng: &mut Rng) -> PolicyExpr {
    let plain = plain(rng, 2);
    let member = || {
        exists(
            "members",
            vec![
                outer_eq("team", "team"),
                session_eq("user", &["claims", "name"]),
            ],
        )
    };
    match rng.next(5) {
        0 => PolicyExpr::or(vec![plain, member()]),
        1 => PolicyExpr::and(vec![plain, member()]),
        2 => member(),
        _ => plain,
    }
}

fn plain(rng: &mut Rng, depth: usize) -> PolicyExpr {
    let atom = |rng: &mut Rng| match rng.next(7) {
        0 => PolicyExpr::True,
        1 => session_eq("owner", &["claims", "name"]),
        2 => session_eq("status", &["claims", "role"]),
        3 => PolicyExpr::eq_literal("level", Literal::Integer(rng.next(3) as i32)),
        4 => PolicyExpr::eq_literal("status", Literal::Text(ROLES[rng.next(ROLES.len())].into())),
        // A range compares ranks in declaration order, which the v2 swap
        // moves; its literal must be a declared variant.
        5 => PolicyExpr::Cmp {
            column: "status".into(),
            op: if rng.next(2) == 0 {
                CmpOp::Lt
            } else {
                CmpOp::Ge
            },
            value: PolicyValue::Literal(Literal::Text(ROLES[rng.next(3)].into())),
        },
        _ => PolicyExpr::False,
    };
    if depth == 0 {
        return atom(rng);
    }
    match rng.next(5) {
        0 => PolicyExpr::not(plain(rng, depth - 1)),
        1 => PolicyExpr::and(vec![plain(rng, depth - 1), plain(rng, depth - 1)]),
        2 => PolicyExpr::or(vec![plain(rng, depth - 1), plain(rng, depth - 1)]),
        _ => atom(rng),
    }
}

/// Random writes, claims changes, a variant tagged mid-run and a schema
/// change (a variant appended, another policy): after each, every kept
/// check of every doc for every reader equals a fresh one.
fn differential(seed_of: u64) {
    let mut rng = Rng(seed_of);
    let first = policy(&mut rng);
    let second = policy(&mut rng);
    let v1 = docs_schema(&["todo", "doing", "done"], first);
    let v2 = docs_schema(&["todo", "doing", "done", "later"], second);
    let db = open(v1.clone(), 1);
    db.set_authority();
    db.keep_variants_for_import(
        [(
            ("docs".into(), "status".into()),
            BTreeSet::from(["ghost".into()]),
        )]
        .into(),
    );
    for t in 0..2 {
        insert(
            &db,
            "teams",
            id(TEAM, t),
            &[("name", text(&format!("t{t}")))],
        );
    }
    for (u, name) in NAMES.into_iter().enumerate() {
        db.set_identity_claims(user(u), claims(name, ROLES[rng.next(ROLES.len())]));
    }
    let mut docs: Vec<usize> = Vec::new();
    let mut members: Vec<usize> = Vec::new();
    let (mut next_doc, mut next_member) = (0, 0);
    let mut swapped = false;
    for step in 0..120 {
        let what = match rng.next(8) {
            0 | 1 => {
                let status = ROLES[rng.next(if swapped { 5 } else { 3 })];
                let status = if status == "later" && !swapped {
                    "todo"
                } else {
                    status
                };
                match doc(
                    &db,
                    next_doc,
                    NAMES[rng.next(4)],
                    rng.next(3) as i32,
                    status,
                ) {
                    Ok(()) => {
                        docs.push(next_doc);
                        next_doc += 1;
                        format!("doc {} ({status})", next_doc - 1)
                    }
                    Err(error) => format!("doc refused: {error}"),
                }
            }
            2 if !docs.is_empty() => {
                let d = docs[rng.next(docs.len())];
                let pairs = match rng.next(3) {
                    0 => ("owner", text(NAMES[rng.next(4)])),
                    1 => ("level", Value::I32(rng.next(3) as i32)),
                    _ => ("status", text(ROLES[rng.next(3)])),
                };
                update(&db, "docs", id(DOC, d), &[pairs]);
                format!("doc {d} updated")
            }
            3 if !docs.is_empty() => {
                let d = docs.remove(rng.next(docs.len()));
                delete(&db, "docs", id(DOC, d));
                format!("doc {d} deleted")
            }
            4 => {
                insert(
                    &db,
                    "members",
                    id(MEMBER, next_member),
                    &[
                        ("team", uuid(id(TEAM, rng.next(2)))),
                        ("user", text(NAMES[rng.next(4)])),
                    ],
                );
                members.push(next_member);
                next_member += 1;
                format!("member {}", next_member - 1)
            }
            5 if !members.is_empty() => {
                let m = members.remove(rng.next(members.len()));
                delete(&db, "members", id(MEMBER, m));
                format!("member {m} deleted")
            }
            6 => {
                let u = rng.next(NAMES.len());
                let role = ROLES[rng.next(ROLES.len())];
                db.set_identity_claims(user(u), claims(NAMES[u], role));
                format!("user {u} role {role}")
            }
            7 if !swapped && step > 40 => {
                block_on(db.activate_schema_with_lenses(v2.clone(), &[lens(&v1, &v2)])).unwrap();
                swapped = true;
                "schema v2".into()
            }
            _ => continue,
        };
        for d in &docs {
            for u in 0..NAMES.len() {
                same(
                    &db,
                    id(DOC, *d),
                    user(u),
                    &format!("seed {seed_of} step {step}: {what}"),
                );
            }
        }
    }
}

#[test]
fn kept_checks_match_fresh_ones_under_random_policies_claims_and_schemas() {
    for seed_of in [3, 17, 29, 41, 58, 73] {
        differential(seed_of);
    }
}

/// A permissions-only activation that fails after the store has taken the
/// new policies as its own (recording the activation errs) still drops the
/// kept checks: a check kept under the old policy is not run under the new
/// one, even where the new schema takes the old one's address.
#[test]
fn a_policy_change_that_fails_to_record_drops_the_kept_checks() {
    use jazz::db::{DbConfig, DbIdentity, FileOptions};
    use jazz::ids::NodeUuid;
    let policy = |read| {
        docs_schema(
            &["todo", "done"],
            if read {
                PolicyExpr::True
            } else {
                PolicyExpr::False
            },
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let db = block_on(Db::open_at(
        DbConfig::new(
            policy(true),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    insert(&db, "teams", id(TEAM, 0), &[("name", text("t0"))]);
    doc(&db, 0, "n0", 0, "todo").unwrap();
    db.reuse_check_key_addresses_for_test();
    assert_eq!(same(&db, id(DOC, 0), user(0), "before")[..2], [true; 2]);
    // Recording the new permissions revision fails.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER refuse_permissions BEFORE INSERT ON _jazz_permissions \
             BEGIN SELECT RAISE(ABORT, 'refused'); END;",
        )
        .unwrap();
    assert!(block_on(db.activate_schema(policy(false))).is_err());
    let fresh = same(&db, id(DOC, 0), user(0), "after a failed activation");
    assert_eq!(fresh[..2], [fresh[0]; 2]);
}
