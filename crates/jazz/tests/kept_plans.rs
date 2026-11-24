//! Kept read statements of live results (`fanout.rs`, `db.rs`): a live
//! result keeps its relations' compiled children statements across runs
//! (`Inner::nest_relations_kept`) and its re-check's root statement per
//! number of ids (`Inner::root_plan`), and rebinds only the parents or the
//! ids. A kept statement is used only while what it was compiled from is
//! unchanged: the schema, the reader's claims and the store's enum tags.
//!
//! Every result here is compared with a fresh one-shot read after every
//! write, on SQLite file storage (`fanout_correlation::check`), and, with
//! the plan oracle the store helpers turn on, every kept statement with
//! one compiled afresh. The gates show the statements are kept (the
//! counters) and that a variant tagged after a statement was kept is
//! matched by its tag; the differential mixes writes, claims changes and
//! a variant tagged mid-run.

use jazz_types::legacy_test_future::ResultFutureExt;
use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{Db, block_on, duplex};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};
use crate::fanout_correlation::{
    Live, check, id, insert, open, open_as, subscribe, text, update, user, uuid,
};

const TEAM: u8 = 0x51;
const DOC: u8 = 0x52;
const NOTE: u8 = 0x53;
const REVIEW: u8 = 0x54;

/// `teams`, `docs(team, status, title)`, `notes(doc, body)` and
/// `reviews(doc, body)`, docs readable where their status is the reader's
/// `role` claim.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .fk_column("team", "teams")
                    .column(
                        "status",
                        ColumnType::Enum {
                            variants: vec!["todo".to_owned(), "done".to_owned()],
                        },
                    )
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(role_reads())),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("doc", "docs")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("reviews")
                    .fk_column("doc", "docs")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// Readable where the status (or, never in these tests, the title) is the
/// reader's `role` claim. The second arm keeps the policy from pinning
/// the root to one status: a result watched by that key would not wake
/// for a variant tagged after it subscribed at all (a separate, older
/// staleness of `stored_canon`, which these gates are not about), so the
/// docs are watched whole and every write reaches the kept statements.
fn role_reads() -> PolicyExpr {
    PolicyExpr::Or(vec![
        session_eq("status", &["claims", "role"]),
        session_eq("title", &["claims", "role"]),
    ])
}

fn claims(role: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("\0claims:role".to_owned(), text(role))])
}

fn team(db: &Db, n: usize) {
    insert(
        db,
        "teams",
        id(TEAM, n),
        &[("name", text(&format!("t{n}")))],
    );
}

fn doc(db: &Db, n: usize, team: usize, status: &str) {
    insert(
        db,
        "docs",
        id(DOC, n),
        &[
            ("team", uuid(id(TEAM, team))),
            ("status", text(status)),
            ("title", text(&format!("d{n}"))),
        ],
    );
}

fn retitle(db: &Db, n: usize, title: &str) {
    update(db, "docs", id(DOC, n), &[("title", text(title))]);
}

/// Teams with their docs: the relation statement a run keeps.
fn teams_with_docs() -> Query {
    Query::from("teams").array_subquery(ArraySubquery::new("docs", "docs", "team", "id"))
}

/// Docs alone: re-checked by id, the root statement it keeps.
fn docs() -> Query {
    Query::from("docs")
}

/// Let a store keep `variant` of `docs.status`, as an import of rows
/// holding a removed variant does: the next write of it tags it.
fn keep_variant(db: &Db, variant: &str) {
    db.keep_variants_for_import(
        [(
            ("docs".to_owned(), "status".to_owned()),
            BTreeSet::from([variant.to_owned()]),
        )]
        .into(),
    );
}

fn store(role: &str) -> crate::fanout_correlation::Store {
    let db = open(schema(), 1);
    jazz::db::block_on(db.set_authority());
    jazz::db::block_on(db.set_identity_claims(user(0), claims(role)));
    db
}

/// G1. A live result's relation statement is compiled once and reused by
/// its later runs: each refresh after the first binds the new parents into
/// the kept statement.
#[test]
fn a_live_results_relation_statement_is_kept_across_runs() {
    let db = store("todo");
    team(&db, 0);
    doc(&db, 0, 0, "todo");
    let mut lives = vec![subscribe(&db, "teams", teams_with_docs(), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    for n in 1..4 {
        doc(&db, n, 0, "todo");
        check(&db, &mut lives, &format!("doc {n}"));
    }
    assert!(
        db.fanout_stats().kept_plans >= 2,
        "the later runs reused the relation statement: {:?}",
        db.fanout_stats()
    );
}

/// G2. A re-check's root statement is compiled once per number of ids and
/// reused: each later re-check of one id binds it into the kept statement.
#[test]
fn a_recheck_root_statement_is_kept_per_number_of_ids() {
    let db = store("todo");
    team(&db, 0);
    for n in 0..3 {
        doc(&db, n, 0, "todo");
    }
    let mut lives = vec![subscribe(&db, "docs", docs(), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    for n in 0..3 {
        let before = db.fanout_stats().rechecked;
        retitle(&db, n, &format!("renamed {n}"));
        check(&db, &mut lives, &format!("retitle {n}"));
        assert_eq!(
            db.fanout_stats().rechecked,
            before + 1,
            "doc {n} was re-checked"
        );
    }
    assert_eq!(
        db.fanout_stats().kept_root_plans,
        2,
        "the second and third re-checks of one id reused the first one's statement"
    );
}

/// G3. A variant with no tag yet: the docs' read policy compares the enum
/// column with the reader's `role` claim, bound as text, which matches no
/// cell. A relation statement kept then would keep binding the text after
/// a write tags the variant, and miss the new row a fresh read returns.
/// (The plan oracle is off here, so a stale statement shows as a stale
/// result rather than as the oracle's panic.)
#[test]
fn a_variant_tagged_after_a_relation_statement_was_kept_is_matched_by_its_tag() {
    let db = store("ghost");
    jazz::db::set_plan_oracle_for_test(false);
    team(&db, 0);
    let mut lives = vec![subscribe(&db, "teams", teams_with_docs(), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    doc(&db, 0, 0, "todo");
    check(&db, &mut lives, "a todo doc (kept: 'ghost' as text)");
    doc(&db, 1, 0, "done");
    check(&db, &mut lives, "a done doc (the kept statement)");
    keep_variant(&db, "ghost");
    doc(&db, 2, 0, "ghost");
    let rows = check(&db, &mut lives, "a ghost doc (tagged now)");
    let docs = rows[0][0]
        .1
        .iter()
        .find(|(name, _)| name == "docs")
        .and_then(|(_, value)| value.clone());
    assert!(
        matches!(&docs, Some(Value::Array(docs)) if docs.len() == 1),
        "the ghost doc, and only it, is in the team's docs: {docs:?}"
    );
}

/// G4. The same for a re-check's kept root statement.
#[test]
fn a_variant_tagged_after_a_recheck_statement_was_kept_is_matched_by_its_tag() {
    let db = store("ghost");
    jazz::db::set_plan_oracle_for_test(false);
    team(&db, 0);
    doc(&db, 0, 0, "todo");
    doc(&db, 1, 0, "todo");
    let mut lives = vec![subscribe(&db, "docs", docs(), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    retitle(&db, 0, "renamed");
    check(&db, &mut lives, "re-check one id (kept: 'ghost' as text)");
    retitle(&db, 1, "renamed");
    check(&db, &mut lives, "re-check one id (the kept statement)");
    keep_variant(&db, "ghost");
    doc(&db, 2, 0, "ghost");
    let rows = check(&db, &mut lives, "a ghost doc (tagged now)");
    assert_eq!(
        rows[0].len(),
        1,
        "the ghost doc is readable to the ghost role"
    );
}

/// Retitle docs `ns` in one transaction: the result re-checks them as one
/// batch of `ns.len()` ids.
fn retitle_together(db: &Db, ns: &[usize], title: &str) {
    jazz::db::block_on(async {
        let tx = db.mergeable_tx().await?;
        for n in ns {
            tx.update(
                "docs",
                id(DOC, *n),
                BTreeMap::from([("title".to_owned(), text(title))]),
                Default::default(),
            )
            .await?;
        }
        tx.commit().await
    })
    .unwrap();
}

/// G5. Root statements are kept per number of ids: a re-check of two ids
/// after re-checks of one compiles its own statement, binding both, and
/// the next re-check of two reuses that one.
#[test]
fn a_recheck_of_more_ids_compiles_its_own_kept_statement() {
    let db = store("todo");
    team(&db, 0);
    for n in 0..4 {
        doc(&db, n, 0, "todo");
    }
    let mut lives = vec![subscribe(&db, "docs", docs(), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    retitle(&db, 0, "one");
    check(&db, &mut lives, "re-check one id");
    retitle(&db, 1, "one");
    check(&db, &mut lives, "re-check one id (kept)");
    let before = db.fanout_stats();
    retitle_together(&db, &[2, 3], "two");
    check(&db, &mut lives, "re-check two ids");
    retitle_together(&db, &[0, 1], "two");
    let rows = check(&db, &mut lives, "re-check two ids (kept)");
    let after = db.fanout_stats();
    assert_eq!(
        (
            after.rechecked - before.rechecked,
            after.reran - before.reran
        ),
        (2, 0),
        "both transactions were re-checked, not re-run"
    );
    assert_eq!(
        after.kept_root_plans - before.kept_root_plans,
        1,
        "the second re-check of two ids reused the first one's statement"
    );
    assert_eq!(rows[0].len(), 4, "every doc is still in the result");
}

/// G6. Two levels reading relations of one name (teams' `items` are docs,
/// docs' `items` are notes) keep two statements: a statement is kept per
/// path from the root, not per name.
#[test]
fn relations_of_one_name_at_two_levels_keep_their_own_statements() {
    let db = store("todo");
    team(&db, 0);
    doc(&db, 0, 0, "todo");
    let query = Query::from("teams").array_subquery(
        ArraySubquery::new("items", "docs", "team", "id")
            .nested(ArraySubquery::new("items", "notes", "doc", "id")),
    );
    let mut lives = vec![subscribe(&db, "teams", query, Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    for n in 0..3 {
        insert(
            &db,
            "notes",
            id(NOTE, n),
            &[("doc", uuid(id(DOC, 0))), ("body", text(&format!("n{n}")))],
        );
        check(&db, &mut lives, &format!("note {n}"));
    }
    doc(&db, 1, 0, "todo");
    check(&db, &mut lives, "a second doc");
    assert!(
        db.fanout_stats().kept_plans >= 2,
        "the later runs reused both levels' statements: {:?}",
        db.fanout_stats()
    );
}

/// G7. While the store serves a session, the session's vouched claims
/// stand for its identity's (`Db::serve`), with no refresh of the
/// identity's results before or after. A result of that identity woken
/// inside that window (here by the session's upload) compiles under the
/// vouched claims: the statements kept under the stored claims are not
/// used there, nor the window's afterwards. Every doc is readable under
/// either role (its status is one, its title the other), so the result
/// is the same whichever claims a run read under and only the statements
/// differ: the plan oracle compares them. (A run in the window reads what
/// the vouched claims allow, kept statements or not: that is
/// `with_claims`, not this gate's subject.)
#[test]
fn a_run_under_a_sessions_vouched_claims_uses_no_statement_kept_under_the_stored_ones() {
    let server = store("todo");
    team(&server, 0);
    doc(&server, 0, 0, "todo");
    doc(&server, 1, 0, "todo");
    let mut lives = vec![
        subscribe(&server, "docs", docs(), Some(user(0))),
        subscribe(&server, "teams", teams_with_docs(), Some(user(0))),
    ];
    check(&server, &mut lives, "subscribe");
    // Titled "done", the docs are readable under either role.
    retitle(&server, 0, "done");
    check(&server, &mut lives, "re-check one id (kept under 'todo')");
    retitle(&server, 1, "done");
    check(&server, &mut lives, "re-check one id (the kept statement)");

    // The same user's replica, on a link that vouches another role.
    let client = open_as(schema(), 2, user(0));
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber_with_claims(server_side, user(0), Some(claims("done")))
        .unwrap();
    let before = server.fanout_stats();
    insert(
        &client,
        "docs",
        id(DOC, 2),
        &[
            ("team", uuid(id(TEAM, 0))),
            ("status", text("todo")),
            ("title", text("done")),
        ],
    );
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    let rows = check(&server, &mut lives, "an upload served under vouched claims");
    assert_eq!(rows[0].len(), 3, "the uploaded doc reached the docs result");
    let served = server.fanout_stats();
    assert!(
        served.rechecked > before.rechecked,
        "the upload re-checked the docs result: {before:?} -> {served:?}"
    );
    assert_eq!(
        served.kept_root_plans, before.kept_root_plans,
        "the re-check under the vouched claims compiled its own statement"
    );

    retitle(&server, 0, "done");
    check(&server, &mut lives, "re-check one id (under 'todo' again)");
    assert_eq!(
        server.fanout_stats().kept_root_plans,
        served.kept_root_plans,
        "nor is the statement compiled under the vouched claims used afterwards"
    );
    retitle(&server, 1, "done");
    check(&server, &mut lives, "re-check one id (the kept statement)");
    assert_eq!(
        server.fanout_stats().kept_root_plans,
        served.kept_root_plans + 1
    );
}

/// Teams with their docs, each doc with its notes (optional) and its
/// reviews (required: a doc without a review is not in the relation).
fn teams_with_reviewed_docs() -> Query {
    Query::from("teams").array_subquery(
        ArraySubquery::new("docs", "docs", "team", "id")
            .nested(ArraySubquery::new("notes", "notes", "doc", "id"))
            .nested(
                ArraySubquery::new("reviews", "reviews", "doc", "id")
                    .requirement(ArraySubqueryRequirement::AtLeastOne),
            ),
    )
}

fn note(db: &Db, n: usize, doc: usize) {
    insert(
        db,
        "notes",
        id(NOTE, n),
        &[
            ("doc", uuid(id(DOC, doc))),
            ("body", text(&format!("n{n}"))),
        ],
    );
}

/// A doc's required sibling relation is part of the docs statement (it
/// filters the docs). A write under the optional sibling (a note) scopes
/// the re-nest to `docs.notes`; the docs level must still be read with
/// its required `reviews`, or the statement kept for the path `docs`
/// would be the one without the requirement, and every later run would
/// reuse it and list the doc that has no review.
fn required_sibling(plan_oracle: bool) {
    let db = store("todo");
    jazz::db::set_plan_oracle_for_test(plan_oracle);
    team(&db, 0);
    doc(&db, 0, 0, "todo");
    doc(&db, 1, 0, "todo");
    insert(
        &db,
        "reviews",
        id(REVIEW, 0),
        &[("doc", uuid(id(DOC, 0))), ("body", text("ok"))],
    );
    let mut lives = vec![subscribe(
        &db,
        "teams",
        teams_with_reviewed_docs(),
        Some(user(0)),
    )];
    check(&db, &mut lives, "subscribe");
    let before = db.fanout_stats();
    note(&db, 0, 0);
    check(&db, &mut lives, "a note under the reviewed doc");
    let after = db.fanout_stats();
    assert!(
        after.renested > before.renested,
        "the note re-nested the team in place: {before:?} -> {after:?}"
    );
    note(&db, 1, 0);
    check(&db, &mut lives, "a second note");
    update(&db, "teams", id(TEAM, 0), &[("name", text("renamed"))]);
    let rows = check(
        &db,
        &mut lives,
        "the team renamed (re-checked with its relations)",
    );
    let docs = rows[0][0]
        .1
        .iter()
        .find(|(name, _)| name == "docs")
        .and_then(|(_, value)| value.clone());
    assert!(
        matches!(&docs, Some(Value::Array(docs)) if docs.len() == 1),
        "only the reviewed doc is in the team's docs: {docs:?}"
    );
    assert!(
        db.fanout_stats().kept_plans > 0,
        "the later runs reused kept statements: {:?}",
        db.fanout_stats()
    );
}

/// G8. With the plan oracle (a kept statement that lost the requirement
/// differs from a fresh one).
#[test]
fn a_scoped_re_nest_keeps_a_levels_statement_with_its_required_sibling() {
    required_sibling(true);
}

/// G8, without the plan oracle: a statement kept without the requirement
/// shows as a doc that a fresh read does not return.
#[test]
fn a_scoped_re_nest_keeps_a_levels_required_sibling_in_later_results() {
    required_sibling(false);
}

/// G9. A result keeps root statements for at most `ROOT_PLANS_MAX` (32)
/// numbers of ids: the 33rd number starts the map over, so a re-check of
/// a number kept before compiles again.
#[test]
fn root_statements_are_kept_for_a_bounded_number_of_id_counts() {
    const COUNTS: usize = 33;
    let db = store("todo");
    team(&db, 0);
    for n in 0..COUNTS {
        doc(&db, n, 0, "todo");
    }
    let mut lives = vec![subscribe(&db, "docs", docs(), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    let all: Vec<usize> = (0..COUNTS).collect();
    for count in 1..=COUNTS {
        let before = db.fanout_stats();
        retitle_together(&db, &all[..count], &format!("of {count}"));
        check(&db, &mut lives, &format!("re-check {count} ids"));
        let after = db.fanout_stats();
        assert_eq!(
            (
                after.rechecked - before.rechecked,
                after.reran - before.reran
            ),
            (1, 0),
            "{count} ids were re-checked, not re-run"
        );
        assert_eq!(
            after.kept_root_plans, before.kept_root_plans,
            "a first re-check of {count} ids compiles its statement"
        );
    }
    let before = db.fanout_stats().kept_root_plans;
    retitle(&db, 0, "one again");
    check(&db, &mut lives, "re-check one id, past the bound");
    assert_eq!(
        db.fanout_stats().kept_root_plans,
        before,
        "the 33rd count started the map over: one id compiles again"
    );
    retitle(&db, 1, "one again");
    check(&db, &mut lives, "re-check one id (kept again)");
    assert_eq!(db.fanout_stats().kept_root_plans, before + 1);
}

// ---- randomized differential ------------------------------------------------

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

const TEAMS: usize = 3;
const DOCS: usize = 12;

/// Random doc writes (inserts, status and title changes, team moves),
/// claims changes and, once, a variant tagged mid-run; live results of
/// both kinds (relation statements, re-check statements), as two readers.
/// After every step each result equals a fresh read, and every kept
/// statement a fresh compile (the plan oracle).
fn differential(seed: u64) {
    let db = store("todo");
    jazz::db::block_on(db.set_identity_claims(user(1), claims("done")));
    let mut rng = Rng(seed);
    for n in 0..TEAMS {
        team(&db, n);
    }
    let mut lives: Vec<Live> = Vec::new();
    for reader in 0..2 {
        lives.push(subscribe(
            &db,
            &format!("teams as u{reader}"),
            teams_with_docs(),
            Some(user(reader)),
        ));
        lives.push(subscribe(
            &db,
            &format!("docs as u{reader}"),
            docs(),
            Some(user(reader)),
        ));
    }
    check(&db, &mut lives, "subscribe");
    let mut present: BTreeSet<usize> = BTreeSet::new();
    let mut variants = vec!["todo", "done"];
    let tag_at = 20 + rng.next(60);
    for step in 0..120 {
        if step == tag_at {
            keep_variant(&db, "ghost");
            variants.push("ghost");
        }
        let n = rng.next(DOCS);
        let status = variants[rng.next(variants.len())];
        let what = match rng.next(6) {
            0 | 1 if !present.contains(&n) => {
                doc(&db, n, rng.next(TEAMS), status);
                present.insert(n);
                format!("insert d{n} {status}")
            }
            0 | 1 | 2 => {
                if !present.contains(&n) {
                    continue;
                }
                update(&db, "docs", id(DOC, n), &[("status", text(status))]);
                format!("d{n} status {status}")
            }
            3 => {
                if !present.contains(&n) {
                    continue;
                }
                retitle(&db, n, &format!("s{step}"));
                format!("retitle d{n}")
            }
            4 => {
                if !present.contains(&n) {
                    continue;
                }
                let to = rng.next(TEAMS);
                update(&db, "docs", id(DOC, n), &[("team", uuid(id(TEAM, to)))]);
                format!("d{n} to t{to}")
            }
            _ => {
                let reader = rng.next(2);
                let role = variants[rng.next(variants.len())];
                jazz::db::block_on(db.set_identity_claims(user(reader), claims(role)));
                format!("u{reader} role {role}")
            }
        };
        check(&db, &mut lives, &format!("seed {seed} step {step}: {what}"));
    }
    let stats = db.fanout_stats();
    assert!(
        stats.kept_plans > 0 && stats.kept_root_plans > 0,
        "seed {seed}: both kinds of kept statement were used: {stats:?}"
    );
}

#[test]
fn kept_statements_match_fresh_reads_under_random_writes_claims_and_tags() {
    for seed in 1..=6 {
        differential(seed);
    }
}
