//! A live result keyed by an enum variant the store has not tagged yet.
//!
//! An enum column stores each variant as its tag. A variant the current
//! schema no longer declares can still be kept by a store (an import of
//! rows holding it, `Db::keep_variants_for_import`); it gets its tag only
//! when a row first stores it. A live result whose query filter or read
//! policy pins its root to such a variant is watched by that variant's
//! key: the key must name the tag the rows will carry, so the first row
//! stored with the variant wakes the result.
//!
//! A variant that has a tag can be renumbered (the current schema's
//! declaration order is tag order): the keys are rebuilt with the schema.
//!
//! Every result is compared with a fresh one-shot read after every write
//! (`fanout_correlation::check`), on SQLite file storage.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{Db, ReadOpts, ReadTier, block_on, duplex};
use jazz::ids::AuthorSubject;
use jazz::protocol::{MigrationLens, TableLens};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use crate::fanout_correlation::{
    check, check_with, id, insert, open, open_as, snapshot, subscribe, text, update, user, uuid,
};

const DOC: u8 = 0x61;
const FLAG: u8 = 0x62;
const SEAT: u8 = 0x64;

fn schema(policy: Option<PolicyExpr>) -> JazzSchema {
    schema_of(&["todo", "done"], policy)
}

fn schema_of(variants: &[&str], policy: Option<PolicyExpr>) -> JazzSchema {
    let docs = TableSchemaBuilder::new("docs")
        .column(
            "status",
            ColumnType::Enum {
                variants: variants.iter().map(|v| (*v).to_owned()).collect(),
            },
        )
        .column("title", ColumnType::Text);
    let docs = match policy {
        Some(policy) => docs.policies(read_and_allow_all_writes(policy)),
        None => docs.policies(allow_all_policies()),
    };
    compile_schema(&SchemaBuilder::new().table(docs).build())
}

/// Let `db` keep `variant` of `docs.status` untagged, as an import of rows
/// holding a removed variant does.
fn keep_variant(db: &Db, variant: &str) {
    keep(db, "docs", "status", variant);
}

fn keep(db: &Db, table: &str, column: &str, variant: &str) {
    db.keep_variants_for_import(
        [(
            (table.to_owned(), column.to_owned()),
            BTreeSet::from([variant.to_owned()]),
        )]
        .into(),
    );
}

/// `docs` readable by whoever's `role` claim names the kind of some flag:
/// the policy reads `flags` as evidence, pinned to that kind.
fn flagged() -> JazzSchema {
    let docs = TableSchemaBuilder::new("docs")
        .column(
            "status",
            ColumnType::Enum {
                variants: vec!["todo".to_owned(), "done".to_owned()],
            },
        )
        .column("title", ColumnType::Text)
        .policies(read_and_allow_all_writes(exists(
            "flags",
            vec![session_eq("kind", &["claims", "role"])],
        )));
    let flags = TableSchemaBuilder::new("flags")
        .column(
            "kind",
            ColumnType::Enum {
                variants: vec!["todo".to_owned(), "done".to_owned()],
            },
        )
        .policies(allow_all_policies());
    compile_schema(&SchemaBuilder::new().table(docs).table(flags).build())
}

/// `docs` readable where a seat points at them; a seat readable by whoever's
/// `role` claim names the kind of some flag. A reader of docs is sent the
/// seats as the support of the docs' policy, where it may read them.
fn seated() -> JazzSchema {
    let kinds = || ColumnType::Enum {
        variants: vec!["todo".to_owned(), "done".to_owned()],
    };
    let docs = TableSchemaBuilder::new("docs")
        .column("status", kinds())
        .column("title", ColumnType::Text)
        .policies(read_and_allow_all_writes(exists(
            "seats",
            vec![outer_eq("doc", "id")],
        )));
    let seats = TableSchemaBuilder::new("seats")
        .fk_column("doc", "docs")
        .policies(read_and_allow_all_writes(exists(
            "flags",
            vec![session_eq("kind", &["claims", "role"])],
        )));
    let flags = TableSchemaBuilder::new("flags")
        .column("kind", kinds())
        .policies(allow_all_policies());
    compile_schema(
        &SchemaBuilder::new()
            .table(docs)
            .table(seats)
            .table(flags)
            .build(),
    )
}

fn archived_role(db: &Db) {
    db.set_identity_claims(
        user(0),
        BTreeMap::from([("\0claims:role".to_owned(), Value::String("archived".into()))]),
    );
}

fn flag(db: &Db, n: usize, kind: &str) {
    insert(db, "flags", id(FLAG, n), &[("kind", text(kind))]);
}

fn doc(db: &Db, n: usize, status: &str) {
    insert(
        db,
        "docs",
        id(DOC, n),
        &[("status", text(status)), ("title", text(&format!("d{n}")))],
    );
}

/// A query filtered on the untagged variant: its first row wakes it.
#[test]
fn a_filter_on_a_variant_tagged_after_it_subscribed_wakes_for_its_rows() {
    let db = open(schema(None), 1);
    keep_variant(&db, "archived");
    doc(&db, 0, "todo");
    let query = Query::from("docs").filter(eq(col("status"), lit(text("archived"))));
    let mut lives = vec![subscribe(&db, "archived", query, None)];
    check(&db, &mut lives, "subscribe");
    doc(&db, 1, "archived");
    check(&db, &mut lives, "first archived doc");
    doc(&db, 2, "archived");
    update(&db, "docs", id(DOC, 1), &[("title", text("renamed"))]);
    check(&db, &mut lives, "more archived writes");
}

/// A read policy that pins the root to the reader's `role` claim, the
/// claim naming the untagged variant: its first row wakes the result.
#[test]
fn a_policy_pinned_to_a_variant_tagged_after_it_subscribed_wakes_for_its_rows() {
    let db = open(schema(Some(session_eq("status", &["claims", "role"]))), 1);
    db.set_authority();
    db.set_identity_claims(
        user(0),
        BTreeMap::from([("\0claims:role".to_owned(), Value::String("archived".into()))]),
    );
    keep_variant(&db, "archived");
    doc(&db, 0, "todo");
    let mut lives = vec![subscribe(&db, "role", Query::from("docs"), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    doc(&db, 1, "archived");
    check(&db, &mut lives, "first archived doc");
    doc(&db, 2, "archived");
    check(&db, &mut lives, "second archived doc");
}

/// Results keyed by tagged variants (a filter, a policy pin) wake for the
/// right rows after the store reopens under a schema that declares the
/// variants in another order and so renumbers their tags.
#[test]
fn results_keyed_by_a_variant_keep_waking_after_its_tag_is_renumbered() {
    let policy = || Some(session_eq("status", &["claims", "role"]));
    let v1 = schema_of(&["todo", "done"], policy());
    let db = open(v1.clone(), 1);
    db.set_authority();
    let claims = |db: &Db| {
        db.set_identity_claims(
            user(0),
            BTreeMap::from([("\0claims:role".to_owned(), Value::String("done".into()))]),
        )
    };
    claims(&db);
    doc(&db, 0, "todo");
    doc(&db, 1, "done");
    let filtered = |status: &str| Query::from("docs").filter(eq(col("status"), lit(text(status))));
    let lives = |db: &Db| {
        vec![
            subscribe(db, "done", filtered("done"), None),
            subscribe(db, "todo", filtered("todo"), None),
            subscribe(db, "role", Query::from("docs"), Some(user(0))),
        ]
    };
    let mut held = lives(&db);
    check(&db, &mut held, "subscribe");
    drop(held);

    // `done` was tag 1 and is tag 0 from here; `todo` the reverse.
    let v2 = schema_of(&["done", "todo"], policy());
    let lens = MigrationLens::new(
        v1.version_id(),
        v2.version_id(),
        vec![TableLens {
            source_table: "docs".into(),
            target_table: "docs".into(),
            ops: vec![],
        }],
    )
    .unwrap();
    let db = db.reinstall(v2, 1, vec![lens]);
    claims(&db);
    let mut lives = lives(&db);
    check(&db, &mut lives, "the renumbering schema");
    doc(&db, 2, "done");
    check(&db, &mut lives, "a done doc");
    doc(&db, 3, "todo");
    check(&db, &mut lives, "a todo doc");
    update(&db, "docs", id(DOC, 3), &[("status", text("done"))]);
    check(&db, &mut lives, "todo to done");
    update(&db, "docs", id(DOC, 1), &[("status", text("todo"))]);
    check(&db, &mut lives, "done to todo");
}

/// A read policy that reads another table as evidence (`EXISTS flags WHERE
/// kind = session.role`), the claim naming the untagged variant: the first
/// flag stored with it wakes the result it makes readable.
#[test]
fn an_exists_pinned_to_a_variant_tagged_after_it_subscribed_wakes_for_its_rows() {
    let db = open(flagged(), 1);
    db.set_authority();
    archived_role(&db);
    keep(&db, "flags", "kind", "archived");
    doc(&db, 0, "todo");
    flag(&db, 0, "todo");
    let mut lives = vec![subscribe(
        &db,
        "flagged",
        Query::from("docs"),
        Some(user(0)),
    )];
    let held = check(&db, &mut lives, "subscribe");
    assert!(held[0].is_empty(), "no flag is archived yet");
    flag(&db, 1, "archived");
    let held = check(&db, &mut lives, "first archived flag");
    assert_eq!(held[0].len(), 1, "the flag makes the doc readable");
    update(&db, "flags", id(FLAG, 1), &[("kind", text("todo"))]);
    let held = check(&db, &mut lives, "the flag leaves the variant");
    assert!(held[0].is_empty());
}

/// The same policy served to a session: the authority decides which of a
/// tick's writes can change what the session reads by the same pin, so the
/// first flag stored with the untagged variant reaches the session's read.
#[test]
fn a_sessions_read_wakes_for_an_exists_pinned_to_a_variant_tagged_later() {
    let server = open(flagged(), 1);
    let client = open_as(flagged(), 2, user(0));
    let (client_side, server_side) = duplex();
    let _link = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(0)).unwrap();
    archived_role(&server);
    archived_role(&client);
    keep(&server, "flags", "kind", "archived");
    keep(&client, "flags", "kind", "archived");
    let pump = || {
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    doc(&server, 0, "todo");
    flag(&server, 0, "todo");
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut lives = vec![subscribe(&client, "flagged", Query::from("docs"), None)];
    let truth = || {
        let query = server.prepare_query(&Query::from("docs")).unwrap();
        snapshot(
            &block_on(server.all_nested_for_test(&query, ReadOpts::default(), Some(user(0))))
                .unwrap(),
        )
    };
    pump();
    check_with(&client, &mut lives, "sync", local.clone());
    assert!(truth().is_empty() && lives[0].rows.is_empty());
    flag(&server, 1, "archived");
    pump();
    check_with(&client, &mut lives, "first archived flag", local.clone());
    assert_eq!(truth().len(), 1, "the flag makes the doc readable");
    assert_eq!(lives[0].rows, truth(), "the session never heard of the doc");
}

/// A row the session holds only as the support of another table's policy
/// (a seat, for the docs it reads) is in no result of its own: the
/// authority's re-check of held rows ships it once it becomes readable.
/// That re-check is decided by the seat policy's pin, made anew for each
/// tick's writes, so the flag that first stores the variant already meets
/// it (this held before; no key outlives the tag here).
#[test]
fn a_session_is_sent_a_policys_support_row_made_readable_by_a_variant_tagged_later() {
    let server = open(seated(), 1);
    let client = open_as(seated(), 2, user(0));
    let (client_side, server_side) = duplex();
    let _link = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(0)).unwrap();
    for db in [&*server, &*client] {
        archived_role(db);
        keep(db, "flags", "kind", "archived");
    }
    let pump = || {
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    doc(&server, 0, "todo");
    insert(&server, "seats", id(SEAT, 0), &[("doc", uuid(id(DOC, 0)))]);
    flag(&server, 0, "todo");
    let _docs = subscribe(&client, "docs", Query::from("docs"), None);
    // What the store holds, whoever may read it.
    let held = |db: &Db, table: &str| {
        let query = db.prepare_query(&Query::from(table)).unwrap();
        let local = ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        };
        snapshot(
            &block_on(db.all_nested_for_test(&query, local, Some(AuthorSubject::SYSTEM))).unwrap(),
        )
        .len()
    };
    pump();
    assert_eq!(held(&client, "seats"), 0, "the seat is not readable yet");
    flag(&server, 1, "archived");
    pump();
    assert_eq!(
        held(&client, "seats"),
        1,
        "the session was not sent the seat it may now read"
    );
}

/// An update that moves an existing row into the untagged variant (its
/// first store) wakes a query filtered on it.
#[test]
fn an_update_into_a_variant_tagged_after_it_subscribed_wakes_its_filter() {
    let db = open(schema(None), 1);
    keep_variant(&db, "archived");
    doc(&db, 0, "todo");
    doc(&db, 1, "done");
    let query = Query::from("docs").filter(eq(col("status"), lit(text("archived"))));
    let mut lives = vec![subscribe(&db, "archived", query, None)];
    check(&db, &mut lives, "subscribe");
    update(&db, "docs", id(DOC, 0), &[("status", text("archived"))]);
    check(&db, &mut lives, "first doc moved into archived");
    update(&db, "docs", id(DOC, 1), &[("status", text("archived"))]);
    check(&db, &mut lives, "second doc moved into archived");
}

/// Wakes of `db`'s live results for `writes`.
fn wakes(db: &Db, writes: impl FnOnce()) -> u64 {
    let before = db.fanout_stats().woken;
    writes();
    db.fanout_stats().woken - before
}

/// A filter on a name that is no variant (and not one the store keeps)
/// matches no row ever: it stays keyed by that name, so writes of other
/// rows do not wake it, in a plain store and in one that keeps a variant.
#[test]
fn a_filter_on_a_name_that_is_no_variant_is_not_woken_by_other_rows() {
    for keeps in [false, true] {
        let db = open(schema(None), 1);
        if keeps {
            keep_variant(&db, "archived");
        }
        let query = Query::from("docs").filter(eq(col("status"), lit(text("typo"))));
        let mut lives = vec![subscribe(&db, "typo", query, None)];
        check(&db, &mut lives, "subscribe");
        let woken = wakes(&db, || {
            for n in 0..6 {
                doc(&db, n, if n % 2 == 0 { "todo" } else { "done" });
            }
        });
        check(&db, &mut lives, "other rows");
        assert_eq!(woken, 0, "keeps a variant: {keeps}");
    }
}

/// A read policy pinning the root to a claim, the claim naming no variant:
/// no row is readable, and writes of rows do not wake the result.
#[test]
fn a_policy_pinned_to_a_claim_outside_the_enum_is_not_woken_by_other_rows() {
    let db = open(schema(Some(session_eq("status", &["claims", "role"]))), 1);
    db.set_authority();
    db.set_identity_claims(
        user(0),
        BTreeMap::from([("\0claims:role".to_owned(), Value::String("admin".into()))]),
    );
    let mut lives = vec![subscribe(&db, "role", Query::from("docs"), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    let woken = wakes(&db, || {
        for n in 0..6 {
            doc(&db, n, if n % 2 == 0 { "todo" } else { "done" });
        }
    });
    check(&db, &mut lives, "other rows");
    assert_eq!(woken, 0);
}

/// An EXISTS pinned to a claim naming no variant: writes of evidence rows
/// with other kinds do not wake the reader's result.
#[test]
fn an_exists_pinned_to_a_claim_outside_the_enum_is_not_woken_by_other_flags() {
    let db = open(flagged(), 1);
    db.set_authority();
    db.set_identity_claims(
        user(0),
        BTreeMap::from([("\0claims:role".to_owned(), Value::String("admin".into()))]),
    );
    doc(&db, 0, "todo");
    let mut lives = vec![subscribe(&db, "role", Query::from("docs"), Some(user(0)))];
    check(&db, &mut lives, "subscribe");
    let woken = wakes(&db, || {
        for n in 0..6 {
            flag(&db, n, if n % 2 == 0 { "todo" } else { "done" });
        }
    });
    check(&db, &mut lives, "other flags");
    assert_eq!(woken, 0);
}
