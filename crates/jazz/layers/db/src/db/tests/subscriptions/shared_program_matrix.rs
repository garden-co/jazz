//! A shape that does share one server program (OR of a claim equality and a claim-keyed EXISTS) under a
//! window and a plain listing, with live proof changes (a share re-pointed
//! from carol to bob) and new rows. Each client's local store must hold only
//! rows its own claims admit.

use super::*;

fn shared_matrix_schema() -> JazzSchema {
    let all = |select: PublicPolicyExpr| {
        PublicTablePolicies::new()
            .with_select(select)
            .with_insert(PublicPolicyExpr::True)
            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("shares")
                    .column("owner_id", PublicColumnType::Text)
                    .column("user_id", PublicColumnType::Text)
                    .policies(all(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("notes")
                    .column("owner_id", PublicColumnType::Text)
                    .column("n", PublicColumnType::Integer)
                    .policies(all(PublicPolicyExpr::Or(vec![
                        public_session_eq("owner_id", &["claims", "sub"]),
                        public_exists(
                            "shares",
                            [
                                public_outer_eq("owner_id", "owner_id"),
                                public_session_eq("user_id", &["claims", "sub"]),
                            ],
                        ),
                    ]))),
            ),
    )
}

fn shared_matrix_e2e(query: Query) -> Vec<String> {
    let schema = shared_matrix_schema();
    let server = open_core(0x62, AuthorSubject::SYSTEM, &schema);
    let subjects = ["subject-a", "subject-b", "subject-c"];
    // Each subject owns two notes, n = 10*i + {1, 2}.
    let note = |i: usize, j: usize| row(0x40 + (i * 2 + j) as u8);
    for i in 0..3 {
        for j in 0..2 {
            server
                .insert_with_id(
                    "notes",
                    note(i, j),
                    BTreeMap::from([
                        ("owner_id".to_owned(), Value::String(subjects[i].to_owned())),
                        ("n".to_owned(), Value::I32((10 * i + j + 1) as i32)),
                    ]),
                )
                .unwrap();
        }
    }
    // alice shares her notes with carol.
    let share = row(0x30);
    server
        .insert_with_id(
            "shares",
            share,
            BTreeMap::from([
                ("owner_id".to_owned(), Value::String(subjects[0].to_owned())),
                ("user_id".to_owned(), Value::String(subjects[2].to_owned())),
            ]),
        )
        .unwrap();
    let mut sessions = Vec::new();
    for (i, subject) in subjects.iter().enumerate() {
        let author = AuthorSubject::for_test_bytes([0x70 + i as u8; 16]);
        let claims = BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String((*subject).to_owned()),
        )]);
        let client = open_db(0x63 + i as u8, AuthorSubject::SYSTEM, &schema);
        let (client_transport, server_transport) = duplex();
        let upstream = crate::local_executor::block_on(client.connect_upstream(client_transport));
        let subscriber = server.accept_subscriber_with_claims(server_transport, author, claims);
        let prepared = client.prepare_query_bound(&query, BTreeMap::new()).unwrap();
        let sub = block_on(client.subscribe(
            &prepared,
            ReadOpts {
                tier: crate::db::ReadTier::Remote,
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        sessions.push((client, upstream, subscriber, sub));
    }
    macro_rules! tick {
        () => {
            for _ in 0..3 {
                for s in &sessions {
                    s.0.tick().unwrap();
                }
                server.tick().unwrap();
            }
            for s in &sessions {
                s.0.tick().unwrap();
            }
        };
    }
    tick!();
    let mut leaks = Vec::new();
    // Readable notes by phase: owner always; alice's notes by carol before
    // the re-point, by bob after it. The fresh note 0x4f is bob's.
    let readable = |phase: u8, reader: usize, r: RowUuid| -> bool {
        let owner = (0..3).find(|i| r == note(*i, 0) || r == note(*i, 1));
        match owner {
            Some(o) if o == reader => true,
            Some(0) if phase == 0 => reader == 2,
            Some(0) => reader == 1 || reader == 2, // carol may keep a stale row until removal
            Some(_) => false,
            None => r == row(0x4f) && reader == 1,
        }
    };
    macro_rules! check {
        ($phase:expr, $label:expr) => {{
            for (i, s) in sessions.iter_mut().enumerate() {
                while s.3.try_next_event().is_some() {}
                let local = ReadOpts {
                    tier: crate::db::ReadTier::LocalOnly,
                    ..ReadOpts::default()
                };
                for r in row_ids(&prepared_all(&s.0, &Query::from("notes"), local)) {
                    if !readable($phase, i, r) {
                        leaks.push(format!("{}: client {i} holds note {r:?}", $label));
                    }
                }
            }
        }};
    }
    check!(0, "initial");
    server
        .update(
            "shares",
            share,
            BTreeMap::from([("user_id".to_owned(), Value::String(subjects[1].to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "notes",
            row(0x4f),
            BTreeMap::from([
                ("owner_id".to_owned(), Value::String(subjects[1].to_owned())),
                ("n".to_owned(), Value::I32(0)),
            ]),
        )
        .unwrap();
    tick!();
    check!(1, "after-repoint");
    leaks
}

#[test]
fn shared_matrix_e2e_or_claim_exists_rows() {
    let leaks = shared_matrix_e2e(Query::from("notes"));
    assert!(leaks.is_empty(), "{leaks:#?}");
}

#[test]
fn shared_matrix_e2e_or_claim_exists_window() {
    let leaks = shared_matrix_e2e(
        Query::from("notes")
            .order_by("n", OrderDirection::Asc)
            .limit(2)
            .offset(1),
    );
    assert!(leaks.is_empty(), "{leaks:#?}");
}
