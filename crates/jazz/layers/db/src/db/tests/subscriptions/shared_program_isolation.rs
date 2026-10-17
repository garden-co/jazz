//! Client/server: subscribers sharing one prepared server program must each
//! receive only rows their own binding authorizes.

use super::*;

fn shared_program_schema(messages_private: bool) -> JazzSchema {
    let member = |outer: &str| {
        public_exists(
            "chat_members",
            [
                public_outer_eq("chat_id", outer),
                public_session_eq("user_id", &["claims", "sub"]),
            ],
        )
    };
    let all = |select: PublicPolicyExpr| {
        PublicTablePolicies::new()
            .with_select(select)
            .with_insert(PublicPolicyExpr::True)
            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chat_members")
                    .fk_column("chat_id", "chats")
                    .column("user_id", PublicColumnType::Text)
                    .policies(all(public_session_eq("user_id", &["claims", "sub"]))),
            )
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .policies(all(member("id"))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chat_id", "chats")
                    .column("body", PublicColumnType::Text)
                    .policies(all(if messages_private {
                        member("chat_id")
                    } else {
                        PublicPolicyExpr::True
                    })),
            ),
    )
}

/// Server + three clients (alice, bob, carol), each the only member of its
/// own chat. Each client subscribes `query` bound to its own chat. After bob's
/// chat gets a new message and a rename, inspect what each client's local
/// store holds (read as SYSTEM on the client, so no local policy hides it).
fn shared_program_e2e(messages_private: bool, query: Query, bind_chat: bool) -> Vec<String> {
    let schema = shared_program_schema(messages_private);
    let server = open_core(0x62, AuthorSubject::SYSTEM, &schema);
    let subjects = ["subject-a", "subject-b", "subject-c"];
    let chats = [row(0x11), row(0x12), row(0x13)];
    let msgs = [row(0x31), row(0x32), row(0x33)];
    for i in 0..3 {
        server
            .insert_with_id(
                "chats",
                chats[i],
                BTreeMap::from([("title".to_owned(), Value::String(format!("secret {i}")))]),
            )
            .unwrap();
        server
            .insert_with_id(
                "chat_members",
                row(0x21 + i as u8),
                BTreeMap::from([
                    ("chat_id".to_owned(), Value::Uuid(chats[i].0)),
                    ("user_id".to_owned(), Value::String(subjects[i].to_owned())),
                ]),
            )
            .unwrap();
        server
            .insert_with_id(
                "messages",
                msgs[i],
                BTreeMap::from([
                    ("chat_id".to_owned(), Value::Uuid(chats[i].0)),
                    ("body".to_owned(), Value::String(format!("private {i}"))),
                ]),
            )
            .unwrap();
    }
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
        let params = if bind_chat {
            BTreeMap::from([("chat".to_owned(), Value::Uuid(chats[i].0))])
        } else {
            BTreeMap::new()
        };
        let prepared = client.prepare_query_bound(&query, params).unwrap();
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
    macro_rules! check {
        ($phase:expr) => {{
            let phase: &str = $phase;
            for (i, s) in sessions.iter_mut().enumerate() {
                while s.3.try_next_event().is_some() {}
                let local = ReadOpts {
                    tier: crate::db::ReadTier::LocalOnly,
                    ..ReadOpts::default()
                };
                let m = row_ids(&prepared_all(&s.0, &Query::from("messages"), local.clone()));
                let c = row_ids(&prepared_all(&s.0, &Query::from("chats"), local));
                let own_m = [msgs[i], row(0x40)];
                for r in &m {
                    if !own_m.contains(r) || (*r == row(0x40) && i != 1) {
                        leaks.push(format!("{phase}: client {i} holds message {r:?}"));
                    }
                }
                for r in &c {
                    if *r != chats[i] {
                        leaks.push(format!("{phase}: client {i} holds chat {r:?}"));
                    }
                }
            }
        }};
    }
    check!("initial");
    server
        .insert_with_id(
            "messages",
            row(0x40),
            BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(chats[1].0)),
                ("body".to_owned(), Value::String("bob only".to_owned())),
            ]),
        )
        .unwrap();
    server
        .update(
            "chats",
            chats[1],
            BTreeMap::from([("title".to_owned(), Value::String("bob renamed".to_owned()))]),
        )
        .unwrap();
    tick!();
    check!("after-bob-writes");
    leaks
}

#[test]
fn shared_program_param_count_member_only_messages() {
    let q = Query::from("messages")
        .filter(eq(col("chat_id"), param("chat")))
        .count();
    let leaks = shared_program_e2e(true, q, true);
    assert!(leaks.is_empty(), "{leaks:#?}");
}

#[test]
fn shared_program_param_rows_public_messages_private_chats() {
    let q = Query::from("messages").filter(eq(col("chat_id"), param("chat")));
    let leaks = shared_program_e2e(false, q, true);
    assert!(leaks.is_empty(), "{leaks:#?}");
}

#[test]
fn shared_program_param_rows_member_only_messages() {
    let q = Query::from("messages").filter(eq(col("chat_id"), param("chat")));
    let leaks = shared_program_e2e(true, q, true);
    assert!(leaks.is_empty(), "{leaks:#?}");
}

#[test]
fn shared_program_count_member_only_messages_without_params() {
    let leaks = shared_program_e2e(true, Query::from("messages").count(), false);
    assert!(leaks.is_empty(), "{leaks:#?}");
}
