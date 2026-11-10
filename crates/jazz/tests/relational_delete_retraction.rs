//! A server-side delete of a row whose read policy is relational (an
//! `EXISTS` over a membership table) leaves a replica's local store and live
//! local subscriptions (#4167): live, across a reconnect and across a
//! relaunch from the durable file, whether the membership is deleted after
//! the row or in the same transaction.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq};

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

const ME: [&str; 3] = ["user", "identity", "subject"];

/// Games readable through a participant row naming the reader, and the
/// participant rows readable by the participant.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("games")
                    .column("name", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "participants",
                        vec![outer_eq("game", "id"), session_eq("user_id", &ME)],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("participants")
                    .fk_column("game", "games")
                    .column("user_id", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq("user_id", &ME))),
            )
            .build(),
    )
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x41; 16])
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

/// A core holding alice's game "planner" and her participant row.
struct Fixture {
    core: Db,
    game: RowUuid,
    participant: RowUuid,
}

impl Fixture {
    fn new() -> Self {
        let core = block_on(Db::open_history_complete(config(
            0xc0,
            AuthorSubject::SYSTEM,
        )))
        .unwrap();
        let game = block_on(core.insert(
            "games",
            cells(&[("name", Value::String("planner".into()))]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid();
        let participant = block_on(core.insert(
            "participants",
            cells(&[
                ("game", Value::Uuid(game.0)),
                ("user_id", Value::String(alice().principal_parts().1)),
            ]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid();
        Fixture {
            core,
            game,
            participant,
        }
    }

    fn connect(&self, client: &Db) -> Upstream {
        let (client_side, server_side) = duplex_transport::duplex();
        let upstream = block_on(client.connect_upstream(client_side));
        self.core.accept_subscriber(server_side, alice()).unwrap();
        upstream
    }

    fn pump(&self, client: &Db) {
        for _ in 0..6 {
            block_on(client.tick()).unwrap();
            block_on(self.core.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    }

    fn delete(&self, table: &str, row: RowUuid) {
        block_on(self.core.delete(table, row, Default::default())).unwrap();
    }

    /// The game first while alice is still a participant, then her
    /// participant row: the order in the report.
    fn delete_game_then_participant(&self) {
        self.delete("games", self.game);
        self.delete("participants", self.participant);
    }

    /// Both deletes in one transaction.
    fn delete_tree_at_once(&self) {
        let tx = block_on(self.core.mergeable_tx()).unwrap();
        block_on(tx.delete("participants", self.participant, Default::default())).unwrap();
        block_on(tx.delete("games", self.game, Default::default())).unwrap();
        block_on(tx.commit()).unwrap();
    }
}

/// Alice's games as her replica holds them, never asking the server.
fn local_games(client: &Db) -> Vec<RowUuid> {
    let prepared = client.prepare_query(&Query::from("games")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(client.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

/// Subscribe alice to her participant rows and to the games, as the app
/// does, on the default (local) tier.
fn subscribe(client: &Db) -> Vec<SubscriptionStream> {
    ["participants", "games"]
        .into_iter()
        .map(|table| {
            let prepared = client.prepare_query(&Query::from(table)).unwrap();
            block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap()
        })
        .collect()
}

/// The rows a games subscription currently shows, folding its events.
fn shown(stream: &mut SubscriptionStream, shown: &mut Vec<RowUuid>) {
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            ..
        } = event
        {
            if reset {
                shown.clear();
            }
            for row in removed {
                shown.retain(|id| *id != row.row_uuid);
            }
            shown.extend(added.iter().map(|row| row.row_uuid()));
        }
    }
}

type Deletes = fn(&Fixture);

const ORDERS: [(&str, Deletes); 2] = [
    (
        "game, then participant",
        Fixture::delete_game_then_participant,
    ),
    ("both in one transaction", Fixture::delete_tree_at_once),
];

/// Live: alice is connected and subscribed while the server deletes.
///
/// ```
/// core ──delete game (alice still a participant)──► alice: game leaves
///      ──delete participant────────────────────────► alice: stays gone
/// ```
#[test]
fn a_live_delete_leaves_the_replica_and_its_subscription() {
    for (order, deletes) in ORDERS {
        let fixture = Fixture::new();
        let client = block_on(Db::open(config(0xa1, alice()))).unwrap();
        let _upstream = fixture.connect(&client);
        let mut streams = subscribe(&client);
        fixture.pump(&client);
        let mut on_screen = Vec::new();
        shown(&mut streams[1], &mut on_screen);
        assert_eq!(local_games(&client), [fixture.game], "{order}");
        assert_eq!(on_screen, [fixture.game], "{order}");

        deletes(&fixture);
        fixture.pump(&client);
        shown(&mut streams[1], &mut on_screen);
        assert_eq!(
            local_games(&client),
            [],
            "{order}: the local store drops it"
        );
        assert_eq!(on_screen, [], "{order}: the subscription retracts it");
    }
}

/// Offline: alice's replica is disconnected (backgrounded) while the
/// server deletes; on reconnect the game leaves her store.
#[test]
fn a_delete_made_while_offline_leaves_the_replica_on_reconnect() {
    for (order, deletes) in ORDERS {
        let fixture = Fixture::new();
        let client = block_on(Db::open(config(0xa1, alice()))).unwrap();
        let upstream = fixture.connect(&client);
        let _streams = subscribe(&client);
        fixture.pump(&client);
        assert_eq!(local_games(&client), [fixture.game], "{order}");

        assert!(client.detach_connection(&upstream));
        deletes(&fixture);
        let _upstream = fixture.connect(&client);
        fixture.pump(&client);
        assert_eq!(local_games(&client), [], "{order}");
    }
}

/// Relaunch: alice's app quits holding the game in its durable store; the
/// server deletes while it is closed; on relaunch and reconnect the game
/// leaves the store.
#[test]
fn a_delete_made_while_closed_leaves_the_durable_replica_after_relaunch() {
    for (order, deletes) in ORDERS {
        let fixture = Fixture::new();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        {
            let client = block_on(Db::open_at(
                config(0xa1, alice()),
                &path,
                FileOptions::default(),
            ))
            .unwrap();
            let _upstream = fixture.connect(&client);
            let _streams = subscribe(&client);
            fixture.pump(&client);
            assert_eq!(local_games(&client), [fixture.game], "{order}");
            block_on(client.close()).unwrap();
        }
        deletes(&fixture);

        let client = block_on(Db::open_at(
            config(0xa1, alice()),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        assert_eq!(
            local_games(&client),
            [fixture.game],
            "{order}: offline, the replica still holds what it last heard"
        );
        let _upstream = fixture.connect(&client);
        let mut streams = subscribe(&client);
        fixture.pump(&client);
        let mut on_screen = Vec::new();
        shown(&mut streams[1], &mut on_screen);
        assert_eq!(
            local_games(&client),
            [],
            "{order}: the relaunched store drops it"
        );
        assert_eq!(on_screen, [], "{order}");
    }
}
