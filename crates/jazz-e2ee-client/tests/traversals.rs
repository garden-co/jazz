//! Joins, reachability and inheritance through an encrypted table, on plain
//! columns (core hooks note, H4): the runtime passes what core allows, rows
//! come back decrypted, and a condition that names an encrypted column is
//! still refused by core. The queries run in the device's own store and at
//! the server, which evaluates them over plain columns only.

mod common;

use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, DbConfig, ReadOpts, ReadTier, block_on};
use jazz::e2ee::{E2eeDeclarations, EncryptionEntry};
use jazz::ids::RowUuid;
use jazz::query::{Operand, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_e2ee_client::MemoryStore;

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("team", ColumnType::Uuid)
                    .column("body", ColumnType::Bytea)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("projectId", "projects")
                    .column("user", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("edges")
                    .column("child", ColumnType::Uuid)
                    .column("parent", ColumnType::Uuid)
                    .policies(allow_all()),
            )
            .build(),
    )
    .unwrap()
}

fn declarations() -> E2eeDeclarations {
    E2eeDeclarations {
        app_id: app_id(),
        environment: ENVIRONMENT.to_owned(),
        settings: Default::default(),
        tables: BTreeMap::from([(
            "notes".to_owned(),
            EncryptionEntry {
                space: "projectId".to_owned(),
                scope: "projects".to_owned(),
                columns: BTreeMap::from([("body".to_owned(), encrypted(ColumnType::Text, false))]),
                equality: Vec::new(),
            },
        )]),
    }
}

struct World {
    server: Db,
    db: Db,
    _client: jazz_e2ee_client::E2eeClient,
    _link: jazz::db::Upstream,
    /// Alice is a member of `mine` only; `mine` and `other` have a note,
    /// `empty` has none.
    mine: uuid::Uuid,
    other: uuid::Uuid,
    empty: uuid::Uuid,
    team: uuid::Uuid,
    notes: Vec<RowUuid>,
}

fn insert(db: &Db, table: &str, cells: Vec<(&str, Value)>) -> RowUuid {
    let cells = cells
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    block_on(db.insert(table, cells, Default::default()))
        .unwrap()
        .row_uuid()
}

fn world() -> World {
    let server = block_on(Db::open(DbConfig::new(schema(), identity(0)))).unwrap();
    server.set_authority().unwrap();
    let client = open_client_with(Rc::new(MemoryStore::new()), declarations());
    let db = block_on(Db::open(
        DbConfig::new(schema(), identity(1)).with_e2ee(client.hooks()),
    ))
    .unwrap();
    let (link, _) = link(&db, &server);
    pump(&server, &db);
    assert!(client.prepare(&db).unwrap());
    let mine = project(&db, "mine");
    let other = project(&db, "other");
    let empty = project(&db, "empty");
    hold_new_space(&client, &db, mine);
    hold_new_space(&client, &db, other);
    let team = uuid::Uuid::new_v4();
    let note = |space: uuid::Uuid, body: &str| {
        insert(
            &db,
            "notes",
            vec![
                ("projectId", Value::Uuid(space)),
                ("team", Value::Uuid(team)),
                ("body", text(body)),
            ],
        )
    };
    let notes = vec![note(mine, "MINE-SECRET"), note(other, "OTHER-SECRET")];
    insert(
        &db,
        "members",
        vec![("projectId", Value::Uuid(mine)), ("user", text("alice"))],
    );
    insert(
        &db,
        "members",
        vec![("projectId", Value::Uuid(other)), ("user", text("bob"))],
    );
    pump(&server, &db);
    World {
        server,
        db,
        _client: client,
        _link: link,
        mine,
        other,
        empty,
        team,
        notes,
    }
}

impl World {
    /// The rows `query` returns in the device's own store and at the server,
    /// each as (row, cells), after checking the two agree.
    fn read(&self, query: &Query) -> Vec<(RowUuid, BTreeMap<String, Value>)> {
        let mut answers = Vec::new();
        for tier in [ReadTier::LocalOnly, ReadTier::Remote] {
            let prepared = self.db.prepare_query(query).unwrap();
            let mut rows: Vec<_> = drive(
                &self.server,
                &[&self.db],
                self.db.all(&prepared, ReadOpts::for_read_tier(tier)),
            )
            .unwrap()
            .into_iter()
            .map(|row| {
                let cells = ["name", "body"]
                    .into_iter()
                    .filter_map(|name| row.cell(name).map(|value| (name.to_owned(), value)))
                    .collect();
                (row.row_uuid(), cells)
            })
            .collect();
            rows.sort_by_key(|(id, _)| *id);
            answers.push(rows);
        }
        assert_eq!(answers[0], answers[1], "the store and the server agree");
        answers.remove(0)
    }

    fn ids(rows: &[(RowUuid, BTreeMap<String, Value>)]) -> Vec<RowUuid> {
        let mut ids: Vec<_> = rows.iter().map(|(id, _)| *id).collect();
        ids.sort();
        ids
    }
}

/// Contract: the notes (encrypted) of the projects alice is a member of: a
/// traversal through the plain membership table on an encrypted root. The
/// rows come back decrypted, and only those of her project.
#[test]
fn notes_of_the_projects_i_am_a_member_of() {
    let world = world();
    let query = Query::from("notes").join_via_column(
        "members",
        "projectId",
        "projectId",
        [eq(col("user"), lit(text("alice")))],
    );
    let rows = world.read(&query);
    assert_eq!(World::ids(&rows), [world.notes[0]]);
    assert_eq!(rows[0].1["body"], text("MINE-SECRET"));
}

/// Contract: the projects that have a note: a traversal through an
/// encrypted table from a plain root; the project without one is left out.
#[test]
fn projects_that_have_a_note() {
    let world = world();
    let query = Query::from("projects").join_via("notes", "projectId", []);
    let rows = world.read(&query);
    let mut expected = [world.mine, world.other].map(RowUuid);
    expected.sort();
    assert_eq!(World::ids(&rows), expected);
    assert!(rows.iter().all(|(id, _)| id.0 != world.empty));
}

/// Contract: a hierarchy through an encrypted table on plain columns: the
/// projects reachable from a team through the notes' team column and a plain
/// edge table (reachability); the project without a note is not. (Core does
/// not take `inherits` in a query yet, only in policies.)
#[test]
fn a_hierarchy_through_an_encrypted_table_on_plain_columns() {
    let world = world();
    insert(
        &world.db,
        "edges",
        vec![
            ("child", Value::Uuid(world.team)),
            ("parent", Value::Uuid(uuid::Uuid::new_v4())),
        ],
    );
    pump(&world.server, &world.db);
    let reachable = world.read(&Query::from("projects").reachable_via(
        "notes",
        "projectId",
        "team",
        Operand::Literal(Value::Uuid(world.team)),
        "edges",
        "child",
        "parent",
        [],
    ));
    let mut expected = [world.mine, world.other].map(RowUuid);
    expected.sort();
    assert_eq!(World::ids(&reachable), expected);
}
