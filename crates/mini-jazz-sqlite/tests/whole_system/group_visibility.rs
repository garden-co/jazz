use super::*;
use mini_jazz_sqlite::RowView;

#[test]
fn mini_sqlite_todo_fixture_db_enforces_project_and_todo_visibility_by_user_or_group() {
    let mut db = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "seed-node",
        SchemaDef::mini_sqlite_todo_fixture(),
    )
    .unwrap();

    seed_group_visibility_fixture(&mut db);

    db.run_as_user("user-alice", |alice| {
        assert_eq!(
            visible_ids(alice.read_rows("projects").unwrap()),
            vec!["project-alice", "project-engineering"]
        );
        assert_eq!(
            visible_ids(open_todos_query(alice)),
            vec!["todo-alice", "todo-engineering"]
        );
    });

    db.run_as_user("user-bob", |bob| {
        assert_eq!(
            visible_ids(bob.read_rows("projects").unwrap()),
            vec!["project-bob", "project-engineering"]
        );
        assert_eq!(
            visible_ids(open_todos_query(bob)),
            vec!["todo-bob", "todo-engineering"]
        );
    });
}

#[test]
fn mini_sqlite_todo_fixture_allows_group_reads_but_only_authors_can_delete_todos() {
    let mut db = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "seed-node",
        SchemaDef::mini_sqlite_todo_fixture(),
    )
    .unwrap();

    seed_group_visibility_fixture(&mut db);

    db.run_as_user("user-bob", |bob| {
        assert_eq!(
            visible_ids(open_todos_query(bob)),
            vec!["todo-bob", "todo-engineering"]
        );

        let tx = bob.delete_row("todos", "todo-engineering").unwrap();
        assert_eq!(
            bob.transaction_info(&tx).unwrap().rejection_code,
            Some("policy_denied".to_owned())
        );
        assert_eq!(
            visible_ids(open_todos_query(bob)),
            vec!["todo-bob", "todo-engineering"]
        );
    });

    db.run_as_user("user-alice", |alice| {
        let tx = alice.delete_row("todos", "todo-engineering").unwrap();
        assert_eq!(alice.transaction_info(&tx).unwrap().rejection_code, None);
        assert_eq!(visible_ids(open_todos_query(alice)), vec!["todo-alice"]);
    });
}

fn seed_group_visibility_fixture(db: &mut Runtime) {
    for (id, name) in [
        ("user-alice", "Alice"),
        ("user-bob", "Bob"),
        ("user-cara", "Cara"),
    ] {
        db.insert_row(
            "users",
            id,
            BTreeMap::from([("name".to_owned(), json!(name))]),
        )
        .unwrap();
    }

    for (id, name) in [
        ("group-engineering", "Engineering"),
        ("group-design", "Design"),
    ] {
        db.insert_row(
            "groups",
            id,
            BTreeMap::from([("name".to_owned(), json!(name))]),
        )
        .unwrap();
    }

    for (id, user, group) in [
        (
            "group-member-alice-engineering",
            "user-alice",
            "group-engineering",
        ),
        (
            "group-member-bob-engineering",
            "user-bob",
            "group-engineering",
        ),
        ("group-member-bob-design", "user-bob", "group-design"),
    ] {
        db.insert_row(
            "group_members",
            id,
            BTreeMap::from([
                ("user".to_owned(), json!(user)),
                ("group".to_owned(), json!(group)),
            ]),
        )
        .unwrap();
    }

    for (id, title) in [
        ("project-engineering", "Engineering roadmap"),
        ("project-alice", "Alice private"),
        ("project-bob", "Bob private"),
    ] {
        db.insert_row(
            "projects",
            id,
            BTreeMap::from([("title".to_owned(), json!(title))]),
        )
        .unwrap();
    }
    db.run_attributing_to_user("user-alice", |alice| {
        alice
            .insert_row(
                "projects",
                "project-created-without-membership",
                BTreeMap::from([("title".to_owned(), json!("Created without membership"))]),
            )
            .unwrap();
    });

    for (id, project, member) in [
        (
            "project-member-engineering",
            "project-engineering",
            "group:group-engineering",
        ),
        ("project-member-alice", "project-alice", "user:user-alice"),
        ("project-member-bob", "project-bob", "user:user-bob"),
    ] {
        db.insert_row(
            "project_members",
            id,
            BTreeMap::from([
                ("project".to_owned(), json!(project)),
                ("member".to_owned(), json!(member)),
            ]),
        )
        .unwrap();
    }

    for (id, title, project, author) in [
        (
            "todo-engineering",
            "Plan sync protocol",
            "project-engineering",
            "user-alice",
        ),
        (
            "todo-alice",
            "Review launch notes",
            "project-alice",
            "user-alice",
        ),
        ("todo-bob", "Bob-only task", "project-bob", "user-bob"),
        (
            "todo-created-without-membership",
            "Created by Alice without membership",
            "project-created-without-membership",
            "user-alice",
        ),
    ] {
        db.run_attributing_to_user(author, |author_db| {
            author_db
                .insert_row(
                    "todos",
                    id,
                    BTreeMap::from([
                        ("title".to_owned(), json!(title)),
                        ("done".to_owned(), json!(false)),
                        ("project".to_owned(), json!(project)),
                    ]),
                )
                .unwrap();
        });
    }
}

fn open_todos_query(db: &Runtime) -> Vec<RowView> {
    db.query(
        BuiltQuery::from_json_value(json!({
            "table": "todos",
            "conditions": [{"column": "done", "op": "eq", "value": false}],
            "orderBy": [["$createdAt", "desc"]],
            "limit": 10
        }))
        .unwrap(),
    )
    .unwrap()
}

fn visible_ids(rows: Vec<RowView>) -> Vec<String> {
    let mut ids = rows.into_iter().map(|row| row.id).collect::<Vec<_>>();
    ids.sort();
    ids
}
