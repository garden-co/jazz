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
            vec!["project-alice", "project-company", "project-engineering"]
        );
        assert_eq!(
            visible_ids(open_todos_query(alice)),
            vec!["todo-alice", "todo-company", "todo-engineering"]
        );
    });

    db.run_as_user("user-bob", |bob| {
        assert_eq!(
            visible_ids(bob.read_rows("projects").unwrap()),
            vec!["project-bob", "project-company", "project-engineering"]
        );
        assert_eq!(
            visible_ids(open_todos_query(bob)),
            vec!["todo-bob", "todo-company", "todo-engineering"]
        );
    });

    db.run_as_user("user-cara", |cara| {
        assert_eq!(
            visible_ids(cara.read_rows("projects").unwrap()),
            vec!["project-company"]
        );
        assert_eq!(visible_ids(open_todos_query(cara)), vec!["todo-company"]);
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
            vec!["todo-bob", "todo-company", "todo-engineering"]
        );

        let tx = bob.delete_row("todos", "todo-engineering").unwrap();
        assert_eq!(
            bob.transaction_info(&tx).unwrap().rejection_code,
            Some("policy_denied".to_owned())
        );
        assert_eq!(
            visible_ids(open_todos_query(bob)),
            vec!["todo-bob", "todo-company", "todo-engineering"]
        );
    });

    db.run_as_user("user-alice", |alice| {
        let tx = alice.delete_row("todos", "todo-engineering").unwrap();
        assert_eq!(alice.transaction_info(&tx).unwrap().rejection_code, None);
        assert_eq!(
            visible_ids(open_todos_query(alice)),
            vec!["todo-alice", "todo-company"]
        );
    });
}

#[test]
fn mini_sqlite_todo_fixture_allows_visible_done_updates_but_only_authors_can_rename_todos() {
    let mut db = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "seed-node",
        SchemaDef::mini_sqlite_todo_fixture(),
    )
    .unwrap();

    seed_group_visibility_fixture(&mut db);

    db.run_as_user("user-bob", |bob| {
        let tx = bob
            .update_row(
                "todos",
                "todo-engineering",
                BTreeMap::from([("done".to_owned(), json!(true))]),
            )
            .unwrap();
        assert_eq!(bob.transaction_info(&tx).unwrap().rejection_code, None);
        let todo = bob
            .read_rows("todos")
            .unwrap()
            .into_iter()
            .find(|row| row.id == "todo-engineering")
            .unwrap();
        assert_eq!(todo.values["done"], json!(true));

        let tx = bob
            .update_row(
                "todos",
                "todo-engineering",
                BTreeMap::from([("title".to_owned(), json!("Bob renames Alice's task"))]),
            )
            .unwrap();
        assert_eq!(
            bob.transaction_info(&tx).unwrap().rejection_code,
            Some("policy_denied".to_owned())
        );
        let todo = bob
            .read_rows("todos")
            .unwrap()
            .into_iter()
            .find(|row| row.id == "todo-engineering")
            .unwrap();
        assert_eq!(todo.values["title"], json!("Plan sync protocol"));
    });

    db.run_as_user("user-alice", |alice| {
        let tx = alice
            .update_row(
                "todos",
                "todo-engineering",
                BTreeMap::from([("title".to_owned(), json!("Alice renames her task"))]),
            )
            .unwrap();
        assert_eq!(alice.transaction_info(&tx).unwrap().rejection_code, None);
        let todo = alice
            .read_rows("todos")
            .unwrap()
            .into_iter()
            .find(|row| row.id == "todo-engineering")
            .unwrap();
        assert_eq!(todo.values["title"], json!("Alice renames her task"));
    });
}

#[test]
fn mini_sqlite_todo_fixture_explains_project_visibility_query() {
    let mut db = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "seed-node",
        SchemaDef::mini_sqlite_todo_fixture(),
    )
    .unwrap();

    seed_group_visibility_fixture(&mut db);

    db.run_as_user("user-alice", |alice| {
        let plan = alice.explain_query_plan(&project_list_query()).unwrap();

        assert!(plan.sql.contains("SELECT"));
        assert!(!plan.plan.is_empty());
        assert!(plan.plan.iter().any(|row| !row.detail.is_empty()));
    });
}

#[test]
fn generic_policy_expressions_model_nested_group_project_visibility() {
    let mut db = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "seed-node",
        nested_group_policy_schema(),
    )
    .unwrap();

    seed_group_visibility_fixture(&mut db);

    db.run_as_user("user-alice", |alice| {
        assert_eq!(
            visible_ids(alice.read_rows("projects").unwrap()),
            vec!["project-alice", "project-company", "project-engineering"]
        );
        assert_eq!(
            visible_ids(open_todos_query(alice)),
            vec!["todo-alice", "todo-company", "todo-engineering"]
        );
    });

    db.run_as_user("user-cara", |cara| {
        assert_eq!(
            visible_ids(cara.read_rows("projects").unwrap()),
            vec!["project-company"]
        );
        assert_eq!(visible_ids(open_todos_query(cara)), vec!["todo-company"]);
    });
}

#[test]
fn generic_policy_export_includes_group_dependencies() {
    let schema = nested_group_policy_schema();
    let mut source =
        Runtime::open_trusted_with_schema(Storage::Memory, "source-node", schema.clone()).unwrap();
    seed_group_visibility_fixture(&mut source);

    let bundle = source
        .run_as_user("user-alice", |alice| {
            alice.export_table_history("project_members")
        })
        .unwrap();

    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "user-alice", schema).unwrap();
    peer.apply_bundle(&bundle).unwrap();

    assert_eq!(
        visible_ids(peer.read_rows("project_members").unwrap()),
        vec![
            "project-member-alice",
            "project-member-company",
            "project-member-engineering"
        ]
    );
}

fn nested_group_policy_schema() -> SchemaDef {
    SchemaDef::new()
        .table("users", |table| {
            table.text("name");
            table.read_if_row_id_equals_user();
        })
        .table("groups", |table| {
            table.text("name");
            table.read_if_inherits_referencing("group_members", "group");
        })
        .table("group_members", |table| {
            table.optional_ref("user", "users");
            table.optional_ref("member_group", "groups");
            table.ref_("group", "groups");
            table.index("by_user", ["user", "group"]);
            table.index("by_member_group", ["member_group", "group"]);
            table.read_if_user_or_ref_readable("user", "member_group");
        })
        .table("projects", |table| {
            table.text("title");
            table.read_if_inherits_referencing("project_members", "project");
        })
        .table("project_members", |table| {
            table.ref_("project", "projects");
            table.optional_ref("user", "users");
            table.optional_ref("group", "groups");
            table.index("by_user", ["user", "project"]);
            table.index("by_group", ["group", "project"]);
            table.index("by_project_user", ["project", "user"]);
            table.index("by_project_group", ["project", "group"]);
            table.read_if_user_or_ref_readable("user", "group");
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.index("open_created", ["done", "$createdAt"]);
            table.index("created", ["$createdAt"]);
            table.index("by_title", ["title"]);
            table.index("open_visible", ["done", "project", "$createdAt"]);
            table.read_if_inherits("project");
            table.write_if_ref_readable("project");
            table.update_protected_fields_if_created_by_user(["title", "project"]);
            table.delete_if_created_by_user();
        })
        .table("labels", |table| {
            table.text("name");
            table.index("by_name", ["name"]);
        })
        .table("todo_labels", |table| {
            table.ref_("todo", "todos");
            table.ref_("label", "labels");
            table.index("by_todo", ["todo"]);
            table.index("by_todo_created", ["todo", "$createdAt"]);
            table.index("by_label", ["label"]);
            table.read_if_inherits("todo");
        })
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
        ("group-company", "Company"),
        ("group-design", "Design"),
        ("group-support", "Support"),
    ] {
        db.insert_row(
            "groups",
            id,
            BTreeMap::from([("name".to_owned(), json!(name))]),
        )
        .unwrap();
    }

    for (id, member, group) in [
        (
            "group-member-alice-engineering",
            "user:user-alice",
            "group-engineering",
        ),
        (
            "group-member-bob-engineering",
            "user:user-bob",
            "group-engineering",
        ),
        (
            "group-member-engineering-company",
            "group:group-engineering",
            "group-company",
        ),
        (
            "group-member-design-company",
            "group:group-design",
            "group-company",
        ),
        (
            "group-member-support-company",
            "group:group-support",
            "group-company",
        ),
        ("group-member-bob-design", "user:user-bob", "group-design"),
        (
            "group-member-cara-support",
            "user:user-cara",
            "group-support",
        ),
    ] {
        db.insert_row("group_members", id, group_member_values(member, group))
            .unwrap();
    }

    for (id, title) in [
        ("project-engineering", "Engineering roadmap"),
        ("project-company", "Company strategy"),
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
        (
            "project-member-company",
            "project-company",
            "group:group-company",
        ),
        ("project-member-alice", "project-alice", "user:user-alice"),
        ("project-member-bob", "project-bob", "user:user-bob"),
    ] {
        db.insert_row(
            "project_members",
            id,
            project_member_values(project, member),
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
            "todo-company",
            "Review company plan",
            "project-company",
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

fn group_member_values(member: &str, group: &str) -> BTreeMap<String, serde_json::Value> {
    let (user, member_group) = split_member_ref(member);
    BTreeMap::from([
        ("user".to_owned(), user),
        ("member_group".to_owned(), member_group),
        ("group".to_owned(), json!(group)),
    ])
}

fn project_member_values(project: &str, member: &str) -> BTreeMap<String, serde_json::Value> {
    let (user, group) = split_member_ref(member);
    BTreeMap::from([
        ("project".to_owned(), json!(project)),
        ("user".to_owned(), user),
        ("group".to_owned(), group),
    ])
}

fn split_member_ref(member: &str) -> (serde_json::Value, serde_json::Value) {
    if let Some(user) = member.strip_prefix("user:") {
        (json!(user), serde_json::Value::Null)
    } else if let Some(group) = member.strip_prefix("group:") {
        (serde_json::Value::Null, json!(group))
    } else {
        (serde_json::Value::Null, serde_json::Value::Null)
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

fn project_list_query() -> BuiltQuery {
    BuiltQuery::from_json_value(json!({
        "table": "projects",
        "orderBy": [["title", "asc"]]
    }))
    .unwrap()
}

fn visible_ids(rows: Vec<RowView>) -> Vec<String> {
    let mut ids = rows.into_iter().map(|row| row.id).collect::<Vec<_>>();
    ids.sort();
    ids
}
