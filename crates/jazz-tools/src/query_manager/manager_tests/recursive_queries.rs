use super::*;

#[test]
fn recursive_query_expands_transitive_team_edges() {
    let sync_manager = SyncManager::new();
    let schema = recursive_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Seed team and edge data:
    // 1 -> 2 -> 3 -> 1 (cycle)
    qm.insert(&mut storage, "teams", &[Value::Integer(1)])
        .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Integer(1), Value::Integer(2)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Integer(2), Value::Integer(3)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Integer(3), Value::Integer(1)],
    )
    .unwrap();

    let query = qm
        .query("teams")
        .select(&["team_id"])
        .filter_eq("team_id", Value::Integer(1))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "team_id")
                .select(&["parent_team"])
                .max_depth(10)
        })
        .build();

    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let mut ids: Vec<i32> = results
        .into_iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    ids.sort_unstable();

    assert_eq!(ids, vec![1, 2, 3], "Should compute recursive closure");
}

#[test]
fn recursive_query_with_hop_expands_transitive_closure() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let team1 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-3".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
    )
    .unwrap();

    let query = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();

    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let mut names: Vec<String> = results
        .into_iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["team-1", "team-2", "team-3"]);
}

#[test]
fn recursive_query_with_self_referential_hop_expands_ancestors() {
    use crate::query_manager::query::Query;
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, KeyRef, PredicateCmpOp, PredicateExpr, ProjectColumn,
        ProjectExpr, RelExpr, RowIdRef, ValueRef,
    };

    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("org_id", ColumnType::Uuid).nullable(),
            ColumnDescriptor::new("parent_id", ColumnType::Uuid).nullable(),
        ])
        .into(),
    );
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let root = qm
        .insert(
            &mut storage,
            "teams",
            &[Value::Text("root".into()), Value::Null, Value::Null],
        )
        .unwrap();
    let mid = qm
        .insert(
            &mut storage,
            "teams",
            &[
                Value::Text("mid".into()),
                Value::Null,
                Value::Uuid(root.row_id),
            ],
        )
        .unwrap();
    let leaf = qm
        .insert(
            &mut storage,
            "teams",
            &[
                Value::Text("leaf".into()),
                Value::Null,
                Value::Uuid(mid.row_id),
            ],
        )
        .unwrap();

    let mut query = Query::new("teams");
    query.relation_ir = RelExpr::Gather {
        seed: Box::new(RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("teams"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::scoped("teams", "id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Uuid(leaf.row_id)),
            },
        }),
        step: Box::new(RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("teams"),
                    }),
                    predicate: PredicateExpr::Cmp {
                        left: ColumnRef::scoped("teams", "id"),
                        op: PredicateCmpOp::Eq,
                        right: ValueRef::RowId(RowIdRef::Frontier),
                    },
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("teams", "parent_id"),
                    right: ColumnRef::scoped("__recursive_hop_0", "id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            columns: vec![
                ProjectColumn {
                    alias: "id".into(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "id")),
                },
                ProjectColumn {
                    alias: "name".into(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "name")),
                },
                ProjectColumn {
                    alias: "parent_id".into(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "parent_id")),
                },
                ProjectColumn {
                    alias: "org_id".into(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "org_id")),
                },
            ],
        }),
        frontier_key: KeyRef::RowId(RowIdRef::Current),
        max_depth: 10,
        dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
    };

    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let mut ids: Vec<ObjectId> = results.into_iter().map(|(id, _)| id).collect();
    ids.sort();

    let mut expected = vec![root.row_id, mid.row_id, leaf.row_id];
    expected.sort();
    assert_eq!(ids, expected);
}

#[test]
fn recursive_query_with_join_project_step_is_rejected() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query_result = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .join("teams")
                .alias("__recursive_hop_0")
                .on("team_edges.parent_team", "__recursive_hop_0.id")
                .result_element_index(1)
                .max_depth(10)
        })
        .try_build();

    assert!(
        query_result.is_err(),
        "recursive join-projection shape should be rejected"
    );
}

#[test]
fn recursive_hop_query_subscriptions_receive_expansion_updates() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let team1 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-3".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
    )
    .unwrap();

    let query = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let _initial_updates = qm.take_updates();

    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
    )
    .unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Recursive hop subscription should receive updates");
    let team_descriptor = qm
        .schema_context()
        .current_schema
        .get(&TableName::new("teams"))
        .unwrap();
    let added_names: Vec<String> = delta
        .added
        .iter()
        .filter_map(|row| {
            match decode_row(&team_descriptor.columns, &row.data)
                .ok()?
                .first()
            {
                Some(Value::Text(name)) => Some(name.clone()),
                _ => None,
            }
        })
        .collect();
    assert!(
        added_names.contains(&"team-3".to_string()),
        "team-3 should be added when edge team-2 -> team-3 is inserted"
    );
}

#[test]
fn contributing_ids_for_recursive_hop_include_recursive_dependencies() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let team1 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-3".into())])
        .unwrap();

    let edge1 = qm
        .insert(
            &mut storage,
            "team_edges",
            &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
        )
        .unwrap();
    let edge2 = qm
        .insert(
            &mut storage,
            "team_edges",
            &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
        )
        .unwrap();

    let query = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        5,
        "Recursive hop outputs depend on both discovered rows and traversal edges"
    );
    assert!(contributing.contains(&(team1.row_id, branch)));
    assert!(contributing.contains(&(team2.row_id, branch)));
    assert!(contributing.contains(&(team3.row_id, branch)));
    assert!(contributing.contains(&(edge1.row_id, branch)));
    assert!(contributing.contains(&(edge2.row_id, branch)));
}

#[test]
fn e2e_client_receives_recursive_hop_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = recursive_hop_team_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let team1 = server
        .insert(&mut server_io, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = server
        .insert(&mut server_io, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = server
        .insert(&mut server_io, "teams", &[Value::Text("team-3".into())])
        .unwrap();
    server
        .insert(
            &mut server_io,
            "team_edges",
            &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "team_edges",
            &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
        )
        .unwrap();
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let mut names: Vec<String> = client
        .get_subscription_results(sub_id)
        .into_iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect();
    names.sort();

    assert_eq!(
        names,
        vec!["team-1", "team-2", "team-3"],
        "Client should receive recursive dependencies needed to replay the closure"
    );
}
