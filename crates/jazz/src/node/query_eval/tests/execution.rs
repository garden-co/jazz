//! execution query-evaluation tests.

use super::*;

#[test]
fn reachable_query_rows_uses_prepared_groove_plan() {
    let (_dir, mut node) = open_recursive_node();
    let schema = recursive_schema();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let resource1 = row(101);
    let resource2 = row(102);
    commit_global_cells(
        &mut node,
        "resources",
        resource1,
        BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
        10,
        1,
    );
    commit_global_cells(
        &mut node,
        "resources",
        resource2,
        BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
        11,
        2,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(201),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ]),
        12,
        3,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(202),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource2.0)),
            ("team".to_owned(), Value::Uuid(team1.0)),
        ]),
        13,
        4,
    );
    for (idx, member, parent, seq) in [(301, team1, team2, 5), (302, team2, team3, 6)] {
        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(idx),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            10 + seq,
            seq,
        );
    }

    let shape = recursive_shape(&schema);
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
        .unwrap();
    assert!(
        !node
            .query
            .query_shape_cache
            .keys()
            .any(|(shape_id, tier, _)| {
                *shape_id == shape.shape_id() && *tier == DurabilityTier::Global
            })
    );

    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(rows, BTreeSet::from([resource1, resource2]));
    assert!(matches!(
        node.query
            .query_shape_cache
            .iter()
            .find(|((shape_id, tier, _), _)| {
                *shape_id == shape.shape_id() && *tier == DurabilityTier::Global
            })
            .map(|(_, plan)| plan.as_ref()),
        Some(PreparedQueryPlan::Prepared { .. })
    ));
}

#[test]
fn reachable_relation_seed_query_rows_lowers_through_query_engine() {
    let (_dir, mut node) = open_recursive_node();
    let schema = recursive_schema();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let team4 = row(4);
    let resource1 = row(101);
    let resource2 = row(102);
    commit_global_cells(
        &mut node,
        "resources",
        resource1,
        BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
        10,
        1,
    );
    commit_global_cells(
        &mut node,
        "resources",
        resource2,
        BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
        11,
        2,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(201),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ]),
        12,
        3,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(202),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource2.0)),
            ("team".to_owned(), Value::Uuid(team4.0)),
        ]),
        13,
        4,
    );
    commit_global_cells(
        &mut node,
        "teamSeeds",
        row(401),
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team1.0)),
            ("kind".to_owned(), Value::String("sync".to_owned())),
        ]),
        14,
        5,
    );
    commit_global_cells(
        &mut node,
        "teamSeeds",
        row(402),
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team4.0)),
            ("kind".to_owned(), Value::String("other".to_owned())),
        ]),
        15,
        6,
    );
    for (idx, member, parent, seq) in [(301, team1, team2, 7), (302, team2, team3, 8)] {
        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(idx),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            10 + seq,
            seq,
        );
    }

    let mut query = Query::from("resources").reachable_via(
        "resourceAccess",
        "resource",
        "team",
        lit("ignored-by-relation-seed"),
        "teamTeamMemberships",
        "member",
        "parent",
        [eq(col("onlyAdmins"), lit(false))],
    );
    query.reachable[0].seed = Some(crate::query::ReachableSeed {
        table: "teamSeeds".to_owned(),
        user_column: None,
        user_claim: None,
        team_column: "team".to_owned(),
        filters: vec![gt(col("kind"), param("seed_kind_lower_bound"))],
    });
    let shape = query.validate(&schema).unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "seed_kind_lower_bound".to_owned(),
            Value::String("s".to_owned()),
        )]))
        .unwrap();

    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(rows, BTreeSet::from([resource1]));
}

#[test]
fn reachable_relation_seed_hydrates_from_primary_key_scan() {
    let (_dir, mut node) = open_recursive_node();
    let schema = recursive_schema();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let team4 = row(4);
    let resource1 = row(101);
    let resource2 = row(102);
    let seed = row(401);
    commit_global_cells(
        &mut node,
        "resources",
        resource1,
        BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
        10,
        1,
    );
    commit_global_cells(
        &mut node,
        "resources",
        resource2,
        BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
        11,
        2,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(201),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ]),
        12,
        3,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(202),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource2.0)),
            ("team".to_owned(), Value::Uuid(team4.0)),
        ]),
        13,
        4,
    );
    for idx in 0..128 {
        commit_global_cells(
            &mut node,
            "teamSeeds",
            row(500 + idx),
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team4.0)),
                ("kind".to_owned(), Value::String(format!("noise-{idx}"))),
            ]),
            1_000 + idx as u64,
            20 + idx as u64,
        );
    }
    commit_global_cells(
        &mut node,
        "teamSeeds",
        seed,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team1.0)),
            ("kind".to_owned(), Value::String("sync".to_owned())),
        ]),
        14,
        5,
    );
    for (idx, member, parent, seq) in [(301, team1, team2, 7), (302, team2, team3, 8)] {
        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(idx),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            10 + seq,
            seq,
        );
    }

    let mut query = Query::from("resources").reachable_via(
        "resourceAccess",
        "resource",
        "team",
        lit("ignored-by-relation-seed"),
        "teamTeamMemberships",
        "member",
        "parent",
        [eq(col("onlyAdmins"), lit(false))],
    );
    query.reachable[0].seed = Some(crate::query::ReachableSeed {
        table: "teamSeeds".to_owned(),
        user_column: None,
        user_claim: None,
        team_column: "team".to_owned(),
        filters: vec![eq(col("id"), lit(Value::Uuid(seed.0)))],
    });
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    node.reset_query_engine_read_metrics();
    let selected = node
        .query_rows_for_link(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    let selected_metrics = node.query_engine_read_metrics().clone();
    node.reset_query_engine_read_metrics();
    let forced = node
        .query_rows_for_link_forced_full_scan_for_test(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    let forced_metrics = node.query_engine_read_metrics().clone();

    assert_eq!(selected, forced);
    assert_eq!(selected, BTreeSet::from([resource1]));
    assert_eq!(selected_metrics.source_primary_key_scans, 1);
    assert!(
        forced_metrics.source_full_scans > selected_metrics.source_full_scans,
        "forced full scan must scan the seed source instead of using its point lookup"
    );
}

#[test]
fn query_rows_at_lowers_reachable_against_historical_current_sources() {
    let (_dir, mut node) = open_recursive_node();
    let schema = recursive_schema();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let resource1 = row(101);
    let resource2 = row(102);
    commit_global_cells(
        &mut node,
        "resources",
        resource1,
        BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
        10,
        1,
    );
    commit_global_cells(
        &mut node,
        "resources",
        resource2,
        BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
        11,
        2,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(201),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ]),
        12,
        3,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(202),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource2.0)),
            ("team".to_owned(), Value::Uuid(team1.0)),
        ]),
        13,
        4,
    );
    commit_global_cells(
        &mut node,
        "teamTeamMemberships",
        row(301),
        BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team1.0)),
            ("parent".to_owned(), Value::Uuid(team2.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ]),
        14,
        5,
    );
    commit_global_cells(
        &mut node,
        "teamTeamMemberships",
        row(302),
        BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team2.0)),
            ("parent".to_owned(), Value::Uuid(team3.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ]),
        15,
        6,
    );
    let shape = recursive_shape(&schema);
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
        .unwrap();

    let before_delete = node
        .query_rows_at(&shape, &binding, GlobalTime(6))
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    delete_global(&mut node, "teamTeamMemberships", row(302), 16, 7);
    let after_delete = node
        .query_rows_at(&shape, &binding, GlobalTime(7))
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(before_delete, BTreeSet::from([resource1, resource2]));
    assert!(
        after_delete == BTreeSet::from([resource2]),
        "later historical cuts should see the edge deletion while preserving direct access"
    );
}

#[test]
fn query_filter_matches_naive_local_scan() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let bob = author(2);
    let mut expected = BTreeSet::new();
    for idx in 0..48 {
        let state = if idx % 3 == 0 { "done" } else { "open" };
        let assignee = if idx % 2 == 0 { alice } else { bob };
        if state == "open" && assignee == alice {
            expected.insert(row(idx));
        }
        commit_issue(&mut node, idx, state, assignee);
    }
    let shape = Query::from("issues")
        .filter(eq(col("state"), lit("open")))
        .filter(eq(col("assignee"), param("user")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();
    let actual = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn text_range_predicates_use_lexicographic_row_comparison() {
    assert_eq!(
        compare_values(
            &Value::String("beta".to_owned()),
            &Value::String("alpha".to_owned())
        ),
        Some(std::cmp::Ordering::Greater)
    );
    assert_eq!(
        compare_values(
            &Value::String("alpha".to_owned()),
            &Value::String("alpha".to_owned())
        ),
        Some(std::cmp::Ordering::Equal)
    );
    assert_eq!(
        compare_values(
            &Value::String("alpha".to_owned()),
            &Value::String("beta".to_owned())
        ),
        Some(std::cmp::Ordering::Less)
    );
}

#[test]
fn text_range_query_filters_rows_lexicographically() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    for idx in 0..6 {
        commit_issue(&mut node, idx, "open", alice);
    }
    let shape = Query::from("issues")
        .filter(gt(col("title"), lit("issue-2")))
        .filter(lte(col("title"), lit("issue-4")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let actual = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, BTreeSet::from([row(3), row(4)]));
}

#[test]
fn public_id_equality_query_filters_rows_by_row_uuid() {
    let (_dir, mut node) = open_node();
    for idx in 0..4 {
        commit_issue(&mut node, idx, "open", author(1));
    }
    let shape = Query::from("issues")
        .filter(eq(col("id"), lit(Value::Uuid(row(2).0))))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let actual = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![row(2)]);
}

#[test]
fn public_id_in_query_filters_rows_by_row_uuid() {
    let (_dir, mut node) = open_node();
    for idx in 0..5 {
        commit_issue(&mut node, idx, "open", author(1));
    }
    let shape = Query::from("issues")
        .filter(in_list(
            col("id"),
            [lit(Value::Uuid(row(1).0)), lit(Value::Uuid(row(3).0))],
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let actual = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, BTreeSet::from([row(1), row(3)]));
}

#[test]
fn public_id_range_query_and_order_by_use_row_uuid() {
    let (_dir, mut node) = open_node();
    for idx in [3, 1, 4, 0, 2] {
        commit_issue(&mut node, idx, "open", author(1));
    }
    let shape = Query::from("issues")
        .filter(gt(col("id"), lit(Value::Uuid(row(1).0))))
        .order_by("id", OrderDirection::Desc)
        .limit(2)
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let actual = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![row(4), row(3)]);
}

#[test]
fn query_order_by_sorts_before_limit_offset() {
    let (_dir, mut node) = open_node();
    for idx in [3, 1, 4, 0, 2] {
        commit_issue(&mut node, idx, "open", author(1));
    }

    let asc_shape = Query::from("issues")
        .order_by("title", OrderDirection::Asc)
        .validate(&schema())
        .unwrap();
    let asc_binding = asc_shape.bind(BTreeMap::new()).unwrap();
    let asc_rows = node
        .query_rows(&asc_shape, &asc_binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(asc_rows, vec![row(0), row(1), row(2), row(3), row(4)]);

    let shape = Query::from("issues")
        .order_by("title", OrderDirection::Desc)
        .offset(1)
        .limit(2)
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();

    assert_eq!(rows, vec![row(3), row(2)]);
}

#[test]
fn query_order_by_multi_key_is_deterministic() {
    let (_dir, mut node) = open_node();
    commit_issue(&mut node, 3, "done", author(1));
    commit_issue(&mut node, 1, "open", author(1));
    commit_issue(&mut node, 2, "open", author(1));
    commit_issue(&mut node, 0, "done", author(1));

    let shape = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .order_by("title", OrderDirection::Desc)
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();

    assert_eq!(rows, vec![row(3), row(0), row(2), row(1)]);
}

#[test]
fn aggregate_count_over_filtered_query() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let bob = author(2);
    for idx in 0..8 {
        let assignee = if idx % 2 == 0 { alice } else { bob };
        let state = if idx == 6 { "done" } else { "open" };
        commit_issue(&mut node, idx, state, assignee);
    }
    let shape = Query::from("issues")
        .filter(eq(col("state"), lit("open")))
        .filter(eq(col("assignee"), param("user")))
        .count()
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].test_cells_by_descriptor()["count"], Value::U64(3));
}

#[test]
fn aggregate_sum_min_max_over_filtered_query() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let bob = author(2);
    for idx in 0..6 {
        let assignee = if idx % 2 == 0 { alice } else { bob };
        commit_issue(&mut node, idx, "open", assignee);
    }
    let shape = Query::from("issues")
        .filter(eq(col("assignee"), param("user")))
        .aggregate([
            Aggregate::sum("priority"),
            Aggregate::min("priority"),
            Aggregate::max("priority"),
        ])
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();
    let cells = rows[0].test_cells_by_descriptor();
    assert_eq!(cells["sum_priority"], Value::U64(6));
    assert_eq!(cells["min_priority"], Value::U64(0));
    assert_eq!(cells["max_priority"], Value::U64(4));
}

#[test]
fn aggregate_sum_avg_min_max_support_signed_i64_inputs() {
    let schema = signed_metric_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xb5; 16]), schema.clone());
    commit_signed_metric(&mut node, 0x10, "a", -3);
    commit_signed_metric(&mut node, 0x11, "a", 2);
    let shape = Query::from("metrics")
        .aggregate([
            Aggregate::sum("score"),
            Aggregate::avg("score"),
            Aggregate::min("score"),
            Aggregate::max("score"),
        ])
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();
    let cells = rows[0].test_cells_by_descriptor();
    assert_eq!(cells["sum_score"], Value::I64(-1));
    assert_eq!(cells["avg_score"], Value::F64(-0.5));
    assert_eq!(cells["min_score"], Value::I64(-3));
    assert_eq!(cells["max_score"], Value::I64(2));
}

#[test]
fn aggregate_explicit_user_prefix_alias_remains_a_logical_name() {
    let schema = signed_metric_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xb6; 16]), schema.clone());
    commit_signed_metric(&mut node, 0x12, "a", -3);
    commit_signed_metric(&mut node, 0x13, "a", 2);
    let shape = Query::from("metrics")
        .aggregate([Aggregate::sum("score").alias("user_total")])
        .validate(&schema)
        .expect("explicit user-prefix aggregate alias is valid");
    let rows = node
        .query_rows(
            &shape,
            &shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].test_cells_by_descriptor()["user_total"],
        Value::I64(-1),
    );
}

#[test]
fn aggregate_grouped_count_orders_before_limit_offset() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    for idx in 0..6 {
        let state = match idx {
            0 => "done",
            1 | 2 => "open",
            _ => "blocked",
        };
        commit_issue(&mut node, idx, state, alice);
    }
    let shape = Query::from("issues")
        .count()
        .group_by("state")
        .order_by("count", OrderDirection::Desc)
        .order_by("state", OrderDirection::Asc)
        .offset(1)
        .limit(1)
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0].test_cells_by_descriptor();
    assert_eq!(cells["state"], Value::String("open".to_owned()));
    assert_eq!(cells["count"], Value::U64(2));
}

#[test]
fn query_join_via_matches_junction_semantics() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let bob = author(2);
    for idx in 0..6 {
        commit_issue(&mut node, idx, "open", bob);
    }
    commit_member(&mut node, 0, row(0), alice);
    commit_member(&mut node, 1, row(2), alice);
    commit_member(&mut node, 2, row(2), bob);
    commit_member(&mut node, 3, row(5), bob);
    let shape = Query::from("issues")
        .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
        .validate(&schema())
        .unwrap();
    let alice_binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();
    let bob_binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(bob.test_uuid()),
        )]))
        .unwrap();
    let alice_rows = node
        .query_rows(&shape, &alice_binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    let bob_rows = node
        .query_rows(&shape, &bob_binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(alice_rows, BTreeSet::from([row(0), row(2)]));
    assert_eq!(bob_rows, BTreeSet::from([row(2), row(5)]));
}

#[test]
fn query_join_via_nested_joins_filters_visible_roots() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let bob = author(2);
    commit_global_user(&mut node, alice, "Alice", 1);
    commit_global_user(&mut node, bob, "Bob", 2);
    for idx in 0..4 {
        commit_issue(&mut node, idx, "open", bob);
    }
    commit_member(&mut node, 0, row(0), alice);
    commit_member(&mut node, 1, row(1), bob);
    commit_member(&mut node, 2, row(2), alice);

    let nested = Query::from("issue_members")
        .join_via_row_id("users", "user", [eq(col("name"), lit("Alice"))])
        .joins
        .into_iter()
        .next()
        .unwrap();
    let shape = Query::from("issues")
        .join_via_with_nested_joins("issue_members", "issue", [], [nested])
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(rows, BTreeSet::from([row(0), row(2)]));
}

#[test]
fn query_join_via_source_lookup_filters_visible_roots() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let bob = author(2);
    commit_global_user(&mut node, alice, "Alice", 1);
    commit_global_user(&mut node, bob, "Bob", 2);
    commit_issue(&mut node, 0, "open", alice);
    commit_issue(&mut node, 1, "open", bob);
    commit_issue(&mut node, 2, "open", alice);
    commit_member(&mut node, 0, row(100), alice);
    commit_member(&mut node, 1, row(101), bob);

    let shape = Query::from("issues")
        .join_via_source_lookup(
            "issue_members",
            "user",
            JoinSourceLookup {
                table: "users".to_owned(),
                row_id_source_column: "assignee".to_owned(),
                value_column: "id".to_owned(),
            },
            [eq(col("issue"), lit(Value::Uuid(row(100).0)))],
        )
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(rows, BTreeSet::from([row(0), row(2)]));
}

#[test]
fn unsettled_query_reads_own_pending_write() {
    let (_dir, mut node) = open_node();
    commit_issue(&mut node, 1, "open", author(1));
    let shape = Query::from("issues")
        .filter(eq(col("state"), lit("open")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        node.query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        node.query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn tx_query_snapshot_is_stable_after_concurrent_arrival() {
    let (_dir, mut node) = open_node();
    commit_issue(&mut node, 1, "open", author(1));
    let shape = Query::from("issues")
        .filter(eq(col("state"), lit("open")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let tx = OpenTransactionId::new();
    node.open_exclusive(tx).unwrap();
    assert_eq!(node.tx_query(tx, &shape, &binding).unwrap().len(), 1);
    commit_issue(&mut node, 2, "open", author(1));
    assert_eq!(node.tx_query(tx, &shape, &binding).unwrap().len(), 1);
    node.abandon_tx(tx).unwrap();
}

#[test]
fn tx_query_reachable_uses_shared_snapshot_sources() {
    let (_dir, mut node) = open_recursive_node();
    let schema = recursive_schema();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let team4 = row(4);
    let resource1 = row(101);
    let resource2 = row(102);
    commit_global_cells(
        &mut node,
        "resources",
        resource1,
        BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
        10,
        1,
    );
    commit_global_cells(
        &mut node,
        "resources",
        resource2,
        BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
        11,
        2,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(201),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ]),
        12,
        3,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(202),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource2.0)),
            ("team".to_owned(), Value::Uuid(team4.0)),
        ]),
        13,
        4,
    );
    for (idx, member, parent, seq) in [(301, team1, team2, 5), (302, team2, team3, 6)] {
        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(idx),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            10 + seq,
            seq,
        );
    }

    let shape = recursive_shape(&schema);
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
        .unwrap();
    let tx = OpenTransactionId::new();
    node.open_exclusive(tx).unwrap();
    let rows = node
        .tx_query(tx, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(rows, BTreeSet::from([resource1]));

    commit_global_cells(
        &mut node,
        "teamTeamMemberships",
        row(303),
        BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team3.0)),
            ("parent".to_owned(), Value::Uuid(team4.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ]),
        20,
        7,
    );
    let rows = node
        .tx_query(tx, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(rows, BTreeSet::from([resource1]));
    node.abandon_tx(tx).unwrap();
}
