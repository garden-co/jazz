#![cfg(feature = "test")]

use jazz_tools::{QueryBuilder, Value};

use crate::common::{
    ClientPair, NO_DELTA_WINDOW, QUERY_TIMEOUT, create_file, create_file_part, create_org,
    create_post, create_team, create_user,
};
use crate::support::{
    collect_stream_deltas, has_added, has_any_change, wait_for_rows, wait_for_subscription_update,
};

/// Verifies that a join subscription becomes visible when the joined table row
/// that satisfies the join predicate is inserted after the base row.
///
/// ```text
/// writer ──create user Alice──────────► server
/// writer ──create post author=Alice──► server ──► subscriber add Alice
/// ```
#[tokio::test]
async fn subscribe_all_join_emits_when_matching_joined_row_is_inserted() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    wait_for_rows(
        &pair.subscriber,
        QueryBuilder::new("users").build(),
        "subscriber sees base join user before joined-table insert",
        |rows| rows.iter().any(|(id, _)| *id == user_id).then_some(rows),
    )
    .await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.author_name")
        .build();
    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to join query");
    let mut log = Vec::new();

    create_post(&pair.writer, 100, "Test Post", "Alice").await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "joined-table insert emits joined result",
        |log| has_added(log, user_id),
    )
    .await;

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query contains newly matched row",
        |rows| (rows.len() == 1 && rows[0].0 == user_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Text("Alice".to_string()),
            Value::Null,
            Value::Integer(100),
            Value::Text("Test Post".to_string()),
            Value::Text("Alice".to_string()),
        ]
    );

    pair.shutdown().await;
}

/// Verifies that a default join result is keyed by the base row and contains
/// values from both the base and joined tables.
#[tokio::test]
async fn subscribe_all_join_returns_base_and_joined_table_values() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    let post_id = create_post(&pair.writer, 100, "Hello World", "Alice").await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.author_name")
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query returns combined tuple",
        |rows| (rows.len() == 1 && rows[0].0 == user_id).then_some(rows),
    )
    .await;

    assert_ne!(
        rows[0].0, post_id,
        "default join output should be keyed by the base row id"
    );
    assert_eq!(
        rows[0].1,
        vec![
            Value::Text("Alice".to_string()),
            Value::Null,
            Value::Integer(100),
            Value::Text("Hello World".to_string()),
            Value::Text("Alice".to_string()),
        ]
    );

    pair.shutdown().await;
}

/// Verifies that filters can target a column supplied by the joined table.
#[tokio::test]
async fn subscribe_all_join_filter_on_joined_table_column() {
    let pair = ClientPair::start().await;

    create_user(&pair.writer, "Alice", None).await;
    let bob_id = create_user(&pair.writer, "Bob", None).await;
    create_post(&pair.writer, 100, "Hello World", "Alice").await;
    create_post(&pair.writer, 101, "Learning Rust", "Bob").await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.author_name")
        .filter_eq("title", Value::Text("Learning Rust".to_string()))
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query filters by joined table title",
        |rows| (rows.len() == 1 && rows[0].0 == bob_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Text("Bob".to_string()));
    assert_eq!(rows[0].1[3], Value::Text("Learning Rust".to_string()));

    pair.shutdown().await;
}

/// Verifies that alias-qualified filters resolve against the intended side of
/// a join.
#[tokio::test]
async fn subscribe_all_join_filter_on_scoped_alias_columns() {
    let pair = ClientPair::start().await;

    create_user(&pair.writer, "Alice", None).await;
    let bob_id = create_user(&pair.writer, "Bob", None).await;
    create_post(&pair.writer, 100, "Hello World", "Alice").await;
    create_post(&pair.writer, 101, "Learning Rust", "Bob").await;

    let query = QueryBuilder::new("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.name", "p.author_name")
        .filter_eq("u.name", Value::Text("Bob".to_string()))
        .filter_eq("p.title", Value::Text("Learning Rust".to_string()))
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query filters by scoped aliases",
        |rows| (rows.len() == 1 && rows[0].0 == bob_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Text("Bob".to_string()));
    assert_eq!(rows[0].1[3], Value::Text("Learning Rust".to_string()));

    pair.shutdown().await;
}

/// Verifies that a two-hop projected join traverses users -> teams -> orgs and
/// emits the org row (result_element_index 2) as the subscription result.
///
/// The writer creates an org, a team in that org, and a user in that team. The
/// subscription must surface the org as an add delta once all three rows are
/// present, and the query result must contain the org's data.
///
/// ```text
/// writer ──create org──────────────────► server
/// writer ──create team (org_id=org)───► server
/// writer ──create user (team_id=team)─► server
///                                          │
///              query: users -> teams -> orgs [result=orgs]
///                                          │
///                                          └──► subscriber (add delta: org ✓)
/// ```
#[tokio::test]
async fn subscribe_all_supports_hop_queries_via_projected_joins() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("users")
        .join("teams")
        .on("users.team_id", "teams._id")
        .join("orgs")
        .on("teams.org_id", "orgs._id")
        .result_element_index(2)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to hop query");
    let mut log = Vec::new();

    let org_id = create_org(&pair.writer, "Hop Org").await;
    let team_id = create_team(&pair.writer, "Hop Team", Some(org_id), None).await;
    let _user_id = create_user(&pair.writer, "Hop User", Some(team_id)).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "hop query add delta",
        |log| has_added(log, org_id),
    )
    .await;

    let rows = wait_for_rows(&pair.subscriber, query, "hop query rows", |rows| {
        (rows.len() == 1 && rows[0].0 == org_id).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, org_id);
    assert_eq!(rows[0].1[0], Value::Text("Hop Org".to_string()));

    pair.shutdown().await;
}

/// Verifies that updating a scalar foreign key causes a projected join
/// subscription to swap from the old joined row to the new one.
///
/// A user "Mover" starts assigned to Team A. The subscription projects
/// users -> teams (result_element_index 1), so it surfaces the team row. The
/// writer reassigns the user to Team B. Both Team A and Team B must appear
/// somewhere in the stream log (one exits, one enters), and the final query
/// result must contain only Team B.
///
/// ```text
/// writer ──create user (team_id=team_a)──► server
/// subscriber query result: [team_a]
///
/// writer ──update user.team_id -> team_b──► server ──► subscriber
///   stream: change for team_a AND team_b
///   query result: [team_b]
/// ```
#[tokio::test]
async fn subscribe_all_reacts_to_scalar_fk_updates_in_projected_join_queries() {
    let pair = ClientPair::start().await;

    let org_a = create_org(&pair.writer, "Org A").await;
    let org_b = create_org(&pair.writer, "Org B").await;
    let team_a = create_team(&pair.writer, "Team A", Some(org_a), None).await;
    let team_b = create_team(&pair.writer, "Team B", Some(org_b), None).await;
    let user_id = create_user(&pair.writer, "Mover", Some(team_a)).await;

    let query = QueryBuilder::new("users")
        .join("teams")
        .on("users.team_id", "teams._id")
        .result_element_index(1)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to team hop query");
    let mut log = Vec::new();

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial team row",
        |rows| (rows.len() == 1 && rows[0].0 == team_a).then_some(()),
    )
    .await;

    pair.writer
        .update(user_id, vec![("team_id".to_string(), Value::Uuid(team_b))])
        .expect("move user to new team");

    let rows = wait_for_rows(&pair.subscriber, query, "updated team row", |rows| {
        (rows.len() == 1 && rows[0].0 == team_b).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, team_b);
    assert_eq!(rows[0].1[0], Value::Text("Team B".to_string()));

    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    assert!(has_any_change(&log, team_a));
    assert!(has_any_change(&log, team_b));

    pair.shutdown().await;
}

/// Verifies that replacing a UUID array foreign key causes a projected join
/// subscription to react to both the departing and arriving joined rows.
///
/// A file starts with `parts = [part_a]`. The subscription projects
/// files -> file_parts (result_element_index 1), surfacing part rows. The
/// writer replaces the array with `[part_b]`. Both part_a and part_b must
/// appear somewhere in the stream log, and the final query result must
/// contain only part_b.
///
/// ```text
/// writer ──create file (parts=[part_a])────► server
/// subscriber query result: [part_a]
///
/// writer ──update file.parts -> [part_b]───► server ──► subscriber
///   stream: change for part_a AND part_b
///   query result: [part_b]
/// ```
#[tokio::test]
async fn subscribe_all_reacts_to_uuid_array_fk_updates_in_projected_join_queries() {
    let pair = ClientPair::start().await;

    let part_a = create_file_part(&pair.writer, "A").await;
    let part_b = create_file_part(&pair.writer, "B").await;
    let file_id = create_file(&pair.writer, "File", &[part_a]).await;

    let query = QueryBuilder::new("files")
        .join("file_parts")
        .on("files.parts", "file_parts._id")
        .result_element_index(1)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to file parts hop query");
    let mut log = Vec::new();

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial file part row",
        |rows| (rows.len() == 1 && rows[0].0 == part_a).then_some(()),
    )
    .await;

    pair.writer
        .update(
            file_id,
            vec![("parts".to_string(), Value::Array(vec![Value::Uuid(part_b)]))],
        )
        .expect("swap file part ids");

    let rows = wait_for_rows(&pair.subscriber, query, "updated file part row", |rows| {
        (rows.len() == 1 && rows[0].0 == part_b).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, part_b);
    assert_eq!(rows[0].1[0], Value::Text("B".to_string()));

    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    assert!(has_any_change(&log, part_a));
    assert!(has_any_change(&log, part_b));

    pair.shutdown().await;
}
