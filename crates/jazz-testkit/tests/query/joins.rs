use jazz::query::{Query, col, eq, lit, table};
use jazz::tools::{DurabilityTier, OrderedRowDelta, QueryResult, ResultKey, Value};

use crate::common::{
    ClientPair, QUERY_TIMEOUT, create_file, create_file_part, create_org, create_post, create_team,
    create_user,
};
use crate::support::{wait_for_query_results, wait_for_rows, wait_for_subscription_update};

fn has_added_key(log: &[OrderedRowDelta], key: &ResultKey) -> bool {
    log.iter()
        .any(|delta| delta.added.iter().any(|change| &change.id == key))
}

fn has_removed_key(log: &[OrderedRowDelta], key: &ResultKey) -> bool {
    log.iter()
        .any(|delta| delta.removed.iter().any(|change| &change.id == key))
}

fn field<'a>(result: &'a QueryResult, name: &str) -> &'a Value {
    result
        .get(name)
        .unwrap_or_else(|| panic!("joined result is missing field {name}: {result:?}"))
}

macro_rules! local_tokio_test {
    ($(#[$attr:meta])* async fn $name:ident() $body:block) => {
        $(#[$attr])*
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            tokio::task::LocalSet::new()
                .run_until(async $body)
                .await;
        }
    };
}

local_tokio_test! {
/// Verifies that a join subscription becomes visible when the joined table row
/// that satisfies the join predicate is inserted after the base row.
///
/// ```text
/// writer ──create user Alice──────────► server
/// writer ──create post author=Alice──► server ──► subscriber add Alice
/// ```
async fn subscribe_all_join_emits_when_matching_joined_row_is_inserted() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    wait_for_rows(
        &pair.subscriber,
        jazz::query::Query::from("users"),
        "subscriber sees base join user before joined-table insert",
        |rows| rows.iter().any(|(id, _)| *id == user_id).then_some(rows),
    )
    .await;

    let query = Query::from("users").flat_join("posts", "users.name", "posts.author_name");
    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to join query");
    let mut log = Vec::new();

    create_post(&pair.writer, 100, "Test Post", "Alice").await;

    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "join query contains newly matched row",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    let result_key = rows[0].key.clone();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "joined-table insert emits joined result",
        |log| has_added_key(log, &result_key),
    )
    .await;

    assert_eq!(rows[0].key.row_id(), None);
    assert_eq!(
        field(&rows[0], "users.name"),
        &Value::Text("Alice".to_string())
    );
    assert_eq!(field(&rows[0], "users.team_id"), &Value::Null);
    assert_eq!(field(&rows[0], "posts.id"), &Value::Integer(100));
    assert_eq!(
        field(&rows[0], "posts.title"),
        &Value::Text("Test Post".to_string())
    );
    assert_eq!(
        field(&rows[0], "posts.author_name"),
        &Value::Text("Alice".to_string())
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that a default join result has a stable composite key and contains
/// values from both the base and joined tables.
async fn subscribe_all_join_returns_base_and_joined_table_values() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    let post_id = create_post(&pair.writer, 100, "Hello World", "Alice").await;

    let query = Query::from("users").flat_join("posts", "users.name", "posts.author_name");
    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "join query returns combined tuple",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    assert_ne!(rows[0].key, user_id);
    assert_ne!(rows[0].key, post_id);
    assert_eq!(rows[0].key.row_id(), None);
    assert_eq!(
        field(&rows[0], "users.name"),
        &Value::Text("Alice".to_string())
    );
    assert_eq!(field(&rows[0], "users.team_id"), &Value::Null);
    assert_eq!(field(&rows[0], "posts.id"), &Value::Integer(100));
    assert_eq!(
        field(&rows[0], "posts.title"),
        &Value::Text("Hello World".to_string())
    );
    assert_eq!(
        field(&rows[0], "posts.author_name"),
        &Value::Text("Alice".to_string())
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that filters can target a column supplied by the joined table.
async fn subscribe_all_join_filter_on_joined_table_column() {
    let pair = ClientPair::start().await;

    create_user(&pair.writer, "Alice", None).await;
    let bob_id = create_user(&pair.writer, "Bob", None).await;
    create_post(&pair.writer, 100, "Hello World", "Alice").await;
    create_post(&pair.writer, 101, "Learning Rust", "Bob").await;

    let query = Query::from("users")
        .flat_join("posts", "users.name", "posts.author_name")
        .filter(eq(col("title"), lit("Learning Rust")));
    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "join query filters by joined table title",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    assert_ne!(rows[0].key, bob_id);
    assert_eq!(
        field(&rows[0], "users.name"),
        &Value::Text("Bob".to_string())
    );
    assert_eq!(
        field(&rows[0], "posts.title"),
        &Value::Text("Learning Rust".to_string())
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that alias-qualified filters resolve against the intended side of
/// a join.
async fn subscribe_all_join_filter_on_scoped_alias_columns() {
    let pair = ClientPair::start().await;

    create_user(&pair.writer, "Alice", None).await;
    let bob_id = create_user(&pair.writer, "Bob", None).await;
    create_post(&pair.writer, 100, "Hello World", "Alice").await;
    create_post(&pair.writer, 101, "Learning Rust", "Bob").await;

    let query = Query::from(table("users").alias("u"))
        .flat_join(table("posts").alias("p"), "u.name", "p.author_name")
        .filter(eq(col("u.name"), lit("Bob")))
        .filter(eq(col("p.title"), lit("Learning Rust")));
    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "join query filters by scoped aliases",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    assert_ne!(rows[0].key, bob_id);
    assert_eq!(field(&rows[0], "u.name"), &Value::Text("Bob".to_string()));
    assert_eq!(
        field(&rows[0], "p.title"),
        &Value::Text("Learning Rust".to_string())
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that a two-hop flat join traverses users -> teams -> orgs and
/// emits the combined occurrence containing the org row.
///
/// The writer creates an org, a team in that org, and a user in that team. The
/// subscription must surface the combined occurrence as an add delta once all
/// three rows are present, and the query result must contain the org's data.
///
/// ```text
/// writer ──create org──────────────────► server
/// writer ──create team (org_id=org)───► server
/// writer ──create user (team_id=team)─► server
///                                          │
///              query: users -> teams -> orgs
///                                          │
///                                          └──► subscriber (add delta: tuple ✓)
/// ```
async fn subscribe_all_supports_hop_queries_via_projected_joins() {
    let pair = ClientPair::start().await;
    let query = Query::from("users")
        .flat_join("teams", "users.team_id", "teams.id")
        .flat_join("orgs", "teams.org_id", "orgs.id");

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to hop query");
    let mut log = Vec::new();

    let org_id = create_org(&pair.writer, "Hop Org").await;
    let team_id = create_team(&pair.writer, "Hop Team", Some(org_id), None).await;
    create_user(&pair.writer, "Hop User", Some(team_id)).await;

    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "hop query rows",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    let result_key = rows[0].key.clone();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "hop query add delta",
        |log| has_added_key(log, &result_key),
    )
    .await;

    assert_ne!(rows[0].key, org_id);
    assert_eq!(
        field(&rows[0], "orgs.name"),
        &Value::Text("Hop Org".to_string())
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that updating a scalar foreign key causes a flat-join subscription
/// to swap from the old joined occurrence to the new one.
///
/// A user "Mover" starts assigned to Team A. The writer reassigns the user to
/// Team B. The Team A occurrence must exit, the Team B occurrence must enter,
/// and the final query result must contain only Team B.
///
/// ```text
/// writer ──create user (team_id=team_a)──► server
/// subscriber query result: [team_a]
///
/// writer ──update user.team_id -> team_b──► server ──► subscriber
///   stream: change for team_a AND team_b
///   query result: [team_b]
/// ```
async fn subscribe_all_reacts_to_scalar_fk_updates_in_projected_join_queries() {
    let pair = ClientPair::start().await;

    let org_a = create_org(&pair.writer, "Org A").await;
    let org_b = create_org(&pair.writer, "Org B").await;
    let team_a = create_team(&pair.writer, "Team A", Some(org_a), None).await;
    let team_b = create_team(&pair.writer, "Team B", Some(org_b), None).await;
    let user_id = create_user(&pair.writer, "Mover", Some(team_a)).await;

    let query = Query::from("users").flat_join("teams", "users.team_id", "teams.id");

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to team hop query");
    let mut log = Vec::new();

    let initial_rows = wait_for_query_results(
        &pair.subscriber,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "initial team row",
        |rows| {
            (rows.len() == 1 && field(&rows[0], "teams.name") == &Value::Text("Team A".to_string()))
                .then_some(rows)
        },
    )
    .await;
    let team_a_key = initial_rows[0].key.clone();

    pair.writer
        .update(user_id, vec![("team_id".to_string(), Value::Uuid(team_b))])
        .expect("move user to new team");

    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "updated team row",
        |rows| {
            (rows.len() == 1 && field(&rows[0], "teams.name") == &Value::Text("Team B".to_string()))
                .then_some(rows)
        },
    )
    .await;
    let team_b_key = rows[0].key.clone();
    assert_ne!(team_a_key, team_b_key);
    assert_ne!(team_a_key, team_a);
    assert_ne!(team_b_key, team_b);
    assert_eq!(
        field(&rows[0], "teams.name"),
        &Value::Text("Team B".to_string())
    );

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "scalar FK retarget removes Team A occurrence and adds Team B occurrence",
        |log| has_removed_key(log, &team_a_key) && has_added_key(log, &team_b_key),
    )
    .await;

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that replacing a UUID array foreign key causes a flat-join
/// subscription to react to both the departing and arriving occurrences.
///
/// A file starts with `parts = [part_a]`. The writer replaces the array with
/// `[part_b]`. The part A occurrence must exit, the part B occurrence must
/// enter, and the final query result must contain only part B.
///
/// ```text
/// writer ──create file (parts=[part_a])────► server
/// subscriber query result: [part_a]
///
/// writer ──update file.parts -> [part_b]───► server ──► subscriber
///   stream: change for part_a AND part_b
///   query result: [part_b]
/// ```
async fn subscribe_all_reacts_to_uuid_array_fk_updates_in_projected_join_queries() {
    let pair = ClientPair::start().await;

    let part_a = create_file_part(&pair.writer, "A").await;
    let part_b = create_file_part(&pair.writer, "B").await;
    let file_id = create_file(&pair.writer, "File", &[part_a]).await;

    let query =
        Query::from("files").flat_join("file_parts", "files.parts", "file_parts.id");

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to file parts hop query");
    let mut log = Vec::new();

    let initial_rows = wait_for_query_results(
        &pair.subscriber,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "initial file part row",
        |rows| {
            (rows.len() == 1
                && field(&rows[0], "file_parts.label") == &Value::Text("A".to_string()))
                .then_some(rows)
        },
    )
    .await;
    let part_a_key = initial_rows[0].key.clone();

    pair.writer
        .update(
            file_id,
            vec![("parts".to_string(), Value::Array(vec![Value::Uuid(part_b)]))],
        )
        .expect("swap file part ids");

    let rows = wait_for_query_results(
        &pair.subscriber,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "updated file part row",
        |rows| {
            (rows.len() == 1
                && field(&rows[0], "file_parts.label") == &Value::Text("B".to_string()))
                .then_some(rows)
        },
    )
    .await;
    let part_b_key = rows[0].key.clone();
    assert_ne!(part_a_key, part_b_key);
    assert_ne!(part_a_key, part_a);
    assert_ne!(part_b_key, part_b);
    assert_eq!(
        field(&rows[0], "file_parts.label"),
        &Value::Text("B".to_string())
    );

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "UUID-array FK retarget removes part A occurrence and adds part B occurrence",
        |log| has_removed_key(log, &part_a_key) && has_added_key(log, &part_b_key),
    )
    .await;

    pair.shutdown().await;
}
}
