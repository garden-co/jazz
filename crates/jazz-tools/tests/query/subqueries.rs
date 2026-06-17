#![cfg(feature = "test")]

use std::collections::BTreeMap;
use std::time::Duration;

use crate::support::{
    QueryRows, TestingClient, has_added, has_any_change, wait_for_rows,
    wait_for_subscription_update,
};
use jazz_tools::row_input;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, Query, QueryBuilder, Schema, SchemaBuilder, TableSchema,
    Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn subquery_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Integer)
                .column("title", ColumnType::Text)
                .fk_column("author_id", "users"),
        )
        .table(
            TableSchema::builder("comments")
                .column("id", ColumnType::Integer)
                .column("text", ColumnType::Text)
                .fk_column("post_id", "posts")
                .fk_column("author_id", "users"),
        )
        .table(
            TableSchema::builder("groups")
                .column("id", ColumnType::Integer)
                .column("name", ColumnType::Text)
                .array_fk_column("member_ids", "users"),
        )
        .table(TableSchema::builder("file_parts").column("label", ColumnType::Text))
        .table(
            TableSchema::builder("files")
                .column("name", ColumnType::Text)
                .array_fk_column("parts", "file_parts"),
        )
        .build()
}

struct Clients {
    server: TestingServer,
    alice: JazzClient,
    bob: JazzClient,
}

impl Clients {
    async fn start() -> Self {
        let schema = subquery_schema();
        let server = TestingServer::start_with_schema(schema.clone()).await;
        let alice = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id("subqueries-alice")
            .ready_on("users", READY_TIMEOUT)
            .connect()
            .await;
        let bob = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema)
            .with_user_id("subqueries-bob")
            .ready_on("users", READY_TIMEOUT)
            .connect()
            .await;

        Self { server, alice, bob }
    }

    async fn shutdown(self) {
        self.alice.shutdown().await.expect("shutdown alice");
        self.bob.shutdown().await.expect("shutdown bob");
        self.server.shutdown().await;
    }
}

async fn create_user(client: &JazzClient, name: &str) -> ObjectId {
    client
        .insert("users", row_input!("name" => name))
        .expect("create user")
        .0
}

async fn create_user_with_id(client: &JazzClient, object_id: ObjectId, name: &str) -> ObjectId {
    client
        .insert_with_id("users", *object_id.uuid(), row_input!("name" => name))
        .expect("create user with id")
        .0
}

async fn create_post(client: &JazzClient, id: i32, title: &str, author_id: ObjectId) -> ObjectId {
    client
        .insert(
            "posts",
            row_input!("id" => id, "title" => title, "author_id" => author_id),
        )
        .expect("create post")
        .0
}

async fn create_comment(
    client: &JazzClient,
    id: i32,
    text: &str,
    post_id: ObjectId,
    author_id: ObjectId,
) -> ObjectId {
    client
        .insert(
            "comments",
            row_input!(
                "id" => id,
                "text" => text,
                "post_id" => post_id,
                "author_id" => author_id,
            ),
        )
        .expect("create comment")
        .0
}

async fn create_group(
    client: &JazzClient,
    id: i32,
    name: &str,
    member_ids: &[ObjectId],
) -> ObjectId {
    client
        .insert(
            "groups",
            row_input!(
                "id" => id,
                "name" => name,
                "member_ids" => Value::Array(
                    member_ids.iter().copied().map(Value::Uuid).collect(),
                ),
            ),
        )
        .expect("create group")
        .0
}

async fn create_file_part(client: &JazzClient, label: &str) -> ObjectId {
    client
        .insert("file_parts", row_input!("label" => label))
        .expect("create file part")
        .0
}

async fn create_file(client: &JazzClient, name: &str, parts: &[ObjectId]) -> ObjectId {
    client
        .insert(
            "files",
            row_input!(
                "name" => name,
                "parts" => Value::Array(parts.iter().copied().map(Value::Uuid).collect()),
            ),
        )
        .expect("create file")
        .0
}

fn users_with_posts_query() -> Query {
    QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "_id")
        })
        .build()
}

fn posts_array(values: &[Value]) -> &[Value] {
    values[1].as_array().expect("posts should be an array")
}

fn row_values(value: &Value) -> &[Value] {
    value.as_row().expect("included element should be a row")
}

fn post_ids(posts: &[Value]) -> Vec<i32> {
    posts
        .iter()
        .map(|post| match row_values(post)[0] {
            Value::Integer(id) => id,
            ref other => panic!("post id should be an integer, got {other:?}"),
        })
        .collect()
}

fn find_row_by_id(rows: &QueryRows, id: ObjectId) -> &[Value] {
    rows.iter()
        .find_map(|(row_id, values)| (*row_id == id).then_some(values.as_slice()))
        .expect("row should be present")
}

fn rows_by_name(rows: &QueryRows) -> BTreeMap<String, &[Value]> {
    rows.iter()
        .map(|(_, values)| match &values[0] {
            Value::Text(name) => (name.clone(), values.as_slice()),
            other => panic!("user name should be text, got {other:?}"),
        })
        .collect()
}

fn has_user_post_count(rows: &QueryRows, user_id: ObjectId, count: usize) -> bool {
    rows.iter().any(|(id, values)| {
        *id == user_id
            && values
                .get(1)
                .and_then(Value::as_array)
                .is_some_and(|posts| posts.len() == count)
    })
}

/// Verifies that a projected array include starts empty when the parent row has
/// no related rows.
///
/// Actors: alice writes one user with no posts, bob subscribes to users with
/// included posts and sees an empty array for that include.
#[tokio::test]
async fn array_subquery_subscription_adds_parent_with_empty_array() {
    let clients = Clients::start().await;
    let query = users_with_posts_query();
    let mut stream = clients
        .bob
        .subscribe(query.clone())
        .await
        .expect("subscribe to users with posts");
    let mut log = Vec::new();

    let user_id = create_user(&clients.alice, "Owner").await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "include query add delta",
        |log| has_added(log, user_id),
    )
    .await;

    let rows = wait_for_rows(&clients.bob, query, "include query row", |rows| {
        rows.iter().any(|(id, _)| *id == user_id).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    let values = find_row_by_id(&rows, user_id);
    assert_eq!(values[0], Value::Text("Owner".to_string()));
    assert!(
        posts_array(values).is_empty(),
        "new owner should start with an empty included posts array"
    );

    clients.shutdown().await;
}

/// Verifies that a projected array include can contain a joined subquery.
///
/// Actors: alice writes one user, two posts, and comments on each post; bob
/// reads the user with an array of joined post/comment tuples.
#[tokio::test]
async fn array_subquery_with_join_returns_joined_elements() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    let post_a = create_post(&clients.alice, 100, "Post A", user_id).await;
    let post_b = create_post(&clients.alice, 101, "Post B", user_id).await;
    create_comment(&clients.alice, 1000, "Comment on A", post_a, user_id).await;
    create_comment(&clients.alice, 1001, "Another on A", post_a, user_id).await;
    create_comment(&clients.alice, 1002, "Comment on B", post_b, user_id).await;

    let query = QueryBuilder::new("users")
        .with_array("post_comments", |sub| {
            sub.from("posts")
                .join("comments")
                .on("posts._id", "comments.post_id")
                .correlate("author_id", "users._id")
        })
        .build();
    let rows = wait_for_rows(
        &clients.bob,
        query,
        "array include with joined subquery returns all post/comment pairs",
        |rows| (rows.len() == 1 && rows[0].0 == user_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Text("Alice".to_string()));
    let post_comments = rows[0].1[1]
        .as_array()
        .expect("post_comments should be an array");
    assert_eq!(
        post_comments.len(),
        3,
        "Post A has two comments and Post B has one"
    );
    for pair in post_comments {
        let values = pair
            .as_row()
            .expect("joined subquery element should be a row");
        assert_eq!(values.len(), 7);
        assert!(pair.row_id().is_some(), "joined row should retain an id");
        let Value::Integer(post_number) = values[0] else {
            panic!("joined post id should be an integer");
        };
        let expected_post_id = match post_number {
            100 => post_a,
            101 => post_b,
            other => panic!("unexpected joined post id: {other}"),
        };
        assert_eq!(values[5], Value::Uuid(expected_post_id));
    }

    clients.shutdown().await;
}

/// Verifies that an array subquery returns related rows for one parent.
///
/// Actors: alice writes a user and posts, bob reads the user with included
/// posts.
#[tokio::test]
async fn array_subquery_returns_related_rows_for_single_parent() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    create_post(&clients.alice, 100, "Alice Post 1", user_id).await;
    create_post(&clients.alice, 101, "Alice Post 2", user_id).await;

    let rows = wait_for_rows(
        &clients.bob,
        users_with_posts_query(),
        "bob sees Alice with both posts",
        |rows| has_user_post_count(&rows, user_id, 2).then_some(rows),
    )
    .await;

    let values = find_row_by_id(&rows, user_id);
    assert_eq!(values[0], Value::Text("Alice".to_string()));
    assert_eq!(post_ids(posts_array(values)), vec![100, 101]);

    clients.shutdown().await;
}

/// Verifies that array subquery correlation is scoped to each parent row.
///
/// Actors: alice writes two users and one post for each, bob reads the users
/// with their own included posts.
#[tokio::test]
async fn array_subquery_correlates_rows_per_parent() {
    let clients = Clients::start().await;

    let alice_id = create_user(&clients.alice, "Alice").await;
    let bob_id = create_user(&clients.alice, "Bob").await;
    create_post(&clients.alice, 100, "Alice Post", alice_id).await;
    create_post(&clients.alice, 200, "Bob Post", bob_id).await;

    let rows = wait_for_rows(
        &clients.bob,
        users_with_posts_query(),
        "bob sees each user with only their own posts",
        |rows| {
            let by_name = rows_by_name(&rows);
            let alice_ok = by_name
                .get("Alice")
                .is_some_and(|values| post_ids(posts_array(values)) == vec![100]);
            let bob_ok = by_name
                .get("Bob")
                .is_some_and(|values| post_ids(posts_array(values)) == vec![200]);
            (alice_ok && bob_ok).then_some(rows)
        },
    )
    .await;

    let by_name = rows_by_name(&rows);
    assert_eq!(post_ids(posts_array(by_name["Alice"])), vec![100]);
    assert_eq!(post_ids(posts_array(by_name["Bob"])), vec![200]);

    clients.shutdown().await;
}

/// Verifies that an inner-table insert emits a live change for the subscribed
/// parent row.
///
/// Actors and flow:
///
/// alice -> insert user and first post -> server -> bob subscribes
/// alice -> insert second post -> server -> bob receives parent-row change
#[tokio::test]
async fn array_subquery_subscription_changes_parent_when_inner_row_is_inserted() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    create_post(&clients.alice, 100, "Post 1", user_id).await;

    let query = users_with_posts_query();
    let initial_rows = wait_for_rows(
        &clients.bob,
        query.clone(),
        "bob sees Alice with one post before subscribing",
        |rows| has_user_post_count(&rows, user_id, 1).then_some(rows),
    )
    .await;
    assert_eq!(
        post_ids(posts_array(find_row_by_id(&initial_rows, user_id))),
        vec![100]
    );

    let mut stream = clients
        .bob
        .subscribe(query.clone())
        .await
        .expect("subscribe");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial Alice add",
        |log| has_added(log, user_id),
    )
    .await;

    create_post(&clients.alice, 101, "Post 2", user_id).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "Alice changes when a second post is inserted",
        |log| has_any_change(log, user_id) && log.len() > 1,
    )
    .await;

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees Alice with both posts",
        |rows| has_user_post_count(&rows, user_id, 2).then_some(rows),
    )
    .await;
    assert_eq!(
        post_ids(posts_array(find_row_by_id(&rows, user_id))),
        vec![100, 101]
    );

    clients.shutdown().await;
}

/// Verifies that recomputing an array include after an inner insert preserves
/// the parent row's base columns.
///
/// Actors and flow:
///
/// bob subscribes -> alice inserts Alice without posts -> bob sees empty array
/// alice inserts post -> bob sees Alice unchanged with one included post
#[tokio::test]
async fn array_subquery_preserves_parent_columns_when_inner_row_arrives() {
    let clients = Clients::start().await;

    let query = users_with_posts_query();
    let mut stream = clients
        .bob
        .subscribe(query.clone())
        .await
        .expect("subscribe");
    let mut log = Vec::new();

    let user_id = create_user(&clients.alice, "Alice").await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "Alice add with empty posts",
        |log| has_added(log, user_id),
    )
    .await;
    wait_for_rows(
        &clients.bob,
        query.clone(),
        "bob sees Alice with no posts",
        |rows| has_user_post_count(&rows, user_id, 0).then_some(()),
    )
    .await;

    create_post(&clients.alice, 100, "First Post", user_id).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "Alice changes when first post arrives",
        |log| has_any_change(log, user_id) && log.len() > 1,
    )
    .await;

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees Alice with one post and intact base columns",
        |rows| {
            let has_expected_row = rows.iter().any(|(id, values)| {
                *id == user_id
                    && values[0] == Value::Text("Alice".to_string())
                    && values[1]
                        .as_array()
                        .is_some_and(|posts| post_ids(posts) == vec![100])
            });
            has_expected_row.then_some(rows)
        },
    )
    .await;

    let values = find_row_by_id(&rows, user_id);
    assert_eq!(values[0], Value::Text("Alice".to_string()));
    assert_eq!(post_ids(posts_array(values)), vec![100]);

    clients.shutdown().await;
}

/// Verifies that a newly inserted parent row includes matching inner rows that
/// already exist on the server.
///
/// Actors and flow:
///
/// alice -> insert Bob's post -> server -> bob subscribes
/// alice -> insert Bob -> server -> bob receives Bob with that post included
#[tokio::test]
async fn array_subquery_subscription_adds_parent_with_existing_inner_rows() {
    let clients = Clients::start().await;

    let bob_id = ObjectId::new();
    create_post(&clients.alice, 200, "Bob Post", bob_id).await;
    wait_for_rows(
        &clients.bob,
        QueryBuilder::new("posts")
            .filter_eq("author_id", Value::Uuid(bob_id))
            .build(),
        "bob sees Bob's post before Bob exists",
        |rows| (rows.len() == 1).then_some(()),
    )
    .await;

    let query = users_with_posts_query();
    let mut stream = clients
        .bob
        .subscribe(query.clone())
        .await
        .expect("subscribe");
    let mut log = Vec::new();

    let user_id = create_user_with_id(&clients.alice, bob_id, "Bob").await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "Bob is added with existing post",
        |log| has_added(log, user_id),
    )
    .await;

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees Bob with the existing post included",
        |rows| has_user_post_count(&rows, user_id, 1).then_some(rows),
    )
    .await;
    assert_eq!(
        post_ids(posts_array(find_row_by_id(&rows, user_id))),
        vec![200]
    );

    clients.shutdown().await;
}

/// Verifies that `require_result` hides a parent until the correlated array has
/// at least one row.
///
/// Actors and flow:
///
/// alice -> insert Alice without posts -> server -> bob sees no required row
/// alice -> insert Alice's post -> server -> bob sees Alice
#[tokio::test]
async fn array_subquery_require_result_hides_parent_until_inner_row_exists() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    wait_for_rows(
        &clients.bob,
        QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".to_string()))
            .build(),
        "bob sees Alice in the base table",
        |rows| (rows.len() == 1).then_some(()),
    )
    .await;

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "_id")
                .require_result()
        })
        .build();

    wait_for_rows(
        &clients.bob,
        query.clone(),
        "required array include hides Alice with no posts",
        |rows| rows.is_empty().then_some(()),
    )
    .await;

    create_post(&clients.alice, 100, "Hello world", user_id).await;

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "required array include reveals Alice after a post exists",
        |rows| has_user_post_count(&rows, user_id, 1).then_some(rows),
    )
    .await;
    assert_eq!(
        post_ids(posts_array(find_row_by_id(&rows, user_id))),
        vec![100]
    );

    clients.shutdown().await;
}

/// Verifies that full-cardinality matching hides a parent until every id in an
/// array correlation resolves to an inner row.
///
/// Actors and flow:
///
/// alice -> insert Alice and a group referencing Alice plus future Bob -> server
/// bob sees no group
/// alice -> insert Bob with the preallocated id -> server -> bob sees the group
#[tokio::test]
async fn array_subquery_requires_all_array_refs_to_resolve() {
    let clients = Clients::start().await;

    let alice_id = create_user(&clients.alice, "Alice").await;
    let bob_id = ObjectId::new();
    let group_id = create_group(&clients.alice, 10, "Maintainers", &[alice_id, bob_id]).await;

    wait_for_rows(
        &clients.bob,
        QueryBuilder::new("groups").build(),
        "bob sees the group in the base table",
        |rows| (rows.len() == 1).then_some(()),
    )
    .await;
    wait_for_rows(
        &clients.bob,
        QueryBuilder::new("users").build(),
        "bob sees Alice before Bob exists",
        |rows| (rows.len() == 1).then_some(()),
    )
    .await;

    let query = QueryBuilder::new("groups")
        .with_array("members", |sub| {
            sub.from("users")
                .correlate("id", "groups.member_ids")
                .require_match_correlation_cardinality()
        })
        .build();

    wait_for_rows(
        &clients.bob,
        query.clone(),
        "group is hidden while one referenced member is missing",
        |rows| rows.is_empty().then_some(()),
    )
    .await;

    create_user_with_id(&clients.alice, bob_id, "Bob").await;

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "group appears once all referenced members exist",
        |rows| {
            let has_expected_row = rows.iter().any(|(id, values)| {
                *id == group_id
                    && values[3]
                        .as_array()
                        .is_some_and(|members| members.len() == 2)
            });
            has_expected_row.then_some(rows)
        },
    )
    .await;

    let values = find_row_by_id(&rows, group_id);
    assert_eq!(values[0], Value::Integer(10));
    assert_eq!(values[1], Value::Text("Maintainers".to_string()));
    assert_eq!(
        values[2],
        Value::Array(vec![Value::Uuid(alice_id), Value::Uuid(bob_id)])
    );
    assert_eq!(values[3].as_array().expect("members array").len(), 2);

    clients.shutdown().await;
}

/// Verifies that an array subquery can order its inner rows.
///
/// Actors: alice writes posts out of order, bob reads Alice with posts ordered
/// by id descending.
#[tokio::test]
async fn array_subquery_orders_inner_rows() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    create_post(&clients.alice, 102, "Middle", user_id).await;
    create_post(&clients.alice, 100, "First", user_id).await;
    create_post(&clients.alice, 101, "Last", user_id).await;

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "_id")
                .order_by_desc("id")
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees Alice's posts ordered descending",
        |rows| {
            let has_expected_row = rows.iter().any(|(id, values)| {
                *id == user_id && post_ids(posts_array(values)) == vec![102, 101, 100]
            });
            has_expected_row.then_some(rows)
        },
    )
    .await;

    assert_eq!(
        post_ids(posts_array(find_row_by_id(&rows, user_id))),
        vec![102, 101, 100]
    );

    clients.shutdown().await;
}

/// Verifies that an array subquery can limit ordered inner rows.
///
/// Actors: alice writes five posts, bob reads only the first two ordered by id.
#[tokio::test]
async fn array_subquery_limits_ordered_inner_rows() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    for id in 100..105 {
        create_post(&clients.alice, id, &format!("Post {id}"), user_id).await;
    }

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "_id")
                .order_by("id")
                .limit(2)
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees the first two posts by id",
        |rows| {
            let has_expected_row = rows.iter().any(|(id, values)| {
                *id == user_id && post_ids(posts_array(values)) == vec![100, 101]
            });
            has_expected_row.then_some(rows)
        },
    )
    .await;

    assert_eq!(
        post_ids(posts_array(find_row_by_id(&rows, user_id))),
        vec![100, 101]
    );

    clients.shutdown().await;
}

/// Verifies that an array subquery can project selected inner columns.
///
/// Actors: alice writes a post, bob reads Alice with a posts array containing
/// only the selected post columns.
#[tokio::test]
async fn array_subquery_selects_inner_columns() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    create_post(&clients.alice, 100, "Post Title", user_id).await;

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "_id")
                .select(&["id", "title"])
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees selected post columns",
        |rows| has_user_post_count(&rows, user_id, 1).then_some(rows),
    )
    .await;

    let posts = posts_array(find_row_by_id(&rows, user_id));
    let post = row_values(&posts[0]);
    assert_eq!(post.len(), 2);
    assert!(posts[0].row_id().is_some(), "post row should retain an id");
    assert_eq!(post[0], Value::Integer(100));
    assert_eq!(post[1], Value::Text("Post Title".to_string()));

    clients.shutdown().await;
}

/// Verifies that an array subquery can project selected magic timestamp
/// columns.
///
/// Actors: alice writes a post, bob reads Alice with post data plus created and
/// updated timestamps.
#[tokio::test]
async fn array_subquery_selects_magic_timestamp_columns() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    create_post(&clients.alice, 100, "Post Title", user_id).await;

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "_id").select(&[
                "id",
                "title",
                "$createdAt",
                "$updatedAt",
            ])
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees selected post magic timestamps",
        |rows| has_user_post_count(&rows, user_id, 1).then_some(rows),
    )
    .await;

    let posts = posts_array(find_row_by_id(&rows, user_id));
    let post = row_values(&posts[0]);
    assert_eq!(post.len(), 4);
    assert_eq!(post[0], Value::Integer(100));
    assert_eq!(post[1], Value::Text("Post Title".to_string()));
    assert!(matches!(post[2], Value::Timestamp(_)));
    assert!(matches!(post[3], Value::Timestamp(_)));

    clients.shutdown().await;
}

/// Verifies that array subqueries can be nested.
///
/// Actors: alice writes a user, posts, and comments; bob reads the user with
/// posts, and each included post has its own comments array.
#[tokio::test]
async fn array_subquery_supports_nested_arrays() {
    let clients = Clients::start().await;

    let user_id = create_user(&clients.alice, "Alice").await;
    let post_a_id = create_post(&clients.alice, 100, "Post A", user_id).await;
    let post_b_id = create_post(&clients.alice, 101, "Post B", user_id).await;
    create_comment(&clients.alice, 1000, "Comment 1 on A", post_a_id, user_id).await;
    create_comment(&clients.alice, 1001, "Comment 2 on A", post_a_id, user_id).await;
    create_comment(&clients.alice, 1002, "Comment on B", post_b_id, user_id).await;

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "_id")
                .with_array("comments", |nested| {
                    nested.from("comments").correlate("post_id", "_id")
                })
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees Alice's posts with nested comments",
        |rows| {
            let has_expected_row = rows.iter().any(|(id, values)| {
                if *id != user_id {
                    return false;
                }
                let posts = posts_array(values);
                posts.len() == 2
                    && posts.iter().all(|post| {
                        let post = row_values(post);
                        match post[0] {
                            Value::Integer(100) => post[3]
                                .as_array()
                                .is_some_and(|comments| comments.len() == 2),
                            Value::Integer(101) => post[3]
                                .as_array()
                                .is_some_and(|comments| comments.len() == 1),
                            _ => false,
                        }
                    })
            });
            has_expected_row.then_some(rows)
        },
    )
    .await;

    let posts = posts_array(find_row_by_id(&rows, user_id));
    assert_eq!(posts.len(), 2);
    for post in posts {
        let post = row_values(post);
        assert_eq!(post.len(), 4);
        let comments = post[3].as_array().expect("comments should be an array");
        match post[0] {
            Value::Integer(100) => assert_eq!(comments.len(), 2),
            Value::Integer(101) => assert_eq!(comments.len(), 1),
            ref other => panic!("unexpected post id {other:?}"),
        }
    }

    clients.shutdown().await;
}

/// Verifies that a query can project multiple independent array subquery
/// columns.
///
/// Actors: alice writes users, posts, and comments; bob reads each user with
/// both their authored posts and authored comments arrays.
#[tokio::test]
async fn array_subquery_supports_multiple_array_columns() {
    let clients = Clients::start().await;

    let alice_id = create_user(&clients.alice, "Alice").await;
    let bob_id = create_user(&clients.alice, "Bob").await;
    let alice_post_1 = create_post(&clients.alice, 100, "Alice Post 1", alice_id).await;
    create_post(&clients.alice, 101, "Alice Post 2", alice_id).await;
    let bob_post = create_post(&clients.alice, 102, "Bob Post", bob_id).await;
    create_comment(
        &clients.alice,
        1000,
        "Alice comment",
        alice_post_1,
        alice_id,
    )
    .await;
    create_comment(&clients.alice, 1001, "Bob comment 1", bob_post, bob_id).await;
    create_comment(&clients.alice, 1002, "Bob comment 2", bob_post, bob_id).await;

    let query = QueryBuilder::new("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "_id")
        })
        .with_array("comments", |sub| {
            sub.from("comments").correlate("author_id", "_id")
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees each user with posts and comments arrays",
        |rows| {
            let by_name = rows_by_name(&rows);
            let alice_ok = by_name.get("Alice").is_some_and(|values| {
                values[1].as_array().is_some_and(|posts| posts.len() == 2)
                    && values[2]
                        .as_array()
                        .is_some_and(|comments| comments.len() == 1)
            });
            let bob_ok = by_name.get("Bob").is_some_and(|values| {
                values[1].as_array().is_some_and(|posts| posts.len() == 1)
                    && values[2]
                        .as_array()
                        .is_some_and(|comments| comments.len() == 2)
            });
            (alice_ok && bob_ok).then_some(rows)
        },
    )
    .await;

    let by_name = rows_by_name(&rows);
    assert_eq!(
        by_name["Alice"][1]
            .as_array()
            .expect("Alice posts should be an array")
            .len(),
        2
    );
    assert_eq!(
        by_name["Alice"][2]
            .as_array()
            .expect("Alice comments should be an array")
            .len(),
        1
    );
    assert_eq!(
        by_name["Bob"][1]
            .as_array()
            .expect("Bob posts should be an array")
            .len(),
        1
    );
    assert_eq!(
        by_name["Bob"][2]
            .as_array()
            .expect("Bob comments should be an array")
            .len(),
        2
    );

    clients.shutdown().await;
}

/// Verifies that a UUID-array foreign-key include materializes inner rows in
/// the order of the outer array, including duplicate references.
///
/// Actors: alice writes file parts and a file, bob reads the file with resolved
/// part rows.
#[tokio::test]
async fn array_subquery_materializes_uuid_array_refs_in_order_with_duplicates() {
    let clients = Clients::start().await;

    let part_a = create_file_part(&clients.alice, "A").await;
    let part_b = create_file_part(&clients.alice, "B").await;
    let file_id = create_file(&clients.alice, "bundle", &[part_b, part_a, part_b]).await;

    let query = QueryBuilder::new("files")
        .with_array("part_rows", |sub| {
            sub.from("file_parts").correlate("id", "files.parts")
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees file parts resolved in outer-array order",
        |rows| {
            let has_expected_row = rows.iter().any(|(id, values)| {
                *id == file_id
                    && values[2].as_array().is_some_and(|parts| {
                        let labels: Vec<&str> = parts
                            .iter()
                            .map(|part| match &row_values(part)[0] {
                                Value::Text(label) => label.as_str(),
                                _ => "",
                            })
                            .collect();
                        labels == ["B", "A", "B"]
                    })
            });
            has_expected_row.then_some(rows)
        },
    )
    .await;

    let values = find_row_by_id(&rows, file_id);
    let parts = values[2].as_array().expect("part rows should be an array");
    let labels: Vec<&str> = parts
        .iter()
        .map(|part| match &row_values(part)[0] {
            Value::Text(label) => label.as_str(),
            other => panic!("part label should be text, got {other:?}"),
        })
        .collect();
    assert_eq!(labels, vec!["B", "A", "B"]);
    assert!(
        parts.iter().all(|part| part.row_id().is_some()),
        "included file parts should retain row ids"
    );

    clients.shutdown().await;
}

/// Verifies that reverse membership over a UUID-array foreign-key column updates
/// when the array changes.
///
/// Actors and flow:
///
/// alice -> insert parts A/B and file [A, B, B] -> server -> bob sees both
/// alice -> update file to [B] -> server -> bob sees only B linked to file
#[tokio::test]
async fn array_subquery_reverse_uuid_array_membership_updates_when_array_changes() {
    let clients = Clients::start().await;

    let part_a = create_file_part(&clients.alice, "A").await;
    let part_b = create_file_part(&clients.alice, "B").await;
    let file_id = create_file(&clients.alice, "bundle", &[part_a, part_b, part_b]).await;

    let query = QueryBuilder::new("file_parts")
        .with_array("files", |sub| {
            sub.from("files").correlate("parts", "file_parts.id")
        })
        .build();

    wait_for_rows(
        &clients.bob,
        query.clone(),
        "bob sees both parts linked to the file",
        |rows| {
            let counts = file_counts_by_part_label(&rows);
            (counts.get("A") == Some(&1) && counts.get("B") == Some(&1)).then_some(())
        },
    )
    .await;

    clients
        .alice
        .update(
            file_id,
            vec![("parts".to_string(), Value::Array(vec![Value::Uuid(part_b)]))],
        )
        .expect("update file parts");

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees removed part membership after file update",
        |rows| {
            let counts = file_counts_by_part_label(&rows);
            (counts.get("A") == Some(&0) && counts.get("B") == Some(&1)).then_some(rows)
        },
    )
    .await;

    let counts = file_counts_by_part_label(&rows);
    assert_eq!(counts.get("A"), Some(&0));
    assert_eq!(counts.get("B"), Some(&1));

    clients.shutdown().await;
}

fn file_counts_by_part_label(rows: &QueryRows) -> BTreeMap<String, usize> {
    rows.iter()
        .map(|(_, values)| {
            let label = match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("file part label should be text, got {other:?}"),
            };
            let file_count = values[1]
                .as_array()
                .expect("files include should be an array")
                .len();
            (label, file_count)
        })
        .collect()
}
