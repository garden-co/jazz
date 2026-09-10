//! Recursive, union, filter, projection, join, and antijoin graph subscriptions.

use super::*;

#[futures_test::test]
async fn query_subscriptions_receive_filtered_projected_messages() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn query_projection_aliases_drive_output_schema() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::aliased(col("title"), "album_title")])
                .from([TableRef::named("albums")]),
        ))
        .await
        .unwrap();
    let output = database
        .ivm_runtime
        .subscription_output(subscription_id.id())
        .unwrap();
    assert_eq!(output.fields()[0].name.as_deref(), Some("album_title"));

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn query_subscriptions_can_read_from_simple_ctes() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let cte = Cte::new(
        "recent",
        select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::GtEq,
                    Expr::Literal(Value::U64(10)),
                )),
        ),
    );
    let subscription_id = database
        .subscribe_query(Query::With(Box::new(WithQuery::new(
            [cte],
            select_query(
                Select::new([SelectItem::aliased(col("title"), "recent_title")])
                    .from([TableRef::named("recent")]),
            ),
        ))))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(10), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn query_subscriptions_support_literal_on_left_predicates() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::Literal(Value::U64(10)),
                    BinaryOp::Lt,
                    col("id"),
                )),
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(10), Value::String("Boundary".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn query_subscriptions_support_multi_key_inner_joins() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(tenant_albums_artists_schema(), storage)
        .await
        .unwrap();
    let join = TableRef::Join {
        left: Box::new(TableRef::named("albums").aliased("a")),
        right: Box::new(TableRef::named("artists").aliased("r")),
        kind: JoinKind::Inner,
        constraint: JoinConstraint::On(Expr::binary(
            Expr::binary(qcol("a", "tenant_id"), BinaryOp::Eq, qcol("r", "tenant_id")),
            BinaryOp::And,
            Expr::binary(qcol("a", "artist_id"), BinaryOp::Eq, qcol("r", "id")),
        )),
    };
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([
                SelectItem::aliased(qcol("a", "title"), "album_title"),
                SelectItem::aliased(qcol("r", "name"), "artist_name"),
            ])
            .from([join]),
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Coltrane".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![
            Value::U64(2),
            Value::U64(8),
            Value::String("Wrong Tenant".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(42),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into(), "Coltrane".into()], 1)]
    );
}
#[futures_test::test]
async fn query_subscriptions_do_not_match_null_inner_join_keys() {
    let storage =
        MemoryStorage::new(&["left_rows", "right_rows"]).expect("valid memory storage families");
    let mut database = Database::new(nullable_join_schema(), storage)
        .await
        .unwrap();

    let query = select_query(
        Select::new([
            SelectItem::aliased(qcol("l", "value"), "left_value"),
            SelectItem::aliased(qcol("r", "value"), "right_value"),
        ])
        .from([TableRef::Join {
            left: Box::new(TableRef::named("left_rows").aliased("l")),
            right: Box::new(TableRef::named("right_rows").aliased("r")),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(Expr::binary(
                qcol("l", "join_key"),
                BinaryOp::Eq,
                qcol("r", "join_key"),
            )),
        }]),
    );

    let mut batch = database.open_batch();
    batch.insert(
        "left_rows",
        vec![
            Value::U64(1),
            Value::Nullable(None),
            Value::String("left-null".to_owned()),
        ],
    );
    batch.insert(
        "right_rows",
        vec![
            Value::U64(1),
            Value::Nullable(None),
            Value::String("right-null".to_owned()),
        ],
    );
    batch.insert(
        "left_rows",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::U64(7)))),
            Value::String("left-seven".to_owned()),
        ],
    );
    batch.insert(
        "right_rows",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::U64(7)))),
            Value::String("right-seven".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database.subscribe_query(query.clone()).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                Value::String("left-seven".to_owned()),
                Value::String("right-seven".to_owned()),
            ],
            1,
        )]
    );
    assert_eq!(
        database
            .query(query.clone())
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::String("left-seven".to_owned()),
                Value::String("right-seven".to_owned()),
            ],
            1,
        )]
    );

    let mut batch = database.open_batch();
    batch.update(
        "left_rows",
        vec![
            Value::U64(2),
            Value::Nullable(None),
            Value::String("left-now-null".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                Value::String("left-seven".to_owned()),
                Value::String("right-seven".to_owned()),
            ],
            -1,
        )]
    );
    assert!(
        database
            .query(query)
            .await
            .unwrap()
            .to_values()
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn query_subscriptions_support_qualified_wildcards_after_join() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let join = TableRef::Join {
        left: Box::new(TableRef::named("albums").aliased("a")),
        right: Box::new(TableRef::named("artists").aliased("r")),
        kind: JoinKind::Inner,
        constraint: JoinConstraint::On(Expr::binary(
            qcol("a", "artist_id"),
            BinaryOp::Eq,
            qcol("r", "id"),
        )),
    };
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::QualifiedWildcard(vec!["a".to_owned()])]).from([join]),
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

mod joins;
mod recursion;
