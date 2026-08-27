//! Server-side subscription lifecycle hygiene: one-shot reads leave no live
//! delivery behind, dropped subscriptions stop delivery and resubscribe
//! cleanly, and deletions reach live and persisted subscribers.

use jazz_testkit as support;

use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Schema, SchemaBuilder, TableSchema,
    permissions, policy_expr as pe,
};
use jazz_server::JazzServer;
use support::{
    TestingClient, has_added_id, has_removed, wait_for_edge_query_ready, wait_for_edge_txs,
    wait_for_query, wait_for_rows, wait_for_subscription_update,
};

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const DELTA_TIMEOUT: Duration = Duration::from_secs(25);
const ABSENCE_WINDOW: Duration = Duration::from_millis(600);
const ABSENCE_POLL_INTERVAL: Duration = Duration::from_millis(50);

const ALICE_ID: &str = "9750dcc2-516e-5ea0-8a26-54fa6ff6986b";
const BOB_ID: &str = "756886b3-2033-583f-bd5a-a22f02fb5a6b";
const CAROL_ID: &str = "263ae6d4-cf47-5333-9fcd-c81d5d12a27c";

/// Two unrelated tables: `projects` exists so a test can run a one-shot query
/// on a second table as a same-connection ordering probe after commands that
/// have no direct acknowledgement, such as an unsubscription.
fn documents_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("documents").column("title", ColumnType::Text))
        .table(TableSchema::builder("projects").column("name", ColumnType::Text))
        .build()
}

/// Documents are readable only while a `memberships` row grants the session's
/// user access to the document's folder, so deleting that membership row is a
/// policy-dependency deletion two hops away from the document.
fn membership_documents_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("name", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read()
                        .where_(pe::exists(pe::table("memberships").where_(
                            pe::rel::all_of([
                                pe::rel::eq_outer("folder_id", "id"),
                                pe::rel::eq_session("member_id", vec!["claims", "sub"]),
                            ]),
                        )));
                })),
        )
        .table(
            TableSchema::builder("memberships")
                .fk_column("folder_id", "folders")
                .column("member_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("documents")
                .fk_column("folder_id", "folders")
                .column("title", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::allowed_to_read("folder_id"));
                })),
        )
        .build()
}

async fn connect_trusted(server: &JazzServer, schema: &Schema, user_id: &str) -> JazzClient {
    let client =
        jazz_testkit::connect(server.make_client_context_for_user(schema.clone(), user_id))
            .await
            .expect("connect trusted client");
    wait_for_edge_query_ready(&client, "documents", READY_TIMEOUT).await;
    client
}

/// Repeatedly runs a local-only query for `window` and fails if `row_id` ever
/// shows up. Local reads only contain rows the server delivered to this
/// client, so a persistent absence here means no live server-side delivery.
async fn assert_row_stays_absent_locally(
    client: &JazzClient,
    query: &Query,
    row_id: ObjectId,
    description: &str,
) {
    let deadline = tokio::time::Instant::now() + ABSENCE_WINDOW;
    loop {
        let rows = client
            .query(query.clone(), None)
            .await
            .expect("local query during absence window");
        assert!(
            rows.iter().all(|(id, _)| *id != row_id),
            "{description}: row {row_id} was delivered without a live subscription: {rows:?}"
        );
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(ABSENCE_POLL_INTERVAL).await;
    }
}

/// A one-shot edge query must be answered once with its full result and must
/// not leave a live server-side delivery behind: a later matching write by
/// another client is not pushed to the one-shot reader, while a fresh one-shot
/// still returns it.
///
/// Actors: alice runs one-shot reads, bob writes, carol is a still-subscribed
/// control proving the server broadcast the write to live subscribers.
///
/// ```text
/// alice ──one-shot query──► server ──► row1 (served once)
/// bob ──insert row2───────► server ──broadcast──► carol (live stream)
///                              │
///                              └──✗ no push ──► alice (nothing installed)
/// alice ──one-shot query──► server ──► row1 + row2
/// ```
#[tokio::test]
async fn one_shot_query_is_served_once_without_installing_live_delivery() {
    tokio::task::LocalSet::new()
        .run_until(one_shot_query_is_served_once_without_installing_live_delivery_impl())
        .await
}

async fn one_shot_query_is_served_once_without_installing_live_delivery_impl() {
    let schema = documents_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_trusted(&server, &schema, ALICE_ID).await;
    let bob = connect_trusted(&server, &schema, BOB_ID).await;
    let carol = connect_trusted(&server, &schema, CAROL_ID).await;

    let query = Query::from("documents");
    let mut carol_stream = carol
        .subscribe(query.clone())
        .await
        .expect("carol subscribes as live control");
    let mut carol_log = Vec::new();

    let (row1, row1_values, row1_tx) = bob
        .insert("documents", row_input!("title" => "before one-shot"))
        .expect("bob inserts first document");
    wait_for_edge_txs(
        &bob,
        &[row1_tx.expect("ordinary mutation commits immediately")],
    )
    .await;

    let one_shot_rows = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        DELTA_TIMEOUT,
        "alice's one-shot query returns the first document",
        |rows| {
            rows.iter()
                .any(|(id, values)| *id == row1 && *values == row1_values)
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(
        one_shot_rows.len(),
        1,
        "one-shot must return the full result"
    );

    // A one-shot query on an unrelated table round-trips on the same
    // connection, so the server has processed the earlier one-shot teardown
    // before bob's next write arrives.
    alice
        .query(Query::from("projects"), Some(DurabilityTier::EdgeServer))
        .await
        .expect("alice's ordering probe on projects");

    let (row2, _, row2_tx) = bob
        .insert("documents", row_input!("title" => "after one-shot"))
        .expect("bob inserts second document");
    wait_for_edge_txs(
        &bob,
        &[row2_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut carol_stream,
        &mut carol_log,
        DELTA_TIMEOUT,
        "carol's live stream receives bob's second document",
        |log| has_added_id(log, row2),
    )
    .await;

    // The first one-shot delivered row1 into alice's local store; row2 must
    // not join it without a live subscription.
    let local_rows = alice
        .query(query.clone(), None)
        .await
        .expect("alice reads locally after the one-shot");
    assert!(
        local_rows.iter().any(|(id, _)| *id == row1),
        "the one-shot result should remain locally readable: {local_rows:?}"
    );
    assert_row_stays_absent_locally(&alice, &query, row2, "after alice's one-shot completed").await;

    wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        DELTA_TIMEOUT,
        "alice's second one-shot query returns both documents",
        |rows| {
            (rows.iter().any(|(id, _)| *id == row1) && rows.iter().any(|(id, _)| *id == row2))
                .then_some(())
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    server.shutdown().await;
}

/// After a subscription stream is dropped, later matching writes are no longer
/// delivered to that client, and a later resubscription serves a correct fresh
/// snapshot and receives new writes again.
///
/// Actors: alice subscribes and unsubscribes, bob writes, carol is a
/// still-subscribed control.
///
/// ```text
/// alice ──subscribe──► server ──► row1 delta
/// alice ──drop stream─► server (delivery stops)
/// bob ──insert row2───► server ──► carol only
/// alice ──resubscribe─► server ──► snapshot row1 + row2, then row3 delta
/// ```
#[tokio::test]
async fn dropped_subscription_stops_delivery_and_resubscribes_cleanly() {
    tokio::task::LocalSet::new()
        .run_until(dropped_subscription_stops_delivery_and_resubscribes_cleanly_impl())
        .await
}

async fn dropped_subscription_stops_delivery_and_resubscribes_cleanly_impl() {
    let schema = documents_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_trusted(&server, &schema, ALICE_ID).await;
    let bob = connect_trusted(&server, &schema, BOB_ID).await;
    let carol = connect_trusted(&server, &schema, CAROL_ID).await;

    let query = Query::from("documents");
    let mut carol_stream = carol
        .subscribe(query.clone())
        .await
        .expect("carol subscribes as live control");
    let mut carol_log = Vec::new();

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes");
    let mut alice_log = Vec::new();

    let (row1, _, row1_tx) = bob
        .insert("documents", row_input!("title" => "while subscribed"))
        .expect("bob inserts while alice is subscribed");
    wait_for_edge_txs(
        &bob,
        &[row1_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        DELTA_TIMEOUT,
        "alice receives bob's insert while subscribed",
        |log| has_added_id(log, row1),
    )
    .await;

    drop(alice_stream);
    // A one-shot query on an unrelated table round-trips on the same
    // connection, so the server has processed alice's unsubscription before
    // bob's next write arrives.
    alice
        .query(Query::from("projects"), Some(DurabilityTier::EdgeServer))
        .await
        .expect("alice's ordering probe on projects");

    let (row2, _, row2_tx) = bob
        .insert("documents", row_input!("title" => "after unsubscribe"))
        .expect("bob inserts after alice unsubscribed");
    wait_for_edge_txs(
        &bob,
        &[row2_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut carol_stream,
        &mut carol_log,
        DELTA_TIMEOUT,
        "carol's live stream receives the post-unsubscribe insert",
        |log| has_added_id(log, row2),
    )
    .await;

    let local_rows = alice
        .query(query.clone(), None)
        .await
        .expect("alice reads locally after unsubscribing");
    assert!(
        local_rows.iter().any(|(id, _)| *id == row1),
        "rows delivered while subscribed should remain locally readable: {local_rows:?}"
    );
    assert_row_stays_absent_locally(&alice, &query, row2, "after alice dropped her stream").await;

    let mut resubscribed_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice resubscribes");
    let mut resubscribed_log = Vec::new();
    wait_for_subscription_update(
        &mut resubscribed_stream,
        &mut resubscribed_log,
        DELTA_TIMEOUT,
        "alice's resubscription serves a fresh snapshot with both rows",
        |log| has_added_id(log, row1) && has_added_id(log, row2),
    )
    .await;

    let (row3, _, row3_tx) = bob
        .insert("documents", row_input!("title" => "after resubscribe"))
        .expect("bob inserts after alice resubscribed");
    wait_for_edge_txs(
        &bob,
        &[row3_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut resubscribed_stream,
        &mut resubscribed_log,
        DELTA_TIMEOUT,
        "alice's resubscription receives new writes",
        |log| has_added_id(log, row3),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    server.shutdown().await;
}

/// Dropping a subscription stream and immediately resubscribing to the same
/// query must leave a fully live subscription: the teardown of the dropped
/// stream must not erase the state the resubscription registered, so a
/// subsequent write by another client is still delivered.
///
/// Actors: alice drops and resubscribes back-to-back, bob writes.
///
/// ```text
/// alice ──subscribe──drop──subscribe──► server
/// bob ──insert row──────────────────► server ──► alice's new stream (delta)
/// ```
#[tokio::test]
async fn rapid_drop_and_resubscribe_keeps_a_live_subscription() {
    tokio::task::LocalSet::new()
        .run_until(rapid_drop_and_resubscribe_keeps_a_live_subscription_impl())
        .await
}

async fn rapid_drop_and_resubscribe_keeps_a_live_subscription_impl() {
    let schema = documents_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_trusted(&server, &schema, ALICE_ID).await;
    let bob = connect_trusted(&server, &schema, BOB_ID).await;

    let query = Query::from("documents");
    let first_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes the first time");
    // Drop and resubscribe with no intervening await, so the unsubscription
    // and the resubscription reach the server back-to-back.
    drop(first_stream);
    let mut second_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice resubscribes immediately");
    let mut second_log = Vec::new();

    let (row_id, _, row_tx) = bob
        .insert(
            "documents",
            row_input!("title" => "after rapid resubscribe"),
        )
        .expect("bob inserts after alice's rapid resubscribe");
    wait_for_edge_txs(
        &bob,
        &[row_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut second_stream,
        &mut second_log,
        DELTA_TIMEOUT,
        "alice's rapid resubscription receives bob's insert",
        |log| has_added_id(log, row_id),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Deleting a row a read policy depends on must revoke visibility for both a
/// live subscriber and a persisted subscriber that was offline during the
/// deletion: on reconnect the persisted client's edge-settled query must no
/// longer include the document, even though its local store still holds the
/// stale membership grant.
///
/// Actors: alice is the trusted admin writer; bob reads through folder
/// memberships, once via a persistent-storage client and once via a live
/// subscription stream.
///
/// ```text
/// alice ──folders + memberships + docs──► server ──► bob (persistent) syncs 3 docs
/// bob (persistent) ──shutdown (offline)
/// alice ──delete membership_live────────► server ──► bob live stream (doc_live removed)
/// alice ──delete membership_off─────────► server
/// bob ──reconnect (same data dir)───────► edge query: doc_keep only
/// ```
#[tokio::test]
async fn deleted_membership_row_revokes_documents_for_live_and_persisted_subscribers() {
    tokio::task::LocalSet::new()
        .run_until(
            deleted_membership_row_revokes_documents_for_live_and_persisted_subscribers_impl(),
        )
        .await
}

async fn deleted_membership_row_revokes_documents_for_live_and_persisted_subscribers_impl() {
    let schema = membership_documents_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice =
        support::connect_ready_client(&server, &schema, ALICE_ID, "documents", READY_TIMEOUT).await;

    let (folder_keep, _, folder_keep_tx) = alice
        .insert("folders", row_input!("name" => "kept folder"))
        .expect("alice creates the kept folder");
    let (folder_live, _, folder_live_tx) = alice
        .insert("folders", row_input!("name" => "online-revoked folder"))
        .expect("alice creates the online-revoked folder");
    let (folder_off, _, folder_off_tx) = alice
        .insert("folders", row_input!("name" => "offline-revoked folder"))
        .expect("alice creates the offline-revoked folder");
    let (_, _, keep_membership_tx) = alice
        .insert(
            "memberships",
            row_input!("folder_id" => folder_keep, "member_id" => BOB_ID),
        )
        .expect("alice grants bob the kept folder");
    let (membership_live, _, live_membership_tx) = alice
        .insert(
            "memberships",
            row_input!("folder_id" => folder_live, "member_id" => BOB_ID),
        )
        .expect("alice grants bob the folder revoked while online");
    let (membership_off, _, off_membership_tx) = alice
        .insert(
            "memberships",
            row_input!("folder_id" => folder_off, "member_id" => BOB_ID),
        )
        .expect("alice grants bob the folder revoked while offline");
    let (doc_keep, _, doc_keep_tx) = alice
        .insert(
            "documents",
            row_input!("folder_id" => folder_keep, "title" => "stays visible"),
        )
        .expect("alice creates the kept document");
    let (doc_live, _, doc_live_tx) = alice
        .insert(
            "documents",
            row_input!("folder_id" => folder_live, "title" => "revoked while online"),
        )
        .expect("alice creates the online-revoked document");
    let (doc_off, _, doc_off_tx) = alice
        .insert(
            "documents",
            row_input!("folder_id" => folder_off, "title" => "revoked while offline"),
        )
        .expect("alice creates the offline-revoked document");
    wait_for_edge_txs(
        &alice,
        &[
            folder_keep_tx.expect("ordinary mutation commits immediately"),
            folder_live_tx.expect("ordinary mutation commits immediately"),
            folder_off_tx.expect("ordinary mutation commits immediately"),
            keep_membership_tx.expect("ordinary mutation commits immediately"),
            live_membership_tx.expect("ordinary mutation commits immediately"),
            off_membership_tx.expect("ordinary mutation commits immediately"),
            doc_keep_tx.expect("ordinary mutation commits immediately"),
            doc_live_tx.expect("ordinary mutation commits immediately"),
            doc_off_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let query = Query::from("documents");

    // The persistent client syncs all three documents into its durable store,
    // then goes offline before either membership deletion.
    let (bob_context, bob_persistent) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(BOB_ID)
        .as_user()
        .with_persistent_storage()
        .ready_on("documents", READY_TIMEOUT)
        .connect_with_context()
        .await;
    wait_for_rows(
        &bob_persistent,
        query.clone(),
        "bob's persistent client sees all three membership-granted documents",
        |rows| {
            (rows.iter().any(|(id, _)| *id == doc_keep)
                && rows.iter().any(|(id, _)| *id == doc_live)
                && rows.iter().any(|(id, _)| *id == doc_off))
            .then_some(())
        },
    )
    .await;
    bob_persistent.shutdown().await.expect("bob goes offline");

    // A second client of the same user observes the online half through a
    // live subscription stream.
    let bob_live =
        support::connect_ready_user(&server, &schema, BOB_ID, "documents", READY_TIMEOUT).await;
    let mut bob_stream = bob_live
        .subscribe(query.clone())
        .await
        .expect("bob's live client subscribes");
    let mut bob_log = Vec::new();
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        DELTA_TIMEOUT,
        "bob's live stream sees all three membership-granted documents",
        |log| {
            has_added_id(log, doc_keep) && has_added_id(log, doc_live) && has_added_id(log, doc_off)
        },
    )
    .await;

    let live_delete_tx = alice
        .delete(membership_live)
        .expect("alice deletes the online membership");
    wait_for_edge_txs(
        &alice,
        &[live_delete_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        DELTA_TIMEOUT,
        "bob's live stream removes the document whose membership was deleted",
        |log| has_removed(log, doc_live),
    )
    .await;

    let off_delete_tx = alice
        .delete(membership_off)
        .expect("alice deletes a membership while bob's persistent client is offline");
    wait_for_edge_txs(
        &alice,
        &[off_delete_tx.expect("ordinary mutation commits immediately")],
    )
    .await;

    let reopened_bob = jazz_testkit::connect(bob_context)
        .await
        .expect("bob reconnects with his persisted store");
    wait_for_edge_query_ready(&reopened_bob, "documents", READY_TIMEOUT).await;
    let rows_after_reconnect = wait_for_rows(
        &reopened_bob,
        query,
        "bob's reconnect query drops the offline-revoked document",
        |rows| {
            (rows.iter().any(|(id, _)| *id == doc_keep)
                && rows.iter().all(|(id, _)| *id != doc_off)
                && rows.iter().all(|(id, _)| *id != doc_live))
            .then_some(rows)
        },
    )
    .await;
    assert!(
        rows_after_reconnect.iter().any(|(id, _)| *id == doc_keep),
        "the still-granted document must survive the reconnect: {rows_after_reconnect:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    reopened_bob
        .shutdown()
        .await
        .expect("shutdown reopened bob");
    server.shutdown().await;
}

/// Deleting a subscribed row as another client must surface as a removal
/// delta on the subscriber's live stream, not merely as absence from a later
/// query.
///
/// Actors: alice subscribes, bob inserts and then deletes the row.
///
/// ```text
/// bob ──insert row──► server ──► alice stream (add)
/// bob ──delete row──► server ──► alice stream (remove)
/// ```
#[tokio::test]
async fn deleting_a_subscribed_row_emits_a_removal_delta() {
    tokio::task::LocalSet::new()
        .run_until(deleting_a_subscribed_row_emits_a_removal_delta_impl())
        .await
}

async fn deleting_a_subscribed_row_emits_a_removal_delta_impl() {
    let schema = documents_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_trusted(&server, &schema, ALICE_ID).await;
    let bob = connect_trusted(&server, &schema, BOB_ID).await;

    let query = Query::from("documents");
    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes");
    let mut alice_log = Vec::new();

    let (row_id, _, insert_tx) = bob
        .insert("documents", row_input!("title" => "short-lived"))
        .expect("bob inserts the document");
    wait_for_edge_txs(
        &bob,
        &[insert_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        DELTA_TIMEOUT,
        "alice receives the insert delta",
        |log| has_added_id(log, row_id),
    )
    .await;

    let delete_tx = bob.delete(row_id).expect("bob deletes the document");
    wait_for_edge_txs(
        &bob,
        &[delete_tx.expect("ordinary mutation commits immediately")],
    )
    .await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        DELTA_TIMEOUT,
        "alice receives the removal delta for the deleted row",
        |log| has_removed(log, row_id),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
