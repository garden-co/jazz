//! Deleting an accepted invitation reaches its recipient's local store.
//!
//! Mirrors the record-player `playlist-auth-reconnect` topology: an owner
//! invites an editor, the editor accepts, both edit offline and reconnect,
//! and the owner then deletes the invitation. The recipient can still read
//! the invitation row by its `subject`, so the deletion itself (not merely a
//! scope loss) must reach the recipient and suppress local reads too.

use jazz_testkit as support;

use std::time::Duration;

use jazz::db::ReadOpts;
use jazz::query::{Query, col, eq, lit};
use jazz::row_input;
use jazz::tools::policy_expr::rel;
use jazz::tools::public_schema::{RelPredicateCmpOp, RelValueRef, RowIdRef};
use jazz::tools::test_support::{disconnect_client, reconnect_client};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, ReadTier, Schema, SchemaBuilder, TableSchema,
    TransactionId, Value, permissions, policy_expr as pe,
};
use jazz_server::JazzServer;
use support::{TestingClient, wait_for_query};

// The server requires UUID principals.
const OWNER_ID: &str = "9750dcc2-516e-5ea0-8a26-54fa6ff6986b";
const EDITOR_ID: &str = "756886b3-2033-583f-bd5a-a22f02fb5a6b";

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const TIMEOUT: Duration = Duration::from_secs(20);

fn me() -> jazz::tools::policy_expr::PolicyValueInput {
    pe::session(vec!["claims", "sub"])
}

/// Playlists readable by their owner or through an accepted invitation;
/// invitations readable by their subject or the playlist owner, accepted
/// only by their subject, and deleted only by the owner.
fn schema() -> Schema {
    let accepted_invitation = pe::exists(pe::table("invitations").where_(rel::all_of([
        rel::eq_session("subject", vec!["claims", "sub"]),
        rel::eq_literal("status", Value::Text("accepted".into())),
        rel::cmp(
            "playlist_id",
            RelPredicateCmpOp::Eq,
            RelValueRef::RowId(RowIdRef::Outer),
        ),
    ])));
    SchemaBuilder::new()
        .table(
            TableSchema::builder("playlists")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().where_(pe::eq("owner_id", me()));
                    p.allow_read()
                        .where_(pe::any_of([pe::eq("owner_id", me()), accepted_invitation]));
                })),
        )
        .table(
            TableSchema::builder("invitations")
                .fk_column("playlist_id", "playlists")
                .column("owner_id", ColumnType::Text)
                .column("subject", ColumnType::Text)
                .column("role", ColumnType::Text)
                .column("status", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().where_(pe::eq("owner_id", me()));
                    p.allow_read().where_(pe::any_of([
                        pe::eq("subject", me()),
                        pe::eq("owner_id", me()),
                    ]));
                    p.allow_update()
                        .where_old(pe::any_of([
                            pe::eq("owner_id", me()),
                            pe::all_of([pe::eq("subject", me()), pe::eq("status", "pending")]),
                        ]))
                        .where_new(pe::any_of([
                            pe::eq("owner_id", me()),
                            pe::all_of([pe::eq("subject", me()), pe::eq("status", "accepted")]),
                        ]));
                    p.allow_delete().where_(pe::eq("owner_id", me()));
                })),
        )
        .table(
            TableSchema::builder("entries")
                .fk_column("playlist_id", "playlists")
                .column("position", ColumnType::Integer)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::allowed_to_read("playlist_id"));
                })),
        )
        .build()
}

async fn connect(server: &JazzServer, user_id: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema())
        .with_user_id(user_id)
        .as_user()
        .ready_on("invitations", READY_TIMEOUT)
        .connect()
        .await
}

async fn settle(client: &JazzClient, transaction_id: Option<TransactionId>, tier: DurabilityTier) {
    client
        .wait_for_transaction(
            transaction_id.expect("ordinary mutation commits immediately"),
            tier,
        )
        .await
        .expect("write settles");
}

fn by_id(table: &str, id: ObjectId) -> Query {
    Query::from(table).filter(eq(col("id"), lit(*id.uuid())))
}

/// Read only what the client already holds, without installing an upstream
/// subscription that could repair a stale row.
async fn local_rows(client: &JazzClient, query: Query) -> Vec<(ObjectId, Vec<Value>)> {
    client
        .query_with_opts(
            query,
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..Default::default()
            },
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("inspect local cache")
}

/// The owner's deletion of an accepted invitation suppresses the recipient's
/// local read, not only its remote view.
///
/// Actors: `owner` creates a playlist and invites `editor`; `editor` accepts.
/// Both go offline, each queues an entry, and both reconnect. The owner then
/// deletes the invitation. `editor` still satisfies the invitation's read
/// policy (it is the subject), so the deletion must reach its store.
///
/// ```text
/// owner ──playlist + invite──► server ──► editor
/// editor ──accept────────────► server
/// owner, editor ─offline─ insert entries ─reconnect─► server
/// owner ──delete invite──────► server ──deletion──► editor (remote and local: gone)
/// ```
#[tokio::test]
async fn deleted_accepted_invitation_is_gone_from_the_recipient_local_store() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(schema())
                .await
                .expect("start test server");
            let owner = connect(&server, OWNER_ID).await;
            let editor = connect(&server, EDITOR_ID).await;

            let (playlist, _, tx) = owner
                .insert(
                    "playlists",
                    row_input!("owner_id" => OWNER_ID, "name" => "Road tape"),
                )
                .expect("owner creates the playlist");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            let (invite, _, tx) = owner
                .insert(
                    "invitations",
                    row_input!(
                        "playlist_id" => playlist,
                        "owner_id" => OWNER_ID,
                        "subject" => EDITOR_ID,
                        "role" => "editor",
                        "status" => "pending"
                    ),
                )
                .expect("owner invites the editor");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;

            wait_for_query(
                &editor,
                by_id("invitations", invite),
                ReadTier::Remote,
                TIMEOUT,
                "editor observes the pending invitation",
                |rows| (rows.len() == 1).then_some(()),
            )
            .await;
            let tx = editor
                .update(
                    "invitations",
                    invite,
                    vec![("status".into(), Value::Text("accepted".into()))],
                )
                .expect("editor accepts");
            settle(&editor, tx, DurabilityTier::GlobalServer).await;

            assert!(disconnect_client(&owner), "owner goes offline");
            assert!(disconnect_client(&editor), "editor goes offline");
            let (_, _, owner_entry) = owner
                .insert(
                    "entries",
                    row_input!("playlist_id" => playlist, "position" => 30),
                )
                .expect("owner queues an entry");
            let (_, _, editor_entry) = editor
                .insert(
                    "entries",
                    row_input!("playlist_id" => playlist, "position" => 31),
                )
                .expect("editor queues an entry");
            settle(&owner, owner_entry, DurabilityTier::Local).await;
            settle(&editor, editor_entry, DurabilityTier::Local).await;
            assert!(reconnect_client(&owner).await.expect("owner reconnects"));
            assert!(reconnect_client(&editor).await.expect("editor reconnects"));
            settle(&owner, owner_entry, DurabilityTier::GlobalServer).await;
            settle(&editor, editor_entry, DurabilityTier::GlobalServer).await;
            for (client, who) in [(&owner, "owner"), (&editor, "editor")] {
                wait_for_query(
                    client,
                    Query::from("entries"),
                    ReadTier::Remote,
                    TIMEOUT,
                    format!("{who} converges on both entries"),
                    |rows| (rows.len() == 2).then_some(()),
                )
                .await;
            }

            let tx = owner
                .delete("invitations", invite)
                .expect("owner deletes the invitation");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            wait_for_query(
                &editor,
                by_id("invitations", invite),
                ReadTier::Remote,
                TIMEOUT,
                "editor's remote view loses the deleted invitation",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
            assert_eq!(
                local_rows(&editor, by_id("invitations", invite)).await,
                vec![],
                "the admitted deletion suppresses the editor's local read"
            );

            owner.shutdown().await.expect("shutdown owner");
            editor.shutdown().await.expect("shutdown editor");
            server.shutdown().await;
        })
        .await;
}

/// Revoking an editor by deleting its accepted invitation reaches the
/// editor's default local read as a deletion, while rows that merely lost
/// authorization stay cached (INV-SYNC-14).
///
/// Mirrors the record-player browser topology's "converge, then owner
/// revokes editor" phase: the editor holds the playlist, one entry and its
/// invitation from earlier reads; the owner deletes the invitation, which is
/// also what grants the editor access to the playlist and its entries. The
/// editor's concurrent Remote (Global) waits all reach zero rows, and only
/// then does a default-tier (`LocalFirst`) read run.
///
/// ```text
/// owner ──playlist + entry + invite──► server ──► editor (accepts)
/// editor ──Remote reads (holds playlist, entry, invite)
/// owner ──delete invite──► server
/// editor ──Remote waits: entries, entry, playlist, invite all empty
/// editor ──LocalFirst reads: invite gone; playlist and entry still cached
/// ```
#[tokio::test]
async fn revoked_editor_default_read_drops_deleted_invitation_but_keeps_unauthorized_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(schema())
                .await
                .expect("start test server");
            let owner = connect(&server, OWNER_ID).await;
            let editor = connect(&server, EDITOR_ID).await;

            let (playlist, _, tx) = owner
                .insert(
                    "playlists",
                    row_input!("owner_id" => OWNER_ID, "name" => "Road tape"),
                )
                .expect("owner creates the playlist");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            let (entry, _, tx) = owner
                .insert(
                    "entries",
                    row_input!("playlist_id" => playlist, "position" => 1),
                )
                .expect("owner adds an entry");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            let (invite, _, tx) = owner
                .insert(
                    "invitations",
                    row_input!(
                        "playlist_id" => playlist,
                        "owner_id" => OWNER_ID,
                        "subject" => EDITOR_ID,
                        "role" => "editor",
                        "status" => "pending"
                    ),
                )
                .expect("owner invites the editor");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;

            wait_for_query(
                &editor,
                by_id("invitations", invite),
                ReadTier::Remote,
                TIMEOUT,
                "editor observes the pending invitation",
                |rows| (rows.len() == 1).then_some(()),
            )
            .await;
            let tx = editor
                .update(
                    "invitations",
                    invite,
                    vec![("status".into(), Value::Text("accepted".into()))],
                )
                .expect("editor accepts");
            settle(&editor, tx, DurabilityTier::GlobalServer).await;

            let entries_of_playlist =
                || Query::from("entries").filter(eq(col("playlist_id"), lit(*playlist.uuid())));
            tokio::join!(
                wait_for_query(
                    &editor,
                    entries_of_playlist(),
                    ReadTier::Remote,
                    TIMEOUT,
                    "accepted editor reads the playlist's entries",
                    |rows| (rows.len() == 1).then_some(()),
                ),
                wait_for_query(
                    &editor,
                    by_id("entries", entry),
                    ReadTier::Remote,
                    TIMEOUT,
                    "accepted editor reads the entry by id",
                    |rows| (rows.len() == 1).then_some(()),
                ),
                wait_for_query(
                    &editor,
                    by_id("playlists", playlist),
                    ReadTier::Remote,
                    TIMEOUT,
                    "accepted editor reads the playlist",
                    |rows| (rows.len() == 1).then_some(()),
                ),
                wait_for_query(
                    &editor,
                    by_id("invitations", invite),
                    ReadTier::Remote,
                    TIMEOUT,
                    "accepted editor reads the accepted invitation",
                    |rows| (rows.len() == 1).then_some(()),
                ),
            );

            let tx = owner
                .delete("invitations", invite)
                .expect("owner deletes the invitation");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            tokio::join!(
                wait_for_query(
                    &editor,
                    entries_of_playlist(),
                    ReadTier::Remote,
                    TIMEOUT,
                    "revoked editor loses the playlist's entries",
                    |rows| rows.is_empty().then_some(()),
                ),
                wait_for_query(
                    &editor,
                    by_id("entries", entry),
                    ReadTier::Remote,
                    TIMEOUT,
                    "revoked editor loses the entry by id",
                    |rows| rows.is_empty().then_some(()),
                ),
                wait_for_query(
                    &editor,
                    by_id("playlists", playlist),
                    ReadTier::Remote,
                    TIMEOUT,
                    "revoked editor loses the playlist",
                    |rows| rows.is_empty().then_some(()),
                ),
                wait_for_query(
                    &editor,
                    by_id("invitations", invite),
                    ReadTier::Remote,
                    TIMEOUT,
                    "revoked editor loses the deleted invitation",
                    |rows| rows.is_empty().then_some(()),
                ),
            );

            let default_ids = |query: Query| {
                let editor = &editor;
                async move {
                    editor
                        .query(query, ReadTier::LocalFirst)
                        .await
                        .map(jazz::tools::test_support::ordinary_rows)
                        .expect("default-tier read")
                        .into_iter()
                        .map(|(id, _)| id)
                        .collect::<Vec<_>>()
                }
            };
            assert!(
                default_ids(by_id("invitations", invite)).await.is_empty(),
                "the deleted invitation is gone from the editor's default read"
            );
            assert_eq!(
                default_ids(by_id("playlists", playlist)).await,
                vec![playlist],
                "the undeleted playlist stays cached after losing authorization"
            );
            assert_eq!(
                default_ids(by_id("entries", entry)).await,
                vec![entry],
                "the undeleted entry stays cached after losing authorization"
            );

            owner.shutdown().await.expect("shutdown owner");
            editor.shutdown().await.expect("shutdown editor");
            server.shutdown().await;
        })
        .await;
}

/// A held row deleted at Core is deleted locally even when the editor's
/// query runtime never idles while its strict reads settle (INV-SYNC-48).
///
/// Actors: `owner` creates a playlist with one entry and invites `editor`,
/// who accepts and reads all three rows. While `editor` keeps issuing fresh
/// Remote reads without pause (the churn a browser client's polling
/// `waitForQuery` loops produce), `owner` deletes the invitation and the
/// editor's Remote waits reach zero rows. Deletion reconciliation must not
/// wait for an idle runtime or give up at the settlement bound: the deleted
/// invitation eventually leaves the editor's local store, while the playlist
/// and entry, which merely lost authorization, stay cached (INV-SYNC-14).
///
/// ```text
/// owner ──playlist + entry + invite──► server ──► editor (accepts, reads all)
/// editor ──fresh Remote reads, continuously ─────────────────────────────┐
/// owner ──delete invite──► server                                        │
/// editor ──Remote waits: invite, playlist, entry all empty               │
/// editor ──local store: invite gone; playlist and entry still cached ◄───┘
/// ```
#[tokio::test]
async fn deleted_invitation_reaches_local_store_while_remote_reads_keep_the_runtime_busy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(schema())
                .await
                .expect("start test server");
            let owner = connect(&server, OWNER_ID).await;
            let editor = connect(&server, EDITOR_ID).await;

            let (playlist, _, tx) = owner
                .insert(
                    "playlists",
                    row_input!("owner_id" => OWNER_ID, "name" => "Road tape"),
                )
                .expect("owner creates the playlist");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            let (entry, _, tx) = owner
                .insert(
                    "entries",
                    row_input!("playlist_id" => playlist, "position" => 1),
                )
                .expect("owner adds an entry");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            let (invite, _, tx) = owner
                .insert(
                    "invitations",
                    row_input!(
                        "playlist_id" => playlist,
                        "owner_id" => OWNER_ID,
                        "subject" => EDITOR_ID,
                        "role" => "editor",
                        "status" => "pending"
                    ),
                )
                .expect("owner invites the editor");
            settle(&owner, tx, DurabilityTier::GlobalServer).await;
            wait_for_query(
                &editor,
                by_id("invitations", invite),
                ReadTier::Remote,
                TIMEOUT,
                "editor observes the pending invitation",
                |rows| (rows.len() == 1).then_some(()),
            )
            .await;
            let tx = editor
                .update(
                    "invitations",
                    invite,
                    vec![("status".into(), Value::Text("accepted".into()))],
                )
                .expect("editor accepts");
            settle(&editor, tx, DurabilityTier::GlobalServer).await;
            for (query, what) in [
                (by_id("invitations", invite), "the accepted invitation"),
                (by_id("playlists", playlist), "the playlist"),
                (by_id("entries", entry), "the entry"),
            ] {
                wait_for_query(
                    &editor,
                    query,
                    ReadTier::Remote,
                    TIMEOUT,
                    format!("accepted editor reads {what}"),
                    |rows| (rows.len() == 1).then_some(()),
                )
                .await;
            }

            // Fresh Remote reads, back to back, for the rest of the scenario.
            let stop = std::cell::Cell::new(false);
            let churn = |query: fn() -> Query| {
                let (editor, stop) = (&editor, &stop);
                async move {
                    while !stop.get() {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            editor.query(query(), ReadTier::Remote),
                        )
                        .await;
                    }
                }
            };
            let scenario = async {
                let tx = owner
                    .delete("invitations", invite)
                    .expect("owner deletes the invitation");
                settle(&owner, tx, DurabilityTier::GlobalServer).await;
                tokio::join!(
                    wait_for_query(
                        &editor,
                        by_id("invitations", invite),
                        ReadTier::Remote,
                        TIMEOUT,
                        "revoked editor loses the deleted invitation",
                        |rows| rows.is_empty().then_some(()),
                    ),
                    wait_for_query(
                        &editor,
                        by_id("playlists", playlist),
                        ReadTier::Remote,
                        TIMEOUT,
                        "revoked editor loses the playlist",
                        |rows| rows.is_empty().then_some(()),
                    ),
                    wait_for_query(
                        &editor,
                        by_id("entries", entry),
                        ReadTier::Remote,
                        TIMEOUT,
                        "revoked editor loses the entry",
                        |rows| rows.is_empty().then_some(()),
                    ),
                );
                // Reconciliation may finish after the reads settled; it must
                // finish, not be abandoned.
                let deadline = tokio::time::Instant::now() + TIMEOUT;
                while !local_rows(&editor, by_id("invitations", invite))
                    .await
                    .is_empty()
                {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "the deleted invitation never left the editor's local store"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                stop.set(true);
            };
            // Boxed: four concurrent read futures inline would otherwise sit
            // on the test thread's stack.
            tokio::join!(
                Box::pin(churn(|| Query::from("entries"))),
                Box::pin(churn(|| Query::from("playlists"))),
                Box::pin(churn(|| Query::from("invitations"))),
                Box::pin(scenario),
            );

            let default_ids = |query: Query| {
                let editor = &editor;
                async move {
                    editor
                        .query(query, ReadTier::LocalFirst)
                        .await
                        .map(jazz::tools::test_support::ordinary_rows)
                        .expect("default-tier read")
                        .into_iter()
                        .map(|(id, _)| id)
                        .collect::<Vec<_>>()
                }
            };
            assert!(
                default_ids(by_id("invitations", invite)).await.is_empty(),
                "the deleted invitation is gone from the editor's default read"
            );
            assert_eq!(
                default_ids(by_id("playlists", playlist)).await,
                vec![playlist],
                "the undeleted playlist stays cached after losing authorization"
            );
            assert_eq!(
                default_ids(by_id("entries", entry)).await,
                vec![entry],
                "the undeleted entry stays cached after losing authorization"
            );

            owner.shutdown().await.expect("shutdown owner");
            editor.shutdown().await.expect("shutdown editor");
            server.shutdown().await;
        })
        .await;
}
