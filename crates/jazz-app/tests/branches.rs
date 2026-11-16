//! Branches and drafts (TS's `branch`/`base` options and `captureSnapshot`),
//! through the bindings generated for tests/fixtures/drafts.

#[path = "bindings/drafts.rs"]
mod bindings;

use std::time::Duration;

use bindings::{Document, DocumentUpdate, NewDocument, NewDraft, documents, drafts};
use futures::StreamExt;
use jazz_app_support::{
    AppFiles, AppId, Backend, Branch, Config, Error, Jazz, Order, ReadOptions, ReadTier, Rows,
    Snapshot,
};
use jazz_server::JazzServer;

fn titles(rows: &[Document]) -> Vec<(&str, &str)> {
    rows.iter()
        .map(|row| (row.branch.as_str(), row.title.as_str()))
        .collect()
}

fn by_title() -> jazz_app_support::Query<Document> {
    documents::query().order_by(documents::title(), Order::Asc)
}

/// Reads fall back to the base; writes land in the head and leave the base
/// as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draft_reads_through_its_base_and_writes_into_itself() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let main = jazz.on_branch("main");
    let draft = jazz.on_branch(Branch::from("draft").over("main"));

    let intro = main
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();
    let outro = main
        .insert(NewDocument::new("main", "Outro"))
        .await
        .unwrap();
    assert_eq!(intro.branch, "main");

    // The draft inherits main's rows, under its own coordinate.
    let seen = draft.all(by_title()).await.unwrap();
    assert_eq!(titles(&seen), [("draft", "Intro"), ("draft", "Outro")]);
    assert_eq!(seen[0].id, intro.id);
    // Without a branch view, a read sees every branch-local row as stored:
    // the branch column is ordinary data.
    assert_eq!(
        titles(&jazz.all(by_title()).await.unwrap()),
        [("main", "Intro"), ("main", "Outro")]
    );

    // An update copies the inherited row into the draft.
    draft
        .update(
            intro.id,
            DocumentUpdate {
                title: Some("Intro, reworked".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // A delete hides the inherited row in the draft only.
    draft.delete::<Document>(outro.id).await.unwrap();
    let added = draft
        .insert(NewDocument::new("draft", "Appendix"))
        .await
        .unwrap();
    assert_eq!(added.branch, "draft");

    assert_eq!(
        titles(&draft.all(by_title()).await.unwrap()),
        [("draft", "Appendix"), ("draft", "Intro, reworked")]
    );
    assert_eq!(
        titles(&main.all(by_title()).await.unwrap()),
        [("main", "Intro"), ("main", "Outro")]
    );
    // Without a view: main's rows and the draft's own, side by side.
    assert_eq!(
        titles(
            &jazz
                .all(by_title().filter(documents::branch().eq("draft")))
                .await
                .unwrap()
        ),
        [("draft", "Appendix"), ("draft", "Intro, reworked")]
    );
    // The draft alone (no base) holds only what was written into it.
    assert_eq!(
        titles(&jazz.on_branch("draft").all(by_title()).await.unwrap()),
        [("draft", "Appendix"), ("draft", "Intro, reworked")]
    );
    assert_eq!(
        draft.get::<Document>(outro.id).await.unwrap(),
        None,
        "deleted in the draft"
    );

    // Restoring brings the draft's deleted row back, as main has it.
    let restored = draft.restore::<Document>(outro.id).await.unwrap();
    assert_eq!(restored.title, "Outro");

    // Tables without branchBy are shared by every branch.
    let row = draft.insert(NewDraft::new("draft")).await.unwrap();
    assert_eq!(
        jazz.get::<bindings::Draft>(row.id).await.unwrap(),
        Some(row)
    );

    jazz.shutdown().await.unwrap();
}

/// A frozen base keeps showing the data as it was at the snapshot, and the
/// snapshot survives being stored as a string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frozen_base_keeps_the_snapshot() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let main = jazz.on_branch("main");
    let intro = main
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();

    let captured = jazz.capture_snapshot().await.unwrap();
    // The draft's row stores its base and the snapshot, as the TS recipe does.
    let row = jazz
        .insert(NewDraft {
            base: Some("main".into()),
            base_snapshot_ref: Some(captured.to_persisted()),
            ..NewDraft::new("frozen")
        })
        .await
        .unwrap();

    main.update(
        intro.id,
        DocumentUpdate {
            title: Some("Intro, later".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    main.insert(NewDocument::new("main", "Later"))
        .await
        .unwrap();

    let stored = jazz
        .one(drafts::query().filter(drafts::name().eq("frozen")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, row);
    let at: Snapshot = stored.base_snapshot_ref.unwrap().parse().unwrap();
    assert_eq!(at, captured);

    let frozen = jazz.on_branch(Branch::from("draft").over_snapshot(stored.base.unwrap(), at));
    assert_eq!(
        titles(&frozen.all(by_title()).await.unwrap()),
        [("draft", "Intro")]
    );
    let live = jazz.on_branch(Branch::from("draft").over("main"));
    assert_eq!(
        titles(&live.all(by_title()).await.unwrap()),
        [("draft", "Intro, later"), ("draft", "Later")]
    );

    // A write through the frozen view copies the frozen row.
    frozen
        .update(
            intro.id,
            DocumentUpdate {
                title: Some("Intro, drafted".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        titles(&frozen.all(by_title()).await.unwrap()),
        [("draft", "Intro, drafted")]
    );

    assert!(matches!(
        Snapshot::from_persisted("not a snapshot"),
        Err(Error::App(_))
    ));
    jazz.shutdown().await.unwrap();
}

/// A subscription through a branch sees the base's changes to rows the head
/// inherits, and not to rows the head overrides.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draft_subscription_follows_its_live_base() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let main = jazz.on_branch("main");
    let draft = jazz.on_branch(Branch::from("draft").over("main"));
    let intro = main
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();

    let mut changes = draft.subscribe(documents::query()).await.unwrap();
    let mut rows = Rows::default();
    rows.apply(next(&mut changes).await);
    assert_eq!(titles(rows.as_slice()), [("draft", "Intro")]);

    main.insert(NewDocument::new("main", "Outro"))
        .await
        .unwrap();
    while rows.as_slice().len() < 2 {
        rows.apply(next(&mut changes).await);
    }
    main.update(
        intro.id,
        DocumentUpdate {
            title: Some("Intro, from main".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    while !rows
        .as_slice()
        .iter()
        .any(|row| row.title == "Intro, from main")
    {
        rows.apply(next(&mut changes).await);
    }
    jazz.shutdown().await.unwrap();
}

/// Transactions write into branches and commit them together; selectors
/// are checked against the schema's branch columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactions_and_selectors() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let intro = jazz
        .on_branch("main")
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();

    jazz.transaction(move |tx| async move {
        let draft = tx.on_branch(Branch::from("draft").over("main"))?;
        draft.update(
            intro.id,
            DocumentUpdate {
                title: Some("Intro, in a transaction".into()),
                ..Default::default()
            },
        )?;
        draft.insert(NewDocument::new("draft", "Added"))?;
        tx.insert(NewDraft::new("draft"))?;
        // A mergeable transaction reads through the branch, staged writes
        // included, and so can write what it reads back.
        let seen = draft
            .get::<Document>(intro.id)
            .await?
            .expect("the draft's row");
        assert_eq!(
            (seen.branch.as_str(), seen.title.as_str()),
            ("draft", "Intro, in a transaction")
        );
        let upserted = draft
            .upsert(
                jazz_app_support::ObjectId::new(),
                NewDocument::new("draft", "Upserted"),
            )
            .await?;
        assert_eq!(upserted.branch, "draft");
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        titles(
            &jazz
                .on_branch(Branch::from("draft").over("main"))
                .all(by_title())
                .await
                .unwrap()
        ),
        [
            ("draft", "Added"),
            ("draft", "Intro, in a transaction"),
            ("draft", "Upserted")
        ]
    );
    assert_eq!(jazz.all(drafts::query()).await.unwrap().len(), 1);

    // The typed and the named qualified forms name the same branch.
    let typed = jazz.on_branch(Branch::by(documents::branch(), "draft"));
    let named = jazz.on_branch(Branch::named("branch", "draft".to_owned()));
    assert_eq!(
        typed.all(by_title()).await.unwrap(),
        named.all(by_title()).await.unwrap()
    );
    assert_eq!(typed.all(by_title()).await.unwrap().len(), 3);

    for wrong in [
        Branch::named("title", "draft".to_owned()),
        Branch::by(documents::branch(), "draft").and(documents::title(), "x"),
        Branch::new(3_i32),
    ] {
        let refused = jazz.on_branch(wrong.clone()).all(documents::query()).await;
        assert!(
            matches!(refused, Err(Error::Jazz(_))),
            "{wrong:?}: {refused:?}"
        );
    }
    jazz.shutdown().await.unwrap();
}

/// A serializable transaction reads shared rows at its start and writes
/// into a branch; the branch writes commit with it, and an ignored refusal
/// of a read-back write commits nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_serializable_transaction_writes_into_a_branch() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let intro = jazz
        .on_branch("main")
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();
    let row = jazz.insert(NewDraft::new("review")).await.unwrap();

    let renamed = jazz
        .serializable_transaction(move |tx| async move {
            // A read of an unbranched table, outside the branch.
            let draft_row = tx
                .get::<bindings::Draft>(row.id)
                .await?
                .expect("the draft row");
            let draft = tx.on_branch(Branch::from("draft").over("main"))?;
            draft.update(
                intro.id,
                DocumentUpdate {
                    title: Some(format!("Intro, for {}", draft_row.name)),
                    ..Default::default()
                },
            )?;
            draft.insert(NewDocument::new("draft", "Notes"))?;
            // It reads through the branch (jazz#5806), its staged update
            // included, under the draft's coordinate.
            let seen = draft
                .get::<Document>(intro.id)
                .await?
                .expect("the draft's row");
            assert_eq!(
                (seen.branch.as_str(), seen.title.as_str()),
                ("draft", "Intro, for review")
            );
            // Writes that read back are still refused before staging in a
            // serializable transaction: this ignored upsert commits nothing.
            let _ignored = draft
                .upsert(
                    jazz_app_support::ObjectId::new(),
                    NewDocument::new("draft", "Never"),
                )
                .await;
            Ok(draft_row.name)
        })
        .await
        .unwrap();
    assert_eq!(renamed, "review");
    assert_eq!(
        titles(
            &jazz
                .on_branch(Branch::from("draft").over("main"))
                .all(by_title())
                .await
                .unwrap()
        ),
        [("draft", "Intro, for review"), ("draft", "Notes")]
    );
    assert_eq!(
        titles(&jazz.on_branch("main").all(by_title()).await.unwrap()),
        [("main", "Intro")]
    );
    jazz.shutdown().await.unwrap();
}

/// `ReadOptions::branch` is TS's per-call `branch`/`base`: reads and
/// subscriptions on the plain handle, through a branch view.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_options_read_through_a_branch() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let main = jazz.on_branch("main");
    let intro = main
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();
    jazz.on_branch(Branch::from("draft").over("main"))
        .update(
            intro.id,
            DocumentUpdate {
                title: Some("Intro, drafted".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let at = jazz.capture_snapshot().await.unwrap();
    main.insert(NewDocument::new("main", "Later"))
        .await
        .unwrap();

    let draft = ReadOptions::default().branch(Branch::from("draft").over("main"));
    assert_eq!(
        titles(
            &jazz
                .all_with_options(by_title(), draft.clone())
                .await
                .unwrap()
        ),
        [("draft", "Intro, drafted"), ("draft", "Later")]
    );
    // Without a base: the draft's own rows.
    assert_eq!(
        titles(
            &jazz
                .all_with_options(by_title(), ReadOptions::default().branch("draft"))
                .await
                .unwrap()
        ),
        [("draft", "Intro, drafted")]
    );
    // A frozen base, with the other options.
    let frozen = ReadOptions::default()
        .tier(ReadTier::LocalFirst)
        .branch(Branch::from("draft").over_snapshot("main", at));
    assert_eq!(
        titles(&jazz.all_with_options(by_title(), frozen).await.unwrap()),
        [("draft", "Intro, drafted")]
    );
    // Without a branch, every branch-local row as stored.
    assert_eq!(
        jazz.all_with_options(by_title(), ReadOptions::default())
            .await
            .unwrap()
            .len(),
        3
    );

    let mut changes = jazz
        .subscribe_with_options(documents::query(), draft)
        .await
        .unwrap();
    let mut rows = Rows::default();
    rows.apply(next(&mut changes).await);
    assert_eq!(rows.as_slice().len(), 2);
    main.insert(NewDocument::new("main", "Outro"))
        .await
        .unwrap();
    while rows.as_slice().len() < 3 {
        rows.apply(next(&mut changes).await);
    }
    assert!(rows.as_slice().iter().all(|row| row.branch == "draft"));

    // A selector that doesn't match the schema fails the read.
    let wrong = ReadOptions::default().branch(Branch::named("title", "x".to_owned()));
    assert!(matches!(
        jazz.all_with_options(by_title(), wrong).await,
        Err(Error::Jazz(_))
    ));
    jazz.shutdown().await.unwrap();
}

/// A draft written by one backend is read through its base by another,
/// through the server; the base stays as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drafts_sync_through_the_server() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/drafts");
    let files = || AppFiles::read(dir).unwrap();
    let config = |user: &str| {
        Config::new(app_id)
            .server(server.base_url())
            .backend(Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                user,
            ))
    };
    let writer = Jazz::open(files(), config("alice")).await.unwrap();
    let reader = Jazz::open(files(), config("bob")).await.unwrap();

    let intro = writer
        .on_branch("main")
        .insert(NewDocument::new("main", "Intro"))
        .await
        .unwrap();
    let draft = writer
        .on_branch(Branch::from("draft").over("main"))
        .durable(jazz_app_support::DurabilityTier::GlobalServer);
    draft
        .update(
            intro.id,
            DocumentUpdate {
                title: Some("Intro, drafted".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    draft
        .insert(NewDocument::new("draft", "Appendix"))
        .await
        .unwrap();
    writer.wait_for_sync().await.unwrap();

    let seen = |view: jazz_app_support::OnBranch, want: usize| async move {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let rows = view.all_with(by_title(), ReadTier::Remote).await.unwrap();
                if rows.len() == want {
                    break rows;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the rows reach the other backend through the server")
    };
    let through = seen(reader.on_branch(Branch::from("draft").over("main")), 2).await;
    assert_eq!(
        titles(&through),
        [("draft", "Appendix"), ("draft", "Intro, drafted")]
    );
    let base = seen(reader.on_branch("main"), 1).await;
    assert_eq!(titles(&base), [("main", "Intro")]);

    // Policies see the branch column: `locked` may be inserted into, but
    // not updated or read.
    let locked = writer
        .on_branch(Branch::from("locked").over("main"))
        .durable(jazz_app_support::DurabilityTier::GlobalServer);
    let hidden = locked
        .insert(NewDocument::new("locked", "Hidden"))
        .await
        .unwrap();
    let denied = locked
        .update(
            intro.id,
            DocumentUpdate {
                title: Some("Intro, locked".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(denied.is_err(), "an update the policy denies: {denied:?}");
    let unseen = reader
        .on_branch("locked")
        .all_with(by_title(), ReadTier::Remote)
        .await
        .unwrap();
    assert!(
        unseen.iter().all(|row| row.id != hidden.id),
        "a read the policy denies: {unseen:?}"
    );
    assert!(seen(reader.on_branch("main"), 1).await[0].title == "Intro");

    writer.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
    server.shutdown().await;
}

async fn next(
    changes: &mut jazz_app_support::Subscription<Document>,
) -> jazz_app_support::Delta<Document> {
    tokio::time::timeout(Duration::from_secs(5), changes.next())
        .await
        .expect("a delta")
        .expect("an open stream")
        .expect("a typed delta")
}
