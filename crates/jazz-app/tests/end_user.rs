//! End users signed in through the app's own backend (`createJazz`), with the
//! app's bearer, as a React Native client is.
//!
//! These need a running `createJazz` backend for the `notes` fixture whose
//! `authenticate` accepts `Authorization: Bearer user:<id>[:<ttl>:<iat>]`
//! (see `tests/fixtures/notes/README.md`), so they are ignored by default:
//!
//! ```sh
//! JAZZ_APP_E2E_BACKEND=http://127.0.0.1:4555/jazz JAZZ_APP_E2E_APP_ID=<app id> \
//!   cargo test -p jazz-app --test end_user -- --ignored
//! ```

#[path = "bindings/notes.rs"]
mod bindings;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bindings::{NewNote, Note, notes};
use jazz_app::{AppId, Config, EndUser, Error, Jazz, ReadTier};

fn backend() -> (String, AppId) {
    let url = std::env::var("JAZZ_APP_E2E_BACKEND").expect("JAZZ_APP_E2E_BACKEND");
    let app = std::env::var("JAZZ_APP_E2E_APP_ID").expect("JAZZ_APP_E2E_APP_ID");
    (url, AppId::from_string(&app).expect("an app id"))
}

fn unique(name: &str) -> String {
    format!("{name}-{}", uuid_like())
}

fn uuid_like() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}

async fn open_as(end_user: EndUser) -> jazz_app::Result<Jazz> {
    let (url, app_id) = backend();
    bindings::open(Config::new(app_id).server(url).end_user(end_user)).await
}

/// Reads at the server until `check` holds or a deadline passes.
async fn eventually<F: Fn(&[Note]) -> bool>(jazz: &Jazz, check: F) -> Vec<Note> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let rows = jazz
            .all_with(notes::query(), ReadTier::Remote)
            .await
            .unwrap();
        if check(&rows) || tokio::time::Instant::now() > deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a createJazz backend (see the module docs)"]
async fn an_end_user_syncs_as_themselves() {
    let alice = unique("alice");
    let bob = unique("bob");
    let laptop = open_as(EndUser::bearer(format!("user:{alice}")))
        .await
        .unwrap();
    let account = laptop.account().expect("the signed-in user's account");

    let note = laptop
        .insert(NewNote {
            text: "from the laptop".into(),
        })
        .await
        .unwrap();

    // Another device of the same user, with a fresh store, gets it from the
    // server; another user does not (notes are read by their creator).
    let phone = open_as(EndUser::bearer(format!("user:{alice}")))
        .await
        .unwrap();
    assert_eq!(phone.account(), Some(account), "one user, one account");
    let rows = eventually(&phone, |rows| !rows.is_empty()).await;
    assert_eq!(rows.iter().map(|n| n.id).collect::<Vec<_>>(), vec![note.id]);

    let other = open_as(EndUser::bearer(format!("user:{bob}")))
        .await
        .unwrap();
    assert_ne!(other.account(), Some(account));
    let theirs = other
        .all_with(notes::query(), ReadTier::Remote)
        .await
        .unwrap();
    assert!(
        theirs.is_empty(),
        "bob reads only his own notes: {theirs:?}"
    );

    for jazz in [laptop, phone, other] {
        jazz.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a createJazz backend (see the module docs)"]
async fn a_bearer_the_backend_refuses_is_signed_out() {
    let error = open_as(EndUser::bearer("not-a-user")).await.unwrap_err();
    assert!(matches!(error, Error::SignedOut), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a createJazz backend (see the module docs)"]
async fn a_short_lived_bearer_is_renewed() {
    let carol = unique("carol");
    let asked = Arc::new(AtomicUsize::new(0));
    let token = {
        let carol = carol.clone();
        let asked = asked.clone();
        move || {
            let carol = carol.clone();
            asked.fetch_add(1, Ordering::SeqCst);
            async move {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                // Expires in 4 s: the backend ends its socket then, and
                // refuses the bearer after.
                Ok(format!("user:{carol}:4:{now}"))
            }
        }
    };
    let jazz = open_as(EndUser::new(token)).await.unwrap();
    jazz.insert(NewNote {
        text: "before".into(),
    })
    .await
    .unwrap();

    // Past several expiries.
    tokio::time::sleep(Duration::from_secs(14)).await;
    assert!(
        asked.load(Ordering::SeqCst) >= 3,
        "renewed: {}",
        asked.load(Ordering::SeqCst)
    );

    jazz.insert(NewNote {
        text: "after".into(),
    })
    .await
    .unwrap();
    let other_device = open_as(EndUser::bearer(format!("user:{carol}")))
        .await
        .unwrap();
    let rows = eventually(&other_device, |rows| rows.len() == 2).await;
    let mut texts: Vec<_> = rows.into_iter().map(|n| n.text).collect();
    texts.sort();
    assert_eq!(texts, ["after", "before"], "both writes reached the server");

    jazz.shutdown().await.unwrap();
    other_device.shutdown().await.unwrap();
}
