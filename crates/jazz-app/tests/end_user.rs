//! End users signed in through the app's own backend (`createJazz`), with the
//! app's bearer, as a React Native client is.
//!
//! The tests start `tests/fixtures/notes/server.mjs` (a `createJazz` backend
//! whose `authenticate` accepts `Authorization: Bearer user:<id>[:<ttl>:<iat>]`)
//! with the workspace's built `jazz-tools`, so they run only with the
//! `e2e-backend` feature (see `tests/fixtures/notes/README.md`):
//!
//! ```sh
//! cargo test -p jazz-app --features e2e-backend --test end_user
//! ```

#[path = "bindings/notes.rs"]
mod bindings;

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bindings::{NewNote, Note, notes};
use jazz_app::{AppId, Config, EndUser, Error, Jazz, ReadTier};

/// The backend every test here shares: started once, and stopped when the
/// test process ends (it exits when its stdin closes).
struct Backend {
    url: String,
    app_id: AppId,
    _process: Child,
}

fn backend() -> &'static Backend {
    static BACKEND: OnceLock<Backend> = OnceLock::new();
    BACKEND.get_or_init(|| {
        let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let tools = std::env::var_os("JAZZ_APP_E2E_JAZZ_TOOLS")
            .map(PathBuf::from)
            .unwrap_or_else(|| crate_dir.join("../../packages/jazz-tools"));
        assert!(
            tools.join("dist/backend/index.js").exists(),
            "the end-user tests need a built jazz-tools at {} \
             (see tests/fixtures/notes/README.md)",
            tools.display()
        );
        let mut process = Command::new("node")
            .arg(crate_dir.join("tests/fixtures/notes/server.mjs"))
            .arg(&tools)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start the createJazz backend (node)");
        // createJazz logs to stdout too: the address is on the `serving` line.
        let mut lines = BufReader::new(process.stdout.take().unwrap()).lines();
        let line = loop {
            match lines.next() {
                Some(Ok(line)) if line.starts_with("serving ") => {
                    break line["serving ".len()..].to_owned();
                }
                Some(Ok(_)) => continue,
                other => panic!("the backend did not start: {other:?}"),
            }
        };
        // Keep draining its stdout, so its logs never block it.
        std::thread::spawn(move || lines.for_each(drop));
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Serving {
            app_id: String,
            url: String,
        }
        let serving: Serving = serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("the backend did not start ({error}): {line:?}"));
        Backend {
            url: serving.url,
            app_id: AppId::from_string(&serving.app_id).expect("an app id"),
            _process: process,
        }
    })
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
    let backend = backend();
    bindings::open(
        Config::new(backend.app_id)
            .server(backend.url.clone())
            .end_user(end_user),
    )
    .await
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
async fn a_bearer_the_backend_refuses_is_signed_out() {
    let error = open_as(EndUser::bearer("not-a-user")).await.unwrap_err();
    assert!(matches!(error, Error::SignedOut), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
