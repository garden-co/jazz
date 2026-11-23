//! One open app per store directory: a second open is refused
//! (`StoreHeld`) whether it is in this process or another, and one that
//! follows the holder's close, drop or failed open is not.

#[path = "bindings/guarded.rs"]
mod bindings;

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use bindings::{NewNote, notes};
use jazz_app_support::{AppFiles, AppId, Config, Error, Jazz, ObjectId, Storage};

fn config(app_id: AppId, dir: &Path) -> Config {
    Config::new(app_id).storage(Storage::Dir(dir.to_path_buf()))
}

async fn open(app_id: AppId, dir: &Path) -> jazz_app_support::Result<Jazz> {
    bindings::open(config(app_id, dir)).await
}

async fn write(jazz: &Jazz, body: &str) -> jazz_app_support::Result<()> {
    jazz.insert(NewNote::new(ObjectId::new(), body)).await?;
    Ok(())
}

fn refused(opened: jazz_app_support::Result<Jazz>) -> String {
    match opened {
        Err(Error::StoreHeld(message)) => message,
        Err(other) => panic!("expected StoreHeld, got {other}"),
        Ok(_) => panic!("a second app opened the store"),
    }
}

/// A child being spawned shares every open file until it execs, which is
/// before `spawn` returns: no look at a lock may fall in between.
static SPAWNING: Mutex<()> = Mutex::new(());

/// Whether the store's lock is free right now, as another process finds it:
/// one look, so a release that comes a moment late is seen.
fn lock_is_free(dir: &Path) -> bool {
    let _no_spawn = SPAWNING.lock().unwrap_or_else(|e| e.into_inner());
    let file = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("jazz.lock"))
        .unwrap();
    file.try_lock().is_ok()
}

/// The second open fails at once, saying what to do, and the first app is
/// untouched.
#[tokio::test]
async fn a_second_open_in_this_process_is_refused() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let first = open(app_id, dir.path()).await.unwrap();

    let started = Instant::now();
    let message = refused(open(app_id, dir.path()).await);
    assert!(started.elapsed() < Duration::from_secs(2), "it waited");
    assert!(
        message.contains("in another app of this process"),
        "{message}"
    );
    assert!(message.contains("share that app's handle"), "{message}");
    let name = dir.path().file_name().unwrap().to_str().unwrap();
    assert!(message.contains(name), "{message}");

    // Another spelling of the same directory is the same store.
    refused(open(app_id, &dir.path().join(".")).await);

    write(&first, "still mine").await.unwrap();
    assert_eq!(first.all(notes::query()).await.unwrap().len(), 1);
    first.shutdown().await.unwrap();
}

/// What a subscription or a clone keeps open is still open.
#[tokio::test]
async fn a_remaining_clone_still_holds_the_store() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let first = open(app_id, dir.path()).await.unwrap();
    let clone = first.clone();
    drop(first);
    refused(open(app_id, dir.path()).await);
    clone.shutdown().await.unwrap();
}

/// Many opens at once have one winner, and only it writes.
#[test]
fn a_race_of_opens_has_one_winner() {
    const OPENERS: usize = 12;
    for round in 0..25 {
        let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
        let start = Arc::new(Barrier::new(OPENERS));
        let threads: Vec<_> = (0..OPENERS)
            .map(|_| {
                let (start, dir) = (start.clone(), dir.path().to_path_buf());
                std::thread::spawn(move || {
                    start.wait();
                    futures::executor::block_on(open(app_id, &dir))
                })
            })
            .collect();
        let mut winners = Vec::new();
        for thread in threads {
            match thread.join().unwrap() {
                Ok(jazz) => winners.push(jazz),
                Err(Error::StoreHeld(_)) => {}
                Err(other) => panic!("round {round}: {other}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}");
        futures::executor::block_on(write(&winners[0], "winner")).unwrap();
        futures::executor::block_on(winners[0].shutdown()).unwrap();
    }
}

/// Two writers at once: one is refused at open, and none of the winner's
/// writes fails (two used to fail with "database is locked", or mint the
/// same transaction id).
#[test]
fn concurrent_writers_are_one_refused_open_and_no_failed_write() {
    for round in 0..10 {
        let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
        let start = Arc::new(Barrier::new(2));
        let writers: Vec<_> = (0..2)
            .map(|_| {
                let (start, dir) = (start.clone(), dir.path().to_path_buf());
                std::thread::spawn(move || {
                    start.wait();
                    let jazz = match futures::executor::block_on(open(app_id, &dir)) {
                        Ok(jazz) => jazz,
                        Err(Error::StoreHeld(_)) => return None,
                        Err(other) => panic!("{other}"),
                    };
                    for i in 0..40 {
                        futures::executor::block_on(write(&jazz, &i.to_string())).unwrap();
                    }
                    Some(jazz)
                })
            })
            .collect();
        let opened: Vec<_> = writers
            .into_iter()
            .filter_map(|writer| writer.join().expect("a write failed"))
            .collect();
        assert_eq!(opened.len(), 1, "round {round}");
        let stored = futures::executor::block_on(opened[0].all(notes::query())).unwrap();
        assert_eq!(stored.len(), 40, "round {round}");
        futures::executor::block_on(opened[0].shutdown()).unwrap();
    }
}

#[tokio::test]
async fn close_then_reopen() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    for round in 0..50 {
        let jazz = open(app_id, dir.path()).await.unwrap();
        assert_eq!(jazz.all(notes::query()).await.unwrap().len(), round);
        write(&jazz, "kept").await.unwrap();
        jazz.shutdown().await.unwrap();
    }
}

/// Every caller's shutdown returns with the store let go of, also the one
/// that found it already stopping.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_shutdown_returns_with_the_store_released() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    for round in 0..50 {
        let jazz = open(app_id, dir.path()).await.unwrap();
        // Something to drain, so there is a while of stopping.
        let busy = jazz.clone();
        let (running, started) = tokio::sync::oneshot::channel();
        let job = tokio::spawn(async move {
            busy.with_client(move |_| async move {
                let _ = running.send(());
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            })
            .await
        });
        started.await.unwrap();
        let other = jazz.clone();
        let first = tokio::spawn(async move { other.shutdown().await });
        tokio::time::sleep(Duration::from_millis(round % 5)).await;
        jazz.shutdown().await.unwrap();
        assert!(lock_is_free(dir.path()), "round {round}: still held");
        first.await.unwrap().unwrap();
        job.await.unwrap().unwrap();
    }
}

/// An open made while the holder is stopping waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_waits_for_a_holder_that_is_closing() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let jazz = open(app_id, dir.path()).await.unwrap();
    let busy = jazz.clone();
    let (running, started) = tokio::sync::oneshot::channel();
    let job = tokio::spawn(async move {
        busy.with_client(move |_| async move {
            let _ = running.send(());
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(())
        })
        .await
    });
    started.await.unwrap();
    let closing = jazz.clone();
    let closed = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let reopened = open(app_id, dir.path())
        .await
        .expect("it waits for the close");
    closed.await.unwrap().unwrap();
    job.await.unwrap().unwrap();
    reopened.shutdown().await.unwrap();
}

/// It waits five seconds, then says the store is still being closed.
#[tokio::test]
async fn an_open_stops_waiting_for_a_close_that_never_ends() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let jazz = open(app_id, dir.path()).await.unwrap();
    jazz.begin_stopping();

    let started = Instant::now();
    let message = refused(open(app_id, dir.path()).await);
    assert!(
        started.elapsed() >= Duration::from_secs(5),
        "it didn't wait"
    );
    assert!(message.contains("still being closed"), "{message}");
    jazz.shutdown().await.unwrap();
}

#[tokio::test]
async fn drop_then_reopen() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    for round in 0..50 {
        let jazz = open(app_id, dir.path()).await.unwrap();
        assert_eq!(jazz.all(notes::query()).await.unwrap().len(), round);
        write(&jazz, "kept").await.unwrap();
        drop(jazz);
    }
}

/// A failed open's error comes with the store let go of.
#[tokio::test]
async fn a_failed_open_releases_the_store_before_it_answers() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    for round in 0..100 {
        let broken = AppFiles::new(b"{".to_vec(), None);
        match Jazz::open(broken, config(app_id, dir.path())).await {
            Err(Error::StoreHeld(message)) => panic!("round {round}: {message}"),
            Err(_) => {}
            Ok(_) => panic!("a broken app file opened"),
        }
        assert!(lock_is_free(dir.path()), "round {round}: still held");
    }
    open(app_id, dir.path())
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn an_abandoned_open_doesnt_refuse_the_next() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    for round in 0..50 {
        {
            let mut opening = std::pin::pin!(open(app_id, dir.path()));
            // Given up somewhere between the lock and the answer.
            let given_up = Duration::from_micros(round * 200);
            let _ = tokio::time::timeout(given_up, &mut opening).await;
        }
        let next = open(app_id, dir.path())
            .await
            .unwrap_or_else(|error| panic!("round {round}: {error}"));
        next.shutdown().await.unwrap();
    }
}

/// A delete is the holder's, and an app that had stopped can't delete the
/// store from under the one that has it now.
#[tokio::test]
async fn a_stopped_app_cant_delete_the_next_ones_store() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let stale = open(app_id, dir.path()).await.unwrap();
    stale.shutdown().await.unwrap();
    let current = open(app_id, dir.path()).await.unwrap();
    write(&current, "kept").await.unwrap();

    let deleted = stale.delete_client_storage().await;
    assert!(matches!(deleted, Err(Error::StoreHeld(_))), "{deleted:?}");
    write(&current, "still open").await.unwrap();
    assert!(dir.path().join("jazz.sqlite").exists());

    current.delete_client_storage().await.unwrap();
    assert!(!dir.path().join("jazz.sqlite").exists());
}

const CHILD_DIR: &str = "JAZZ_STORE_LOCK_TEST_DIR";
const CHILD_APP: &str = "JAZZ_STORE_LOCK_TEST_APP";
const CHILD_HOLDS: &str = "store-lock-child-holds";

/// The other process of the tests below: holds the store until killed.
/// Without its environment it is no test and passes.
#[test]
fn child_process_holding_a_store() {
    let (Ok(dir), Ok(app_id)) = (std::env::var(CHILD_DIR), std::env::var(CHILD_APP)) else {
        return;
    };
    let app_id = AppId::from_string(&app_id).expect("an app id");
    let jazz = futures::executor::block_on(open(app_id, Path::new(&dir))).unwrap();
    futures::executor::block_on(write(&jazz, "the child's")).unwrap();
    println!("\n{CHILD_HOLDS}");
    // Killed, with its app open.
    std::thread::sleep(Duration::from_secs(120));
}

struct Holder(Child);

impl Holder {
    fn start(app_id: AppId, dir: &Path) -> Self {
        let spawning = SPAWNING.lock().unwrap_or_else(|e| e.into_inner());
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["child_process_holding_a_store", "--exact", "--nocapture"])
            .env(CHILD_DIR, dir)
            .env(CHILD_APP, app_id.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        drop(spawning);
        let mut holder = Self(child);
        let mut lines = BufReader::new(holder.0.stdout.take().unwrap()).lines();
        assert!(
            lines.any(|line| line.unwrap().contains(CHILD_HOLDS)),
            "the child never opened the store"
        );
        holder
    }

    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Another process is refused too, and a killed holder lets go.
#[tokio::test]
async fn another_process_holds_the_store_until_it_dies() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let mut holder = Holder::start(app_id, dir.path());

    let started = Instant::now();
    let message = refused(open(app_id, dir.path()).await);
    assert!(started.elapsed() < Duration::from_secs(2), "it waited");
    assert!(message.contains("in another process"), "{message}");

    holder.kill();
    let next = open(app_id, dir.path())
        .await
        .expect("a killed holder lets go");
    let stored = next.all(notes::query()).await.unwrap();
    assert_eq!(stored.len(), 1, "the killed holder's write");
    next.shutdown().await.unwrap();
}

/// An app up to 2.0.0-beta.0 took the lock shared: it still counts.
#[tokio::test]
async fn a_shared_holder_of_an_older_version_is_seen() {
    let (app_id, dir) = (AppId::random(), tempfile::TempDir::new().unwrap());
    let older = std::fs::File::create(dir.path().join("jazz.lock")).unwrap();
    older.lock_shared().unwrap();
    refused(open(app_id, dir.path()).await);
}
