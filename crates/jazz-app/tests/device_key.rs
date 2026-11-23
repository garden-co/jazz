//! `DeviceKey::load_or_create`: one key for everyone who creates it at once.

use std::path::Path;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use jazz_app_support::DeviceKey;

const CREATORS: usize = 8;
const ROUNDS: usize = 40;
/// Set for a process of `processes_creating_at_once_get_one_key`.
const CREATOR_DIR: &str = "JAZZ_DEVICE_KEY_CREATOR_DIR";

/// What's left once the creators are done: the key, readable by its owner
/// only, and no temporary file.
fn assert_kept(dir: &Path, key: &str) {
    let kept = std::fs::read_to_string(dir.join("device.key")).unwrap();
    assert_eq!(kept, key);
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        assert!(!name.ends_with(".tmp"), "left {name}");
        #[cfg(unix)]
        {
            let mode =
                std::os::unix::fs::PermissionsExt::mode(&entry.metadata().unwrap().permissions());
            assert_eq!(mode & 0o777, 0o600, "{name}");
        }
    }
}

/// alice's threads open her key at once, with none kept yet: all get the
/// one that was kept, and a reader meanwhile never sees part of a key.
#[test]
fn threads_creating_at_once_get_one_key() {
    let root = tempfile::tempdir().unwrap();
    for round in 0..ROUNDS {
        let dir = root.path().join(round.to_string());
        let path = dir.join("device.key");
        let start = Barrier::new(CREATORS + 1);
        let done = AtomicBool::new(false);
        let keys: Vec<String> = std::thread::scope(|scope| {
            let creators: Vec<_> = (0..CREATORS)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        DeviceKey::load_or_create(&path).map(|key| key.to_string())
                    })
                })
                .collect();
            let reader = scope.spawn(|| {
                start.wait();
                while !done.load(Ordering::Acquire) {
                    if let Ok(text) = std::fs::read_to_string(&path) {
                        text.parse::<DeviceKey>().expect("a whole key");
                    }
                    std::thread::yield_now();
                }
            });
            let made: Vec<_> = creators.into_iter().map(|creator| creator.join()).collect();
            // Before any panic, or the scope waits on the reader forever.
            done.store(true, Ordering::Release);
            reader.join().unwrap();
            made.into_iter().map(|key| key.unwrap().unwrap()).collect()
        });
        assert!(keys.iter().all(|key| *key == keys[0]), "round {round}");
        assert_kept(&dir, &keys[0]);
    }
}

/// alice starts her app several times at once, with no key kept yet: every
/// process gets the one that was kept.
#[test]
fn processes_creating_at_once_get_one_key() {
    const NAME: &str = "processes_creating_at_once_get_one_key";
    if let Some(root) = std::env::var_os(CREATOR_DIR) {
        return create_in_every_round(Path::new(&root));
    }
    let root = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = (0..CREATORS)
        .map(|_| {
            std::process::Command::new(&exe)
                .args([NAME, "--exact", "--test-threads=1"])
                .env(CREATOR_DIR, root.path())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    let ready = || {
        let entries = std::fs::read_dir(root.path()).unwrap();
        entries
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                name.to_string_lossy().starts_with("ready.")
            })
            .count()
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while ready() < CREATORS && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(root.path().join("go"), "").unwrap();
    let ids: Vec<u32> = children.iter().map(|child| child.id()).collect();
    let exits: Vec<_> = children
        .into_iter()
        .map(|mut child| child.wait().unwrap())
        .collect();
    assert!(exits.iter().all(|exit| exit.success()), "{exits:?}");

    let got: Vec<String> = ids
        .iter()
        .map(|id| std::fs::read_to_string(root.path().join(format!("got.{id}"))).unwrap())
        .collect();
    assert!(got.iter().all(|keys| *keys == got[0]));
    assert_eq!(got[0].lines().count(), ROUNDS);
    for (round, key) in got[0].lines().enumerate() {
        assert_kept(&root.path().join(round.to_string()), key);
    }
}

/// One of those processes: creates each round's key, and says which it got.
fn create_in_every_round(root: &Path) {
    let id = std::process::id();
    std::fs::write(root.join(format!("ready.{id}")), "").unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    while !root.join("go").exists() {
        assert!(Instant::now() < deadline, "never told to start");
        std::thread::yield_now();
    }
    let mut got = String::new();
    for round in 0..ROUNDS {
        let path = root.join(round.to_string()).join("device.key");
        got += &DeviceKey::load_or_create(&path).unwrap().to_string();
        got.push('\n');
    }
    std::fs::write(root.join(format!("got.{id}")), got).unwrap();
}
