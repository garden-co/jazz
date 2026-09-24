// The legacy executable benchmark checks contain real protocol and delivery
// assertions. Keep those assertions in ordinary deterministic tests; timing is
// measured separately by the benchmark systems.
#[path = "../benches/support/mod.rs"]
mod support;

#[allow(dead_code)]
#[path = "../benches/cold_subscription.rs"]
mod cold_subscription;
#[allow(dead_code)]
#[path = "../benches/relation_include_delivery.rs"]
mod relation_include_delivery;
#[allow(dead_code)]
#[path = "../benches/route_subscription_curve.rs"]
mod route_subscription_curve;
#[allow(dead_code)]
#[path = "../benches/sync.rs"]
mod sync;
#[allow(dead_code)]
#[path = "../benches/validation.rs"]
mod validation;

#[test]
fn cold_subscription_correctness_smoke() {
    cold_subscription::correctness_smoke();
}

#[test]
fn sync_correctness_smoke() {
    sync::correctness_smoke();
}

#[test]
fn validation_correctness_smoke() {
    validation::correctness_smoke();
}

#[test]
fn relation_include_delivery_correctness_smoke() {
    relation_include_delivery::correctness_smoke();
}

#[test]
fn route_subscription_curve_correctness_smoke() {
    route_subscription_curve::correctness_smoke();
}

#[test]
fn failed_git_status_is_reported_dirty_in_benchmark_metadata() {
    const CHILD: &str = "JAZZ_BENCH_STATUS_FAILURE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let metadata = support::process_metadata_for_test();
        assert_eq!(metadata["git_dirty"], true);
        assert_eq!(metadata["git_status_available"], false);
        return;
    }

    let temp_dir = std::env::temp_dir().join(format!(
        "jazz-bench-git-status-failure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&temp_dir).unwrap();
    let status = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .current_dir(&temp_dir)
        .output()
        .unwrap();
    assert!(
        !status.status.success(),
        "temporary non-repository must make `git status` fail",
    );
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_git_status_is_reported_dirty_in_benchmark_metadata",
        ])
        .env(CHILD, "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .current_dir(&temp_dir)
        .output()
        .unwrap();
    std::fs::remove_dir_all(&temp_dir).unwrap();

    assert!(
        result.status.success(),
        "child test should observe failed git status as dirty:\n{}",
        String::from_utf8_lossy(&result.stdout),
    );
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("1 passed"),
        "expected child harness to run this regression:\n{}",
        String::from_utf8_lossy(&result.stdout),
    );
}

#[test]
fn successful_empty_status_is_available_without_a_git_commit() {
    const CHILD: &str = "JAZZ_BENCH_EMPTY_STATUS_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let metadata = support::process_metadata_for_test();
        assert_eq!(metadata["git_dirty"], false);
        assert_eq!(metadata["git_status_available"], true);
        return;
    }

    let temp_dir = std::env::temp_dir().join(format!(
        "jazz-bench-empty-git-status-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&temp_dir).unwrap();
    let initialized = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .current_dir(&temp_dir)
        .status()
        .unwrap();
    assert!(
        initialized.success(),
        "temporary repository should initialize"
    );
    let status = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(&temp_dir)
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(status.stdout.is_empty());
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "successful_empty_status_is_available_without_a_git_commit",
        ])
        .env(CHILD, "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .current_dir(&temp_dir)
        .output()
        .unwrap();
    std::fs::remove_dir_all(&temp_dir).unwrap();

    assert!(
        result.status.success(),
        "child test should distinguish successful empty status from missing HEAD:\n{}",
        String::from_utf8_lossy(&result.stdout),
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
}
