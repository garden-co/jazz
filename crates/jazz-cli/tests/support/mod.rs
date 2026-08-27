pub use jazz_testkit::publish_allow_all_permissions;

pub fn cargo_binary(name: &str) -> std::path::PathBuf {
    let env_name = format!("CARGO_BIN_EXE_{name}");
    if let Some(path) = std::env::var_os(&env_name) {
        return path.into();
    }

    let test_executable = std::env::current_exe().expect("resolve integration test executable");
    let deps_dir = test_executable
        .parent()
        .expect("integration test executable has a parent directory");
    assert_eq!(
        deps_dir.file_name().and_then(|name| name.to_str()),
        Some("deps"),
        "integration test executable is not inside Cargo's deps directory: {}",
        test_executable.display()
    );
    let binary = deps_dir
        .parent()
        .expect("Cargo deps directory has a profile parent")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        binary.is_file(),
        "{env_name} is unset and sibling Cargo binary does not exist: {}",
        binary.display()
    );
    binary
}
