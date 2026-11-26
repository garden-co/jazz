// A new id whenever the engine this relay links is rebuilt from changed
// sources: it keys the foreground store image (`relay.rs`), whose layout is
// whatever this build's fresh open produces.
fn main() {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    println!("cargo:rustc-env=JAZZ_RELAY_BUILD_ID={id}");
    for path in [
        "src",
        "Cargo.toml",
        "../jazz",
        "../jazz-text",
        "../jazz-native-transport",
        "../jazz-compression",
        "../../vendor",
        "../../Cargo.toml",
        "../../Cargo.lock",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
}
