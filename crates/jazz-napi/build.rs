extern crate napi_build;

use std::env;

fn main() {
    napi_build::setup();
    println!("cargo:rerun-if-env-changed=JAZZ_NATIVE_ARTIFACT_FINGERPRINT");
    let fingerprint = env::var("JAZZ_NATIVE_ARTIFACT_FINGERPRINT")
        .unwrap_or_else(|_| "missing-build-fingerprint".to_owned());
    // Make the fingerprint a build-script output, rather than relying on an
    // ambient option_env! input reaching rustc through Cargo's incremental
    // cache. A changed value now necessarily changes this crate's rustc inputs.
    println!("cargo:rustc-env=JAZZ_NATIVE_ARTIFACT_FINGERPRINT={fingerprint}");
}
