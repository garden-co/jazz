extern crate napi_build;

fn main() {
    napi_build::setup();
    println!("cargo:rerun-if-env-changed=JAZZ_NATIVE_ARTIFACT_FINGERPRINT");
}
