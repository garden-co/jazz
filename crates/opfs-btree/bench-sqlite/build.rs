use std::process::Command;
fn main() {
    if std::env::var("TARGET").unwrap_or_default() == "wasm32-unknown-unknown" {
        let out = std::env::var("OUT_DIR").unwrap();
        let ar =
            std::env::var("AR_wasm32_unknown_unknown").unwrap_or_else(|_| "llvm-ar".to_string());
        let lib = format!("{out}/libsqlite3.a");
        Command::new(ar)
            .args(["crs", &lib])
            .status()
            .expect("create empty libsqlite3.a");
        println!("cargo:rustc-link-search=native={out}");
    }
}
