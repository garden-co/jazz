fn main() {
    // Match jazz-wasm's shadow stack; this wrapper is the final cdylib.
    println!("cargo:rustc-link-arg-cdylib=-zstack-size=8388608");
}
