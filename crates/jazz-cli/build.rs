fn main() {
    // Heap profiles identify the server binary by its GNU build ID, so that
    // they can be symbolized offline against the unstripped release binary.
    // Not every Linux linker emits one by default (zig's does not).
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-arg-bin=jazz-tools=-Wl,--build-id=sha1");
    }
}
