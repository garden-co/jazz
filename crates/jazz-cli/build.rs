fn main() {
    let linux = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux");

    // `heap_profiling`: this build samples its heap and serves the profile.
    // Flip the `heap-profiling` feature to leave plain mimalloc instead.
    println!("cargo::rustc-check-cfg=cfg(heap_profiling)");
    if linux && std::env::var_os("CARGO_FEATURE_HEAP_PROFILING").is_some() {
        println!("cargo::rustc-cfg=heap_profiling");
    }

    // Heap and CPU profiles identify the server binary by its GNU build ID,
    // so that they can be symbolized offline against the unstripped release
    // binary. Not every Linux linker emits one by default (zig's does not).
    if linux {
        println!("cargo::rustc-link-arg-bin=jazz-tools=-Wl,--build-id=sha1");
    }
}
