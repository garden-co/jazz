//! Link-time settings for the jazz-wasm module.

/// Size of the WebAssembly shadow stack, in bytes.
///
/// Native builds poll database operations through `jazz_db::StackSafeFuture`,
/// which switches to a fresh 8 MiB stack segment whenever less than 4 MiB of
/// stack remains, so no single owner turn is bounded by the host thread's
/// stack. `wasm32-unknown-unknown` cannot switch stacks (`stacker` runs the
/// poll in place there), so the module's one linear-memory stack is the whole
/// budget. The linker default is 1 MiB, and an overflow does not fail
/// cleanly: the stack is placed first in memory, so the stack pointer wraps
/// below address 0 and the next frame traps with "memory access out of
/// bounds", leaving the module unusable. Unoptimized (`wasm-pack --dev`)
/// builds exceed 1 MiB on ordinary reads, for example compiling an
/// include-deleted query. Give wasm the same budget as one native segment.
const WASM_STACK_BYTES: u32 = 8 * 1024 * 1024;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32") {
        println!("cargo:rustc-link-arg-cdylib=-zstack-size={WASM_STACK_BYTES}");
    }
}
