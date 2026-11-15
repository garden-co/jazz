//! `cargo run -p jazz-ffi --features bindgen --bin uniffi-bindgen -- generate
//! --library <libjazz_ffi.so> --language swift|kotlin --out-dir <dir>`
fn main() {
    uniffi::uniffi_bindgen_main()
}
