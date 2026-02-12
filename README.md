# `wasm-tracing`

A maintained fork of [`tracing-wasm`](https://github.com/old-storyai/tracing-wasm) that is compatible with the latest `tracing` and `wasm-bindgen` versions.

Leverage performance profiling with your browser tools with the [tracing crate](https://crates.io/crates/tracing).

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MIT licensed][mit-badge]][mit-url]
[![APACHE licensed][apache-2-badge]][apache-2-url]

[crates-badge]: https://img.shields.io/crates/v/wasm-tracing.svg
[crates-url]: https://crates.io/crates/wasm-tracing
[docs-badge]: https://docs.rs/wasm-tracing/badge.svg
[docs-url]: https://docs.rs/wasm-tracing
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE-MIT
[apache-2-badge]: https://img.shields.io/badge/license-APACHE%202.0-blue.svg
[apache-2-url]: LICENSE-APACHE

Note: `wasm_tracing` uses the global JavaScript `console` and `performance` objects. It will not work in environments where one or both of these are not available, such as Node.js or Cloudflare Workers.


## TODOs
These features are things I'd like to do with `wasm-tracing`. Suggestions, comments, and complaints are encouraged!
Please file an issue if you are passionate about a particular use-case or implementation detail.
- [ ] Traces from `#[tracing::instrument]` macro should be visible in the console
- [ ] Rewrite how the config works, possibly dependent/mirrored by another `tracing` crates' config w/ console specifics

## Usage

For the simplest out of the box set-up, you can simply set `wasm_tracing` as your default tracing Subscriber in wasm_bindgen(start)

We have this declared in our `./src/lib.rs`

```rust
#[wasm_bindgen(start)]
pub fn start() -> Result<(), JsValue> {
    // print pretty errors in wasm https://github.com/rustwasm/console_error_panic_hook
    // This is not needed for tracing_wasm to work, but it is a common tool for getting proper error line numbers for panics.
    console_error_panic_hook::set_once();

    // Add this line:
    wasm_tracing::set_as_global_default();

    Ok(())
}
```
