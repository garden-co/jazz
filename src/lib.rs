#![deny(missing_docs)]
#![doc = r#"
# `wasm-tracing`

Leverages tracing to profile wasm performance via `console`.

## Usage

For the simplest out of the box set-up, you can simply set `wasm_tracing` as your default tracing Subscriber in wasm_bindgen(start)

We have this declared in our `./src/lib.rs`

```rust
use console_error_panic_hook;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn start() -> Result<(), JsValue> {
    // print pretty errors in wasm https://github.com/rustwasm/console_error_panic_hook
    // This is not needed for tracing_wasm to work, but it is a common tool for getting proper error line numbers for panics.
    console_error_panic_hook::set_once();

    wasm_tracing::set_as_global_default();

    Ok(())
}
```

## Features

| Feature | Function |
| ------------ | -------- |
| `rayon` | Logs the rayon worker thread's index within its current pool. |
| `tracing-log` | Provides complete Metadata via tracing-log's [`NormalizeEvent::normalized_metadata`](tracing_log::NormalizeEvent) method. |
"#]

use tracing::dispatcher::SetGlobalDefaultError;
use tracing_subscriber::layer::*;
use tracing_subscriber::registry::*;

use wasm_bindgen::prelude::*;

#[doc(hidden)]
mod config;
pub use config::*;

#[doc(hidden)]
mod layer;
pub use layer::*;
pub(crate) mod recorder;
/// Re-exports of common types
pub mod prelude {
    pub use super::{config::WasmLayerConfig, layer::WasmLayer};
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = performance)]
    fn mark(a: &str);
    #[wasm_bindgen(catch, js_namespace = performance)]
    fn measure(name: String, startMark: String) -> Result<(), JsValue>;
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log1(message: String);
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log4(message1: String, message2: &str, message3: &str, message4: &str);
    #[wasm_bindgen(js_namespace = console, js_name = debug)]
    fn debug1(message: String);
    #[wasm_bindgen(js_namespace = console, js_name = debug)]
    fn debug4(message1: String, message2: &str, message3: &str, message4: &str);
    #[wasm_bindgen(js_namespace = console, js_name = warn)]
    fn warn1(message: String);
    #[wasm_bindgen(js_namespace = console, js_name = warn)]
    fn warn4(message1: String, message2: &str, message3: &str, message4: &str);
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn error1(message: String);
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn error4(message1: String, message2: &str, message3: &str, message4: &str);
}

#[cfg(not(feature = "mark-with-rayon-thread-index"))]
#[inline]
fn thread_display_suffix() -> &'static str {
    ""
}
#[cfg(feature = "mark-with-rayon-thread-index")]
fn thread_display_suffix() -> String {
    let mut message = " #".to_string();
    match rayon::current_thread_index() {
        Some(idx) => message.push_str(&format!("{idx}")),
        None => message.push_str("main"),
    }
    message
}

#[cfg(not(feature = "mark-with-rayon-thread-index"))]
fn mark_name(id: &tracing::Id) -> String {
    format!("t{:x}", id.into_u64())
}
#[cfg(feature = "mark-with-rayon-thread-index")]
fn mark_name(id: &tracing::Id) -> String {
    format!(
        "t{:x}-{}",
        id.into_u64(),
        rayon::current_thread_index().unwrap_or(999)
    )
}

#[doc = r#"
Set the global default recorder with [tracing::subscriber::set_global_default]. Panics if the [WasmLayer] cannot be constructed.

Panics if a global default is already set.

## NOTE

It is discouraged by `tracing` for libraries (such at this one) to call [`tracing::subscriber::set_global_default`].
This function does so, as it is a convenience. If you are a library, please follow the advice of `tracing`.

If you would like to use multiple layers, use this code:
```rust
use tracing_subscriber::layer::*;
use tracing_subscriber::registry::*;
use wasm_tracing::prelude::*;

tracing::subscriber::set_global_default(Registry::default().with(WasmLayer::default())).unwrap();
"#]
pub fn set_as_global_default() {
    tracing::subscriber::set_global_default(
        Registry::default().with(WasmLayer::new(WasmLayerConfig::default())),
    )
    .expect("default global");
}

#[doc = r#"
Set WASM to be the default layer for a [Registry] via [tracing::subscriber::set_global_default].


## Example

```rust
use console_error_panic_hook;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn start() -> Result<(), JsValue> {
    console_error_panic_hook::set_once();

    wasm_tracing::try_set_as_global_default();

    Ok(())
}
```

## NOTE

It is discouraged by `tracing` for libraries (such at this one) to call [`tracing::subscriber::set_global_default`].
This function does so, as it is a convenience. If you are a library, please follow the advice of `tracing`.

If you would like to use multiple layers, use this code:
```rust
use tracing_subscriber::layer::*;
use tracing_subscriber::registry::*;
use wasm_tracing::prelude::*;

tracing::subscriber::set_global_default(Registry::default().with(WasmLayer::default())).unwrap();
"#]
pub fn try_set_as_global_default() -> Result<(), SetGlobalDefaultError> {
    tracing::subscriber::set_global_default(
        Registry::default().with(WasmLayer::new(WasmLayerConfig::default())),
    )
}

#[doc = r#"
Given a [`WasmLayerConfig`], set WASM to be the default layer for a [Registry].


## Example

```rust
use console_error_panic_hook;
use wasm_bindgen::prelude::*;
use wasm_tracing::prelude::*;
use tracing::Level;

#[wasm_bindgen(start)]
pub fn start() -> Result<(), JsValue> {
    console_error_panic_hook::set_once();

    let config = WasmLayerConfig::new().remove_timings().with_max_level(Level::ERROR);

    let _ = wasm_tracing::set_as_global_default_with_config(config);

    Ok(())
}
```

## NOTE

It is discouraged by `tracing` for libraries (such at this one) to call [`tracing::subscriber::set_global_default`].
This function does so, as it is a convenience. If you are a library, please follow the advice of `tracing`.

If you would like to use multiple layers, use this code:
```rust
use tracing_subscriber::layer::*;
use tracing_subscriber::registry::*;
use wasm_tracing::prelude::*;

tracing::subscriber::set_global_default(Registry::default().with(WasmLayer::default())).unwrap();
```
"#]
pub fn set_as_global_default_with_config(
    config: WasmLayerConfig,
) -> Result<(), SetGlobalDefaultError> {
    tracing::subscriber::set_global_default(Registry::default().with(WasmLayer::new(config)))
}
