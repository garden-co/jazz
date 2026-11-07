#![deny(missing_docs)]
#![doc = r#"
# `wasm-tracing`

Leverages tracing to profile wasm performance via `console`.

## Usage

```rust
use wasm_tracing::prelude::*;
use tracing::Level;

let config = WasmLayerConfig::new().with_max_level(Level::ERROR);
let _ = wasm_tracing::set_as_global_default_with_config(config);
```
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
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log4(message1: String, message2: &str, message3: &str, message4: &str);
}

/// Given a [`WasmLayerConfig`], set WASM to be the default layer for a [Registry]
/// via [`tracing::subscriber::set_global_default`].
pub fn set_as_global_default_with_config(
    config: WasmLayerConfig,
) -> Result<(), SetGlobalDefaultError> {
    tracing::subscriber::set_global_default(Registry::default().with(WasmLayer::new(config)))
}
