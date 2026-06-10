#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

mod bundle;
mod format;

#[cfg(target_arch = "wasm32")]
mod app;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub fn run() {
    yew::Renderer::<app::App>::new().render();
}
