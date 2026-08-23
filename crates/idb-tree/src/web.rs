use js_sys::{Array, Promise, Reflect, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{BoxFuture, Commit, Metadata, PageStore};

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
    #[wasm_bindgen(typescript_type = "IndexedDbPageStore")]
    type IndexedDbPageStoreHandle;

    #[wasm_bindgen(method, js_name = metadata)]
    fn metadata_js(this: &IndexedDbPageStoreHandle) -> Promise;

    #[wasm_bindgen(method, js_name = readPage)]
    fn read_page_js(this: &IndexedDbPageStoreHandle, page_id: f64) -> Promise;

    #[wasm_bindgen(method, js_name = commitPages)]
    fn commit_pages_js(
        this: &IndexedDbPageStoreHandle,
        expected_generation: f64,
        page_size: u32,
        root_page_id: f64,
        next_page_id: f64,
        page_ids: &Array,
        page_bytes: &Array,
        deleted_page_ids: &Array,
    ) -> Promise;
}

/// Wasm adapter over the TypeScript IndexedDB page store. Page bodies cross
/// the boundary as Uint8Array values; metadata alone uses named JS fields.
#[derive(Clone)]
pub struct IndexedDbPageStore {
    handle: IndexedDbPageStoreHandle,
}

impl IndexedDbPageStore {
    pub fn from_js(handle: JsValue) -> Self {
        Self {
            handle: handle.unchecked_into(),
        }
    }
}

impl PageStore for IndexedDbPageStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        Box::pin(async move {
            let value = JsFuture::from(self.handle.metadata_js())
                .await
                .map_err(js_error)?;
            if value.is_null() || value.is_undefined() {
                return Ok(None);
            }
            Ok(Some(metadata_from_js(&value)?))
        })
    }

    fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            let value = JsFuture::from(self.handle.read_page_js(page_id_to_f64(page_id)?))
                .await
                .map_err(js_error)?;
            if value.is_null() || value.is_undefined() {
                return Ok(None);
            }
            Ok(Some(Uint8Array::new(&value).to_vec()))
        })
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        Box::pin(async move {
            let page_ids = Array::new();
            let page_bytes = Array::new();
            for (page_id, bytes) in &commit.pages {
                page_ids.push(&JsValue::from_f64(page_id_to_f64(*page_id)?));
                page_bytes.push(&Uint8Array::from(bytes.as_slice()));
            }
            let deleted_page_ids = Array::new();
            for page_id in &commit.deleted_page_ids {
                deleted_page_ids.push(&JsValue::from_f64(page_id_to_f64(*page_id)?));
            }
            let root_page_id = match commit.metadata.root_page_id {
                Some(page_id) => page_id_to_f64(page_id)?,
                None => -1.0,
            };
            let value = JsFuture::from(
                self.handle.commit_pages_js(
                    page_id_to_f64(commit.expected_generation)?,
                    u32::try_from(commit.metadata.page_size)
                        .map_err(|_| "page size does not fit u32".to_owned())?,
                    root_page_id,
                    page_id_to_f64(commit.metadata.next_page_id)?,
                    &page_ids,
                    &page_bytes,
                    &deleted_page_ids,
                ),
            )
            .await
            .map_err(js_error)?;
            metadata_from_js(&value)
        })
    }
}

fn metadata_from_js(value: &JsValue) -> Result<Metadata, String> {
    Ok(Metadata {
        page_size: usize::try_from(integer_field(value, "pageSize")?)
            .map_err(|_| "pageSize does not fit usize".to_owned())?,
        generation: integer_field(value, "generation")?,
        root_page_id: nullable_integer_field(value, "rootPageId")?,
        next_page_id: integer_field(value, "nextPageId")?,
    })
}

fn nullable_integer_field(value: &JsValue, name: &str) -> Result<Option<u64>, String> {
    let field = Reflect::get(value, &JsValue::from_str(name)).map_err(js_error)?;
    if field.is_null() || field.is_undefined() {
        Ok(None)
    } else {
        number_to_page_id(field.as_f64(), name).map(Some)
    }
}

fn integer_field(value: &JsValue, name: &str) -> Result<u64, String> {
    let field = Reflect::get(value, &JsValue::from_str(name)).map_err(js_error)?;
    number_to_page_id(field.as_f64(), name)
}

fn number_to_page_id(value: Option<f64>, name: &str) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("{name} is not a number"))?;
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > 9_007_199_254_740_991.0
    {
        return Err(format!("{name} is not a safe non-negative integer"));
    }
    Ok(value as u64)
}

fn page_id_to_f64(value: u64) -> Result<f64, String> {
    if value > 9_007_199_254_740_991 {
        return Err("IDBTree page id exceeds JavaScript's safe integer range".to_owned());
    }
    Ok(value as f64)
}

fn js_error(value: JsValue) -> String {
    value
        .dyn_ref::<js_sys::Error>()
        .map(|error| error.message().into())
        .unwrap_or_else(|| format!("{value:?}"))
}
