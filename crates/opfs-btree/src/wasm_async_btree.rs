#![cfg(target_arch = "wasm32")]
use crate::{
    BTreeError,
    async_db::{AsyncBTreeOptions, AsyncOpfsBTree},
    async_page_store::{AsyncPageStore, PageStoreCommit, PageStoreMetadata, StoredPage},
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

struct JsStore(JsValue);
async fn call(target: &JsValue, name: &str, arg: Option<JsValue>) -> Result<JsValue, BTreeError> {
    let f: js_sys::Function = js_sys::Reflect::get(target, &JsValue::from_str(name))
        .map_err(js_err)?
        .dyn_into()
        .map_err(|_| BTreeError::Io(format!("missing page-store {name}")))?;
    let p = match arg {
        Some(v) => f.call1(target, &v),
        None => f.call0(target),
    }
    .map_err(js_err)?;
    let p: js_sys::Promise = p
        .dyn_into()
        .map_err(|_| BTreeError::Io(format!("page-store {name} did not return Promise")))?;
    JsFuture::from(p).await.map_err(js_err)
}
fn js_err(e: JsValue) -> BTreeError {
    BTreeError::Io(format!("page store: {:?}", e))
}
impl AsyncPageStore for JsStore {
    async fn metadata(&mut self) -> Result<Option<PageStoreMetadata>, BTreeError> {
        serde_wasm_bindgen::from_value(call(&self.0, "metadata", None).await?)
            .map_err(|e| BTreeError::Io(e.to_string()))
    }
    async fn read_pages(&mut self, ids: &[u64]) -> Result<Vec<StoredPage>, BTreeError> {
        let arg = serde_wasm_bindgen::to_value(ids).map_err(|e| BTreeError::Io(e.to_string()))?;
        serde_wasm_bindgen::from_value(call(&self.0, "readPages", Some(arg)).await?)
            .map_err(|e| BTreeError::Io(e.to_string()))
    }
    async fn commit(&mut self, c: PageStoreCommit) -> Result<(), BTreeError> {
        let arg = serde_wasm_bindgen::to_value(&c).map_err(|e| BTreeError::Io(e.to_string()))?;
        call(&self.0, "commit", Some(arg)).await?;
        Ok(())
    }
}
#[wasm_bindgen]
pub struct WasmAsyncBTree {
    inner: AsyncOpfsBTree<JsStore>,
}
#[wasm_bindgen]
impl WasmAsyncBTree {
    #[wasm_bindgen(js_name=open)]
    pub async fn open(
        store: JsValue,
        page_size: u32,
        cache_pages: u32,
    ) -> Result<WasmAsyncBTree, JsValue> {
        Ok(Self {
            inner: AsyncOpfsBTree::open(
                JsStore(store),
                AsyncBTreeOptions {
                    page_size: page_size as usize,
                    cache_pages: cache_pages as usize,
                },
            )
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?,
        })
    }
    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), JsValue> {
        self.inner
            .put(&key, &value)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    pub async fn get(&mut self, key: Vec<u8>) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(
            &self
                .inner
                .get(&key)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?,
        )
        .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    pub async fn range(
        &mut self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    ) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(
            &self
                .inner
                .range(&start, &end, limit as usize)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?,
        )
        .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    pub async fn checkpoint(&mut self) -> Result<(), JsValue> {
        self.inner
            .checkpoint()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}
