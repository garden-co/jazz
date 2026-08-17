use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::lock::Mutex;
use groove::storage::pollable::{
    OwnedScanBounds, OwnedScanRequest, OwnedStorageOperation, OwnedStorageRequest,
    OwnedStorageResponse, PollableOrderedKvStorage, ScanDirection, StorageRequestId,
};
use groove::storage::{Error, OwnedWriteOperation, apply_storage_delta};
use opfs_btree::BTreeError;
use opfs_btree::async_db::{AsyncPageBTree, AsyncPageBTreeOptions};
use opfs_btree::async_page_store::{
    AsyncPageStore, PageStoreCommit, PageStoreMetadata, StoredPage,
};
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use wasm_bindgen_futures::{JsFuture, spawn_local};

struct JsPageStore(JsValue);

async fn call(
    target: &JsValue,
    name: &str,
    argument: Option<JsValue>,
) -> Result<JsValue, BTreeError> {
    let function: js_sys::Function = js_sys::Reflect::get(target, &JsValue::from_str(name))
        .map_err(js_error)?
        .dyn_into()
        .map_err(|_| BTreeError::Io(format!("missing page-store method {name}")))?;
    let promise = match argument {
        Some(argument) => function.call1(target, &argument),
        None => function.call0(target),
    }
    .map_err(js_error)?
    .dyn_into::<js_sys::Promise>()
    .map_err(|_| BTreeError::Io(format!("page-store {name} did not return a Promise")))?;
    JsFuture::from(promise).await.map_err(js_error)
}

fn js_error(value: JsValue) -> BTreeError {
    BTreeError::Io(
        value
            .as_string()
            .unwrap_or_else(|| "IndexedDB page-store exception".to_owned()),
    )
}

impl AsyncPageStore for JsPageStore {
    async fn metadata(&mut self) -> Result<Option<PageStoreMetadata>, BTreeError> {
        serde_wasm_bindgen::from_value(call(&self.0, "metadata", None).await?)
            .map_err(|error| BTreeError::Io(error.to_string()))
    }

    async fn read_pages(&mut self, page_ids: &[u64]) -> Result<Vec<StoredPage>, BTreeError> {
        let argument = serde_wasm_bindgen::to_value(page_ids)
            .map_err(|error| BTreeError::Io(error.to_string()))?;
        serde_wasm_bindgen::from_value(call(&self.0, "readPages", Some(argument)).await?)
            .map_err(|error| BTreeError::Io(error.to_string()))
    }

    async fn commit(&mut self, commit: PageStoreCommit) -> Result<(), BTreeError> {
        let argument = serde_wasm_bindgen::to_value(&commit)
            .map_err(|error| BTreeError::Io(error.to_string()))?;
        call(&self.0, "commit", Some(argument)).await?;
        Ok(())
    }
}

enum RequestState {
    Running(Waker),
    Complete(Result<OwnedStorageResponse, Error>),
}

/// One thread-affine ordered store over an async opaque-page B-tree.
pub struct IndexedDbOrderedStorage {
    tree: Rc<Mutex<AsyncPageBTree<JsPageStore>>>,
    requests: Rc<RefCell<BTreeMap<StorageRequestId, RequestState>>>,
}

impl IndexedDbOrderedStorage {
    pub async fn open(
        page_store: JsValue,
        page_size: usize,
        cache_pages: usize,
    ) -> Result<Self, Error> {
        let tree = AsyncPageBTree::open(
            JsPageStore(page_store),
            AsyncPageBTreeOptions {
                page_size,
                cache_pages,
            },
        )
        .await
        .map_err(storage_error)?;
        Ok(Self {
            tree: Rc::new(Mutex::new(tree)),
            requests: Rc::new(RefCell::new(BTreeMap::new())),
        })
    }
}

impl PollableOrderedKvStorage for IndexedDbOrderedStorage {
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        let mut requests = self.requests.borrow_mut();
        match requests.remove(&request.id()) {
            Some(RequestState::Complete(result)) => return Poll::Ready(result),
            Some(RequestState::Running(_)) => {
                requests.insert(request.id(), RequestState::Running(context.waker().clone()));
                return Poll::Pending;
            }
            None => {}
        }

        requests.insert(request.id(), RequestState::Running(context.waker().clone()));
        drop(requests);
        let operation = request.operation().clone();
        let request_id = request.id();
        let tree = Rc::clone(&self.tree);
        let requests = Rc::clone(&self.requests);
        spawn_local(async move {
            let result = execute(&mut *tree.lock().await, operation).await;
            let wake = {
                let mut requests = requests.borrow_mut();
                let wake = match requests.remove(&request_id) {
                    Some(RequestState::Running(waker)) => Some(waker),
                    _ => None,
                };
                requests.insert(request_id, RequestState::Complete(result));
                wake
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        });
        Poll::Pending
    }
}

async fn execute(
    tree: &mut AsyncPageBTree<JsPageStore>,
    operation: OwnedStorageOperation,
) -> Result<OwnedStorageResponse, Error> {
    match operation {
        OwnedStorageOperation::Get { column_family, key } => tree
            .get(&storage_key(&column_family, &key))
            .await
            .map(OwnedStorageResponse::Value)
            .map_err(storage_error),
        OwnedStorageOperation::Scan(request) => {
            let family = family_prefix(&request.column_family);
            let (start, end) = match request.bounds {
                OwnedScanBounds::Prefix(prefix) => {
                    let start = append(&family, &prefix);
                    let end = prefix_end(&start).unwrap_or_else(|| prefix_end(&family).unwrap());
                    (start, end)
                }
                OwnedScanBounds::Range { start, end } => {
                    (append(&family, &start), append(&family, &end))
                }
            };
            let mut rows = tree
                .range(&start, &end, usize::MAX)
                .await
                .map_err(storage_error)?
                .into_iter()
                .map(|(key, value)| (key[family.len()..].to_vec(), value))
                .collect::<Vec<_>>();
            if request.direction == ScanDirection::Reverse {
                rows.reverse();
            }
            Ok(OwnedStorageResponse::Rows(rows))
        }
        OwnedStorageOperation::Commit(operations) => {
            for operation in operations {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        tree.put(&storage_key(&cf, &key), &value)
                            .await
                            .map_err(storage_error)?;
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        tree.delete(&storage_key(&cf, &key))
                            .await
                            .map_err(storage_error)?;
                    }
                    OwnedWriteOperation::Delta { cf, key, delta } => {
                        let key = storage_key(&cf, &key);
                        let current = tree.get(&key).await.map_err(storage_error)?;
                        let value = apply_storage_delta(current.as_deref(), &delta.encode()?)?;
                        tree.put(&key, &value).await.map_err(storage_error)?;
                    }
                }
            }
            tree.checkpoint().await.map_err(storage_error)?;
            Ok(OwnedStorageResponse::Committed)
        }
        OwnedStorageOperation::Flush => {
            tree.checkpoint().await.map_err(storage_error)?;
            Ok(OwnedStorageResponse::Flushed)
        }
        OwnedStorageOperation::Close => Ok(OwnedStorageResponse::Closed),
    }
}

fn family_prefix(column_family: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(4 + column_family.len());
    prefix.extend_from_slice(&(column_family.len() as u32).to_be_bytes());
    prefix.extend_from_slice(column_family.as_bytes());
    prefix
}

fn storage_key(column_family: &str, key: &[u8]) -> Vec<u8> {
    append(&family_prefix(column_family), key)
}

fn append(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + suffix.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(suffix);
    key
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn storage_error(error: BTreeError) -> Error {
    Error::Backend {
        backend: "indexeddb-pages",
        message: error.to_string(),
    }
}

async fn complete_request(
    storage: &mut IndexedDbOrderedStorage,
    request: &OwnedStorageRequest,
) -> Result<OwnedStorageResponse, JsValue> {
    futures::future::poll_fn(|context| storage.poll_request(request, context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

/// Browser receipt for the real IndexedDB page adapter at the ordered-storage
/// boundary. Production construction will move into `jazz-wasm`; this export
/// keeps the experiment independently falsifiable while that integration is in
/// progress.
#[wasm_bindgen]
pub async fn verify_indexeddb_ordered_storage(page_store: JsValue) -> Result<JsValue, JsValue> {
    let mut storage = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 3)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let commit = OwnedStorageRequest::new(OwnedStorageOperation::Commit(vec![
        OwnedWriteOperation::Set {
            cf: "rows".to_owned(),
            key: b"b".to_vec(),
            value: b"two".to_vec(),
        },
        OwnedWriteOperation::Set {
            cf: "rows".to_owned(),
            key: b"a".to_vec(),
            value: b"one".to_vec(),
        },
        OwnedWriteOperation::Set {
            cf: "other".to_owned(),
            key: b"a".to_vec(),
            value: b"isolated".to_vec(),
        },
    ]));
    if complete_request(&mut storage, &commit).await? != OwnedStorageResponse::Committed {
        return Err(JsValue::from_str(
            "IndexedDB commit returned the wrong response",
        ));
    }

    let scan = OwnedStorageRequest::new(OwnedStorageOperation::Scan(OwnedScanRequest::prefix(
        "rows",
        Vec::new(),
    )));
    let OwnedStorageResponse::Rows(rows) = complete_request(&mut storage, &scan).await? else {
        return Err(JsValue::from_str(
            "IndexedDB scan returned the wrong response",
        ));
    };
    if rows
        != vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ]
    {
        return Err(JsValue::from_str("IndexedDB ordered scan was incorrect"));
    }

    let delete = OwnedStorageRequest::new(OwnedStorageOperation::Commit(vec![
        OwnedWriteOperation::Delete {
            cf: "rows".to_owned(),
            key: b"a".to_vec(),
        },
    ]));
    complete_request(&mut storage, &delete).await?;
    drop(storage);

    let mut reopened = IndexedDbOrderedStorage::open(page_store, 4096, 3)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let get = OwnedStorageRequest::new(OwnedStorageOperation::Get {
        column_family: "rows".to_owned(),
        key: b"a".to_vec(),
    });
    if complete_request(&mut reopened, &get).await? != OwnedStorageResponse::Value(None) {
        return Err(JsValue::from_str(
            "IndexedDB delete did not survive a clean reopen",
        ));
    }
    Ok(JsValue::from_str(
        "ordered IndexedDB commit/scan/delete/reopen passed",
    ))
}
