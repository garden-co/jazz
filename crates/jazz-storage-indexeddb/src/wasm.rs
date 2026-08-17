use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::lock::Mutex;
use groove::storage::async_ordered::{
    OrderedKvStorage, OwnedScanBounds, OwnedScanRequest, OwnedStorageOperation,
    OwnedStorageRequest, OwnedStorageResponse, ScanDirection, StorageRequestId,
};
use groove::storage::{Error, OwnedWriteOperation, apply_storage_delta};
use groove::{
    db::{Database, DemandDrivenDatabase, GraphBuilder, PollableDatabase},
    records::Value,
    schema::{ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema},
    storage::MemoryStorage,
};
use jazz::db::doctest_support;
use jazz::db::{LocalUpdates, Propagation, ReadOpts, SubscriptionEvent};
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{AuthorityPersistenceScheduler, MergeableCommit, NodeState, PollableNodeOpen};
use jazz::protocol::SyncMessage;
use jazz::tx::DurabilityTier;
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

impl OrderedKvStorage for IndexedDbOrderedStorage {
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        if matches!(
            request.operation(),
            OwnedStorageOperation::EnsureColumnFamilies(_)
        ) {
            // IndexedDB stores column-family identity in the ordered key
            // prefix; no physical namespace creation is required.
            return Poll::Ready(Ok(OwnedStorageResponse::ColumnFamiliesReady));
        }
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
                match requests.remove(&request_id) {
                    Some(RequestState::Running(waker)) => {
                        requests.insert(request_id, RequestState::Complete(result));
                        Some(waker)
                    }
                    // Cancellation terminalized this identity. The backend
                    // transaction may still finish, but its stale completion
                    // cannot enter a replacement request.
                    _ => None,
                }
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        });
        Poll::Pending
    }

    fn cancel_request(&mut self, request: StorageRequestId) -> Result<(), Error> {
        match self.requests.borrow_mut().remove(&request) {
            Some(RequestState::Running(_)) => Err(Error::Backend {
                backend: "indexeddb-pages",
                message: "IndexedDB request cancellation has an ambiguous durable outcome"
                    .to_owned(),
            }),
            Some(RequestState::Complete(_)) | None => Ok(()),
        }
    }
}

async fn execute(
    tree: &mut AsyncPageBTree<JsPageStore>,
    operation: OwnedStorageOperation,
) -> Result<OwnedStorageResponse, Error> {
    match operation {
        OwnedStorageOperation::EnsureColumnFamilies(_) => {
            Ok(OwnedStorageResponse::ColumnFamiliesReady)
        }
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

/// Prove Groove's immediate-local contract while the real IndexedDB commit is
/// suspended, then verify the encoded batch reached the backing tree.
#[wasm_bindgen]
pub async fn verify_indexeddb_groove_visibility(page_store: JsValue) -> Result<JsValue, JsValue> {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let resident = Database::new(schema, MemoryStorage::new(&["rows", "indices"]))
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let persistence = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 3)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut database = PollableDatabase::new(resident, Box::new(persistence));
    let subscription = database
        .resident_mut()
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if !subscription
        .recv()
        .map_err(|error| JsValue::from_str(&error.to_string()))?
        .is_empty()
    {
        return Err(JsValue::from_str(
            "Groove opening was unexpectedly nonempty",
        ));
    }

    let mut batch = database.resident().open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("controlled input".into())],
    );
    database
        .commit_batch(batch)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    let local_delta = subscription
        .recv()
        .map_err(|error| JsValue::from_str(&error.to_string()))?
        .to_values()
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let local_rows = database
        .resident()
        .primary_key_scan("rows", &[Value::U64(1)])
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if local_delta.len() != 1 || local_rows.len() != 1 {
        return Err(JsValue::from_str(
            "Groove local delta and one-shot were not visible synchronously",
        ));
    }

    let mut context = Context::from_waker(Waker::noop());
    if !database.poll_persistence(&mut context).is_pending() {
        return Err(JsValue::from_str(
            "real IndexedDB persistence unexpectedly completed on its first poll",
        ));
    }
    futures::future::poll_fn(|context| database.poll_persistence(context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    drop(database);

    let mut reopened = IndexedDbOrderedStorage::open(page_store, 4096, 3)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let scan = OwnedStorageRequest::new(OwnedStorageOperation::Scan(OwnedScanRequest::prefix(
        "rows",
        Vec::new(),
    )));
    let OwnedStorageResponse::Rows(durable_rows) = complete_request(&mut reopened, &scan).await?
    else {
        return Err(JsValue::from_str(
            "durable Groove scan returned wrong response",
        ));
    };
    if durable_rows.len() != 1 {
        return Err(JsValue::from_str(
            "Groove resident batch did not survive IndexedDB reopen",
        ));
    }
    Ok(JsValue::from_str(
        "Groove local visibility preceded IndexedDB durability",
    ))
}

/// Exercise the application-facing Jazz invariant while the same write's
/// captured Groove batches are persisted by real IndexedDB.
#[wasm_bindgen]
pub async fn verify_indexeddb_jazz_visibility(page_store: JsValue) -> Result<JsValue, JsValue> {
    let db = doctest_support::open_todos_db()
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    db.enable_async_persistence_capture();
    let prepared = db
        .prepare_query(&db.table("todos"))
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let opts = ReadOpts {
        tier: DurabilityTier::None,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    };
    let mut subscription = db
        .subscribe(&prepared, opts.clone())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    subscription
        .next_event()
        .await
        .ok_or_else(|| JsValue::from_str("Jazz subscription closed during opening"))?;

    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("controlled input", false),
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let Some(SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event() else {
        return Err(JsValue::from_str(
            "Jazz immediate subscription delta was not queued inside insert",
        ));
    };
    let rows = db
        .all(&prepared, opts)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if added.len() != 1 || rows.len() != 1 || rows[0].row_uuid() != write.row_uuid() {
        return Err(JsValue::from_str(
            "Jazz immediate one-shot did not observe the local write",
        ));
    }
    if write.wait(DurabilityTier::Local).await.is_ok() {
        return Err(JsValue::from_str(
            "Jazz Local durability resolved before IndexedDB persistence",
        ));
    }

    let batches = db.take_pending_persistence_batches();
    if batches.is_empty() {
        return Err(JsValue::from_str("Jazz emitted no persistence batches"));
    }
    let mut persistence = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 32)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    for batch in batches {
        let request =
            OwnedStorageRequest::new(OwnedStorageOperation::Commit(batch.into_operations()));
        complete_request(&mut persistence, &request).await?;
    }
    drop(persistence);

    let mut reopened = IndexedDbOrderedStorage::open(page_store, 4096, 32)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let transaction_scan = OwnedStorageRequest::new(OwnedStorageOperation::Scan(
        OwnedScanRequest::prefix("jazz_transactions", Vec::new()),
    ));
    let OwnedStorageResponse::Rows(transactions) =
        complete_request(&mut reopened, &transaction_scan).await?
    else {
        return Err(JsValue::from_str(
            "Jazz durable scan returned wrong response",
        ));
    };
    if transactions.is_empty() {
        return Err(JsValue::from_str(
            "Jazz canonical transaction did not survive IndexedDB reopen",
        ));
    }
    Ok(JsValue::from_str(
        "Jazz immediate visibility preceded IndexedDB durability",
    ))
}

/// Prove that an authority Fate remains quarantined through a genuinely
/// pending IndexedDB commit and is released after the complete durable unit.
#[wasm_bindgen]
pub async fn verify_indexeddb_authority_publication(
    page_store: JsValue,
) -> Result<JsValue, JsValue> {
    let schema = doctest_support::schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut writer = NodeState::new(
        NodeUuid::from_bytes([0x31; 16]),
        schema.clone(),
        MemoryStorage::new(&refs),
    )
    .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let (_, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", RowUuid::from_bytes([0x32; 16]), 10)
                .cells(doctest_support::todo_cells("authority", false)),
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        return Err(JsValue::from_str("writer did not produce a commit unit"));
    };
    let tx_id = tx.tx_id;
    let mut authority = NodeState::new(
        NodeUuid::from_bytes([0x33; 16]),
        schema,
        MemoryStorage::new(&refs),
    )
    .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let pending = authority
        .ingest_commit_unit_for_async_persistence(tx, versions, u64::MAX - 60_000, None)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let persistence = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 32)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut scheduler = AuthorityPersistenceScheduler::new(Box::new(persistence));
    scheduler.enqueue(pending);
    let mut context = Context::from_waker(Waker::noop());
    if !scheduler.poll(&mut context).is_pending() {
        return Err(JsValue::from_str(
            "authority Fate escaped before IndexedDB suspended",
        ));
    }
    let responses = futures::future::poll_fn(|context| scheduler.poll(context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if !matches!(
        responses.as_slice(),
        [SyncMessage::FateUpdate {
            tx_id: response_tx,
            durability: Some(DurabilityTier::Global),
            ..
        }] if *response_tx == tx_id
    ) {
        return Err(JsValue::from_str(
            "authority did not release the exact durable Fate",
        ));
    }
    drop(scheduler);

    let mut reopened = IndexedDbOrderedStorage::open(page_store, 4096, 32)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let transaction_scan = OwnedStorageRequest::new(OwnedStorageOperation::Scan(
        OwnedScanRequest::prefix("jazz_transactions", Vec::new()),
    ));
    let OwnedStorageResponse::Rows(transactions) =
        complete_request(&mut reopened, &transaction_scan).await?
    else {
        return Err(JsValue::from_str(
            "authority durable scan returned wrong response",
        ));
    };
    if transactions.is_empty() {
        return Err(JsValue::from_str(
            "authority Fate was released without durable transaction state",
        ));
    }
    Ok(JsValue::from_str(
        "Jazz authority Fate followed IndexedDB durability",
    ))
}

/// Exercise query-driven IndexedDB acquisition followed by a synchronous
/// resident write and asynchronously durable persistence of that write.
#[wasm_bindgen]
pub async fn verify_indexeddb_demand_loading(page_store: JsValue) -> Result<JsValue, JsValue> {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut seed = Database::new(schema.clone(), MemoryStorage::new(&["rows", "indices"]))
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut seed_batch = seed.open_batch();
    seed_batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("durable seed".into())],
    );
    let seed_persistence = seed
        .commit_batch_for_async_persistence(seed_batch)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut durable = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 8)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let seed_request = OwnedStorageRequest::new(OwnedStorageOperation::Commit(
        seed_persistence.into_operations(),
    ));
    complete_request(&mut durable, &seed_request).await?;
    drop(durable);

    let durable = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 8)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut database = DemandDrivenDatabase::new(schema, Box::new(durable))
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut context = Context::from_waker(Waker::noop());
    let mut graph = Some(GraphBuilder::table("rows"));
    if !database
        .poll_subscribe_one_sink(&mut context, &mut graph)
        .is_pending()
    {
        return Err(JsValue::from_str(
            "cold query-driven IndexedDB subscription did not suspend",
        ));
    }
    let subscription =
        futures::future::poll_fn(|context| database.poll_subscribe_one_sink(context, &mut graph))
            .await
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if subscription
        .recv()
        .map_err(|error| JsValue::from_str(&error.to_string()))?
        .to_values()
        .map_err(|error| JsValue::from_str(&error.to_string()))?
        .len()
        != 1
    {
        return Err(JsValue::from_str(
            "opened subscription did not use resident query state",
        ));
    }
    let Poll::Ready(Ok(rows)) = database.poll_read(&mut context, |database| {
        database.primary_key_scan("rows", &[])
    }) else {
        return Err(JsValue::from_str(
            "one-shot after subscription hydration was not resident",
        ));
    };
    if rows.len() != 1 {
        return Err(JsValue::from_str("demand-loaded query missed durable seed"));
    }

    let mut batch = Some(database.resident().open_batch());
    batch.as_mut().unwrap().insert(
        "rows",
        vec![Value::U64(2), Value::String("local input".into())],
    );
    let Poll::Ready(Ok(persistence)) = database.poll_commit_batch(&mut context, &mut batch) else {
        return Err(JsValue::from_str(
            "opened working set did not make local write first-poll ready",
        ));
    };
    let delta = subscription
        .recv()
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if delta
        .to_values()
        .map_err(|error| JsValue::from_str(&error.to_string()))?
        .len()
        != 1
        || database
            .resident()
            .primary_key_scan("rows", &[Value::U64(2)])
            .map_err(|error| JsValue::from_str(&error.to_string()))?
            .len()
            != 1
    {
        return Err(JsValue::from_str(
            "resident callback and one-shot were not synchronous",
        ));
    }
    database.enqueue_persistence(persistence);
    if !database.poll_persistence(&mut context).is_pending() {
        return Err(JsValue::from_str(
            "real IndexedDB write unexpectedly completed in its first poll",
        ));
    }
    futures::future::poll_fn(|context| database.poll_persistence(context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    drop(database);

    let mut reopened = IndexedDbOrderedStorage::open(page_store, 4096, 8)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let scan = OwnedStorageRequest::new(OwnedStorageOperation::Scan(OwnedScanRequest::prefix(
        "rows",
        Vec::new(),
    )));
    let OwnedStorageResponse::Rows(rows) = complete_request(&mut reopened, &scan).await? else {
        return Err(JsValue::from_str("reopen returned wrong response"));
    };
    if rows.len() != 2 {
        return Err(JsValue::from_str(
            "resident write did not survive IndexedDB persistence",
        ));
    }
    Ok(JsValue::from_str(
        "IndexedDB demand loading preserved synchronous resident writes",
    ))
}

/// Open the full Jazz node over IndexedDB, preserve synchronous resident
/// visibility, and reopen the durably committed row through the same pollable
/// node lifecycle.
#[wasm_bindgen]
pub async fn verify_indexeddb_node_lifecycle(page_store: JsValue) -> Result<JsValue, JsValue> {
    let schema = doctest_support::schema();
    let durable = IndexedDbOrderedStorage::open(page_store.clone(), 4096, 8)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut opening = PollableNodeOpen::new(
        NodeUuid::from_bytes([0xd5; 16]),
        schema.clone(),
        Box::new(durable),
    );
    let mut context = Context::from_waker(Waker::noop());
    if !opening.poll(&mut context).is_pending() {
        return Err(JsValue::from_str(
            "cold IndexedDB Jazz node opening did not suspend",
        ));
    }
    let mut runtime = futures::future::poll_fn(|context| opening.poll(context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    drop(opening);

    let row = RowUuid::from_bytes([0xd6; 16]);
    let commit = MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
        "title".to_owned(),
        Value::String("resident IndexedDB input".to_owned()),
    )]));
    futures::future::poll_fn(|context| runtime.poll_mergeable_commit(context, &commit))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let std::task::Poll::Ready(rows) =
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::None)
    else {
        return Err(JsValue::from_str(
            "one-shot local read suspended after its direct row was published",
        ));
    };
    let rows = rows.map_err(|error| JsValue::from_str(&error.to_string()))?;
    if rows.len() != 1 || rows[0].row_uuid() != row {
        return Err(JsValue::from_str(
            "resident node did not expose its local write synchronously",
        ));
    }
    if !runtime.poll_persistence(&mut context).is_pending() {
        return Err(JsValue::from_str(
            "real IndexedDB node commit unexpectedly completed on its first poll",
        ));
    }
    futures::future::poll_fn(|context| runtime.poll_persistence(context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    drop(runtime);

    let durable = IndexedDbOrderedStorage::open(page_store, 4096, 8)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut reopening =
        PollableNodeOpen::new(NodeUuid::from_bytes([0xd5; 16]), schema, Box::new(durable));
    let mut reopened = futures::future::poll_fn(|context| reopening.poll(context))
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let rows = futures::future::poll_fn(|context| {
        reopened.poll_current_rows(context, "todos", DurabilityTier::None)
    })
    .await
    .map_err(|error| JsValue::from_str(&error.to_string()))?;
    if rows.len() != 1 || rows[0].row_uuid() != row {
        let history =
            futures::future::poll_fn(|context| reopened.poll_row_history(context, "todos", row))
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
        return Err(JsValue::from_str(&format!(
            "pollable Jazz node did not reconstruct its IndexedDB row (current={}, history={})",
            rows.len(),
            history.len()
        )));
    }
    Ok(JsValue::from_str(
        "IndexedDB node open/write/reopen preserved resident visibility",
    ))
}
