use crate::browser_worker::{
    BrowserRuntimeWorker, BrowserStorageStats, RuntimeRequestId, RuntimeWorkerInput,
    RuntimeWorkerOutput, WorkerSyncProfile,
};
use crate::worker_codec::JsonCodec;
use gloo_worker::{Spawnable, WorkerBridge};
use js_sys::Date;
use mini_jazz_sqlite::{
    sync::Bundle, BuiltQuery, RowView, RowsSubscription, Runtime, Storage, StorageStats,
    SubscriptionDelta,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::{cell::RefCell, rc::Rc};
use yew::Callback;

pub type SubscriptionId = u64;

#[derive(Clone, Debug)]
pub struct BrowserRuntimeConfig {
    pub db_name: String,
    pub main_node_id: String,
    pub worker_node_id: String,
    pub user: String,
    pub hydrate_queries: Vec<BuiltQuery>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrowserRuntimeStatus {
    pub ready: bool,
    pub syncing: bool,
    pub error: String,
    pub worker_storage_stats: BrowserStorageStats,
    pub last_sync: BrowserSyncProfile,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrowserSyncProfile {
    pub main_export_ms: f64,
    pub main_subscription_ms: f64,
    pub worker_apply_ms: f64,
    pub worker_query_ms: f64,
    pub worker_export_ms: f64,
    pub round_trip_ms: f64,
}

#[derive(Clone)]
pub struct BrowserRuntime {
    inner: Rc<RefCell<Inner>>,
}

struct Inner {
    main: Runtime,
    worker: WorkerBridge<BrowserRuntimeWorker>,
    subscriptions: BTreeMap<SubscriptionId, BrowserRowsSubscription>,
    next_subscription_id: SubscriptionId,
    next_request_id: RuntimeRequestId,
    pending_syncs: BTreeMap<RuntimeRequestId, PendingSync>,
    status: BrowserRuntimeStatus,
    on_status: Callback<BrowserRuntimeStatus>,
}

struct BrowserRowsSubscription {
    query: BuiltQuery,
    subscription: RowsSubscription,
    callback: Callback<SubscriptionDelta>,
}

struct PendingSync {
    started_at_ms: f64,
    main_export_ms: f64,
}

type PendingSubscriptionNotification = (Callback<SubscriptionDelta>, SubscriptionDelta);

impl BrowserRuntime {
    pub fn open(
        config: BrowserRuntimeConfig,
        on_status: Callback<BrowserRuntimeStatus>,
    ) -> Result<Self, String> {
        let main = Runtime::open(Storage::Memory, &config.main_node_id, &config.user)
            .map_err(error_message)?;
        let runtime_slot = Rc::new(RefCell::new(None::<BrowserRuntime>));
        let mut spawner = BrowserRuntimeWorker::spawner().encoding::<JsonCodec>();
        let worker = spawner
            .callback({
                let runtime_slot = runtime_slot.clone();
                move |output| {
                    if let Some(runtime) = runtime_slot.borrow().as_ref() {
                        runtime.handle_worker_output(output);
                    }
                }
            })
            .spawn_with_loader("./worker_loader.js?v=generic-runtime");

        let runtime = Self {
            inner: Rc::new(RefCell::new(Inner {
                main,
                worker,
                subscriptions: BTreeMap::new(),
                next_subscription_id: 0,
                next_request_id: 0,
                pending_syncs: BTreeMap::new(),
                status: BrowserRuntimeStatus::default(),
                on_status,
            })),
        };
        *runtime_slot.borrow_mut() = Some(runtime.clone());

        runtime.with_inner(|inner| {
            inner.worker.send(RuntimeWorkerInput::Open {
                db_name: config.db_name,
                node_id: config.worker_node_id,
                user: config.user,
                hydrate_queries: config.hydrate_queries,
            });
            Ok(())
        })?;

        Ok(runtime)
    }

    pub fn insert_row(
        &self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, Value>,
    ) -> Result<String, String> {
        self.with_inner(|inner| {
            inner
                .main
                .insert_row(table_name, id, values)
                .map_err(error_message)
        })
    }

    pub fn update_row(
        &self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, Value>,
    ) -> Result<String, String> {
        self.with_inner(|inner| {
            inner
                .main
                .update_row(table_name, id, values)
                .map_err(error_message)
        })
    }

    pub fn delete_row(&self, table_name: &str, id: &str) -> Result<String, String> {
        self.with_inner(|inner| inner.main.delete_row(table_name, id).map_err(error_message))
    }

    pub fn query(&self, query: BuiltQuery) -> Result<Vec<RowView>, String> {
        self.with_inner(|inner| inner.main.query(query).map_err(error_message))
    }

    pub fn subscribe(
        &self,
        query: BuiltQuery,
        callback: Callback<SubscriptionDelta>,
    ) -> Result<SubscriptionId, String> {
        let (id, initial, callback) = self.with_inner(|inner| {
            let subscription = inner
                .main
                .subscribe_query(query.clone())
                .map_err(error_message)?;
            let initial = subscription.initial_delta();
            let id = inner.next_subscription_id;
            inner.next_subscription_id = inner
                .next_subscription_id
                .checked_add(1)
                .ok_or_else(|| "subscription id overflow".to_owned())?;
            inner.subscriptions.insert(
                id,
                BrowserRowsSubscription {
                    query,
                    subscription,
                    callback: callback.clone(),
                },
            );
            Ok((id, initial, callback))
        })?;
        callback.emit(initial);
        Ok(id)
    }

    pub fn unsubscribe(&self, id: SubscriptionId) {
        let _ = self.with_inner(|inner| {
            inner.subscriptions.remove(&id);
            Ok(())
        });
    }

    pub fn sync_queries(&self, queries: Vec<BuiltQuery>) -> Result<(), String> {
        let result = self.with_inner(|inner| inner.sync_queries(queries));
        if let Err(error) = &result {
            self.set_error(error.clone());
        } else {
            self.emit_status();
        }
        result
    }

    pub fn storage_stats(&self) -> Result<StorageStats, String> {
        self.with_inner(|inner| inner.main.storage_stats().map_err(error_message))
    }

    pub fn status(&self) -> BrowserRuntimeStatus {
        self.inner.borrow().status.clone()
    }

    fn handle_worker_output(&self, output: RuntimeWorkerOutput) {
        let result = match output {
            RuntimeWorkerOutput::Opened {
                bundles,
                storage_stats,
            } => self.apply_opened(bundles, storage_stats),
            RuntimeWorkerOutput::Applied {
                request_id,
                bundles,
                profile,
                storage_stats,
            } => self.apply_synced(request_id, bundles, profile, storage_stats),
            RuntimeWorkerOutput::Error { message, .. } => Err(message),
            RuntimeWorkerOutput::Exported { .. }
            | RuntimeWorkerOutput::QueryResult { .. }
            | RuntimeWorkerOutput::StorageStats { .. } => Ok(Vec::new()),
        };

        match result {
            Ok(notifications) => {
                for (callback, delta) in notifications {
                    callback.emit(delta);
                }
                self.emit_status();
            }
            Err(error) => self.set_error(error),
        }
    }

    fn apply_opened(
        &self,
        bundles: Vec<Bundle>,
        storage_stats: BrowserStorageStats,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            for bundle in bundles {
                inner.main.apply_bundle(&bundle).map_err(error_message)?;
            }
            let (notifications, main_subscription_ms) = inner.refresh_subscriptions()?;
            inner.status.ready = true;
            inner.status.syncing = !inner.pending_syncs.is_empty();
            inner.status.error.clear();
            inner.status.worker_storage_stats = storage_stats;
            inner.status.last_sync.main_subscription_ms = main_subscription_ms;
            Ok(notifications)
        })
    }

    fn apply_synced(
        &self,
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
        worker_profile: WorkerSyncProfile,
        storage_stats: BrowserStorageStats,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            let pending = inner.pending_syncs.remove(&request_id);
            for bundle in bundles {
                inner.main.apply_bundle(&bundle).map_err(error_message)?;
            }
            let (notifications, main_subscription_ms) = inner.refresh_subscriptions()?;
            if let Some(pending) = pending {
                inner.status.last_sync.main_export_ms = pending.main_export_ms;
                inner.status.last_sync.round_trip_ms = Date::now() - pending.started_at_ms;
            }
            inner.status.last_sync.main_subscription_ms = main_subscription_ms;
            inner.status.last_sync.worker_apply_ms = worker_profile.apply_ms;
            inner.status.last_sync.worker_query_ms = worker_profile.refresh_query_ms;
            inner.status.last_sync.worker_export_ms = worker_profile.refresh_export_ms;
            inner.status.worker_storage_stats = storage_stats;
            inner.status.syncing = !inner.pending_syncs.is_empty();
            inner.status.error.clear();
            Ok(notifications)
        })
    }

    fn with_inner<T>(&self, f: impl FnOnce(&mut Inner) -> Result<T, String>) -> Result<T, String> {
        f(&mut self.inner.borrow_mut())
    }

    fn emit_status(&self) {
        let (callback, status) = {
            let inner = self.inner.borrow();
            (inner.on_status.clone(), inner.status.clone())
        };
        callback.emit(status);
    }

    fn set_error(&self, error: String) {
        {
            let mut inner = self.inner.borrow_mut();
            inner.status.syncing = false;
            inner.status.error = error;
            inner.pending_syncs.clear();
        }
        self.emit_status();
    }
}

impl Inner {
    fn sync_queries(&mut self, queries: Vec<BuiltQuery>) -> Result<(), String> {
        if queries.is_empty() {
            return Ok(());
        }

        let export_started_at = Date::now();
        let bundles = queries
            .into_iter()
            .map(|query| self.main.export_query(query).map_err(error_message))
            .collect::<Result<Vec<_>, _>>()?;
        let main_export_ms = Date::now() - export_started_at;
        let refresh_queries = self.subscription_queries();
        let request_id = self.next_request_id()?;

        self.pending_syncs.insert(
            request_id,
            PendingSync {
                started_at_ms: Date::now(),
                main_export_ms,
            },
        );
        self.status.syncing = true;
        self.status.last_sync.main_export_ms = main_export_ms;
        self.worker.send(RuntimeWorkerInput::ApplyBundles {
            request_id,
            bundles,
            refresh_queries,
        });
        Ok(())
    }

    fn next_request_id(&mut self) -> Result<RuntimeRequestId, String> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| "request id overflow".to_owned())?;
        Ok(request_id)
    }

    fn subscription_queries(&self) -> Vec<BuiltQuery> {
        self.subscriptions
            .values()
            .map(|entry| entry.query.clone())
            .collect()
    }

    fn refresh_subscriptions(
        &mut self,
    ) -> Result<(Vec<PendingSubscriptionNotification>, f64), String> {
        let started_at = Date::now();
        let ids = self
            .subscriptions
            .keys()
            .copied()
            .collect::<BTreeSet<SubscriptionId>>();
        let mut notifications = Vec::new();

        for id in ids {
            let Some(entry) = self.subscriptions.get(&id) else {
                continue;
            };
            let mut next_subscription = entry.subscription.clone();
            let delta = self
                .main
                .subscription_delta(&mut next_subscription)
                .map_err(error_message)?;
            if let Some(entry) = self.subscriptions.get_mut(&id) {
                entry.subscription = next_subscription;
                if !delta.delta.is_empty() {
                    notifications.push((entry.callback.clone(), delta));
                }
            }
        }

        Ok((notifications, Date::now() - started_at))
    }
}

fn error_message(error: mini_jazz_sqlite::Error) -> String {
    error.to_string()
}
