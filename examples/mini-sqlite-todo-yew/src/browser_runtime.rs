use crate::browser_worker::{
    BrowserStorageStats, RuntimeRequestId, RuntimeWorkerInput, RuntimeWorkerOutput,
};
use crate::worker_bridge::WorkerClient;
use js_sys::{Function, Promise};
use mini_jazz_sqlite::connection::{DownstreamConnectionManager, DownstreamConnectionSubscription};
use mini_jazz_sqlite::protocol::{ClientMessage, ServerMessage, SettlementTier};
use mini_jazz_sqlite::{
    sync::Bundle, BuiltQuery, RowView, RowsSubscription, Runtime, SchemaDef, Storage, StorageStats,
    SubscriptionDelta,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::{closure::Closure, prelude::*, JsCast};
use wasm_bindgen_futures::{spawn_local, JsFuture};
use yew::Callback;

pub type SubscriptionId = u64;

#[derive(Clone, Debug)]
pub struct BrowserRuntimeConfig {
    pub db_name: String,
    pub main_node_id: String,
    pub worker_node_id: String,
    pub user: String,
    pub schema: SchemaDef,
    pub hydrate_queries: Vec<BuiltQuery>,
    pub native_sync_url: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrowserRuntimeStatus {
    pub ready: bool,
    pub syncing: bool,
    pub error: String,
    pub worker_storage_stats: BrowserStorageStats,
}

#[derive(Clone)]
pub struct BrowserRuntime {
    inner: Rc<RefCell<Inner>>,
}

struct Inner {
    main: Runtime,
    worker: WorkerClient<RuntimeWorkerInput, RuntimeWorkerOutput>,
    connection_manager: DownstreamConnectionManager,
    subscriptions: BTreeMap<SubscriptionId, BrowserRowsSubscription>,
    next_subscription_id: SubscriptionId,
    next_request_id: RuntimeRequestId,
    pending_syncs: BTreeSet<RuntimeRequestId>,
    pending_hydrates: BTreeSet<RuntimeRequestId>,
    pending_protocols: BTreeSet<RuntimeRequestId>,
    status: BrowserRuntimeStatus,
    on_status: Callback<BrowserRuntimeStatus>,
}

struct BrowserRowsSubscription {
    connection_subscription: DownstreamConnectionSubscription,
    subscription: RowsSubscription,
    callback: Callback<SubscriptionDelta>,
}

type PendingSubscriptionNotification = (Callback<SubscriptionDelta>, SubscriptionDelta);

impl BrowserRuntime {
    pub fn open(
        config: BrowserRuntimeConfig,
        on_status: Callback<BrowserRuntimeStatus>,
    ) -> Result<Self, String> {
        let main = Runtime::open_with_schema(
            Storage::Memory,
            &config.main_node_id,
            &config.user,
            config.schema.clone(),
        )
        .map_err(error_message)?;
        let mut connection_manager = DownstreamConnectionManager::new(
            format!("{}-session", config.main_node_id),
            config.main_node_id.clone(),
            main.local_schema_fingerprint(),
            main.local_policy_fingerprint(),
        );
        let opening_client_messages = connection_manager.open().map_err(error_message)?;
        let runtime_slot = Rc::new(RefCell::new(None::<BrowserRuntime>));
        let worker = WorkerClient::spawn("./worker_loader.js?v=generic-runtime", {
            let runtime_slot = runtime_slot.clone();
            move |output| {
                if let Some(runtime) = runtime_slot.borrow().as_ref() {
                    runtime.handle_worker_output(output);
                }
            }
        })?;

        let runtime = Self {
            inner: Rc::new(RefCell::new(Inner {
                main,
                worker,
                connection_manager,
                subscriptions: BTreeMap::new(),
                next_subscription_id: 0,
                next_request_id: 0,
                pending_syncs: BTreeSet::new(),
                pending_hydrates: BTreeSet::new(),
                pending_protocols: BTreeSet::new(),
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
                schema: config.schema,
                hydrate_queries: config.hydrate_queries,
                client_messages: opening_client_messages,
                native_sync_url: config.native_sync_url,
            })?;
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
            let (connection_subscription, client_messages) = inner
                .connection_manager
                .subscribe(query.clone(), SettlementTier::Local)
                .map_err(error_message)?;
            inner.subscriptions.insert(
                id,
                BrowserRowsSubscription {
                    connection_subscription,
                    subscription,
                    callback: callback.clone(),
                },
            );
            inner.send_protocol_messages(client_messages)?;
            Ok((id, initial, callback))
        })?;
        callback.emit(initial);
        Ok(id)
    }

    pub fn unsubscribe(&self, id: SubscriptionId) {
        let result = self.with_inner(|inner| {
            if let Some(entry) = inner.subscriptions.remove(&id) {
                let client_messages = inner
                    .connection_manager
                    .unsubscribe(&entry.connection_subscription)
                    .map_err(error_message)?;
                inner.send_protocol_messages(client_messages)?;
            }
            Ok(())
        });
        match result {
            Ok(()) => self.emit_status(),
            Err(error) => self.set_error(error),
        }
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

    pub fn sync_queries_after_render(&self, queries: Vec<BuiltQuery>) {
        let runtime = self.clone();
        spawn_local(async move {
            let _ = JsFuture::from(next_tick()).await;
            let _ = runtime.sync_queries(queries);
        });
    }

    pub fn hydrate_query(&self, query: BuiltQuery) -> Result<(), String> {
        self.hydrate_queries(vec![query])
    }

    pub fn hydrate_queries(&self, queries: Vec<BuiltQuery>) -> Result<(), String> {
        let result = self.with_inner(|inner| inner.hydrate_queries(queries));
        if let Err(error) = &result {
            self.set_error(error.clone());
        } else {
            self.emit_status();
        }
        result
    }

    pub fn hydrate_query_after_render(&self, query: BuiltQuery) {
        self.hydrate_queries_after_render(vec![query]);
    }

    pub fn hydrate_queries_after_render(&self, queries: Vec<BuiltQuery>) {
        let runtime = self.clone();
        spawn_local(async move {
            let _ = JsFuture::from(next_tick()).await;
            let _ = runtime.hydrate_queries(queries);
        });
    }

    pub fn refresh_subscriptions(&self) -> Result<(), String> {
        let result = self.with_inner(|inner| inner.refresh_subscriptions());
        match result {
            Ok(notifications) => {
                self.emit_notifications(notifications);
                self.emit_status();
                Ok(())
            }
            Err(error) => {
                self.set_error(error.clone());
                Err(error)
            }
        }
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
                server_messages,
                storage_stats,
            } => self.apply_opened(bundles, server_messages, storage_stats),
            RuntimeWorkerOutput::Applied {
                request_id,
                server_messages,
                storage_stats,
            } => self.apply_synced(request_id, server_messages, storage_stats),
            RuntimeWorkerOutput::Protocol {
                request_id,
                server_messages,
                storage_stats,
            } => self.apply_protocol(request_id, server_messages, storage_stats),
            RuntimeWorkerOutput::Pushed {
                server_messages,
                storage_stats,
            } => self.apply_pushed(server_messages, storage_stats),
            RuntimeWorkerOutput::Exported { request_id, bundle } => {
                self.apply_hydrated(request_id, vec![bundle])
            }
            RuntimeWorkerOutput::ExportedQueries {
                request_id,
                bundles,
            } => self.apply_hydrated(request_id, bundles),
            RuntimeWorkerOutput::Error { message, .. } => Err(message),
            RuntimeWorkerOutput::QueryResult { .. } | RuntimeWorkerOutput::StorageStats { .. } => {
                Ok(Vec::new())
            }
        };

        match result {
            Ok(notifications) => {
                self.emit_notifications(notifications);
                self.emit_status();
            }
            Err(error) => self.set_error(error),
        }
    }

    fn emit_notifications(&self, notifications: Vec<PendingSubscriptionNotification>) {
        for (callback, delta) in notifications {
            callback.emit(delta);
        }
    }

    fn apply_opened(
        &self,
        bundles: Vec<Bundle>,
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            for bundle in bundles {
                inner.main.apply_bundle(&bundle).map_err(error_message)?;
            }
            let client_messages = inner.apply_protocol_messages(server_messages)?;
            inner.send_protocol_messages(client_messages)?;
            let notifications = inner.refresh_subscriptions()?;
            inner.status.ready = true;
            inner.status.syncing = inner.has_pending_work();
            inner.status.error.clear();
            inner.status.worker_storage_stats = storage_stats;
            Ok(notifications)
        })
    }

    fn apply_synced(
        &self,
        request_id: RuntimeRequestId,
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            if !inner.pending_syncs.remove(&request_id) {
                return Ok(Vec::new());
            }
            let client_messages = inner.apply_protocol_messages(server_messages)?;
            inner.send_protocol_messages(client_messages)?;
            let notifications = inner.refresh_subscriptions()?;
            inner.status.worker_storage_stats = storage_stats;
            inner.status.syncing = inner.has_pending_work();
            inner.status.error.clear();
            Ok(notifications)
        })
    }

    fn apply_protocol(
        &self,
        request_id: RuntimeRequestId,
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            if !inner.pending_protocols.remove(&request_id) {
                return Ok(Vec::new());
            }
            let client_messages = inner.apply_protocol_messages(server_messages)?;
            inner.send_protocol_messages(client_messages)?;
            let notifications = inner.refresh_subscriptions()?;
            inner.status.worker_storage_stats = storage_stats;
            inner.status.syncing = inner.has_pending_work();
            inner.status.error.clear();
            Ok(notifications)
        })
    }

    fn apply_pushed(
        &self,
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            if server_messages.is_empty() || !inner.connection_manager.is_ready() {
                return Ok(Vec::new());
            }
            let client_messages = inner.apply_protocol_messages(server_messages)?;
            inner.send_protocol_messages(client_messages)?;
            let notifications = inner.refresh_subscriptions()?;
            inner.status.worker_storage_stats = storage_stats;
            inner.status.syncing = inner.has_pending_work();
            inner.status.error.clear();
            Ok(notifications)
        })
    }

    fn apply_hydrated(
        &self,
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
    ) -> Result<Vec<PendingSubscriptionNotification>, String> {
        self.with_inner(|inner| {
            if !inner.pending_hydrates.remove(&request_id) {
                return Ok(Vec::new());
            }
            for bundle in bundles {
                inner.main.apply_bundle(&bundle).map_err(error_message)?;
            }
            let notifications = inner.refresh_subscriptions()?;
            inner.status.syncing = inner.has_pending_work();
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
            inner.pending_hydrates.clear();
            inner.pending_protocols.clear();
        }
        self.emit_status();
    }
}

impl Inner {
    fn sync_queries(&mut self, _queries: Vec<BuiltQuery>) -> Result<(), String> {
        self.ensure_ready()?;
        let client_messages = self
            .connection_manager
            .flush(&mut self.main)
            .map_err(error_message)?;
        if client_messages.is_empty() {
            return Ok(());
        }

        let request_id = self.next_request_id()?;
        self.pending_protocols.insert(request_id);
        self.status.syncing = true;
        self.worker.send(RuntimeWorkerInput::Protocol {
            request_id,
            client_messages,
        })?;
        Ok(())
    }

    fn hydrate_queries(&mut self, queries: Vec<BuiltQuery>) -> Result<(), String> {
        self.ensure_ready()?;
        if queries.is_empty() {
            return Ok(());
        }

        let request_id = self.next_request_id()?;
        self.pending_hydrates.insert(request_id);
        self.status.syncing = true;
        if queries.len() == 1 {
            let mut queries = queries;
            let query = queries.pop().expect("query exists");
            self.worker
                .send(RuntimeWorkerInput::ExportQuery { request_id, query })?;
        } else {
            self.worker.send(RuntimeWorkerInput::ExportQueries {
                request_id,
                queries,
            })?;
        }
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

    fn ensure_ready(&self) -> Result<(), String> {
        if self.status.ready {
            Ok(())
        } else {
            Err("runtime is opening".to_owned())
        }
    }

    fn apply_protocol_messages(
        &mut self,
        server_messages: Vec<ServerMessage>,
    ) -> Result<Vec<ClientMessage>, String> {
        self.connection_manager
            .receive(&mut self.main, server_messages)
            .map_err(error_message)
    }

    fn send_protocol_messages(
        &mut self,
        client_messages: Vec<ClientMessage>,
    ) -> Result<(), String> {
        if client_messages.is_empty() {
            return Ok(());
        }
        let request_id = self.next_request_id()?;
        self.pending_protocols.insert(request_id);
        self.status.syncing = true;
        self.worker.send(RuntimeWorkerInput::Protocol {
            request_id,
            client_messages,
        })?;
        Ok(())
    }

    fn has_pending_work(&self) -> bool {
        !self.pending_syncs.is_empty()
            || !self.pending_hydrates.is_empty()
            || !self.pending_protocols.is_empty()
    }

    fn refresh_subscriptions(&mut self) -> Result<Vec<PendingSubscriptionNotification>, String> {
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

        Ok(notifications)
    }
}

fn error_message(error: mini_jazz_sqlite::Error) -> String {
    error.to_string()
}

fn next_tick() -> Promise {
    Promise::new(&mut |resolve: Function, _reject: Function| {
        let timeout_resolve = resolve.clone();
        let callback = Closure::once(move || {
            let _ = timeout_resolve.call0(&JsValue::UNDEFINED);
        });
        web_sys::window()
            .expect("window is available")
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                callback.as_ref().unchecked_ref(),
                0,
            )
            .expect("schedule next tick");
        callback.forget();
    })
}
