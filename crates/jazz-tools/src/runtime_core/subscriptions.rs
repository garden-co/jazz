use super::*;
use crate::query_manager::manager::LocalUpdates;
use crate::sync_manager::QueryPropagation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadDurabilityOptions {
    pub tier: Option<DurabilityTier>,
    pub local_updates: LocalUpdates,
    pub strict_transactions: bool,
}

impl Default for ReadDurabilityOptions {
    fn default() -> Self {
        Self {
            tier: None,
            local_updates: LocalUpdates::Immediate,
            strict_transactions: false,
        }
    }
}

/// Subscription created but not yet executed (2-phase subscribe).
pub(super) struct PendingSubscription {
    pub query: Query,
    pub session: Option<Session>,
    pub durability: ReadDurabilityOptions,
    pub propagation: QueryPropagation,
}

/// Typed builder returned by [`RuntimeCore::subscribe`] — the only thing it
/// can do is have [`Self::execute`] called on it. Forgetting to call execute
/// produces a `must_use` warning at compile time; calling it twice is
/// impossible because `execute` consumes the builder.
///
/// The flat `create_subscription` / `execute_subscription` pair on
/// [`RuntimeCore`] remains for FFI bindings that need separate JS/UniFFI
/// entry points; new Rust callers should prefer the builder.
#[must_use = "PendingSubscriptionRequest must be consumed by .execute(callback) — \
              dropping it leaves a phantom subscription registered in the runtime"]
pub struct PendingSubscriptionRequest<'a, S: Storage, Sch: Scheduler> {
    runtime: &'a mut RuntimeCore<S, Sch>,
    handle: SubscriptionHandle,
}

impl<'a, S: Storage, Sch: Scheduler> PendingSubscriptionRequest<'a, S, Sch> {
    /// Execute the pending subscription with `callback` and return its
    /// stable [`SubscriptionHandle`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn execute<F>(self, callback: F) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.runtime
            .execute_subscription_impl(self.handle, Box::new(callback))?;
        Ok(self.handle)
    }

    /// Execute the pending subscription with `callback` and return its
    /// stable [`SubscriptionHandle`] (WASM variant — no `Send` bound).
    #[cfg(target_arch = "wasm32")]
    pub fn execute<F>(self, callback: F) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.runtime
            .execute_subscription_impl(self.handle, Box::new(callback))?;
        Ok(self.handle)
    }
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    fn allocate_subscription_handle(&mut self) -> SubscriptionHandle {
        let handle = SubscriptionHandle(self.next_subscription_handle);
        self.next_subscription_handle += 1;
        handle
    }

    fn subscribe_query(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> Result<QuerySubscriptionId, RuntimeError> {
        self.schema_manager
            .query_manager_mut()
            .subscribe_with_sync_and_propagation_with_local_updates(
                query,
                session,
                durability.tier,
                durability.local_updates,
                durability.strict_transactions,
                propagation,
            )
            .map_err(|e| RuntimeError::QueryError(e.to_string()))
    }

    fn activate_subscription(
        &mut self,
        handle: SubscriptionHandle,
        query_sub_id: QuerySubscriptionId,
        callback: SubscriptionCallback,
    ) {
        self.subscriptions.insert(
            handle,
            SubscriptionState {
                query_sub_id,
                callback,
            },
        );
        self.subscription_reverse.insert(query_sub_id, handle);
        self.immediate_tick();
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_with_durability_and_propagation(
            query,
            callback,
            session,
            ReadDurabilityOptions::default(),
            QueryPropagation::Full,
        )
    }

    /// Subscribe to a query with a callback (WASM version - no Send required).
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_with_durability_and_propagation(
            query,
            callback,
            session,
            ReadDurabilityOptions::default(),
            QueryPropagation::Full,
        )
    }

    /// Subscribe with explicit durability and propagation options.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe_with_durability_and_propagation<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_impl(query, Box::new(callback), session, durability, propagation)
    }

    /// Subscribe with explicit durability and propagation options (WASM version).
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe_with_durability_and_propagation<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_impl(query, Box::new(callback), session, durability, propagation)
    }

    /// Internal subscribe implementation.
    fn subscribe_impl(
        &mut self,
        query: Query,
        callback: SubscriptionCallback,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError> {
        let _span = debug_span!(
            "subscribe",
            table = query.table.as_str(),
            ?durability.tier,
            local_updates = ?durability.local_updates
        )
        .entered();
        let query_sub_id = self.subscribe_query(query, session, durability, propagation)?;
        let handle = self.allocate_subscription_handle();
        debug!(handle = handle.0, sub_id = query_sub_id.0, "subscribed");
        self.activate_subscription(handle, query_sub_id, callback);
        Ok(handle)
    }

    // =========================================================================
    // Typed-builder subscribe entry point — preferred over create + execute.
    // =========================================================================

    /// Begin a subscription that defers callback registration. Returns a
    /// [`PendingSubscriptionRequest`] whose only legal next step is
    /// `.execute(callback)`, giving a compile-time guarantee against the
    /// flat create/execute pair being misused. Prefer this over the
    /// [`Self::create_subscription`] / [`Self::execute_subscription`] pair
    /// from new Rust call sites.
    pub fn subscribe_pending(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> PendingSubscriptionRequest<'_, S, Sch> {
        let handle = self.create_subscription(query, session, durability, propagation);
        PendingSubscriptionRequest {
            runtime: self,
            handle,
        }
    }

    // =========================================================================
    // Two-phase subscribe: create + execute (kept for FFI bindings)
    // =========================================================================

    /// Phase 1: allocate a handle and store query params. No compilation, no
    /// sync, no tick — just bookkeeping.
    ///
    /// Rust callers should prefer [`Self::subscribe`] which returns a typed
    /// builder; this flat method exists for the napi/wasm/UniFFI wrappers
    /// that need separate JS-/UniFFI-side entry points.
    pub fn create_subscription(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> SubscriptionHandle {
        let handle = self.allocate_subscription_handle();
        debug!(
            handle = handle.0,
            table = query.table.as_str(),
            "subscription created (pending)"
        );
        self.pending_subscriptions.insert(
            handle,
            PendingSubscription {
                query,
                session,
                durability,
                propagation,
            },
        );
        handle
    }

    /// Phase 2: compile graph, register with QueryManager, sync to servers,
    /// attach callback, and run `immediate_tick` to deliver the first delta.
    ///
    /// No-ops silently if the handle was already unsubscribed between create
    /// and execute.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn execute_subscription<F>(
        &mut self,
        handle: SubscriptionHandle,
        callback: F,
    ) -> Result<(), RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.execute_subscription_impl(handle, Box::new(callback))
    }

    /// Phase 2 (WASM version — no Send required).
    #[cfg(target_arch = "wasm32")]
    pub fn execute_subscription<F>(
        &mut self,
        handle: SubscriptionHandle,
        callback: F,
    ) -> Result<(), RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.execute_subscription_impl(handle, Box::new(callback))
    }

    fn execute_subscription_impl(
        &mut self,
        handle: SubscriptionHandle,
        callback: SubscriptionCallback,
    ) -> Result<(), RuntimeError> {
        let Some(pending) = self.pending_subscriptions.remove(&handle) else {
            return Ok(());
        };

        let _span = debug_span!(
            "execute_subscription",
            handle = handle.0,
            table = pending.query.table.as_str(),
            ?pending.durability.tier,
            local_updates = ?pending.durability.local_updates
        )
        .entered();

        let query_sub_id = self.subscribe_query(
            pending.query,
            pending.session,
            pending.durability,
            pending.propagation,
        )?;

        debug!(
            handle = handle.0,
            sub_id = query_sub_id.0,
            "subscription executed"
        );
        self.activate_subscription(handle, query_sub_id, callback);
        Ok(())
    }

    /// Unsubscribe from a query. Works for both pending (created but not
    /// executed) and active subscriptions.
    pub fn unsubscribe(&mut self, handle: SubscriptionHandle) {
        if self.pending_subscriptions.remove(&handle).is_some() {
            debug!(handle = handle.0, "unsubscribed pending subscription");
            return;
        }
        if let Some(state) = self.subscriptions.remove(&handle) {
            self.subscription_reverse.remove(&state.query_sub_id);
            self.schema_manager
                .query_manager_mut()
                .unsubscribe_with_sync(state.query_sub_id);
        }
    }

    /// Subscribe with explicit schema context (for server use).
    pub fn subscribe_with_schema_context(
        &mut self,
        query: Query,
        schema_context: &crate::schema_manager::QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<crate::sync_manager::QueryId, RuntimeError> {
        let query_sub_id = self
            .schema_manager
            .subscribe_with_schema_context(query, schema_context, session)
            .map_err(|e| RuntimeError::QueryError(e.to_string()))?;

        self.immediate_tick();
        Ok(crate::sync_manager::QueryId(query_sub_id.0))
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// Execute a one-shot query.
    pub fn query(&mut self, query: Query, session: Option<Session>) -> QueryFuture {
        self.query_with_propagation(
            query,
            session,
            ReadDurabilityOptions::default(),
            QueryPropagation::Full,
        )
    }

    pub fn query_with_propagation(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> QueryFuture {
        self.query_with_overlay_rows(query, session, durability, propagation, HashMap::new())
    }

    pub fn query_with_local_overlay(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
        overlay: QueryLocalOverlay,
    ) -> QueryFuture {
        let local_overlay_rows = if overlay.row_ids.is_empty() {
            HashMap::new()
        } else {
            overlay
                .row_ids
                .into_iter()
                .map(|row_id| {
                    (
                        row_id,
                        crate::sync_manager::RowBatchKey::new(
                            row_id,
                            overlay.branch_name,
                            overlay.batch_id,
                        ),
                    )
                })
                .collect()
        };
        self.query_with_overlay_rows(query, session, durability, propagation, local_overlay_rows)
    }

    fn query_with_overlay_rows(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
        local_overlay_rows: HashMap<ObjectId, crate::sync_manager::RowBatchKey>,
    ) -> QueryFuture {
        let _span = debug_span!(
            "query",
            table = query.table.as_str(),
            ?durability.tier,
            local_updates = ?durability.local_updates
        )
        .entered();
        let (sender, receiver) = oneshot::channel();

        let sub_id = match self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync_and_propagation_with_local_overlay(
                query,
                session,
                durability.tier,
                crate::query_manager::subscriptions::SubscriptionExecutionOptions {
                    local_updates: durability.local_updates,
                    strict_transactions: durability.strict_transactions,
                    propagation,
                    local_overlay_rows,
                },
            ) {
            Ok(id) => id,
            Err(e) => {
                let _ = sender.send(Err(RuntimeError::QueryError(e.to_string())));
                return QueryFuture::new(receiver);
            }
        };

        let handle = SubscriptionHandle(self.next_subscription_handle);
        self.next_subscription_handle += 1;

        self.pending_one_shot_queries.insert(
            handle,
            PendingOneShotQuery {
                subscription_id: sub_id,
                sender: Some(sender),
            },
        );
        self.subscription_reverse.insert(sub_id, handle);

        self.immediate_tick();
        QueryFuture::new(receiver)
    }
}
