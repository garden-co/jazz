use super::*;
use crate::query_manager::manager::LocalUpdates;
use crate::sync_manager::QueryPropagation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadDurabilityOptions {
    pub tier: Option<DurabilityTier>,
    pub local_updates: LocalUpdates,
}

impl Default for ReadDurabilityOptions {
    fn default() -> Self {
        Self {
            tier: None,
            local_updates: LocalUpdates::Immediate,
        }
    }
}

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
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
        let query_sub_id = self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync_and_propagation_with_local_updates(
                query,
                session,
                durability.tier,
                durability.local_updates,
                propagation,
            )
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))?;

        let handle = SubscriptionHandle(self.next_subscription_handle);
        self.next_subscription_handle += 1;
        debug!(handle = handle.0, sub_id = query_sub_id.0, "subscribed");

        self.subscriptions.insert(
            handle,
            SubscriptionState {
                query_sub_id,
                callback,
            },
        );
        self.subscription_reverse.insert(query_sub_id, handle);

        self.immediate_tick();
        Ok(handle)
    }

    /// Unsubscribe from a query.
    pub fn unsubscribe(&mut self, handle: SubscriptionHandle) {
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
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))?;

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
            .subscribe_with_sync_and_propagation_with_local_updates(
                query,
                session,
                durability.tier,
                durability.local_updates,
                propagation,
            ) {
            Ok(id) => id,
            Err(e) => {
                let _ = sender.send(Err(RuntimeError::QueryError(format!("{:?}", e))));
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
