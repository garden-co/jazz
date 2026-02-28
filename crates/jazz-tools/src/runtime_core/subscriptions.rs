use super::*;
use crate::sync_manager::QueryPropagation;

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
        self.subscribe_impl(
            query,
            Box::new(callback),
            session,
            None,
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
        self.subscribe_impl(
            query,
            Box::new(callback),
            session,
            None,
            QueryPropagation::Full,
        )
    }

    /// Subscribe with optional settled tier.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe_with_settled_tier<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_with_settled_tier_and_propagation(
            query,
            callback,
            session,
            settled_tier,
            QueryPropagation::Full,
        )
    }

    /// Subscribe with settled tier (WASM version - no Send required).
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe_with_settled_tier<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_with_settled_tier_and_propagation(
            query,
            callback,
            session,
            settled_tier,
            QueryPropagation::Full,
        )
    }

    /// Subscribe with settled tier and explicit propagation mode.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe_with_settled_tier_and_propagation<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_impl(
            query,
            Box::new(callback),
            session,
            settled_tier,
            propagation,
        )
    }

    /// Subscribe with settled tier and explicit propagation mode (WASM version).
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe_with_settled_tier_and_propagation<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_impl(
            query,
            Box::new(callback),
            session,
            settled_tier,
            propagation,
        )
    }

    /// Internal subscribe implementation.
    fn subscribe_impl(
        &mut self,
        query: Query,
        callback: SubscriptionCallback,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError> {
        let _span = debug_span!("subscribe", table = query.table.as_str()).entered();
        let query_sub_id = self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync_and_propagation(query, session, settled_tier, propagation)
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

    /// Execute a one-shot query, optionally waiting for a settled tier.
    pub fn query(
        &mut self,
        query: Query,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> QueryFuture {
        self.query_with_propagation(query, session, settled_tier, QueryPropagation::Full)
    }

    pub fn query_with_propagation(
        &mut self,
        query: Query,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
        propagation: QueryPropagation,
    ) -> QueryFuture {
        let _span = debug_span!("query", table = query.table.as_str(), ?settled_tier).entered();
        let (sender, receiver) = oneshot::channel();

        let sub_id = match self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync_and_propagation(query, session, settled_tier, propagation)
        {
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
