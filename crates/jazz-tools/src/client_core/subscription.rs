use crate::query_manager::query::Query;
use crate::runtime_core::{
    ReadDurabilityOptions, Scheduler, SubscriptionDelta, SubscriptionHandle,
};
use crate::storage::Storage;

use super::{ClientError, ClientErrorCode, ClientQueryOptions, JazzClientCore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionCoreHandle(pub u64);

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
        callback: F,
    ) -> Result<SubscriptionCoreHandle, ClientError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_with_runtime_bound(query, options, callback)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
        callback: F,
    ) -> Result<SubscriptionCoreHandle, ClientError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_with_runtime_bound(query, options, callback)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn subscribe_with_runtime_bound<F>(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
        callback: F,
    ) -> Result<SubscriptionCoreHandle, ClientError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        let options = self.resolve_query_options(options);
        let handle = self
            .runtime_mut()
            .subscribe_with_durability_and_propagation(
                query,
                callback,
                options.session,
                ReadDurabilityOptions {
                    tier: Some(options.tier),
                    local_updates: options.local_updates,
                },
                options.propagation,
            )
            .map_err(|error| {
                ClientError::new(ClientErrorCode::InvalidQuery, format!("{error:?}"))
            })?;

        Ok(SubscriptionCoreHandle(handle.0))
    }

    #[cfg(target_arch = "wasm32")]
    fn subscribe_with_runtime_bound<F>(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
        callback: F,
    ) -> Result<SubscriptionCoreHandle, ClientError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        let options = self.resolve_query_options(options);
        let handle = self
            .runtime_mut()
            .subscribe_with_durability_and_propagation(
                query,
                callback,
                options.session,
                ReadDurabilityOptions {
                    tier: Some(options.tier),
                    local_updates: options.local_updates,
                },
                options.propagation,
            )
            .map_err(|error| {
                ClientError::new(ClientErrorCode::InvalidQuery, format!("{error:?}"))
            })?;

        Ok(SubscriptionCoreHandle(handle.0))
    }

    pub fn unsubscribe(&mut self, handle: SubscriptionCoreHandle) -> Result<(), ClientError> {
        self.runtime_mut().unsubscribe(SubscriptionHandle(handle.0));
        Ok(())
    }
}
