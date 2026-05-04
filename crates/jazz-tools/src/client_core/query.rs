use crate::object::ObjectId;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::Value;
use crate::runtime_core::{ReadDurabilityOptions, Scheduler};
use crate::storage::Storage;
use crate::sync_manager::{DurabilityTier, QueryPropagation};

use super::{ClientError, ClientErrorCode, JazzClientCore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQueryOptions {
    pub tier: DurabilityTier,
    pub local_updates: LocalUpdates,
    pub propagation: QueryPropagation,
    pub session: Option<Session>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRowCore {
    pub id: ObjectId,
    pub values: Vec<Value>,
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn resolve_query_options(&self, options: Option<ClientQueryOptions>) -> ClientQueryOptions {
        options.unwrap_or(ClientQueryOptions {
            tier: self.config().resolved_default_durability_tier(),
            local_updates: LocalUpdates::Immediate,
            propagation: QueryPropagation::Full,
            session: None,
        })
    }

    pub async fn query(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
    ) -> Result<Vec<QueryRowCore>, ClientError> {
        let options = self.resolve_query_options(options);
        let future = self.runtime_mut().query_with_propagation(
            query,
            options.session,
            ReadDurabilityOptions {
                tier: Some(options.tier),
                local_updates: options.local_updates,
            },
            options.propagation,
        );

        let rows = future.await.map_err(|error| {
            ClientError::new(ClientErrorCode::InvalidQuery, format!("{error:?}"))
        })?;

        Ok(rows
            .into_iter()
            .map(|(id, values)| QueryRowCore { id, values })
            .collect())
    }
}
