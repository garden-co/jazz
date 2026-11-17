//! Permission advice: would the server allow this read or write? Asks the
//! authority without doing it, as TS's `canRead`, `canInsert`, `canUpdate`
//! and `canDelete` do.

use jazz::tools::{ObjectId, PermissionAdviceRequest};

pub use jazz::protocol::PermissionAdvice as Advice;

use crate::error::Result;
use crate::handle::Jazz;
use crate::table::{Insert, Table, Update};

impl Jazz {
    async fn advise(&self, request: PermissionAdviceRequest) -> Result<Advice> {
        self.with_client(move |client| async move { Ok(client.permission_advice(request).await?) })
            .await
    }

    /// Whether this user may read the row of `T` with this id.
    pub async fn can_read<T: Table>(&self, id: ObjectId) -> Result<Advice> {
        self.advise(PermissionAdviceRequest::Read {
            table: T::NAME.to_owned(),
            row: id,
        })
        .await
    }

    /// Whether this user may insert `new`.
    pub async fn can_insert<N: Insert>(&self, new: N) -> Result<Advice> {
        let values = new.into_cells(&self.info.implementations)?;
        self.advise(PermissionAdviceRequest::Insert {
            table: <N::Row as Table>::NAME.to_owned(),
            values,
        })
        .await
    }

    /// Whether this user may apply `update` to the row with this id.
    pub async fn can_update<U: Update>(&self, id: ObjectId, update: U) -> Result<Advice> {
        let values = update.into_cells(&self.info.implementations)?;
        self.advise(PermissionAdviceRequest::Update {
            table: <U::Row as Table>::NAME.to_owned(),
            row: id,
            values,
        })
        .await
    }

    /// Whether this user may delete the row of `T` with this id.
    pub async fn can_delete<T: Table>(&self, id: ObjectId) -> Result<Advice> {
        self.advise(PermissionAdviceRequest::Delete {
            table: T::NAME.to_owned(),
            row: id,
        })
        .await
    }
}
