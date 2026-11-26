//! Membership of an encrypted space, through ordinary table reads and writes.

use std::collections::HashMap;

use jazz::schema::MEMBERS_TABLE;
use jazz::tools::public_schema::E2eeDeclarations;
use jazz::tools::{ReadTier, Value};

use crate::{
    AccountId, Column, DurabilityTier, Error, Fields, Implementations, Insert, Jazz, NoUpdate,
    ObjectId, Query, Result, Table,
};

/// A membership row. An account can have more than one row in a space.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpaceMember {
    pub id: ObjectId,
    pub scope_id: ObjectId,
    pub identifier: ObjectId,
    pub account_id: ObjectId,
}

#[doc(hidden)]
pub struct NewSpaceMember {
    pub(crate) scope_id: ObjectId,
    pub(crate) identifier: ObjectId,
    pub(crate) account: AccountId,
}

impl Table for SpaceMember {
    const NAME: &'static str = MEMBERS_TABLE;
    const COLUMNS: &'static [&'static str] = &["scopeId", "identifier", "accountId"];
    type New = NewSpaceMember;
    type Update = NoUpdate;

    fn decode(row: &mut Fields<'_>) -> Result<Self> {
        Ok(Self {
            id: row.id(),
            scope_id: row.take("scopeId")?,
            identifier: row.take("identifier")?,
            account_id: row.take("accountId")?,
        })
    }

    fn id(&self) -> ObjectId {
        self.id
    }
}

impl Insert for NewSpaceMember {
    type Row = SpaceMember;

    fn into_cells(self, _: &Implementations) -> Result<HashMap<String, Value>> {
        Ok(HashMap::from([
            ("scopeId".into(), Value::Uuid(self.scope_id)),
            ("identifier".into(), Value::Uuid(self.identifier)),
            (
                "accountId".into(),
                Value::Uuid(ObjectId::from_uuid(self.account.0)),
            ),
        ]))
    }
}

pub(crate) fn require_space(declarations: Option<&E2eeDeclarations>, table: &str) -> Result<()> {
    let declarations = declarations.ok_or_else(|| Error::Encryption {
        code: "not-declared".into(),
        message: "this app declares no end-to-end encryption".into(),
    })?;
    if declarations
        .tables
        .values()
        .any(|entry| entry.scope == table)
    {
        Ok(())
    } else {
        Err(Error::Encryption {
            code: "not-a-space-table".into(),
            message: format!("{table:?} is not the scope of an encryption declaration"),
        })
    }
}

fn members_in(scope_id: ObjectId, id: ObjectId) -> Query<SpaceMember> {
    Query::all()
        .filter(Column::<SpaceMember, ObjectId>::new("scopeId").eq(scope_id))
        .filter(Column::<SpaceMember, ObjectId>::new("identifier").eq(id))
}

impl Jazz {
    /// Admits an account to the space row `id` of `T`. Returns its membership
    /// row with the same durability as [`Jazz::insert`].
    ///
    /// Automatic creator membership applies to inserts. When an upsert creates
    /// a space, explicitly add the creator in the same transaction or call this
    /// method afterward if permissions allow it.
    pub async fn add_member<T: Table>(
        &self,
        id: ObjectId,
        account: AccountId,
    ) -> Result<SpaceMember> {
        self.add_member_by_name(T::NAME, id, account).await
    }

    /// The space's membership rows visible to the caller.
    pub async fn list_members<T: Table>(&self, id: ObjectId) -> Result<Vec<SpaceMember>> {
        self.list_members_by_name(T::NAME, id).await
    }

    /// Deletes the account's visible membership rows in the space. Returns the
    /// count once the authority accepts the serializable transaction. Retries
    /// conflicts up to three attempts in all.
    pub async fn remove_member<T: Table>(&self, id: ObjectId, account: AccountId) -> Result<usize> {
        self.remove_member_by_name(T::NAME, id, account).await
    }

    #[doc(hidden)]
    pub async fn add_member_by_name(
        &self,
        table: &str,
        id: ObjectId,
        account: AccountId,
    ) -> Result<SpaceMember> {
        require_space(self.info.declarations.as_deref(), table)?;
        self.insert(NewSpaceMember {
            scope_id: self.table_identity(table).await?,
            identifier: id,
            account,
        })
        .await
    }

    #[doc(hidden)]
    pub async fn list_members_by_name(
        &self,
        table: &str,
        id: ObjectId,
    ) -> Result<Vec<SpaceMember>> {
        require_space(self.info.declarations.as_deref(), table)?;
        self.all(members_in(self.table_identity(table).await?, id))
            .await
    }

    #[doc(hidden)]
    pub async fn remove_member_by_name(
        &self,
        table: &str,
        id: ObjectId,
        account: AccountId,
    ) -> Result<usize> {
        require_space(self.info.declarations.as_deref(), table)?;
        let scope_id = self.table_identity(table).await?;
        let query = members_in(scope_id, id).filter(
            Column::<SpaceMember, ObjectId>::new("accountId").eq(ObjectId::from_uuid(account.0)),
        );
        let jazz = self.durable(DurabilityTier::GlobalServer);
        for attempt in 1.. {
            let query = query.clone();
            let result = jazz
                .serializable_transaction_validating_reads(move |tx| async move {
                    let rows = tx.all_with(query, ReadTier::Remote).await?;
                    for row in &rows {
                        tx.delete::<SpaceMember>(row.id)?;
                    }
                    Ok(rows.len())
                })
                .await;
            match &result {
                Err(Error::WriteRejected { code, .. })
                    if attempt < 3
                        && matches!(
                            code.as_str(),
                            "serialization_conflict" | "transaction_conflict"
                        ) => {}
                _ => return result,
            }
        }
        unreachable!("the attempt limit returns the result")
    }
}
