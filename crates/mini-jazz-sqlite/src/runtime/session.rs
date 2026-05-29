use super::Runtime;
use crate::{auth::RuntimeAuth, schema, schema::SchemaDef, storage, tx, Result, Storage};

impl Runtime {
    pub fn open_with_schema(
        storage: Storage,
        node_id: &str,
        user: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(storage, node_id, RuntimeAuth::client(user), schema_def)
    }

    pub fn open_trusted_with_schema(
        storage: Storage,
        node_id: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(storage, node_id, RuntimeAuth::trusted_admin(), schema_def)
    }

    pub fn open_trusted_with_session_user(
        storage: Storage,
        node_id: &str,
        user: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(
            storage,
            node_id,
            RuntimeAuth::trusted_as_user(user),
            schema_def,
        )
    }

    pub fn open_trusted_attributing_to_user(
        storage: Storage,
        node_id: &str,
        user: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(
            storage,
            node_id,
            RuntimeAuth::trusted_attributing_to_user(user),
            schema_def,
        )
    }

    fn open_with_schema_and_auth(
        storage: Storage,
        node_id: &str,
        auth: RuntimeAuth,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        let conn = storage::open(storage)?;
        schema::install(&conn, &schema_def)?;
        conn.execute("DELETE FROM jazz_query_read", [])?;
        let node_num = tx::ensure_node(&conn, node_id)?;
        Ok(Self {
            conn,
            schema: schema_def,
            node_id: node_id.to_owned(),
            auth,
            node_num,
            branch_num: 1,
        })
    }

    pub fn is_trusted(&self) -> bool {
        self.auth.is_trusted()
    }

    pub fn session_user(&self) -> &str {
        self.policy_user()
    }

    pub(super) fn policy_user(&self) -> &str {
        self.auth.policy_user()
    }

    pub(super) fn attribution_user(&self) -> &str {
        self.auth.attribution_user()
    }

    pub(super) fn bypasses_policy(&self) -> bool {
        self.auth.bypasses_policy()
    }

    pub fn run_as_user<T>(&mut self, user: &str, f: impl FnOnce(&mut Runtime) -> T) -> T {
        assert!(
            self.is_trusted(),
            "run_as_user is only valid for trusted peers"
        );
        self.with_temporary_auth(RuntimeAuth::trusted_as_user(user), f)
    }

    pub fn run_attributing_to_user<T>(
        &mut self,
        user: &str,
        f: impl FnOnce(&mut Runtime) -> T,
    ) -> T {
        assert!(
            self.is_trusted(),
            "run_attributing_to_user is only valid for trusted peers"
        );
        self.with_temporary_auth(RuntimeAuth::trusted_attributing_to_user(user), f)
    }

    fn with_temporary_auth<T>(
        &mut self,
        auth: RuntimeAuth,
        f: impl FnOnce(&mut Runtime) -> T,
    ) -> T {
        let previous = self.auth.clone();
        self.auth = auth;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        self.auth = previous;
        match result {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ADMIN_SYSTEM_USER;

    #[test]
    fn run_as_user_restores_auth_after_panic() {
        let mut runtime = Runtime::open_trusted_with_schema(
            Storage::Memory,
            "trusted",
            SchemaDef::new().table("docs", |table| {
                table.text("title");
            }),
        )
        .unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.run_as_user("bob", |_| panic!("boom"));
        }));

        assert!(result.is_err());
        assert_eq!(runtime.session_user(), ADMIN_SYSTEM_USER);
    }
}
