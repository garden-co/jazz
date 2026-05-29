use super::*;

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
        let previous = self.auth.clone();
        self.auth = RuntimeAuth::trusted_as_user(user);
        let result = f(self);
        self.auth = previous;
        result
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
        let previous = self.auth.clone();
        self.auth = RuntimeAuth::trusted_attributing_to_user(user);
        let result = f(self);
        self.auth = previous;
        result
    }
}
