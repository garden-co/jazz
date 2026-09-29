//! Fixture helpers that belong to the data model, re-exported from
//! `crate::tools::test_support`.

/// Explicit unrestricted grants for fixtures, never a production default.
#[cfg(any(test, feature = "testing"))]
pub fn allow_all_policies() -> crate::model::public_schema::TablePolicies {
    crate::model::public_schema::permissions(|p| {
        p.allow_read()
            .where_(crate::model::public_schema::policy_expr::always());
        p.allow_insert()
            .where_(crate::model::public_schema::policy_expr::always());
        p.allow_update()
            .where_(crate::model::public_schema::policy_expr::always());
        p.allow_delete()
            .where_(crate::model::public_schema::policy_expr::always());
    })
}

/// Test-only opt-in for fixtures whose subject is unrelated to authorization.
/// Replaces existing policies; apply specific policies after this helper.
#[cfg(any(test, feature = "testing"))]
pub trait AllowAll: Sized {
    fn allow_all(self) -> Self;
}

#[cfg(any(test, feature = "testing"))]
impl AllowAll for crate::model::public_schema::TableSchemaBuilder {
    fn allow_all(self) -> Self {
        self.policies(allow_all_policies())
    }
}

#[cfg(any(test, feature = "testing"))]
impl AllowAll for crate::model::public_schema::Schema {
    fn allow_all(mut self) -> Self {
        for table in self.values_mut() {
            table.policies = allow_all_policies();
        }
        self
    }
}

#[cfg(any(test, feature = "testing"))]
impl AllowAll for crate::schema::JazzSchema {
    fn allow_all(self) -> Self {
        Self::new(&self.public_schema().clone().allow_all())
            .expect("allow-all fixture policies compile")
    }
}
