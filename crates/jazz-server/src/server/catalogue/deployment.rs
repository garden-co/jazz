use super::*;
use jazz::catalogue::CataloguePut;
use jazz::tools::deployment::{DeploymentCatalogue, PreparedDeployment};

pub(crate) use crate::runtime::catalogue::DeploymentResponse;

pub(crate) struct StagedDeployment {
    expected: CatalogueIndex,
    /// The rows the deployment adds, applied with its admission.
    pub put: CataloguePut,
    pub response: DeploymentResponse,
}

impl StoredCatalogue {
    pub(crate) fn deployment_catalogue(&self) -> Result<DeploymentCatalogue, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.deployment_catalogue())
    }

    /// Stage `prepared` as a published bundle: its schemas, lenses and
    /// permissions, activating nothing.
    pub(crate) fn stage_deployment(
        &self,
        prepared: &PreparedDeployment,
    ) -> Result<StagedDeployment, CatalogueError> {
        let expected = self
            .index
            .lock()
            .map_err(|_| CatalogueError::LockError)?
            .clone();
        let (put, response) = expected.stage_deployment(prepared, unix_timestamp_millis())?;
        Ok(StagedDeployment {
            expected,
            put,
            response,
        })
    }

    /// Fail if the catalogue changed since `staged` was prepared.
    pub(crate) fn check_unchanged(&self, staged: &StagedDeployment) -> Result<(), CatalogueError> {
        if *self.index.lock().map_err(|_| CatalogueError::LockError)? != staged.expected {
            return Err(CatalogueError::WriteError(
                "catalogue changed while preparing deployment; fetch the graph and retry".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::tools::public_schema::TablePolicies;
    use jazz::tools::{
        deployment::{DeploymentRequest, prepare_deployment},
        public_schema::{ColumnType, PolicyExpr, SchemaBuilder, TableSchema, Value},
        schema_lens::{LensOp, LensTransform},
    };
    use std::collections::HashMap;

    // Internal: public reads cannot distinguish an unnecessary rewrite from
    // leaving the same durable record untouched. Inspect the actual write batch.
    #[test]
    fn deployment_writes_only_new_artifacts_and_bundles() {
        let table = TableSchema::builder("notes").column("title", ColumnType::Text);
        let base = SchemaBuilder::new().table(table.clone()).build();
        let next = SchemaBuilder::new()
            .table(table.column("body", ColumnType::Text))
            .build();
        let base_hash = SchemaHash::compute(&base);
        let next_hash = SchemaHash::compute(&next);
        let store = StoredCatalogue::new(
            Some(base.clone()),
            crate::runtime::AppStore::memory()
                .unwrap()
                .catalogue()
                .unwrap(),
        )
        .unwrap();
        // The runtime applies a deployment's rows with its admission; here
        // they are written alone.
        let commit = |staged: StagedDeployment| {
            store.check_unchanged(&staged).unwrap();
            let storage = store.storage.lock().unwrap();
            store.write(&storage, staged.put).unwrap();
        };
        let stage = |target_schema_hash, schemas, migrations, permissions| {
            let prepared = prepare_deployment(
                &store.deployment_catalogue().unwrap(),
                DeploymentRequest {
                    target_schema_hash,
                    schemas,
                    migrations,
                    permissions,
                    allow_dropping_history: false,
                },
            )
            .unwrap();
            store.stage_deployment(&prepared).unwrap()
        };
        let initial = stage(base_hash, vec![], vec![], HashMap::new());
        assert_eq!(
            (initial.put.schemas.len(), initial.put.bundles.len()),
            (1, 1),
            "persist the in-memory initial schema and its bundle"
        );
        assert_eq!(
            initial.response.published.schemas,
            vec![base_hash.to_string()]
        );
        commit(initial);

        let lens = Lens::new(
            base_hash,
            next_hash,
            LensTransform::with_ops(vec![LensOp::AddColumn {
                table: "notes".into(),
                column: "body".into(),
                column_type: ColumnType::Text,
                default: Value::Text("default".into()),
            }]),
        );
        let schemas = vec![(base_hash, base), (next_hash, next)];
        let migrated = stage(
            next_hash,
            schemas.clone(),
            vec![lens.clone()],
            HashMap::new(),
        );
        assert_eq!(
            (
                migrated.put.schemas.len(),
                migrated.put.lenses.len(),
                migrated.put.bundles.len()
            ),
            (1, 1, 1),
            "only the new schema, lens and bundle"
        );
        assert_eq!(
            migrated.response.published.schemas,
            vec![next_hash.to_string()]
        );
        assert_eq!(migrated.response.published.migrations.len(), 1);
        commit(migrated);

        let permissions = HashMap::from([(
            "notes".into(),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let policies = stage(
            next_hash,
            schemas.clone(),
            vec![lens.clone()],
            permissions.clone(),
        );
        assert_eq!(
            (
                policies.put.schemas.len(),
                policies.put.lenses.len(),
                policies.put.bundles.len()
            ),
            (0, 0, 1),
            "a permission-only deployment writes only its bundle"
        );
        assert!(policies.response.published.schemas.is_empty());
        assert!(policies.response.published.migrations.is_empty());
        commit(policies);

        let unchanged = stage(next_hash, schemas, vec![lens], permissions);
        assert!(!unchanged.response.changed);
        assert!(unchanged.put.is_empty());
        commit(unchanged);
    }
}
