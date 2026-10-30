use super::*;
use crate::server::permissions_validation::validate_permissions_relations;
use jazz::catalogue::{CatalogueLens, CataloguePut};
use jazz::tools::deployment::{DeploymentCatalogue, PreparedDeployment};

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeploymentResponse {
    pub changed: bool,
    pub published: PublishedArtifacts,
}
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct PublishedArtifacts {
    pub schemas: Vec<String>,
    pub migrations: Vec<PublishedMigration>,
}
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishedMigration {
    pub from_hash: String,
    pub to_hash: String,
}

pub(crate) struct StagedDeployment {
    expected: CatalogueIndex,
    /// The rows the deployment adds, applied with its admission.
    pub put: CataloguePut,
    pub response: DeploymentResponse,
}

impl StoredCatalogue {
    pub(crate) fn deployment_catalogue(&self) -> Result<DeploymentCatalogue, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(DeploymentCatalogue {
            schemas: index.schemas.values().cloned().collect(),
            migrations: index.lenses.values().cloned().collect(),
            active_schema_hash: index
                .active_schema_summary()
                .map(|active| active.schema_hash),
        })
    }

    pub(crate) fn stage_deployment(
        &self,
        prepared: &PreparedDeployment,
        version: u64,
    ) -> Result<StagedDeployment, CatalogueError> {
        let expected = self
            .index
            .lock()
            .map_err(|_| CatalogueError::LockError)?
            .clone();
        let now = unix_timestamp_millis();
        let mut put = CataloguePut::default();
        let mut published = PublishedArtifacts::default();
        for schema in prepared.schemas() {
            let hash = SchemaHash::compute(schema);
            let unchanged = expected.schemas.get(&hash).is_some_and(|stored| {
                let mut structural = stored.clone();
                for table in structural.values_mut() {
                    table.policies = TablePolicies::default();
                }
                structural == *schema
            });
            // A configured initial schema, or one the store admitted, is
            // published only once a deployment publishes it.
            if unchanged && expected.schema_published_at.contains_key(&hash) {
                continue;
            }
            published.schemas.push(hash.to_string());
            put.schemas.push((schema.clone(), now));
        }
        for lens in prepared.migrations() {
            // Preparation already rejects conflicting definitions for this pair.
            if expected
                .lenses
                .contains_key(&(lens.source_hash, lens.target_hash))
            {
                continue;
            }
            published.migrations.push(PublishedMigration {
                from_hash: lens.source_hash.to_string(),
                to_hash: lens.target_hash.to_string(),
            });
            put.lenses.push(CatalogueLens {
                source: lens.source_hash,
                target: lens.target_hash,
                published_at: now,
                forward: lens.forward.clone(),
            });
        }
        let source = prepared.active_schema().public_schema();
        let schema_hash = SchemaHash::compute(source);
        let permissions = source
            .iter()
            .filter(|(_, table)| table.policies != TablePolicies::default())
            .map(|(name, table)| (*name, table.policies.clone()))
            .collect::<HashMap<_, _>>();
        validate_permissions_relations(&permissions).map_err(CatalogueError::WriteError)?;
        let current = expected.active_schema();
        let same_active = current.as_ref().is_some_and(|active| {
            active.summary.schema_hash == schema_hash && active.permissions == permissions
        });
        let changed =
            !same_active || !published.schemas.is_empty() || !published.migrations.is_empty();
        if changed {
            if current
                .as_ref()
                .is_some_and(|active| active.summary.version >= version)
            {
                return Err(CatalogueError::WriteError(
                    "deployment revision must advance".into(),
                ));
            }
            put.permissions
                .push((version, schema_hash, permissions.into_iter().collect(), now));
        }
        published.schemas.sort();
        published
            .migrations
            .sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        Ok(StagedDeployment {
            expected,
            put,
            response: DeploymentResponse { changed, published },
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
    use jazz::tools::{
        deployment::{DeploymentRequest, prepare_deployment},
        public_schema::{ColumnType, PolicyExpr, SchemaBuilder, TableSchema, Value},
        schema_lens::{LensOp, LensTransform},
    };

    // Internal: public reads cannot distinguish an unnecessary rewrite from
    // leaving the same durable record untouched. Inspect the actual write batch.
    #[test]
    fn deployment_writes_only_new_artifacts_and_active_permissions() {
        let table = TableSchema::builder("notes").column("title", ColumnType::Text);
        let base = SchemaBuilder::new().table(table.clone()).build();
        let next = SchemaBuilder::new()
            .table(table.column("body", ColumnType::Text))
            .build();
        let base_hash = SchemaHash::compute(&base);
        let next_hash = SchemaHash::compute(&next);
        let store = StoredCatalogue::new(
            AppId::from_name("deployment-write-delta"),
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
        let stage = |target_schema_hash, schemas, migrations, permissions, revision| {
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
            store.stage_deployment(&prepared, revision).unwrap()
        };
        let initial = stage(base_hash, vec![], vec![], HashMap::new(), 1);
        assert_eq!(
            (initial.put.schemas.len(), initial.put.permissions.len()),
            (1, 1),
            "persist the in-memory initial schema and the permissions revision"
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
            2,
        );
        assert_eq!(
            (
                migrated.put.schemas.len(),
                migrated.put.lenses.len(),
                migrated.put.permissions.len()
            ),
            (1, 1, 1),
            "only the new schema, lens and permissions revision"
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
            3,
        );
        assert_eq!(
            (
                policies.put.schemas.len(),
                policies.put.lenses.len(),
                policies.put.permissions.len()
            ),
            (0, 0, 1),
            "permission-only deployment writes the permissions revision"
        );
        assert!(policies.response.published.schemas.is_empty());
        assert!(policies.response.published.migrations.is_empty());
        commit(policies);

        let unchanged = stage(next_hash, schemas, vec![lens], permissions, 4);
        assert!(!unchanged.response.changed);
        assert!(unchanged.put.is_empty());
        commit(unchanged);
    }
}
