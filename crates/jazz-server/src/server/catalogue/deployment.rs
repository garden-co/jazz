use super::*;
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
    next: CatalogueIndex,
    entries: Vec<CatalogueEntry>,
    pub response: DeploymentResponse,
}

impl StoredCatalogue {
    pub(crate) fn deployment_catalogue(&self) -> Result<DeploymentCatalogue, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(DeploymentCatalogue {
            schemas: index.schemas.values().cloned().collect(),
            migrations: index.lenses.values().cloned().collect(),
            active_schema_hash: index.active_schema.map(|active| active.schema_hash),
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
        let mut next = expected.clone();
        let mut entries = Vec::new();
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
            // A configured initial schema may be known only in memory. Its
            // publication timestamp is present only after a schema record exists.
            if unchanged && expected.schema_published_at.contains_key(&hash) {
                continue;
            }
            let (_, entry) = schema_entry(
                self.app_id,
                schema.clone(),
                expected
                    .schema_published_at
                    .get(&hash)
                    .copied()
                    .unwrap_or_else(unix_timestamp_millis),
            );
            published.schemas.push(hash.to_string());
            next.apply_entry(&entry)?;
            next.schemas.insert(hash, schema.clone());
            entries.push(entry);
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
            let entry = lens_entry(self.app_id, lens);
            next.apply_entry(&entry)?;
            entries.push(entry);
        }
        let source = prepared.active_schema().public_schema();
        let schema_hash = SchemaHash::compute(source);
        let permissions = source
            .iter()
            .filter(|(_, table)| table.policies != TablePolicies::default())
            .map(|(name, table)| (name.clone(), table.policies.clone()))
            .collect::<HashMap<_, _>>();
        let current = expected.active_schema();
        let same_active = current.as_ref().is_some_and(|active| {
            active.summary.schema_hash == schema_hash
                && active
                    .permissions
                    .iter()
                    .filter(|(_, policy)| **policy != TablePolicies::default())
                    .map(|(name, policy)| (name.clone(), policy.clone()))
                    .collect::<HashMap<_, _>>()
                    == permissions
        });
        let changed =
            !same_active || !published.schemas.is_empty() || !published.migrations.is_empty();
        if changed {
            if expected
                .active_schema
                .is_some_and(|active| active.version >= version)
            {
                return Err(CatalogueError::WriteError(
                    "deployment revision must advance".into(),
                ));
            }
            let parent = expected.active_schema.map(|active| active.bundle_object_id);
            let bundle = permissions_bundle_object_id(
                self.app_id,
                schema_hash,
                version,
                parent,
                &permissions,
            );
            for entry in [
                CatalogueEntry {
                    object_id: bundle,
                    metadata: catalogue_metadata(
                        self.app_id,
                        ObjectType::CataloguePermissionsBundle,
                    ),
                    content: encode_permissions_bundle(schema_hash, version, parent, &permissions),
                },
                CatalogueEntry {
                    object_id: permissions_head_object_id(self.app_id),
                    metadata: catalogue_metadata(self.app_id, ObjectType::CataloguePermissionsHead),
                    content: encode_permissions_head(schema_hash, version, parent, bundle),
                },
            ] {
                next.apply_entry(&entry)?;
                entries.push(entry);
            }
        }
        // Fail before writing if any metadata or payload cannot be encoded.
        for entry in &entries {
            entry
                .encode_storage_row()
                .map_err(CatalogueError::WriteError)?;
        }
        published.schemas.sort();
        published
            .migrations
            .sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        Ok(StagedDeployment {
            expected,
            next,
            entries,
            response: DeploymentResponse { changed, published },
        })
    }

    pub(crate) fn commit_deployment(
        &self,
        staged: StagedDeployment,
    ) -> Result<DeploymentResponse, CatalogueError> {
        let mut storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        let mut index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        if *index != staged.expected {
            return Err(CatalogueError::WriteError(
                "catalogue changed while preparing deployment; fetch the graph and retry".into(),
            ));
        }
        if !staged.response.changed {
            return Ok(staged.response);
        }
        storage.upsert_catalogue_entries(&staged.entries)?;
        storage.flush()?;
        storage.flush_wal()?;
        *index = staged.next;
        Ok(staged.response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::catalogue_storage::CatalogueMemoryStorage;
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
            Box::new(CatalogueMemoryStorage::new()),
        )
        .unwrap();
        let stage = |target_schema_hash, schemas, migrations, permissions, revision| {
            let prepared = prepare_deployment(
                &store.deployment_catalogue().unwrap(),
                DeploymentRequest {
                    target_schema_hash,
                    schemas,
                    migrations,
                    permissions,
                },
            )
            .unwrap();
            store.stage_deployment(&prepared, revision).unwrap()
        };
        let initial = stage(base_hash, vec![], vec![], HashMap::new(), 1);
        assert_eq!(
            initial.entries.len(),
            3,
            "persist the in-memory initial schema, bundle and selection"
        );
        assert_eq!(
            initial.response.published.schemas,
            vec![base_hash.to_string()]
        );
        store.commit_deployment(initial).unwrap();

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
            migrated.entries.len(),
            4,
            "only the new schema, lens, bundle and selection"
        );
        assert_eq!(
            migrated.response.published.schemas,
            vec![next_hash.to_string()]
        );
        assert_eq!(migrated.response.published.migrations.len(), 1);
        store.commit_deployment(migrated).unwrap();

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
            policies.entries.len(),
            2,
            "permission-only deployment writes the bundle and selection"
        );
        assert!(policies.response.published.schemas.is_empty());
        assert!(policies.response.published.migrations.is_empty());
        store.commit_deployment(policies).unwrap();

        let unchanged = stage(next_hash, schemas, vec![lens], permissions, 4);
        assert!(!unchanged.response.changed);
        assert!(unchanged.entries.is_empty());
        store.commit_deployment(unchanged).unwrap();
    }
}
