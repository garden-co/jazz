//! The published catalogue as the admin routes read it, and what a
//! deployment publishes into it: every host serves the same routes from
//! the same rows. Reading the rows and writing a deployment's rows stay
//! with each host (the native server's `StoredCatalogue`, a Durable
//! Object's core store).

#[cfg(all(test, feature = "native"))]
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};

use jazz::catalogue::{
    BundleId, CatalogueBundle, CatalogueLens, CataloguePut, CatalogueRows, Permissions,
};
use jazz::tools::deployment::{DeploymentCatalogue, PreparedDeployment};
use jazz::tools::public_schema::{Schema, SchemaHash, TablePolicies};
use jazz::tools::schema_lens::Lens;

/// Tests (native routes): how a client's schema relates to the catalogue.
#[cfg(all(test, feature = "native"))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionSchemaDiagnostics {
    pub client_schema_hash: SchemaHash,
    pub disconnected_permissions_schema_hash: Option<SchemaHash>,
    pub unreachable_schema_hashes: Vec<SchemaHash>,
}

#[cfg(all(test, feature = "native"))]
impl ConnectionSchemaDiagnostics {
    pub(crate) fn has_issues(&self) -> bool {
        self.disconnected_permissions_schema_hash.is_some()
            || !self.unreachable_schema_hashes.is_empty()
    }
}

/// HTTP graph inventory, captured from one catalogue index.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationGraph {
    /// The schema of the bundle published last, which new sessions pin to.
    latest_bundle_schema_hash: Option<String>,
    schemas: Vec<String>,
    migrations: Vec<MigrationGraphEdge>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MigrationGraphEdge {
    from_hash: String,
    to_hash: String,
    /// Derived capability, not persisted provenance: no migration file is needed.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    automatic: bool,
}

/// What a deployment published, as the deploy route answers.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentResponse {
    pub changed: bool,
    pub published: PublishedArtifacts,
    /// The published permissions bundle (see [`jazz::catalogue`]): what
    /// the backend pins its sessions to.
    pub bundle: String,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PublishedArtifacts {
    pub schemas: Vec<String>,
    pub migrations: Vec<PublishedMigration>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedMigration {
    pub from_hash: String,
    pub to_hash: String,
}

/// A deployment staged against one catalogue index: the rows it adds and
/// what it answers.
pub struct StagedDeployment {
    expected: CatalogueIndex,
    /// The rows the deployment adds, applied with its admission.
    pub put: CataloguePut,
    pub response: DeploymentResponse,
}

impl StagedDeployment {
    /// The index this deployment was staged against: a host commits it
    /// only while its catalogue still reads the same.
    pub fn expected(&self) -> &CatalogueIndex {
        &self.expected
    }

    /// The bundle this deployment publishes new to the catalogue, if any:
    /// applying it supersedes every other bundle's sessions.
    pub fn new_bundle(&self) -> Option<BundleId> {
        (!self.put.bundles.is_empty())
            .then(|| BundleId::from_hex(&self.response.bundle))
            .flatten()
    }
}

/// The catalogue as the HTTP routes read it, rebuilt from the store's
/// rows after every change.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct CatalogueIndex {
    schemas: HashMap<SchemaHash, Schema>,
    schema_published_at: HashMap<SchemaHash, u64>,
    lens_edges: HashSet<(SchemaHash, SchemaHash)>,
    lenses: HashMap<(SchemaHash, SchemaHash), Lens>,
    /// Every published permissions bundle (see [`jazz::catalogue`]),
    /// oldest first.
    bundles: Vec<CatalogueBundle>,
    /// The admitted schemas, oldest first.
    admitted: Vec<SchemaHash>,
}

impl CatalogueIndex {
    /// Index `rows`. A fixed-schema server's `initial_schema` is known
    /// (and counts as admitted) before any is published.
    pub fn from_rows(rows: &CatalogueRows, initial_schema: Option<&Schema>) -> Self {
        let mut index = Self::default();
        for schema in &rows.schemas {
            index.schemas.insert(schema.hash, schema.schema.clone());
            if let Some(published_at) = schema.published_at {
                index.schema_published_at.insert(schema.hash, published_at);
            }
        }
        for lens in &rows.lenses {
            let lens = Lens::new(lens.source, lens.target, lens.forward.clone());
            if !lens.is_draft() {
                index
                    .lens_edges
                    .insert((lens.source_hash, lens.target_hash));
                index
                    .lenses
                    .insert((lens.source_hash, lens.target_hash), lens);
            }
        }
        index.bundles = rows.bundles.clone();
        index
            .bundles
            .sort_by_key(|bundle| (bundle.published_at, bundle.id.0));
        let mut admitted: Vec<_> = rows
            .schemas
            .iter()
            .filter_map(|schema| Some((schema.admitted?, schema.hash)))
            .collect();
        admitted.sort_by_key(|(n, _)| *n);
        index.admitted = admitted.into_iter().map(|(_, hash)| hash).collect();
        if let Some(schema) = initial_schema {
            // The store's data was written under it too: the deploy guard
            // counts it as admitted (SPEC 4 §4.9), oldest.
            let hash = index.add_schema(schema.clone());
            if !index.admitted.contains(&hash) {
                index.admitted.insert(0, hash);
            }
        }
        index
    }

    fn add_schema(&mut self, schema: Schema) -> SchemaHash {
        let hash = SchemaHash::compute(&schema);
        self.schemas.entry(hash).or_insert(schema);
        hash
    }

    /// Every known schema's hash, in byte order.
    pub fn known_schema_hashes(&self) -> Vec<SchemaHash> {
        let mut hashes = self.schemas.keys().copied().collect::<Vec<_>>();
        hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        hashes
    }

    /// The schema `hash` names, if known.
    pub fn known_schema(&self, hash: &SchemaHash) -> Option<&Schema> {
        self.schemas.get(hash)
    }

    /// Every published (non-draft) lens, in no particular order.
    pub fn known_lenses(&self) -> impl Iterator<Item = &Lens> {
        self.lenses.values()
    }

    /// When `hash` was published, if it was.
    pub fn schema_published_at(&self, hash: &SchemaHash) -> Option<u64> {
        self.schema_published_at.get(hash).copied()
    }

    /// The schema published last (ties broken by hash).
    pub fn latest_published_schema(&self) -> Option<Schema> {
        let mut candidates = self
            .schema_published_at
            .iter()
            .map(|(hash, published_at)| (*published_at, *hash))
            .collect::<Vec<_>>();
        candidates.sort_by(|(left_time, left_hash), (right_time, right_hash)| {
            left_time
                .cmp(right_time)
                .then_with(|| left_hash.as_bytes().cmp(right_hash.as_bytes()))
        });
        let (_, hash) = candidates.pop()?;
        self.schemas.get(&hash).cloned()
    }

    /// Every published permissions bundle, oldest first.
    pub fn published_bundles(&self) -> &[CatalogueBundle] {
        &self.bundles
    }

    /// The permissions bundle published last, if any.
    pub fn latest_bundle(&self) -> Option<BundleId> {
        Some(self.bundles.last()?.id)
    }

    /// The schemas and their migrations, as the graph route answers.
    pub fn migration_graph(&self) -> MigrationGraph {
        let mut schemas: Vec<_> = self.schemas.keys().map(ToString::to_string).collect();
        schemas.sort();
        let mut migrations: Vec<_> = self
            .lenses
            .values()
            .map(|lens| MigrationGraphEdge {
                from_hash: lens.source_hash.to_string(),
                to_hash: lens.target_hash.to_string(),
                automatic: lens.forward.ops.is_empty()
                    && lens.backward.ops.is_empty()
                    && !lens.is_draft()
                    && self
                        .schemas
                        .get(&lens.source_hash)
                        .zip(self.schemas.get(&lens.target_hash))
                        .is_some_and(|(from, to)| {
                            jazz::tools::deployment::schemas_are_compatible(from, to)
                        }),
            })
            .collect();
        migrations.sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        MigrationGraph {
            latest_bundle_schema_hash: self.bundles.last().map(|bundle| bundle.schema.to_string()),
            schemas,
            migrations,
        }
    }

    /// What a deployment is prepared against.
    pub fn deployment_catalogue(&self) -> DeploymentCatalogue {
        DeploymentCatalogue {
            schemas: self.schemas.values().cloned().collect(),
            migrations: self.lenses.values().cloned().collect(),
            admitted_schema_hashes: self.admitted.clone(),
        }
    }

    /// Stage `prepared` as a published bundle, published at `now` (Unix
    /// ms): its schemas, lenses and permissions, activating nothing.
    pub fn stage_deployment(
        &self,
        prepared: &PreparedDeployment,
        now: u64,
    ) -> Result<StagedDeployment, jazz::db::Error> {
        let expected = self.clone();
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
        // `prepare_deployment` refused what a deployment may not introduce
        // (`check_deployable_policies`), as `jazz-tools validate` does.
        let permissions: Permissions = permissions.into_iter().collect();
        let bundle = jazz::catalogue::bundle_id(&schema_hash, &permissions)?;
        let new = !expected.bundles.iter().any(|known| known.id == bundle);
        if new {
            put.bundles.push((schema_hash, permissions, now));
        }
        let changed = new || !published.schemas.is_empty() || !published.migrations.is_empty();
        published.schemas.sort();
        published
            .migrations
            .sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        Ok(StagedDeployment {
            expected,
            put,
            response: DeploymentResponse {
                changed,
                published,
                bundle: bundle.to_hex(),
            },
        })
    }

    #[cfg(all(test, feature = "native"))]
    pub(crate) fn connection_schema_diagnostics(
        &self,
        client_schema_hash: SchemaHash,
    ) -> ConnectionSchemaDiagnostics {
        let active_permissions_hash = self.known_schema_hashes().into_iter().next();
        let reachable_hashes = self.non_draft_reachable_hashes(client_schema_hash);
        let disconnected_permissions_schema_hash = active_permissions_hash
            .filter(|permissions_hash| !reachable_hashes.contains(permissions_hash));

        let mut unreachable_schema_hashes: Vec<_> = self
            .known_schema_hashes()
            .into_iter()
            .filter(|hash| *hash != client_schema_hash)
            .filter(|hash| !reachable_hashes.contains(hash))
            .filter(|hash| Some(*hash) != disconnected_permissions_schema_hash)
            .collect();
        unreachable_schema_hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        ConnectionSchemaDiagnostics {
            client_schema_hash,
            disconnected_permissions_schema_hash,
            unreachable_schema_hashes,
        }
    }

    #[cfg(all(test, feature = "native"))]
    fn non_draft_reachable_hashes(&self, schema_hash: SchemaHash) -> HashSet<SchemaHash> {
        if !self.schemas.contains_key(&schema_hash) {
            return HashSet::new();
        }

        let mut seen = HashSet::from([schema_hash]);
        let mut queue = VecDeque::from([schema_hash]);
        while let Some(current) = queue.pop_front() {
            for &(source, target) in &self.lens_edges {
                let next = if source == current {
                    Some(target)
                } else if target == current {
                    Some(source)
                } else {
                    None
                };
                if let Some(next) = next
                    && seen.insert(next)
                {
                    queue.push_back(next);
                }
            }
        }
        seen
    }
}
