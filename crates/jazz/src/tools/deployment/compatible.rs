//! Compatible definitions keep their existing hashes. Empty lenses connect them
//! for runtime projection; graph validation treats those connections as no change.
use super::*;
use crate::tools::schema_lens::LensTransform;

/// Whether rows can pass unchanged between these definitions. Branch defaults
/// are retained because they determine row identity, unlike ordinary defaults.
pub fn schemas_are_compatible(left: &Schema, right: &Schema) -> bool {
    let normalize = |schema: &Schema| {
        let mut schema = structural(schema.clone());
        for table in schema.values_mut() {
            table.indexed_columns = None;
            table
                .columns
                .columns
                .sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
            for column in &mut table.columns.columns {
                if !table.branch_by.contains(&column.name) {
                    column.default = None;
                }
                column.merge_strategy = None;
            }
        }
        schema
    };
    normalize(left) == normalize(right)
}

pub(super) fn connect_compatible_schemas(
    stored: &DeploymentCatalogue,
    schemas: &BTreeMap<Hash, Schema>,
    compiled: &BTreeMap<Hash, SchemaVersion>,
    migrations: &mut BTreeMap<Edge, Lens>,
    target: Hash,
) -> Result<BTreeMap<Hash, Hash>, DeploymentError> {
    let order = topological_order(schemas, migrations)?;
    let existing = stored
        .schemas
        .iter()
        .map(|s| SchemaHash::compute(s).0)
        .collect::<BTreeSet<_>>();
    let mut groups = Vec::<Vec<Hash>>::new();
    for hash in order {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| schemas_are_compatible(&schemas[&group[0]], &schemas[&hash]))
        {
            group.push(hash);
        } else {
            groups.push(vec![hash]);
        }
    }
    let mut representatives = BTreeMap::new();
    for group in groups {
        for hash in &group {
            representatives.insert(*hash, group[0]);
        }
        let mut anchor = group
            .iter()
            .find(|h| existing.contains(*h))
            .copied()
            .or_else(|| {
                group
                    .iter()
                    .find(|h| {
                        migrations
                            .keys()
                            .any(|(from, to)| to == *h && !group.contains(from))
                    })
                    .copied()
            })
            .or_else(|| group.iter().find(|h| **h != target).copied())
            .unwrap_or(group[0]);
        let mut admitted = group
            .iter()
            .copied()
            .filter(|h| existing.contains(h))
            .collect::<Vec<_>>();
        admitted.push(anchor);
        let pending = group
            .iter()
            .copied()
            .filter(|h| !existing.contains(h) && *h != anchor)
            .collect::<Vec<_>>();
        for hash in pending {
            // A metadata-only revision of an existing runtime version needs no
            // new runtime predecessor (and must not create a runtime cycle).
            let source = admitted
                .iter()
                .find(|h| compiled[*h].id == compiled[&hash].id)
                .copied()
                .unwrap_or(anchor);
            if !connected(source, hash, migrations) && !connected(hash, source, migrations) {
                migrations.insert(
                    (source, hash),
                    Lens::new(SchemaHash(source), SchemaHash(hash), LensTransform::new()),
                );
            }
            admitted.push(hash);
            anchor = hash;
        }
    }
    Ok(representatives)
}

fn connected(from: Hash, to: Hash, migrations: &BTreeMap<Edge, Lens>) -> bool {
    let mut pending = vec![from];
    let mut seen = BTreeSet::new();
    while let Some(hash) = pending.pop() {
        if hash == to {
            return true;
        }
        if seen.insert(hash) {
            pending.extend(
                migrations
                    .keys()
                    .filter_map(|(source, target)| (*source == hash).then_some(*target)),
            );
        }
    }
    false
}

/// Ignore compatible steps when checking structural forward progress. No new
/// identifiers are created or stored: representatives are ordinary schema hashes.
pub(super) fn validate_compatible_graph(
    schemas: &BTreeMap<Hash, Schema>,
    migrations: &BTreeMap<Edge, Lens>,
    representatives: &BTreeMap<Hash, Hash>,
    stored: &DeploymentCatalogue,
    target: SchemaHash,
) -> Result<(), DeploymentError> {
    let nodes = representatives
        .values()
        .map(|h| (*h, schemas[h].clone()))
        .collect::<BTreeMap<_, _>>();
    let mut edges = BTreeMap::<Edge, Lens>::new();
    for ((from, to), lens) in migrations {
        let edge = (representatives[from], representatives[to]);
        if edge.0 == edge.1 {
            if !lens.forward.ops.is_empty() || !lens.backward.ops.is_empty() || lens.is_draft() {
                return Err(DeploymentError::InvalidMigration {
                    from: SchemaHash(*from),
                    to: SchemaHash(*to),
                    reason: "compatible schema revisions require an identity lens".into(),
                });
            }
            continue;
        }
        if let Some(previous) = edges.get(&edge) {
            let projection = |lens: &Lens| {
                project(identity(&schemas[&lens.source_hash.0]), lens).map_err(|reason| {
                    DeploymentError::InvalidMigration {
                        from: lens.source_hash,
                        to: lens.target_hash,
                        reason,
                    }
                })
            };
            if projection(previous)? != projection(lens)? {
                return Err(DeploymentError::ConflictingPaths {
                    first: vec![previous.source_hash, previous.target_hash],
                    second: vec![lens.source_hash, lens.target_hash],
                });
            }
        } else {
            edges.insert(edge, lens.clone());
        }
    }
    let order = topological_order(&nodes, &edges)?;
    if let Some(active) = stored.active_schema_hash {
        let from = representatives[&active.0];
        let to = representatives[&target.0];
        let previously_published = stored
            .schemas
            .iter()
            .any(|s| SchemaHash::compute(s) == target);
        if !(connected(from, to, &edges) || previously_published && connected(to, from, &edges)) {
            return Err(DeploymentError::UnreachableTarget { active, target });
        }
    }
    // Keep a single connected history, but allow multiple terminal branches.
    // In a DAG, a unique root reaches every node.
    let roots = nodes
        .keys()
        .filter(|hash| !edges.keys().any(|(_, to)| to == *hash))
        .copied()
        .map(SchemaHash)
        .collect::<Vec<_>>();
    if roots.len() != 1 {
        return Err(DeploymentError::DisconnectedGraph { roots });
    }
    validate_parallel_paths(&order, &nodes, &edges)
}
