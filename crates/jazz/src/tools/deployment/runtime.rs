//! Build the complete runtime catalogue without installing any of its entries.
use super::*;
use crate::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, PhysicalIdentityManifest, SchemaLineagePublication,
    SchemaPredecessor,
};

/// Allocate a complete candidate snapshot from validated public artifacts.
/// Existing schema publications are immutable: adding predecessors to an
/// already admitted target requires publishing a new convergent target instead.
pub fn prepare_runtime_snapshot(
    prepared: &PreparedDeployment,
    current: Option<CatalogueSnapshot>,
    revision: u64,
) -> Result<CatalogueSnapshot, String> {
    let all_versions = prepared
        .schemas
        .iter()
        .map(|schema| {
            JazzSchema::new(schema)
                .map(SchemaVersion::new)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Defaults and indexes share runtime identity, while public content hashes
    // retain the exact definition. They must not create duplicate runtime entries.
    let mut seen = BTreeSet::new();
    let versions = all_versions
        .iter()
        .filter(|v| seen.insert(v.id))
        .cloned()
        .collect::<Vec<_>>();
    let root = versions.first().ok_or("deployment has no schema")?;
    let hashes = prepared
        .schemas
        .iter()
        .map(SchemaHash::compute)
        .zip(all_versions.iter())
        .collect::<HashMap<_, _>>();
    let mut incoming = BTreeMap::<_, Vec<SchemaPredecessor>>::new();
    for migration in &prepared.migrations {
        let source = hashes[&migration.source_hash];
        let target = hashes[&migration.target_hash];
        if source.id == target.id {
            continue;
        }
        let lens = compile_lens(
            migration,
            source.schema.public_schema(),
            target.schema.public_schema(),
        )
        .map_err(|error| error.to_string())?;
        let new_tables = migration
            .forward
            .ops
            .iter()
            .filter_map(|op| match op {
                LensOp::AddTable { table, .. } => Some(table.to_string()),
                _ => None,
            })
            .collect();
        let dropped_tables = migration
            .forward
            .ops
            .iter()
            .filter_map(|op| match op {
                LensOp::RemoveTable { table, .. } => Some(table.to_string()),
                _ => None,
            })
            .collect();
        incoming
            .entry(target.id)
            .or_default()
            .push(SchemaPredecessor {
                lens,
                new_tables,
                dropped_tables,
            });
    }
    if versions
        .iter()
        .filter(|schema| !incoming.contains_key(&schema.id))
        .count()
        != 1
    {
        return Err("deployment must have exactly one genesis schema".into());
    }
    let mut snapshot = match current {
        Some(snapshot) => {
            if snapshot
                .schemas
                .iter()
                .any(|schema| !versions.iter().any(|new| new.id == schema.id))
            {
                return Err("deployment cannot omit a runtime schema".into());
            }
            let targets = snapshot
                .lineages
                .iter()
                .map(|(_, p)| p.schema.id)
                .collect::<BTreeSet<_>>();
            let roots = snapshot
                .schemas
                .iter()
                .filter(|schema| !targets.contains(&schema.id))
                .collect::<Vec<_>>();
            if roots.len() != 1 || roots[0].id != root.id {
                return Err("deployment cannot replace the runtime genesis schema".into());
            }
            snapshot
        }
        None => CatalogueSnapshot {
            genesis_physical_identities: PhysicalIdentityManifest::allocate(&root.schema),
            schemas: vec![root.clone()],
            lineages: vec![],
            current_write_schema: CurrentWriteSchema {
                revision: 0,
                schema: root.id,
            },
        },
    };
    let mut sources = BTreeMap::from([(
        root.id,
        (
            root.schema.clone(),
            snapshot.genesis_physical_identities.clone(),
        ),
    )]);
    let mut history = vec![snapshot.genesis_physical_identities.clone()];
    history.extend(
        snapshot
            .lineages
            .iter()
            .map(|(_, p)| p.physical_identities.clone()),
    );
    for schema in versions.iter().skip(1) {
        let mut predecessors = incoming
            .remove(&schema.id)
            .ok_or("missing schema predecessors")?;
        predecessors.sort_by_key(|p| p.lens.source());
        predecessors.dedup();
        let identities = if let Some((_, existing)) = snapshot
            .lineages
            .iter()
            .find(|(_, p)| p.schema.id == schema.id)
        {
            let mut stored = existing.predecessors.clone();
            stored.sort_by_key(|p| p.lens.source());
            if stored != predecessors {
                return Err(format!(
                    "schema {} is already published with different predecessors; publish a new convergent target",
                    SchemaHash::compute(schema.schema.public_schema())
                ));
            }
            existing.physical_identities.clone()
        } else {
            let publication = SchemaLineagePublication::author_from_predecessors(
                schema.clone(),
                predecessors,
                &sources,
                history.clone(),
            )
            .map_err(str::to_owned)?;
            let identities = publication.physical_identities.clone();
            let sequence = snapshot
                .lineages
                .iter()
                .map(|(seq, _)| *seq)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or("catalogue sequence overflow")?;
            snapshot.lineages.push((sequence, publication));
            history.push(identities.clone());
            identities
        };
        sources.insert(schema.id, (schema.schema.clone(), identities));
    }
    snapshot.schemas = versions;
    let active = SchemaVersion::new(prepared.active_schema.clone());
    *snapshot
        .schemas
        .iter_mut()
        .find(|schema| schema.id == active.id)
        .ok_or("active schema missing")? = active.clone();
    snapshot.current_write_schema = CurrentWriteSchema {
        revision,
        schema: active.id,
    };
    Ok(snapshot)
}
