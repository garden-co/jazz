//! Plan a deployment's admission into the store's catalogue, without applying it.
use super::*;
use crate::catalogue::{CatalogueChange, CataloguePut, CatalogueRows};
use crate::ids::SchemaVersionId;
use crate::protocol::MigrationLens;

/// The change that applies `prepared` to a store holding `stored`: `put`
/// (the deployment's new rows), every version the store has not admitted
/// yet in lineage order with the lenses into it, then the write schema
/// made current at `revision`. Admitted versions are immutable: a new lens
/// into one needs a new convergent target instead.
pub fn prepare_catalogue_change(
    prepared: &PreparedDeployment,
    stored: &CatalogueRows,
    put: CataloguePut,
    revision: u64,
) -> Result<CatalogueChange, String> {
    let compiled = prepared
        .schemas
        .iter()
        .map(|schema| JazzSchema::new(schema).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let hashes = prepared
        .schemas
        .iter()
        .map(SchemaHash::compute)
        .zip(compiled.iter())
        .collect::<HashMap<_, _>>();
    // Defaults and indexes share runtime identity, while public content hashes
    // retain the exact definition. They must not admit one version twice.
    let mut seen = BTreeSet::new();
    let versions = compiled
        .iter()
        .filter(|schema| seen.insert(schema.version_id()))
        .collect::<Vec<_>>();
    let mut incoming = BTreeMap::<SchemaVersionId, Vec<(Edge, MigrationLens)>>::new();
    for migration in &prepared.migrations {
        let source = hashes[&migration.source_hash];
        let target = hashes[&migration.target_hash];
        if source.version_id() == target.version_id() {
            continue;
        }
        let lens = compile_lens(migration, source.public_schema(), target.public_schema())
            .map_err(|error| error.to_string())?;
        incoming.entry(target.version_id()).or_default().push((
            (
                *migration.source_hash.as_bytes(),
                *migration.target_hash.as_bytes(),
            ),
            lens,
        ));
    }
    let roots = versions
        .iter()
        .filter(|schema| !incoming.contains_key(&schema.version_id()))
        .collect::<Vec<_>>();
    let [root] = roots[..] else {
        return Err("deployment must have exactly one genesis schema".into());
    };

    let version_of = |schema: &Schema| {
        JazzSchema::new(schema)
            .map(|schema| schema.version_id())
            .map_err(|error| error.to_string())
    };
    let admitted = stored
        .schemas
        .iter()
        .filter_map(|schema| schema.version)
        .collect::<BTreeSet<_>>();
    if admitted
        .iter()
        .any(|id| !versions.iter().any(|schema| schema.version_id() == *id))
    {
        return Err("deployment cannot omit a runtime schema".into());
    }
    let stored_versions = stored
        .schemas
        .iter()
        .map(|schema| Ok((schema.hash, version_of(&schema.schema)?)))
        .collect::<Result<HashMap<_, _>, String>>()?;
    let mut targets = BTreeSet::new();
    for lens in &stored.lenses {
        if let (Some(source), Some(target)) = (
            stored_versions.get(&lens.source),
            stored_versions.get(&lens.target),
        ) && source != target
        {
            targets.insert(*target);
        }
    }
    let stored_roots = admitted
        .iter()
        .filter(|id| !targets.contains(id))
        .collect::<Vec<_>>();
    if !admitted.is_empty() && stored_roots != [&root.version_id()] {
        return Err("deployment cannot replace the runtime genesis schema".into());
    }
    let known = stored
        .lenses
        .iter()
        .map(|lens| (*lens.source.as_bytes(), *lens.target.as_bytes()))
        .collect::<BTreeSet<_>>();

    let lenses_into = |id: SchemaVersionId| {
        let mut lenses = incoming
            .get(&id)
            .into_iter()
            .flatten()
            .map(|(_, lens)| lens.clone())
            .collect::<Vec<_>>();
        lenses.sort_by_key(|lens| lens.source());
        lenses.dedup();
        lenses
    };
    let mut admit = Vec::new();
    for schema in versions
        .iter()
        .filter(|schema| schema.version_id() != root.version_id())
    {
        let id = schema.version_id();
        if admitted.contains(&id) {
            if incoming[&id].iter().any(|(edge, _)| !known.contains(edge)) {
                return Err(format!(
                    "schema {} is already published with different predecessors; publish a new convergent target",
                    SchemaHash::compute(schema.public_schema())
                ));
            }
            continue;
        }
        admit.push(((*schema).clone(), lenses_into(id)));
    }
    let active = prepared.active_schema.clone();
    let lenses = lenses_into(active.version_id());
    Ok(CatalogueChange {
        put,
        admit,
        activate: Some((revision, active, lenses)),
    })
}
