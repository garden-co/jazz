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
    let mut incoming = BTreeMap::<SchemaVersionId, Vec<(SchemaVersionId, MigrationLens)>>::new();
    for migration in &prepared.migrations {
        let source = hashes[&migration.source_hash];
        let target = hashes[&migration.target_hash];
        if source.version_id() == target.version_id() {
            continue;
        }
        let lens = compile_lens(migration, source.public_schema(), target.public_schema())
            .map_err(|error| error.to_string())?;
        incoming
            .entry(target.version_id())
            .or_default()
            .push((source.version_id(), lens));
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
    // The stored lenses, compiled, by version edge: another hash of an
    // admitted predecessor (defaults and indexes do not change the version)
    // is the same edge, and must carry the same lens.
    let stored_schemas = stored
        .schemas
        .iter()
        .map(|schema| (schema.hash, &schema.schema))
        .collect::<HashMap<_, _>>();
    let mut known = BTreeMap::<(SchemaVersionId, SchemaVersionId), Vec<MigrationLens>>::new();
    for lens in &stored.lenses {
        let (Some(source), Some(target)) = (
            stored_schemas.get(&lens.source),
            stored_schemas.get(&lens.target),
        ) else {
            continue;
        };
        let edge = (stored_versions[&lens.source], stored_versions[&lens.target]);
        if edge.0 == edge.1 {
            continue;
        }
        let compiled = compile_lens(
            &Lens::new(lens.source, lens.target, lens.forward.clone()),
            source,
            target,
        )
        .map_err(|error| format!("stored lens {} -> {}: {error}", lens.source, lens.target))?;
        known.entry(edge).or_default().push(compiled);
    }

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
            for (source, lens) in &incoming[&id] {
                let Some(stored) = known.get(&(*source, id)) else {
                    return Err(format!(
                        "schema {} is already published with different predecessors; publish a new convergent target",
                        SchemaHash::compute(schema.public_schema())
                    ));
                };
                if !stored.contains(lens) {
                    return Err(format!(
                        "schema {} is already published with a different lens from that predecessor; publish a new convergent target",
                        SchemaHash::compute(schema.public_schema())
                    ));
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::{CatalogueLens, CatalogueSchema};
    use crate::tools::public_schema::{ColumnType, SchemaBuilder, TableSchema};
    use crate::tools::schema_lens::{LensOp, LensTransform};

    fn notes(extra: Option<&str>, indexed: bool) -> Schema {
        let mut table = TableSchema::builder("notes").column("title", ColumnType::Text);
        if let Some(column) = extra {
            table = table.column(column, ColumnType::Text);
        }
        if indexed {
            table = table.index_only(["title"]);
        }
        SchemaBuilder::new().table(table).build()
    }

    fn adds_a(default: &str) -> LensTransform {
        LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "notes".into(),
            column: "a".into(),
            column_type: ColumnType::Text,
            default: Value::Text(default.into()),
        }])
    }

    /// A store that admitted `a -> b`, and a deployment that also brings
    /// `indexed -> b` (`indexed` is another hash of `a`'s version) adding `a`
    /// with `default`.
    fn redeploy_from_an_equivalent_hash(default: &str) -> Result<CatalogueChange, String> {
        let (a, b, indexed) = (
            notes(None, false),
            notes(Some("a"), false),
            notes(None, true),
        );
        let hash = SchemaHash::compute;
        let admitted = |schema: &Schema, n: u64| CatalogueSchema {
            hash: hash(schema),
            admitted: Some(n),
            version: Some(JazzSchema::new(schema).unwrap().version_id()),
            published_at: Some(1),
            schema: schema.clone(),
        };
        let stored = CatalogueRows {
            schemas: vec![admitted(&a, 0), admitted(&b, 1)],
            lenses: vec![CatalogueLens {
                source: hash(&a),
                target: hash(&b),
                published_at: 1,
                forward: adds_a("default"),
            }],
            permissions: Vec::new(),
        };
        let prepared = PreparedDeployment {
            schemas: vec![a.clone(), b.clone(), indexed.clone()],
            migrations: vec![
                Lens::new(hash(&a), hash(&b), adds_a("default")),
                Lens::new(hash(&indexed), hash(&b), adds_a(default)),
            ],
            active_schema: JazzSchema::new(&b).unwrap(),
        };
        prepare_catalogue_change(&prepared, &stored, CataloguePut::default(), 2)
    }

    #[test]
    fn an_equal_lens_from_an_equivalent_predecessor_hash_is_accepted() {
        redeploy_from_an_equivalent_hash("default").unwrap();
    }

    #[test]
    fn a_different_lens_from_an_equivalent_predecessor_hash_is_refused() {
        let error = redeploy_from_an_equivalent_hash("other").unwrap_err();
        assert!(
            error.contains("different lens from that predecessor"),
            "{error}"
        );
    }
}
