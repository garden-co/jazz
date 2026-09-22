//! Read-only preparation for a complete deployment. Callers must prepare against
//! current stored state under the same serialization boundary as the eventual
//! commit. A prepared value is not a concurrency token or a publication receipt.

mod runtime;
pub use runtime::prepare_runtime_snapshot;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::node::migration_validation::{
    validate_lineage_table_partition, validate_migration_lens_between,
};
use crate::protocol::SchemaVersion;
use crate::schema::JazzSchema;
use crate::tools::public_schema::{Schema, SchemaHash, TableName, TablePolicies, Value};
use crate::tools::schema_lens::{Lens, LensOp, runtime::compile_lens};

type Hash = [u8; 32];
type Edge = (Hash, Hash);

/// A consistent view of the authority catalogue, or an empty offline catalogue.
#[derive(Debug, Default)]
pub struct DeploymentCatalogue {
    pub schemas: Vec<Schema>,
    pub migrations: Vec<Lens>,
    pub active_schema_hash: Option<SchemaHash>,
}

/// Missing artifacts plus the requested target and its complete permission set.
/// Empty permissions explicitly deny all access; embedded schema policies are ignored.
#[derive(Debug)]
pub struct DeploymentRequest {
    pub target_schema_hash: SchemaHash,
    pub schemas: Vec<(SchemaHash, Schema)>,
    pub migrations: Vec<Lens>,
    pub permissions: HashMap<TableName, TablePolicies>,
}

/// Validated combined history, ordered so parents precede children. This does not
/// allocate physical identities, persist artifacts, or activate a schema.
#[derive(Debug)]
pub struct PreparedDeployment {
    schemas: Vec<Schema>,
    migrations: Vec<Lens>,
    active_schema: JazzSchema,
}

impl PreparedDeployment {
    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }
    pub fn migrations(&self) -> &[Lens] {
        &self.migrations
    }
    pub fn active_schema(&self) -> &JazzSchema {
        &self.active_schema
    }
}

/// Stable error categories and schema paths for the future HTTP/binding adapters.
/// These are in-memory types, not a storage or wire encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeploymentError {
    #[error("schema {claimed} does not match its content hash {actual}")]
    SchemaHashMismatch {
        claimed: SchemaHash,
        actual: SchemaHash,
    },
    #[error("missing schema {schema}")]
    MissingSchema { schema: SchemaHash },
    #[error("invalid schema {schema}: {reason}")]
    InvalidSchema { schema: SchemaHash, reason: String },
    #[error("invalid permissions for {schema}: {reason}")]
    InvalidPermissions { schema: SchemaHash, reason: String },
    #[error("conflicting migrations from {from} to {to}")]
    ConflictingMigration { from: SchemaHash, to: SchemaHash },
    #[error("invalid migration from {from} to {to}: {reason}")]
    InvalidMigration {
        from: SchemaHash,
        to: SchemaHash,
        reason: String,
    },
    #[error("migration graph contains a cycle; blocked schemas: {}", display_hashes(.schemas, ", "))]
    Cycle { schemas: Vec<SchemaHash> },
    #[error(
        "every schema must reach target {target} through forward migrations; connect terminal schemas {} to the target", display_hashes(.tips, ", ")
    )]
    NonConvergent {
        target: SchemaHash,
        tips: Vec<SchemaHash>,
    },
    #[error("migration paths disagree: {} and {}", display_hashes(.first, " -> "), display_hashes(.second, " -> "))]
    ConflictingPaths {
        first: Vec<SchemaHash>,
        second: Vec<SchemaHash>,
    },
}

fn display_hashes(hashes: &[SchemaHash], separator: &str) -> String {
    hashes
        .iter()
        .map(SchemaHash::to_hex)
        .collect::<Vec<_>>()
        .join(separator)
}

fn structural(mut schema: Schema) -> Schema {
    for table in schema.values_mut() {
        table.policies = TablePolicies::default();
    }
    schema
}

/// Validate stored + submitted history without performing any I/O or mutation.
pub fn prepare_deployment(
    stored: &DeploymentCatalogue,
    request: DeploymentRequest,
) -> Result<PreparedDeployment, DeploymentError> {
    let mut schemas = BTreeMap::new();
    for schema in &stored.schemas {
        schemas.insert(SchemaHash::compute(schema).0, structural(schema.clone()));
    }
    for (claimed, schema) in request.schemas {
        let actual = SchemaHash::compute(&schema);
        if actual != claimed {
            return Err(DeploymentError::SchemaHashMismatch { claimed, actual });
        }
        schemas.insert(actual.0, structural(schema));
    }
    for hash in [Some(request.target_schema_hash), stored.active_schema_hash]
        .into_iter()
        .flatten()
    {
        if !schemas.contains_key(&hash.0) {
            return Err(DeploymentError::MissingSchema { schema: hash });
        }
    }
    let compiled = schemas
        .iter()
        .map(|(hash, schema)| {
            JazzSchema::new(schema)
                .map(|schema| (*hash, SchemaVersion::new(schema)))
                .map_err(|error| DeploymentError::InvalidSchema {
                    schema: SchemaHash(*hash),
                    reason: error.to_string(),
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut migrations: BTreeMap<Edge, Lens> = BTreeMap::new();
    for lens in stored.migrations.iter().chain(&request.migrations) {
        for hash in [lens.source_hash, lens.target_hash] {
            if !schemas.contains_key(&hash.0) {
                return Err(DeploymentError::MissingSchema { schema: hash });
            }
        }
        let edge = (lens.source_hash.0, lens.target_hash.0);
        if let Some(previous) = migrations.get(&edge) {
            if previous.forward.ops != lens.forward.ops
                || previous.backward.ops != lens.backward.ops
                || previous.forward.draft_ops != lens.forward.draft_ops
                || previous.backward.draft_ops != lens.backward.draft_ops
            {
                return Err(DeploymentError::ConflictingMigration {
                    from: lens.source_hash,
                    to: lens.target_hash,
                });
            }
        } else {
            migrations.insert(edge, lens.clone());
        }
    }
    let order = topological_order(&schemas, &migrations)?;
    let tips = schemas
        .keys()
        .filter(|hash| !migrations.keys().any(|(from, _)| from == *hash))
        .copied()
        .map(SchemaHash)
        .collect::<Vec<_>>();
    if tips != [request.target_schema_hash] {
        return Err(DeploymentError::NonConvergent {
            target: request.target_schema_hash,
            tips: tips
                .into_iter()
                .filter(|hash| *hash != request.target_schema_hash)
                .collect(),
        });
    }
    for ((from, to), lens) in &migrations {
        let check = || -> Result<(), String> {
            if lens.is_draft() {
                return Err("migration contains unreviewed draft operations".into());
            }
            // Public deployment supports auto-invertible lenses only. Historical
            // custom inverses must not silently lose their semantics during preparation.
            if lens.backward.ops != lens.forward.invert().ops {
                return Err("custom backward transforms are not supported by deployment".into());
            }
            let runtime_lens = compile_lens(lens, &schemas[from], &schemas[to])?;
            let new_tables = lens
                .forward
                .ops
                .iter()
                .filter_map(|op| match op {
                    LensOp::AddTable { table, .. } => Some(table.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let dropped_tables = lens
                .forward
                .ops
                .iter()
                .filter_map(|op| match op {
                    LensOp::RemoveTable { table, .. } => Some(table.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            validate_migration_lens_between(&runtime_lens, &compiled[from], &compiled[to])
                .map_err(|error| error.to_string())?;
            validate_lineage_table_partition(
                &compiled[from].schema,
                &compiled[to].schema,
                &runtime_lens,
                &new_tables,
                &dropped_tables,
            )
            .map_err(|error| error.to_string())?;
            validate_public_operations(lens, &schemas[from], &schemas[to])?;
            Ok(())
        };
        check().map_err(|reason| DeploymentError::InvalidMigration {
            from: SchemaHash(*from),
            to: SchemaHash(*to),
            reason,
        })?;
    }
    validate_parallel_paths(&order, &schemas, &migrations)?;
    let mut target = schemas[&request.target_schema_hash.0].clone();
    for (name, policies) in request.permissions {
        let table = target
            .get_mut(&name)
            .ok_or_else(|| DeploymentError::InvalidPermissions {
                schema: request.target_schema_hash,
                reason: format!("unknown table {name}"),
            })?;
        table.policies = policies;
    }
    let active_schema =
        JazzSchema::new(&target).map_err(|error| DeploymentError::InvalidPermissions {
            schema: request.target_schema_hash,
            reason: error.to_string(),
        })?;
    let rank = order
        .iter()
        .enumerate()
        .map(|(i, hash)| (*hash, i))
        .collect::<BTreeMap<_, _>>();
    let mut migrations = migrations.into_values().collect::<Vec<_>>();
    migrations.sort_by_key(|lens| (rank[&lens.target_hash.0], rank[&lens.source_hash.0]));
    Ok(PreparedDeployment {
        schemas: order
            .into_iter()
            .map(|hash| schemas.remove(&hash).unwrap())
            .collect(),
        migrations,
        active_schema,
    })
}

fn topological_order(
    schemas: &BTreeMap<Hash, Schema>,
    migrations: &BTreeMap<Edge, Lens>,
) -> Result<Vec<Hash>, DeploymentError> {
    let mut incoming = schemas
        .keys()
        .map(|hash| (*hash, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut children: BTreeMap<Hash, Vec<Hash>> = BTreeMap::new();
    for (from, to) in migrations.keys() {
        *incoming.get_mut(to).unwrap() += 1;
        children.entry(*from).or_default().push(*to);
    }
    let mut ready = incoming
        .iter()
        .filter_map(|(hash, count)| (*count == 0).then_some(*hash))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::new();
    while let Some(hash) = ready.pop_first() {
        order.push(hash);
        for child in children.get(&hash).into_iter().flatten() {
            let count = incoming.get_mut(child).unwrap();
            *count -= 1;
            if *count == 0 {
                ready.insert(*child);
            }
        }
    }
    if order.len() != schemas.len() {
        return Err(DeploymentError::Cycle {
            schemas: incoming
                .into_iter()
                .filter_map(|(hash, count)| (count > 0).then_some(SchemaHash(hash)))
                .collect(),
        });
    }
    Ok(order)
}

// Runtime validation works on lowered operations. Check the public-only type
// declarations and defaults too, so lowering cannot discard invalid metadata.
fn validate_public_operations(lens: &Lens, source: &Schema, target: &Schema) -> Result<(), String> {
    for op in &lens.forward.ops {
        match op {
            LensOp::AddColumn {
                table,
                column,
                column_type,
                default,
            }
            | LensOp::RemoveColumn {
                table,
                column,
                column_type,
                default,
            } => {
                let schema = if matches!(op, LensOp::AddColumn { .. }) {
                    target
                } else {
                    source
                };
                let mut with_default = schema.clone();
                // Renamed table operations use the destination name in the public API.
                let name = if schema.contains_key(&TableName::from(table.as_str())) {
                    table
                } else {
                    lens.forward
                        .ops
                        .iter()
                        .find_map(|op| match op {
                            LensOp::RenameTable { old_name, new_name } if new_name == table => {
                                Some(old_name)
                            }
                            _ => None,
                        })
                        .unwrap_or(table)
                };
                let descriptor = with_default
                    .get_mut(&TableName::from(name.as_str()))
                    .and_then(|table| {
                        table
                            .columns
                            .columns
                            .iter_mut()
                            .find(|candidate| candidate.name.as_str() == column)
                    })
                    .ok_or_else(|| format!("unknown column {table}.{column}"))?;
                if &descriptor.column_type != column_type {
                    return Err(format!("column type does not match {table}.{column}"));
                }
                descriptor.default = Some(default.clone());
                JazzSchema::new(&with_default).map_err(|error| error.to_string())?;
            }
            LensOp::AddTable { table, schema } | LensOp::RemoveTable { table, schema } => {
                let endpoint = if matches!(op, LensOp::AddTable { .. }) {
                    target
                } else {
                    source
                };
                let mut declared = schema.clone();
                declared.policies = TablePolicies::default();
                if endpoint.get(&TableName::from(table.as_str())) != Some(&declared) {
                    return Err(format!("table declaration does not match {table}"));
                }
            }
            LensOp::RenameTable { .. } | LensOp::RenameColumn { .. } => {}
        }
    }
    // Also rejects operations that lowering could otherwise silently ignore.
    project(identity(source), lens)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ColumnOrigin {
    Source(String),
    Default(Value),
    NewTable,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableProjection {
    source: Option<String>,
    columns: BTreeMap<String, ColumnOrigin>,
}
type Projection = BTreeMap<String, TableProjection>;

fn identity(schema: &Schema) -> Projection {
    schema
        .iter()
        .map(|(name, table)| {
            (
                name.as_str().to_owned(),
                TableProjection {
                    source: Some(name.as_str().to_owned()),
                    columns: table
                        .columns
                        .columns
                        .iter()
                        .map(|column| {
                            (
                                column.name.as_str().to_owned(),
                                ColumnOrigin::Source(column.name.as_str().to_owned()),
                            )
                        })
                        .collect(),
                },
            )
        })
        .collect()
}

fn project(mut state: Projection, lens: &Lens) -> Result<Projection, String> {
    for op in &lens.forward.ops {
        match op {
            LensOp::RenameTable { old_name, new_name } => {
                if state.contains_key(new_name) {
                    return Err(format!("table rename collides with {new_name}"));
                }
                let table = state
                    .remove(old_name)
                    .ok_or_else(|| format!("unknown table {old_name}"))?;
                state.insert(new_name.clone(), table);
            }
            LensOp::AddTable { table, schema } => {
                if state.contains_key(table) {
                    return Err(format!("added table already exists: {table}"));
                }
                state.insert(
                    table.clone(),
                    TableProjection {
                        source: None,
                        columns: schema
                            .columns
                            .columns
                            .iter()
                            .map(|column| (column.name.as_str().to_owned(), ColumnOrigin::NewTable))
                            .collect(),
                    },
                );
            }
            LensOp::RemoveTable { table, .. } => {
                state
                    .remove(table)
                    .ok_or_else(|| format!("unknown table {table}"))?;
            }
            LensOp::AddColumn {
                table,
                column,
                default,
                ..
            } => {
                let projected = state
                    .get_mut(table)
                    .ok_or_else(|| format!("unknown table {table}"))?;
                let origin = if projected.source.is_some() {
                    ColumnOrigin::Default(default.clone())
                } else {
                    ColumnOrigin::NewTable
                };
                if projected.columns.insert(column.clone(), origin).is_some() {
                    return Err(format!("added column already exists: {table}.{column}"));
                }
            }
            LensOp::RemoveColumn { table, column, .. } => {
                state
                    .get_mut(table)
                    .and_then(|table| table.columns.remove(column))
                    .ok_or_else(|| format!("unknown column {table}.{column}"))?;
            }
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => {
                let columns = &mut state
                    .get_mut(table)
                    .ok_or_else(|| format!("unknown table {table}"))?
                    .columns;
                if columns.contains_key(new_name) {
                    return Err(format!("column rename collides with {table}.{new_name}"));
                }
                let origin = columns
                    .remove(old_name)
                    .ok_or_else(|| format!("unknown column {table}.{old_name}"))?;
                columns.insert(new_name.clone(), origin);
            }
        }
    }
    Ok(state)
}

fn validate_parallel_paths(
    order: &[Hash],
    schemas: &BTreeMap<Hash, Schema>,
    migrations: &BTreeMap<Edge, Lens>,
) -> Result<(), DeploymentError> {
    // One symbolic projection per reachable node and origin, not an enumeration
    // of paths. Source tokens distinguish rename/preservation from drop+add even
    // when the final column names, types and defaults are identical.
    for origin in order {
        let mut projections = BTreeMap::from([(
            *origin,
            (identity(&schemas[origin]), vec![SchemaHash(*origin)]),
        )]);
        for from in order {
            let Some((state, path)) = projections.get(from).cloned() else {
                continue;
            };
            for ((source, to), lens) in migrations {
                if source != from {
                    continue;
                }
                let next = project(state.clone(), lens).map_err(|reason| {
                    DeploymentError::InvalidMigration {
                        from: SchemaHash(*from),
                        to: SchemaHash(*to),
                        reason,
                    }
                })?;
                let mut next_path = path.clone();
                next_path.push(SchemaHash(*to));
                if let Some((previous, previous_path)) = projections.get(to) {
                    if previous != &next {
                        return Err(DeploymentError::ConflictingPaths {
                            first: previous_path.clone(),
                            second: next_path,
                        });
                    }
                } else {
                    projections.insert(*to, (next, next_path));
                }
            }
        }
    }
    Ok(())
}
