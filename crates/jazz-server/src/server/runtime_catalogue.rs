//! Bridge the administrative catalogue into the WebSocket-serving core shell.

use std::collections::{BTreeMap, HashMap, HashSet};

use jazz::groove::records::Value as CoreValue;
use jazz::protocol::{LensOp as CoreLensOp, MigrationLens, TableLens};
use jazz::schema::JazzSchema;

use jazz::tools::public_schema::{Schema, SchemaHash, TableName, Value};
use jazz::tools::schema_lens::{Lens, LensOp};

use super::catalogue::{ActiveSchemaSummary, CatalogueError};

use super::{ServerRuntimeHandle, ServerState};

/// Publish newly admitted catalogue entries into the active runtime shell.
///
/// The caller persists administrative entries first. This shared bridge then
/// uses the first schema to bootstrap an absent runtime, admits later schemas
/// atomically with lineage lenses, and applies the current permissions head
/// last so it alone selects the write schema.
pub(crate) async fn publish_runtime_catalogue(
    state: &ServerState,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), String> {
    #[cfg(test)]
    state.run_runtime_catalogue_before_publication_hook_for_test();
    // A bridge re-reads the durable permissions head before queueing the shell
    // update. Keep the read and queued update ordered with every other bridge:
    // otherwise a later head can install first and then be overwritten by this
    // older bridge.
    let _publication = state.runtime_catalogue_publication.lock().await;
    publish_runtime_catalogue_locked(state, schemas, lenses).await
}

pub(crate) enum PermissionsPublicationError {
    Catalogue(CatalogueError),
    LineageUnavailable(String),
    Bridge(String),
}

pub(crate) async fn publish_permissions_and_runtime(
    state: &ServerState,
    schema_hash: SchemaHash,
    permissions: HashMap<TableName, jazz::tools::public_schema::TablePolicies>,
    expected_parent_bundle_object_id: Option<jazz::tools::ObjectId>,
) -> Result<ActiveSchemaSummary, PermissionsPublicationError> {
    #[cfg(test)]
    state.run_runtime_catalogue_before_publication_hook_for_test();
    let _publication = state.runtime_catalogue_publication.lock().await;

    let runtime_enabled =
        state.runtime().is_some() || state.core_server_shell_storage_config.is_some();
    let mut shell = state.runtime();
    if runtime_enabled {
        ensure_structural_lineage_locked(state, schema_hash, &HashMap::new(), &[], &mut shell)
            .await
            .map_err(|error| match error {
                StructuralLineageError::Unavailable(message) => {
                    PermissionsPublicationError::LineageUnavailable(message)
                }
                StructuralLineageError::Bridge(message) => {
                    PermissionsPublicationError::Bridge(message)
                }
            })?;
    }

    // Validate the complete selection before advancing the durable active schema.
    // Omitted table grants are explicitly empty, including for legacy sources.
    let mut source = known_schema(state, &HashMap::new(), schema_hash)
        .map_err(PermissionsPublicationError::Bridge)?;
    for (name, table) in &mut source {
        table.policies = permissions.get(name).cloned().unwrap_or_default();
    }
    if permissions.keys().any(|name| !source.contains_key(name)) {
        return Err(PermissionsPublicationError::Catalogue(
            CatalogueError::WriteError(
                "active schema permissions reference an unknown table".to_owned(),
            ),
        ));
    }
    let compiled = JazzSchema::new(&source).map_err(|error| {
        PermissionsPublicationError::Catalogue(CatalogueError::WriteError(format!(
            "invalid active schema permissions: {error}"
        )))
    })?;

    if let Some(runtime) = shell {
        let current = state
            .catalogue
            .active_schema(&state.catalogue_store)
            .map_err(PermissionsPublicationError::Catalogue)?;
        let revision = match current {
            Some(current)
                if current.summary.schema_hash == schema_hash
                    && current.permissions == permissions =>
            {
                current.summary.version
            }
            Some(current) => current.summary.version.checked_add(1).ok_or_else(|| {
                PermissionsPublicationError::Bridge("active schema revision overflow".to_owned())
            })?,
            None => 1,
        };
        runtime
            .validate_schema_activation(revision, compiled)
            .await
            .map_err(PermissionsPublicationError::Bridge)?;
    }

    state
        .catalogue
        .publish_permissions_bundle(
            &state.catalogue_store,
            schema_hash,
            permissions,
            expected_parent_bundle_object_id,
        )
        .map_err(PermissionsPublicationError::Catalogue)?;
    let head = state
        .catalogue
        .active_schema(&state.catalogue_store)
        .map_err(PermissionsPublicationError::Catalogue)?
        .ok_or_else(|| {
            PermissionsPublicationError::Bridge(
                "published permissions head is missing from the catalogue".to_owned(),
            )
        })?
        .summary;

    publish_runtime_catalogue_locked(state, &[], &[])
        .await
        .map_err(PermissionsPublicationError::Bridge)?;
    Ok(head)
}

async fn publish_runtime_catalogue_locked(
    state: &ServerState,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), String> {
    if state.runtime().is_none() && state.core_server_shell_storage_config.is_none() {
        return Ok(());
    }

    let mut shell = state.runtime();
    let supplied_schemas = schemas
        .iter()
        .cloned()
        .map(|schema| (SchemaHash::compute(&schema), schema))
        .collect::<HashMap<_, _>>();
    for schema in schemas {
        let runtime_schema = JazzSchema::new(schema)
            .map_err(|error| format!("convert catalogue schema for runtime: {error}"))?;
        runtime_shell(state, &mut shell, runtime_schema)?;
    }

    for lens in lenses {
        publish_runtime_lens(state, &mut shell, lens, &supplied_schemas).await?;
    }

    let permissions = state
        .catalogue
        .active_schema(&state.catalogue_store)
        .map_err(|error| format!("read permissions head for runtime: {error}"))?;
    #[cfg(test)]
    state.run_runtime_catalogue_after_permissions_read_hook_for_test();
    let Some(permissions) = permissions else {
        return Ok(());
    };
    ensure_structural_lineage_locked(
        state,
        permissions.summary.schema_hash,
        &supplied_schemas,
        lenses,
        &mut shell,
    )
    .await
    .map_err(StructuralLineageError::into_string)?;

    let schema = known_schema(state, &supplied_schemas, permissions.summary.schema_hash)?;
    let structural_runtime = JazzSchema::new(&schema)
        .map_err(|error| format!("convert active structural schema: {error}"))?;
    let runtime_shell = runtime_shell(state, &mut shell, structural_runtime.clone())?;
    runtime_shell
        .activate_schema(
            permissions.summary.version,
            structural_runtime,
            permissions.permissions,
        )
        .await
        .map_err(|error| format!("activate schema: {error}"))?;
    Ok(())
}

async fn publish_runtime_lens(
    state: &ServerState,
    shell: &mut Option<ServerRuntimeHandle>,
    lens: &Lens,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
) -> Result<(), String> {
    let source_schema = known_schema(state, supplied_schemas, lens.source_hash)?;
    let target_schema = known_schema(state, supplied_schemas, lens.target_hash)?;
    let runtime_lens = convert_lens(lens, &source_schema, &target_schema)?;
    let new_tables = lens
        .forward
        .ops
        .iter()
        .filter_map(|op| match op {
            LensOp::AddTable { table, .. } => Some(table.as_str().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let dropped_tables = lens
        .forward
        .ops
        .iter()
        .filter_map(|op| match op {
            LensOp::RemoveTable { table, .. } => Some(table.as_str().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let initial_schema = JazzSchema::new(&source_schema)
        .map_err(|error| format!("convert lens source schema for runtime: {error}"))?;
    let target_runtime = JazzSchema::new(&target_schema)
        .map_err(|error| format!("convert lens target schema: {error}"))?;
    let runtime_shell = runtime_shell(state, shell, initial_schema)?;
    runtime_shell
        .publish_schema_with_lens(target_runtime, runtime_lens, new_tables, dropped_tables)
        .await
        .map_err(|error| format!("publish schema lineage to runtime shell: {error}"))?;
    Ok(())
}

#[derive(Debug)]
enum StructuralLineageError {
    Unavailable(String),
    Bridge(String),
}

impl StructuralLineageError {
    fn into_string(self) -> String {
        match self {
            Self::Unavailable(message) | Self::Bridge(message) => message,
        }
    }
}

async fn ensure_structural_lineage_locked(
    state: &ServerState,
    target_hash: SchemaHash,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
    supplied_lenses: &[Lens],
    shell: &mut Option<ServerRuntimeHandle>,
) -> Result<(), StructuralLineageError> {
    let target_schema = known_schema(state, supplied_schemas, target_hash)
        .map_err(StructuralLineageError::Bridge)?;
    let target_runtime = JazzSchema::new(&target_schema)
        .map_err(|error| StructuralLineageError::Bridge(error.to_string()))?;

    let Some(runtime) = shell.clone() else {
        runtime_shell(state, shell, target_runtime)
            .map(|_| ())
            .map_err(StructuralLineageError::Bridge)?;
        return Ok(());
    };
    let target_runtime_id = target_runtime.version_id();
    if runtime
        .runtime_catalogue_contains_schema(target_runtime_id)
        .await
        .map_err(StructuralLineageError::Bridge)?
    {
        return Ok(());
    }

    let mut lenses_by_edge = HashMap::new();
    let durable_lenses = state
        .catalogue
        .known_lenses(&state.catalogue_store)
        .map_err(|error| {
            StructuralLineageError::Bridge(format!("read catalogue lenses: {error}"))
        })?;
    for lens in durable_lenses {
        lenses_by_edge.insert((lens.source_hash, lens.target_hash), lens);
    }
    for lens in supplied_lenses.iter().filter(|lens| !lens.is_draft()) {
        lenses_by_edge.insert((lens.source_hash, lens.target_hash), lens.clone());
    }
    let mut lenses = lenses_by_edge.into_values().collect::<Vec<_>>();
    lenses.sort_by(|left, right| {
        left.source_hash
            .as_bytes()
            .cmp(right.source_hash.as_bytes())
            .then_with(|| {
                left.target_hash
                    .as_bytes()
                    .cmp(right.target_hash.as_bytes())
            })
    });

    let mut schemas_by_hash = HashMap::from([(target_hash, target_schema)]);
    let mut valid_lenses = Vec::new();
    for lens in lenses {
        let Some(source_schema) =
            known_schema_if_present(state, supplied_schemas, lens.source_hash)
                .map_err(StructuralLineageError::Bridge)?
        else {
            continue;
        };
        let Some(target_schema) =
            known_schema_if_present(state, supplied_schemas, lens.target_hash)
                .map_err(StructuralLineageError::Bridge)?
        else {
            continue;
        };
        schemas_by_hash.insert(lens.source_hash, source_schema);
        schemas_by_hash.insert(lens.target_hash, target_schema);
        valid_lenses.push(lens);
    }

    let mut runtime_known = HashSet::new();
    for (&hash, schema) in &schemas_by_hash {
        let runtime_schema = JazzSchema::new(schema).map_err(|error| {
            StructuralLineageError::Bridge(format!("convert catalogue schema {hash}: {error}"))
        })?;
        if runtime
            .runtime_catalogue_contains_schema(runtime_schema.version_id())
            .await
            .map_err(StructuralLineageError::Bridge)?
        {
            runtime_known.insert(hash);
        }
    }

    let mut incoming = HashMap::<SchemaHash, Vec<Lens>>::new();
    // Grouping preserves the deterministic order established above.
    for lens in valid_lenses {
        incoming.entry(lens.target_hash).or_default().push(lens);
    }
    let mut visiting = HashSet::new();
    let path = match find_lineage_path(target_hash, &incoming, &runtime_known, &mut visiting) {
        LineagePath::Unique(path) => path,
        LineagePath::None | LineagePath::Ambiguous => {
            return Err(StructuralLineageError::Unavailable(format!(
                "permissions target schema {target_hash} has no unique published lineage in the server shell; publish a migration first"
            )));
        }
    };
    for lens in path {
        publish_runtime_lens(state, shell, &lens, supplied_schemas)
            .await
            .map_err(StructuralLineageError::Bridge)?;
    }
    Ok(())
}

enum LineagePath {
    None,
    Unique(Vec<Lens>),
    Ambiguous,
}

fn find_lineage_path(
    current: SchemaHash,
    incoming: &HashMap<SchemaHash, Vec<Lens>>,
    runtime_known: &HashSet<SchemaHash>,
    visiting: &mut HashSet<SchemaHash>,
) -> LineagePath {
    if runtime_known.contains(&current) {
        return LineagePath::Unique(Vec::new());
    }
    if !visiting.insert(current) {
        return LineagePath::None;
    }

    let mut found = None;
    for lens in incoming.get(&current).into_iter().flatten() {
        match find_lineage_path(lens.source_hash, incoming, runtime_known, visiting) {
            LineagePath::None => {}
            LineagePath::Ambiguous => return LineagePath::Ambiguous,
            LineagePath::Unique(mut path) => {
                path.push(lens.clone());
                if found.is_some() {
                    return LineagePath::Ambiguous;
                }
                found = Some(path);
            }
        }
    }
    visiting.remove(&current);
    found.map_or(LineagePath::None, LineagePath::Unique)
}

fn runtime_shell(
    state: &ServerState,
    shell: &mut Option<ServerRuntimeHandle>,
    initial_schema: jazz::schema::JazzSchema,
) -> Result<ServerRuntimeHandle, String> {
    if let Some(shell) = shell.clone() {
        return Ok(shell);
    }
    let started = state.start_core_server_shell(initial_schema)?;
    *shell = Some(started.clone());
    Ok(started)
}

fn known_schema(
    state: &ServerState,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
    hash: SchemaHash,
) -> Result<Schema, String> {
    known_schema_if_present(state, supplied_schemas, hash)?
        .ok_or_else(|| format!("catalogue schema {hash} is missing"))
}

fn known_schema_if_present(
    state: &ServerState,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
    hash: SchemaHash,
) -> Result<Option<Schema>, String> {
    if let Some(schema) = state
        .catalogue
        .known_schema(&state.catalogue_store, &hash)
        .map_err(|error| format!("read catalogue schema {hash}: {error}"))?
        .or_else(|| supplied_schemas.get(&hash).cloned())
    {
        return Ok(Some(schema));
    }

    let empty_schema = Schema::new();
    if hash == SchemaHash::compute(&empty_schema) {
        return Ok(Some(empty_schema));
    }

    Ok(None)
}

fn convert_lens(lens: &Lens, source: &Schema, target: &Schema) -> Result<MigrationLens, String> {
    let source_runtime =
        JazzSchema::new(source).map_err(|error| format!("convert lens source schema: {error}"))?;
    let target_runtime =
        JazzSchema::new(target).map_err(|error| format!("convert lens target schema: {error}"))?;

    let renamed_tables = lens
        .forward
        .ops
        .iter()
        .filter_map(|op| match op {
            LensOp::RenameTable { old_name, new_name } => {
                Some((old_name.as_str(), new_name.as_str()))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut table_lenses = source
        .iter()
        .filter_map(|(source_name, _)| {
            let source_name = source_name.as_str();
            let target_name = renamed_tables
                .get(source_name)
                .copied()
                .unwrap_or(source_name);
            target
                .contains_key(&TableName::from(target_name))
                .then(|| TableLens {
                    source_table: source_name.to_owned(),
                    target_table: target_name.to_owned(),
                    ops: (source_name != target_name)
                        .then(|| CoreLensOp::RenameTable {
                            from: source_name.to_owned(),
                            to: target_name.to_owned(),
                        })
                        .into_iter()
                        .collect(),
                })
        })
        .collect::<Vec<_>>();

    for op in &lens.forward.ops {
        let (table_name, runtime_op) = match op {
            LensOp::RenameTable { .. } | LensOp::AddTable { .. } | LensOp::RemoveTable { .. } => {
                continue;
            }
            LensOp::AddColumn {
                table,
                column,
                default,
                ..
            } => (
                table.as_str(),
                CoreLensOp::AddColumn {
                    column: column.clone(),
                    default: public_value_to_core(default.clone())?,
                },
            ),
            LensOp::RemoveColumn {
                table,
                column,
                default,
                ..
            } => (
                table.as_str(),
                CoreLensOp::DropColumn {
                    column: column.clone(),
                    backwards_default: public_value_to_core(default.clone())?,
                },
            ),
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => (
                table.as_str(),
                CoreLensOp::RenameColumn {
                    from: old_name.clone(),
                    to: new_name.clone(),
                },
            ),
        };
        let table_lens = table_lenses
            .iter_mut()
            .find(|candidate| {
                candidate.source_table == table_name || candidate.target_table == table_name
            })
            .ok_or_else(|| format!("lens operation references unknown table {table_name}"))?;
        table_lens.ops.push(runtime_op);
    }

    MigrationLens::new(
        source_runtime.version_id(),
        target_runtime.version_id(),
        table_lenses,
    )
    .map_err(str::to_owned)
}

fn public_value_to_core(value: Value) -> Result<CoreValue, String> {
    match value {
        Value::Boolean(value) => Ok(CoreValue::Bool(value)),
        Value::Text(value) => Ok(CoreValue::String(value)),
        Value::Integer(value) => Ok(CoreValue::I32(value)),
        Value::BigInt(value) => Ok(CoreValue::I64(value)),
        Value::Double(value) => Ok(CoreValue::F64(value)),
        Value::Timestamp(value) => Ok(CoreValue::U64(value)),
        Value::Uuid(value) => Ok(CoreValue::Uuid(*value.uuid())),
        Value::Bytea(value) => Ok(CoreValue::Bytes(value)),
        Value::Null => Ok(CoreValue::Nullable(None)),
        Value::Array(values) => values
            .into_iter()
            .map(public_value_to_core)
            .collect::<Result<Vec<_>, _>>()
            .map(CoreValue::Array),
        Value::Enum { .. } => Err(
            "migration lens enum payload default is not supported by the runtime core".to_owned(),
        ),
        Value::TransactionId(_) | Value::Row { .. } => {
            Err("migration lens default is not supported by the runtime core".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Internal bridge tests inspect both stored administrative state and runtime
    // state, including rejection before an administrative write.
    async fn sibling_catalogue() -> (super::super::BuiltServer, Schema, Schema) {
        use jazz::tools::{AppId, ColumnType, SchemaBuilder, TableSchema};
        let schema = |column| {
            SchemaBuilder::new()
                .table(TableSchema::builder("notes").column(column, ColumnType::Text))
                .build()
        };
        let base = schema("body");
        let left = schema("left_body");
        let right = schema("right_body");
        let server = super::super::ServerBuilder::new(AppId::from_name("sibling-selection"))
            .with_schema(base.clone())
            .with_storage(super::super::StorageBackend::InMemory)
            .build()
            .await
            .unwrap();
        for (target, column) in [(&left, "left_body"), (&right, "right_body")] {
            let lens = Lens::new(
                SchemaHash::compute(&base),
                SchemaHash::compute(target),
                jazz::tools::schema_lens::LensTransform::with_ops(vec![LensOp::RenameColumn {
                    table: "notes".into(),
                    old_name: "body".into(),
                    new_name: column.into(),
                }]),
            );
            server
                .state
                .catalogue
                .publish_schema(&server.state.catalogue_store, target.clone())
                .unwrap();
            server
                .state
                .catalogue
                .publish_lens(&server.state.catalogue_store, &lens)
                .unwrap();
            publish_runtime_catalogue(&server.state, &[target.clone()], &[lens])
                .await
                .unwrap();
        }
        (server, left, right)
    }

    #[tokio::test]
    async fn sibling_selection_activates_and_retries_without_advancing_revision() {
        let (server, left, right) = sibling_catalogue().await;
        let first = publish_permissions_and_runtime(
            &server.state,
            SchemaHash::compute(&left),
            HashMap::new(),
            None,
        )
        .await
        .ok()
        .expect("activate left sibling");
        let second = publish_permissions_and_runtime(
            &server.state,
            SchemaHash::compute(&right),
            HashMap::new(),
            Some(first.bundle_object_id),
        )
        .await
        .ok()
        .expect("activate right sibling");
        let replay = publish_permissions_and_runtime(
            &server.state,
            SchemaHash::compute(&right),
            HashMap::new(),
            Some(second.bundle_object_id),
        )
        .await
        .ok()
        .expect("retry selection");
        assert_eq!(replay.version, second.version);
        let runtime = server.state.runtime().unwrap();
        let snapshot = runtime.trusted_catalogue_snapshot_for_test().await.unwrap();
        assert_eq!(
            snapshot.current_write_schema.schema,
            JazzSchema::new(&right).unwrap().version_id()
        );
        assert_eq!(snapshot.current_write_schema.revision, second.version);
        let stored = server
            .state
            .catalogue
            .active_schema(&server.state.catalogue_store)
            .unwrap()
            .unwrap();
        assert_eq!(stored.summary.schema_hash, SchemaHash::compute(&right));
        assert_eq!(stored.summary.version, second.version);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn rejected_activation_does_not_advance_administrative_selection() {
        let (server, left, right) = sibling_catalogue().await;
        let first = publish_permissions_and_runtime(
            &server.state,
            SchemaHash::compute(&left),
            HashMap::new(),
            None,
        )
        .await
        .ok()
        .expect("activate left sibling");
        let runtime = server.state.runtime().unwrap();
        // Simulate a runtime ahead of the administrative store. The proposed
        // revision must be rejected before the store selects the right sibling.
        runtime
            .activate_schema(
                first.version + 2,
                JazzSchema::new(&left).unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap();
        for _ in 0..2 {
            let result = publish_permissions_and_runtime(
                &server.state,
                SchemaHash::compute(&right),
                HashMap::new(),
                Some(first.bundle_object_id),
            )
            .await;
            assert!(
                matches!(result, Err(PermissionsPublicationError::Bridge(message))
                if message.contains("stale active schema revision"))
            );
            let stored = server
                .state
                .catalogue
                .active_schema(&server.state.catalogue_store)
                .unwrap()
                .unwrap();
            assert_eq!(stored.summary.bundle_object_id, first.bundle_object_id);
            assert_eq!(stored.summary.schema_hash, SchemaHash::compute(&left));
            assert_eq!(stored.summary.version, first.version);
        }
        let snapshot = runtime.trusted_catalogue_snapshot_for_test().await.unwrap();
        assert_eq!(
            snapshot.current_write_schema.schema,
            JazzSchema::new(&left).unwrap().version_id()
        );
        assert_eq!(snapshot.current_write_schema.revision, first.version + 2);
        server.shutdown().await;
    }

    #[test]
    fn migration_lens_defaults_preserve_logical_signed_scalars_and_nested_arrays() {
        for value in [i32::MIN, -1, 0, i32::MAX] {
            assert_eq!(
                public_value_to_core(Value::Integer(value)),
                Ok(CoreValue::I32(value))
            );
        }
        for value in [i64::MIN, -1, 0, i64::MAX] {
            assert_eq!(
                public_value_to_core(Value::BigInt(value)),
                Ok(CoreValue::I64(value))
            );
        }

        assert_eq!(
            public_value_to_core(Value::Array(vec![
                Value::Integer(-7),
                Value::Array(vec![
                    Value::Integer(8),
                    Value::BigInt(i64::MIN),
                    Value::Null,
                ]),
            ])),
            Ok(CoreValue::Array(vec![
                CoreValue::I32(-7),
                CoreValue::Array(vec![
                    CoreValue::I32(8),
                    CoreValue::I64(i64::MIN),
                    CoreValue::Nullable(None),
                ]),
            ]))
        );
    }
}
