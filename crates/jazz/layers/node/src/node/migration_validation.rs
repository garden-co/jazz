//! Schema/lens checks shared by runtime ingestion and deployment preparation.
use super::*;
use crate::schema::ColumnSchema;

pub fn validate_migration_lens_between(
    lens: &MigrationLens,
    source: &SchemaVersion,
    target: &SchemaVersion,
) -> Result<(), Error> {
    for table_lens in &lens.table_lenses {
        let source_table = source
            .schema
            .tables
            .iter()
            .find(|table| table.name == table_lens.source_table)
            .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
        let target_table = target
            .schema
            .tables
            .iter()
            .find(|table| table.name == table_lens.target_table)
            .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
        let target_bindings = target_table
            .branch_by
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut branch_columns = source_table
            .branch_by
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut columns = source_table
            .columns
            .iter()
            .cloned()
            .map(|column| (column.name.clone(), column))
            .collect::<BTreeMap<_, _>>();
        let mut saw_table_rename = source_table.name == target_table.name;
        for op in &table_lens.ops {
            match op {
                LensOp::RenameTable { from, to } => {
                    if saw_table_rename || from != &source_table.name || to != &target_table.name {
                        return Err(Error::InvalidCatalogueUpdate(
                            "table rename does not match lens endpoints",
                        ));
                    }
                    saw_table_rename = true;
                }
                LensOp::RenameColumn { from, to } => {
                    if columns.contains_key(to) {
                        return Err(Error::InvalidCatalogueUpdate(
                            "column rename collides with existing column",
                        ));
                    }
                    let mut column = columns.remove(from).ok_or(Error::InvalidCatalogueUpdate(
                        "column rename source is unknown",
                    ))?;
                    column.name = to.clone();
                    columns.insert(to.clone(), column);
                    if branch_columns.remove(from) {
                        branch_columns.insert(to.clone());
                    }
                }
                LensOp::CopyColumn { from, to } => {
                    if columns.contains_key(to) {
                        return Err(Error::InvalidCatalogueUpdate(
                            "column copy collides with existing column",
                        ));
                    }
                    let mut column =
                        columns
                            .get(from)
                            .cloned()
                            .ok_or(Error::InvalidCatalogueUpdate(
                                "column copy source is unknown",
                            ))?;
                    column.name = to.clone();
                    columns.insert(to.clone(), column);
                }
                LensOp::AddColumn { column, .. } => {
                    if columns.contains_key(column) {
                        return Err(Error::InvalidCatalogueUpdate("added column already exists"));
                    }
                    let target_column = target_table
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == *column)
                        .cloned()
                        .ok_or(Error::InvalidCatalogueUpdate(
                            "added column is absent from target",
                        ))?;
                    columns.insert(column.clone(), target_column);
                    if target_bindings.contains(column) && columns[column].default.is_none() {
                        return Err(Error::InvalidCatalogueUpdate(
                            "added branch column requires a migration default",
                        ));
                    }
                }
                LensOp::DropColumn { column, .. } => {
                    if branch_columns.contains(column) {
                        return Err(Error::InvalidCatalogueUpdate(
                            "table branch columns cannot be removed",
                        ));
                    }
                    if columns.remove(column).is_none() {
                        return Err(Error::InvalidCatalogueUpdate(
                            "dropped column is absent from source",
                        ));
                    }
                }
                LensOp::TransformColumn { column, transform } => {
                    if branch_columns.contains(column) {
                        return Err(Error::InvalidCatalogueUpdate(
                            "branch column type and migration default are immutable",
                        ));
                    }
                    validate_transform_column(columns.get(column), transform)?;
                    let source_column = columns.get(column).ok_or(
                        Error::InvalidCatalogueUpdate("transformed column is absent from source"),
                    )?;
                    let target_column = target_table
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == *column)
                        .ok_or(Error::InvalidCatalogueUpdate(
                            "transformed column is absent from target",
                        ))?;
                    if !physical_value_epoch_is_compatible(
                        &source_column.column_type,
                        &target_column.column_type,
                    ) || source_column.large_value_kind != target_column.large_value_kind
                    {
                        return Err(Error::InvalidCatalogueUpdate(
                            "column transform changes physical value or large-value semantic kind",
                        ));
                    }
                    columns.insert(column.clone(), target_column.clone());
                }
                LensOp::RejectSourceDelta { .. } => {}
            }
        }
        if !saw_table_rename {
            return Err(Error::InvalidCatalogueUpdate(
                "renamed table requires an explicit RenameTable operation",
            ));
        }
        if !branch_columns.is_subset(&target_bindings) {
            return Err(Error::InvalidCatalogueUpdate(
                "table branch columns cannot be removed",
            ));
        }
        let target_columns = target_table
            .columns
            .iter()
            .cloned()
            .map(|column| (column.name.clone(), column))
            .collect::<BTreeMap<_, _>>();
        for branch_column in &branch_columns {
            let Some(source_column) = columns.get(branch_column) else {
                continue;
            };
            let Some(target_column) = target_columns.get(branch_column) else {
                continue;
            };
            if source_column.column_type != target_column.column_type
                || source_column.default != target_column.default
            {
                return Err(Error::InvalidCatalogueUpdate(
                    "branch column type and migration default are immutable",
                ));
            }
        }
        // Ordinary defaults affect future inserts, not existing-row projection.
        // Branch defaults were checked above because they determine row identity.
        for column in columns.values_mut() {
            column.default = None;
        }
        let target_columns = target_columns
            .into_iter()
            .map(|(name, mut column)| {
                column.default = None;
                (name, column)
            })
            .collect::<BTreeMap<_, _>>();
        if columns != target_columns {
            return Err(Error::InvalidCatalogueUpdate(
                "lens operations do not reproduce target columns",
            ));
        }
    }
    Ok(())
}

pub fn validate_lineage_table_partition(
    source: &JazzSchema,
    target: &JazzSchema,
    lens: &MigrationLens,
    new_tables: &[String],
    dropped_tables: &[String],
) -> Result<(), Error> {
    let source_tables = source
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    let target_tables = target
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    let related_source = lens
        .table_lenses
        .iter()
        .map(|table| table.source_table.clone())
        .collect::<BTreeSet<_>>();
    let related_target = lens
        .table_lenses
        .iter()
        .map(|table| table.target_table.clone())
        .collect::<BTreeSet<_>>();
    let new = new_tables.iter().cloned().collect::<BTreeSet<_>>();
    let dropped = dropped_tables.iter().cloned().collect::<BTreeSet<_>>();
    if related_source.len() != lens.table_lenses.len()
        || related_target.len() != lens.table_lenses.len()
        || new.len() != new_tables.len()
        || dropped.len() != dropped_tables.len()
        || !related_source.is_disjoint(&dropped)
        || !related_target.is_disjoint(&new)
        || related_source
            .union(&dropped)
            .cloned()
            .collect::<BTreeSet<_>>()
            != source_tables
        || related_target.union(&new).cloned().collect::<BTreeSet<_>>() != target_tables
    {
        return Err(Error::InvalidCatalogueUpdate(
            "lineage table declarations do not partition schemas",
        ));
    }
    Ok(())
}

fn validate_transform_column(column: Option<&ColumnSchema>, transform: &str) -> Result<(), Error> {
    validate_registered_transform(transform)?;
    let Some(_) = column else {
        return Err(Error::InvalidCatalogueUpdate("transform column is unknown"));
    };
    Ok(())
}
