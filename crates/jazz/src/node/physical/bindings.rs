pub(super) fn allocate_provisional_physical_mapping(
    schema: &JazzSchema,
    identities: PhysicalIdentityManifest,
    next_table_id: &mut u64,
    next_column_id: &mut u64,
) -> Result<SchemaPhysicalMapping, Error> {
    identities
        .validate_for_schema(schema)
        .map_err(Error::InvalidCatalogueUpdate)?;
    let mut tables = BTreeMap::new();
    for table in &schema.tables {
        let table_id = PhysicalTableId(*next_table_id);
        *next_table_id = next_table_id
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?;
        let mut columns = BTreeMap::new();
        for column in &table.columns {
            let column_id = PhysicalColumnId(*next_column_id);
            *next_column_id = next_column_id
                .checked_add(1)
                .ok_or(Error::InvalidStoredValue("physical column id exhausted"))?;
            columns.insert(column.name.clone(), column_id);
        }
        tables.insert(
            table.name.clone(),
            TablePhysicalMapping {
                table_id,
                columns,
                variant_cases: Vec::new(),
                scalar_enum_cases: BTreeMap::new(),
                payload_enum_cases: BTreeMap::new(),
                nested_scalar_enum_cases: BTreeMap::new(),
                nested_payload_enum_cases: BTreeMap::new(),
            },
        );
    }
    Ok(SchemaPhysicalMapping { identities, tables })
}

/// Allocate and retain the single hidden Groove row case for one Jazz layout.
/// Allocation consults the whole physical-table lineage; nested column enums
/// have their own registries and never multiply these row cases.
pub(super) fn allocate_physical_variant_cases(
    mappings: &mut BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    fields: BTreeSet<String>,
) -> Result<Vec<PhysicalVariantCase>, Error> {
    let target = mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?;
    let table_id = target.table_id;
    let target_columns = target.columns.keys().cloned().collect::<BTreeSet<_>>();
    if !fields.is_subset(&target_columns) {
        return Err(Error::InvalidStoredValue(
            "physical table variant contains an unknown field",
        ));
    }
    if let Some(existing) = target.variant_cases.first() {
        if target.variant_cases.len() != 1 || existing.fields != fields {
            return Err(Error::InvalidStoredValue(
                "physical table variant case definition changed",
            ));
        }
        return Ok(target.variant_cases.clone());
    }

    let mut used = BTreeMap::<u32, SchemaVersionId>::new();
    for (candidate_schema, mapping) in mappings.iter() {
        let Some((_, table)) = mapping
            .tables
            .iter()
            .find(|(_, table)| table.table_id == table_id)
        else {
            continue;
        };
        if table.variant_cases.is_empty() {
            // The target mapping is still provisional: its alias has never
            // been written as a row tag, and is replaced by the cases below.
            if *candidate_schema == schema_version {
                continue;
            }
            let alias = aliases
                .get(candidate_schema)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "variant-case schema alias missing",
                ))?;
            let tag = groove_variant_tag(alias)?;
            if used.insert(tag, *candidate_schema).is_some() {
                return Err(Error::InvalidStoredValue(
                    "physical table variant tag collision",
                ));
            }
        } else if table.variant_cases.len() != 1
            || used
                .insert(table.variant_cases[0].tag, *candidate_schema)
                .is_some()
        {
            return Err(Error::InvalidStoredValue(
                "physical table variant tag collision",
            ));
        }
    }
    let tag = groove_variant_tag(*aliases.get(&schema_version).ok_or(
        Error::InvalidStoredValue("variant-case schema alias missing"),
    )?)?;
    if used.contains_key(&tag) {
        return Err(Error::InvalidStoredValue(
            "physical table variant tag collision",
        ));
    }
    let allocated = vec![PhysicalVariantCase { tag, fields }];
    mappings
        .get_mut(&schema_version)
        .and_then(|mapping| mapping.tables.get_mut(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?
        .variant_cases = allocated.clone();
    Ok(allocated)
}

pub(super) fn validate_physical_variant_cases(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    let mut by_table = BTreeMap::<PhysicalTableId, BTreeMap<u32, SchemaVersionId>>::new();
    for (schema_version, mapping) in mappings {
        for table in mapping.tables.values() {
            let mut column_ids = BTreeSet::new();
            if table
                .columns
                .values()
                .any(|column_id| !column_ids.insert(*column_id))
            {
                return Err(Error::InvalidStoredValue(
                    "physical table maps multiple columns to one id",
                ));
            }
            let tag = if table.variant_cases.is_empty() {
                groove_variant_tag(*aliases.get(schema_version).ok_or(
                    Error::InvalidStoredValue("variant-case schema alias missing"),
                )?)?
            } else {
                if table.variant_cases.len() != 1 {
                    return Err(Error::InvalidStoredValue(
                        "physical table layout has multiple row cases",
                    ));
                }
                table.variant_cases[0].tag
            };
            let tags = by_table.entry(table.table_id).or_default();
            if tags.insert(tag, *schema_version).is_some() {
                return Err(Error::InvalidStoredValue(
                    "physical table variant tag collision",
                ));
            }
        }
    }
    Ok(())
}

/// Validate physical mapping references after all local schema aliases are
/// recovered. The bytes codec owns structural canonicality; this pass owns
/// cross-record references and semantic registry order.
pub(super) fn validate_physical_mapping_registries(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    for mapping in mappings.values() {
        let mut table_ids = BTreeSet::new();
        for table in mapping.tables.values() {
            if !table_ids.insert(table.table_id) {
                return Err(Error::InvalidStoredValue(
                    "physical mapping aliases multiple tables to one id",
                ));
            }
            let columns = table.columns.values().copied().collect::<BTreeSet<_>>();
            if columns.len() != table.columns.len() {
                return Err(Error::InvalidStoredValue(
                    "physical table maps multiple columns to one id",
                ));
            }
            for field in table.variant_cases.iter().flat_map(|case| &case.fields) {
                if !table.columns.contains_key(field) {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant contains an unknown field",
                    ));
                }
            }
            validate_enum_registries(&columns, &table.scalar_enum_cases, aliases)?;
            validate_payload_enum_registries(&columns, &table.payload_enum_cases, aliases)?;
            validate_nested_enum_registries(&columns, &table.nested_scalar_enum_cases, aliases)?;
            validate_nested_payload_enum_registries(
                &columns,
                &table.nested_payload_enum_cases,
                aliases,
            )?;
        }
    }
    Ok(())
}

/// The registry order is physical storage identity, while a case's authored
/// coordinates are only provenance.  They must still name the authority
/// manifest that minted the UUID: otherwise corrupt durable metadata can
/// retain a valid UUID, forge a different introduction position, and change
/// the order used when descriptors are rebuilt after a restart.
///
/// This deliberately walks both direct and recursive payload registries.  A
/// recursive path is local lowering metadata, but the physical table/column
/// pair and `(introducing_schema, introducing_ordinal, UUID)` are enough to
/// bind each stored tag to the immutable manifest.
pub(super) fn validate_payload_enum_case_provenance(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    if mappings.keys().any(|schema| !aliases.contains_key(schema)) {
        return Err(Error::InvalidStoredValue(
            "payload enum registry provenance schema alias is missing",
        ));
    }
    for mapping in mappings.values() {
        for table in mapping.tables.values() {
            for (column_id, cases) in &table.payload_enum_cases {
                validate_payload_enum_case_provenance_for_column(
                    mappings,
                    aliases,
                    table.table_id,
                    *column_id,
                    cases,
                )?;
            }
            for (column_id, paths) in &table.nested_payload_enum_cases {
                for cases in paths.values() {
                    validate_payload_enum_case_provenance_for_column(
                        mappings,
                        aliases,
                        table.table_id,
                        *column_id,
                        cases,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Scalar enum tags have the same physical-ordering boundary as payload
/// enums.  Their compact `u8` tag is rebuilt from this registry, so decoded
/// introduction coordinates must be manifest-backed before any descriptor is
/// reconstructed.
pub(super) fn validate_scalar_enum_case_provenance(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    if mappings.keys().any(|schema| !aliases.contains_key(schema)) {
        return Err(Error::InvalidStoredValue(
            "scalar enum registry provenance schema alias is missing",
        ));
    }
    for mapping in mappings.values() {
        for table in mapping.tables.values() {
            for (column_id, cases) in &table.scalar_enum_cases {
                validate_scalar_enum_case_provenance_for_column(
                    mappings,
                    aliases,
                    table.table_id,
                    *column_id,
                    cases,
                )?;
            }
            for (column_id, paths) in &table.nested_scalar_enum_cases {
                for cases in paths.values() {
                    validate_scalar_enum_case_provenance_for_column(
                        mappings,
                        aliases,
                        table.table_id,
                        *column_id,
                        cases,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_scalar_enum_case_provenance_for_column(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    table_id: PhysicalTableId,
    column_id: PhysicalColumnId,
    cases: &[GlobalScalarEnumCaseId],
) -> Result<(), Error> {
    for case in cases {
        let Some(origin) = mappings.get(&case.introducing_schema) else {
            return Err(Error::InvalidStoredValue(
                "scalar enum registry case provenance references an unknown schema",
            ));
        };
        let ordinal = usize::from(case.introducing_ordinal);
        let matches_authority = origin.tables.iter().any(|(table_name, table)| {
            table.table_id == table_id
                && table.columns.iter().any(|(column_name, candidate_column)| {
                    *candidate_column == column_id
                        && origin
                            .identities
                            .tables
                            .get(table_name)
                            .and_then(|identity_table| identity_table.columns.get(column_name))
                            .is_some_and(|identity_column| {
                                identity_column
                                    .enum_variants
                                    .values()
                                    .any(|variants| variants.get(ordinal) == Some(&case.id))
                            })
                })
        });
        if !matches_authority {
            return Err(Error::InvalidStoredValue(
                "scalar enum registry case provenance disagrees with authority identities",
            ));
        }
        let earliest_authority = mappings
            .iter()
            .filter(|(_, candidate)| {
                candidate.tables.iter().any(|(table_name, table)| {
                    table.table_id == table_id
                        && table.columns.iter().any(|(column_name, candidate_column)| {
                            *candidate_column == column_id
                                && candidate
                                    .identities
                                    .tables
                                    .get(table_name)
                                    .and_then(|identity_table| {
                                        identity_table.columns.get(column_name)
                                    })
                                    .is_some_and(|identity_column| {
                                        identity_column.enum_variants.values().any(|variants| {
                                            variants.iter().any(|id| id == &case.id)
                                        })
                                    })
                        })
                })
            })
            .min_by_key(|(schema, _)| aliases[schema])
            .map(|(schema, _)| *schema)
            .ok_or(Error::InvalidStoredValue(
                "scalar enum registry case provenance disagrees with authority identities",
            ))?;
        if earliest_authority != case.introducing_schema {
            return Err(Error::InvalidStoredValue(
                "scalar enum registry case provenance does not name its introduction",
            ));
        }
    }
    Ok(())
}

fn validate_payload_enum_case_provenance_for_column(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    table_id: PhysicalTableId,
    column_id: PhysicalColumnId,
    cases: &[GlobalEnumCaseId],
) -> Result<(), Error> {
    for case in cases {
        let Some(origin) = mappings.get(&case.introducing_schema) else {
            return Err(Error::InvalidStoredValue(
                "payload enum registry case provenance references an unknown schema",
            ));
        };
        let ordinal = usize::try_from(case.introducing_ordinal).map_err(|_| {
            Error::InvalidStoredValue("payload enum registry case provenance is invalid")
        })?;
        let matches_authority = origin.tables.iter().any(|(table_name, table)| {
            table.table_id == table_id
                && table.columns.iter().any(|(column_name, candidate_column)| {
                    *candidate_column == column_id
                        && origin
                            .identities
                            .tables
                            .get(table_name)
                            .and_then(|identity_table| identity_table.columns.get(column_name))
                            .is_some_and(|identity_column| {
                                identity_column
                                    .enum_variants
                                    .values()
                                    .any(|variants| variants.get(ordinal) == Some(&case.id))
                            })
                })
        });
        if !matches_authority {
            return Err(Error::InvalidStoredValue(
                "payload enum registry case provenance disagrees with authority identities",
            ));
        }
        let earliest_authority = mappings
            .iter()
            .filter(|(_, candidate)| {
                candidate.tables.iter().any(|(table_name, table)| {
                    table.table_id == table_id
                        && table.columns.iter().any(|(column_name, candidate_column)| {
                            *candidate_column == column_id
                                && candidate
                                    .identities
                                    .tables
                                    .get(table_name)
                                    .and_then(|identity_table| {
                                        identity_table.columns.get(column_name)
                                    })
                                    .is_some_and(|identity_column| {
                                        identity_column.enum_variants.values().any(|variants| {
                                            variants.iter().any(|id| id == &case.id)
                                        })
                                    })
                        })
                })
            })
            .min_by_key(|(schema, _)| aliases[schema])
            .map(|(schema, _)| *schema)
            .ok_or(Error::InvalidStoredValue(
                "payload enum registry case provenance disagrees with authority identities",
            ))?;
        if earliest_authority != case.introducing_schema {
            return Err(Error::InvalidStoredValue(
                "payload enum registry case provenance does not name its introduction",
            ));
        }
    }
    Ok(())
}

fn validate_enum_registries(
    columns: &BTreeSet<PhysicalColumnId>,
    registries: &BTreeMap<PhysicalColumnId, Vec<GlobalScalarEnumCaseId>>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    for (column, cases) in registries {
        if !columns.contains(column) {
            return Err(Error::InvalidStoredValue(
                "physical enum registry references an unknown column",
            ));
        }
        validate_enum_cases(cases, aliases)?;
    }
    Ok(())
}

fn validate_nested_enum_registries(
    columns: &BTreeSet<PhysicalColumnId>,
    registries: &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalScalarEnumCaseId>>>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    for (column, paths) in registries {
        if !columns.contains(column) {
            return Err(Error::InvalidStoredValue(
                "physical enum registry references an unknown column",
            ));
        }
        for cases in paths.values() {
            validate_enum_cases(cases, aliases)?;
        }
    }
    Ok(())
}

fn validate_enum_cases(
    cases: &[GlobalScalarEnumCaseId],
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for case in cases {
        if case.id.0.is_nil() {
            return Err(Error::InvalidStoredValue(
                "physical enum registry contains a nil global identity",
            ));
        }
        if !seen.insert(case.clone()) {
            return Err(Error::InvalidStoredValue(
                "physical enum registry repeats a case identity",
            ));
        }
    }
    if cases.windows(2).any(|pair| {
        compare_scalar_enum_cases(aliases, &pair[0], &pair[1]).is_gt()
    }) {
        return Err(Error::InvalidStoredValue(
            "physical enum registry has non-canonical case order",
        ));
    }
    Ok(())
}

fn validate_payload_enum_registries(
    columns: &BTreeSet<PhysicalColumnId>,
    registries: &BTreeMap<PhysicalColumnId, Vec<GlobalEnumCaseId>>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    for (column, cases) in registries {
        if !columns.contains(column) {
            return Err(Error::InvalidStoredValue(
                "physical enum registry references an unknown column",
            ));
        }
        validate_payload_enum_cases(cases, aliases)?;
    }
    Ok(())
}

fn validate_nested_payload_enum_registries(
    columns: &BTreeSet<PhysicalColumnId>,
    registries: &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalEnumCaseId>>>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    for (column, paths) in registries {
        if !columns.contains(column) {
            return Err(Error::InvalidStoredValue(
                "physical enum registry references an unknown column",
            ));
        }
        for cases in paths.values() {
            validate_payload_enum_cases(cases, aliases)?;
        }
    }
    Ok(())
}

fn validate_payload_enum_cases(
    cases: &[GlobalEnumCaseId],
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for case in cases {
        if case.id.0.is_nil() {
            return Err(Error::InvalidStoredValue(
                "physical enum registry contains a nil global identity",
            ));
        }
        if !seen.insert(case.clone()) {
            return Err(Error::InvalidStoredValue(
                "physical enum registry repeats a case identity",
            ));
        }
    }
    if cases
        .windows(2)
        .any(|pair| compare_global_enum_cases(aliases, &pair[0], &pair[1]).is_gt())
    {
        return Err(Error::InvalidStoredValue(
            "physical enum registry has non-canonical case order",
        ));
    }
    Ok(())
}

pub(super) fn physical_history_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue("physical history schema missing"))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical history table mapping missing",
        ))?;
    let alias =
        schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical history schema alias missing",
            ))?;
    Ok(PhysicalHistoryBinding {
        storage_table: physical_history_storage_table(
            physical_mappings,
            schema_version,
            logical_table,
        )?,
        descriptor: physical_history_descriptor(table, mapping, alias)?,
    })
}

pub(super) fn physical_history_storage_table(
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
) -> Result<String, Error> {
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical history table mapping missing",
        ))?;
    Ok(physical_history_table_name(mapping.table_id))
}

pub(super) fn physical_current_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    class: PhysicalCurrentClass,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue("physical current schema missing"))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical current table mapping missing",
        ))?;
    Ok(PhysicalHistoryBinding {
        storage_table: physical_current_storage_table(
            physical_mappings,
            schema_version,
            logical_table,
            class,
        )?,
        descriptor: physical_current_descriptor(table, mapping)?,
    })
}

pub(super) fn physical_current_storage_table(
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    class: PhysicalCurrentClass,
) -> Result<String, Error> {
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical current table mapping missing",
        ))?;
    Ok(match class {
        PhysicalCurrentClass::Global => physical_global_current_table_name(mapping.table_id),
        PhysicalCurrentClass::Ahead => physical_ahead_current_table_name(mapping.table_id),
    })
}

pub(super) fn physical_rejected_version_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue(
            "physical rejected-version schema missing",
        ))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical rejected-version table mapping missing",
        ))?;
    Ok(PhysicalHistoryBinding {
        storage_table: physical_rejected_versions_table_name(mapping.table_id),
        descriptor: physical_rejected_version_descriptor(table, mapping)?,
    })
}
