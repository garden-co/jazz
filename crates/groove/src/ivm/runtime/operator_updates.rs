//! Stateless source and unary-operator delta application.

use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct NodeRuntimeMeta {
    pub(super) retainers: HashSet<Retainer>,
    pub(super) last_used_tick: u64,
    pub(super) depends_on_context: Option<bool>,
    pub(super) input_signature: Option<Arc<NodeInputSignature>>,
    pub(super) input_generation: u64,
    pub(super) raw_projection_fields: Option<Option<Arc<[RawProjectionField]>>>,
    pub(super) join_left_fields: Option<Arc<[String]>>,
    pub(super) join_right_fields: Option<Arc<[String]>>,
    pub(super) join_output_mapping: Option<Arc<[(usize, usize)]>>,
    pub(super) aggregate_group_fields: Option<Arc<[String]>>,
}

/// Namespace for stateless operator helper methods.
pub(super) struct NodeState;

impl NodeState {
    pub(super) fn table_source_request(
        input: &TableSourceOp,
    ) -> Result<Option<super::evaluation_session::StorageRequestKey>, IvmRuntimeError> {
        Ok(match input.scan.as_ref().map(scan_bounds).transpose()? {
            None => Some(super::evaluation_session::StorageRequestKey::ScanPrefix {
                family: input.table.clone(),
                prefix: Vec::new(),
            }),
            Some(StaticScanBounds::Prefix(prefix)) => {
                Some(super::evaluation_session::StorageRequestKey::ScanPrefix {
                    family: input.table.clone(),
                    prefix,
                })
            }
            Some(StaticScanBounds::Range { start, end }) if start < end => {
                Some(super::evaluation_session::StorageRequestKey::ScanRange {
                    family: input.table.clone(),
                    start,
                    end,
                })
            }
            Some(StaticScanBounds::Range { .. }) => None,
        })
    }

    pub(super) fn index_source_request(
        input: &IndexSourceOp,
    ) -> Result<Option<super::evaluation_session::StorageRequestKey>, IvmRuntimeError> {
        if input.row_projection.is_some() {
            if !input.intersections.is_empty() {
                let StaticScanBounds::Prefix(prefix) =
                    persisted_index_scan_bounds(&input.table, &input.index, input.scan.as_ref())?
                else {
                    return Err(IvmRuntimeError::UnsupportedIndexIntersectionScan);
                };
                let intersections = input
                    .intersections
                    .iter()
                    .map(|(index, scan)| {
                        let StaticScanBounds::Prefix(prefix) =
                            persisted_index_scan_bounds(&input.table, index, Some(scan))?
                        else {
                            return Err(IvmRuntimeError::UnsupportedIndexIntersectionScan);
                        };
                        Ok((index.clone(), prefix))
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                return Ok(Some(
                    super::evaluation_session::StorageRequestKey::IndexedRowsIntersection {
                        table: input.table.clone(),
                        index: input.index.clone(),
                        prefix,
                        intersections,
                    },
                ));
            }
            let max_items = scan_max_items(input.scan.as_ref());
            return Ok(Some(
                match persisted_index_scan_bounds(&input.table, &input.index, input.scan.as_ref())?
                {
                    StaticScanBounds::Prefix(prefix) => {
                        if let Some(max_items) = max_items {
                            super::evaluation_session::StorageRequestKey::IndexedRowsPrefixLimit {
                                table: input.table.clone(),
                                index: input.index.clone(),
                                prefix,
                                max_items,
                            }
                        } else {
                            super::evaluation_session::StorageRequestKey::IndexedRowsPrefix {
                                table: input.table.clone(),
                                index: input.index.clone(),
                                prefix,
                            }
                        }
                    }
                    StaticScanBounds::Range { start, end } => {
                        super::evaluation_session::StorageRequestKey::IndexedRowsRange {
                            table: input.table.clone(),
                            index: input.index.clone(),
                            start,
                            end,
                        }
                    }
                },
            ));
        }
        Ok(
            match persisted_index_scan_bounds(&input.table, &input.index, input.scan.as_ref())? {
                StaticScanBounds::Prefix(prefix) => {
                    let max_items = scan_max_items(input.scan.as_ref());
                    Some(match max_items {
                        Some(max_items) => {
                            super::evaluation_session::StorageRequestKey::ScanPrefixLimit {
                                family: "indices".to_owned(),
                                prefix,
                                max_items,
                            }
                        }
                        None => super::evaluation_session::StorageRequestKey::ScanPrefix {
                            family: "indices".to_owned(),
                            prefix,
                        },
                    })
                }
                StaticScanBounds::Range { start, end } if start < end => {
                    Some(super::evaluation_session::StorageRequestKey::ScanRange {
                        family: "indices".to_owned(),
                        start,
                        end,
                    })
                }
                StaticScanBounds::Range { .. } => None,
            },
        )
    }

    pub(super) fn update_table_source_from_inputs(
        input: &TableSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        inputs: &mut super::evaluation_session::EvaluationInputs,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let Some(request) = Self::table_source_request(input)? else {
            return Ok(RecordDeltas::empty(*output_desc));
        };
        let table_schema = schema
            .table(&input.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(input.table.clone()))?;
        let mut grouped = HashMap::<(u32, RecordDescriptor), Vec<RecordDelta>>::default();
        for (_, stored) in inputs.rows(request)? {
            let (variant_tag, payload) = crate::records::split_variant_record(stored)?;
            let descriptor = table_schema
                .record_schema_for_variant(variant_tag)
                .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                    table: input.table.clone(),
                    version: u64::from(variant_tag),
                })?;
            grouped
                .entry((variant_tag, descriptor))
                .or_default()
                .push(RecordDelta {
                    record: Bytes::copy_from_slice(payload),
                    weight: 1,
                });
        }
        let table_deltas = grouped
            .into_iter()
            .map(|((variant_tag, descriptor), deltas)| TableDelta {
                table: input.table.clone(),
                variant_tag,
                descriptor,
                deltas,
            })
            .collect::<Vec<_>>();
        Self::update_table_source(
            input,
            schema,
            variant_projections,
            output_desc,
            &table_deltas,
        )
    }

    pub(super) fn update_index_source_from_inputs(
        input: &IndexSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        inputs: &mut super::evaluation_session::EvaluationInputs,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let Some(request) = Self::index_source_request(input)? else {
            return Ok(RecordDeltas::empty(*output_desc));
        };
        let rows = inputs.rows(request)?;
        if let Some(row_projection) = &input.row_projection {
            let table_schema = schema
                .table(&input.table)
                .ok_or_else(|| IvmRuntimeError::TableNotFound(input.table.clone()))?;
            let mut grouped = HashMap::<(u32, RecordDescriptor), Vec<RecordDelta>>::default();
            for (_, stored) in rows {
                let (variant_tag, payload) = crate::records::split_variant_record(stored)?;
                let descriptor = table_schema
                    .record_schema_for_variant(variant_tag)
                    .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                        table: input.table.clone(),
                        version: u64::from(variant_tag),
                    })?;
                grouped
                    .entry((variant_tag, descriptor))
                    .or_default()
                    .push(RecordDelta {
                        record: Bytes::copy_from_slice(payload),
                        weight: 1,
                    });
            }
            let table_deltas = grouped
                .into_iter()
                .map(|((variant_tag, descriptor), deltas)| TableDelta {
                    table: input.table.clone(),
                    variant_tag,
                    descriptor,
                    deltas,
                })
                .collect::<Vec<_>>();
            return Self::update_table_source(
                &TableSourceOp {
                    table: input.table.clone(),
                    scan: None,
                    variant_projection: Some(row_projection.clone()),
                },
                schema,
                variant_projections,
                output_desc,
                &table_deltas,
            );
        }
        let deltas = rows
            .iter()
            .map(|(_, record)| RecordDelta {
                record: Bytes::copy_from_slice(record),
                weight: 1,
            })
            .collect();
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    pub(super) fn update_table_source(
        input: &TableSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        table_deltas: &[TableDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let table_schema = schema
            .table(&input.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(input.table.clone()))?;
        let primary_key_fields = input
            .scan
            .as_ref()
            .map(|_| primary_key_field_indices(table_schema, output_desc))
            .transpose()?
            .unwrap_or_default();
        let mut deltas = Vec::new();
        let projection = input
            .variant_projection
            .as_ref()
            .map(|target| {
                variant_projections
                    .get(&VariantProjectionKey {
                        table: input.table.clone(),
                        target: target.clone(),
                    })
                    .ok_or_else(|| IvmRuntimeError::VariantProjectionNotFound {
                        table: input.table.clone(),
                        target: variant_projection_target_name(target).to_owned(),
                    })
            })
            .transpose()?;
        for delta in table_deltas
            .iter()
            .filter(|delta| delta.table == input.table)
        {
            let projected;
            let source_deltas = if let Some(projection) = projection {
                if !projection.output.registry_compatible_with(output_desc) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let case = projection.cases.get(&delta.variant_tag).ok_or_else(|| {
                    IvmRuntimeError::VariantProjectionCaseNotFound {
                        table: input.table.clone(),
                        target: input
                            .variant_projection
                            .as_ref()
                            .map(variant_projection_target_name)
                            .unwrap_or_default()
                            .to_owned(),
                        version: u64::from(delta.variant_tag),
                    }
                })?;
                if !case.source().registry_compatible_with(&delta.descriptor) {
                    return Err(IvmRuntimeError::VariantProjectionSourceMismatch {
                        table: input.table.clone(),
                        target: input
                            .variant_projection
                            .as_ref()
                            .map(variant_projection_target_name)
                            .unwrap_or_default()
                            .to_owned(),
                        version: u64::from(delta.variant_tag),
                    });
                }
                match case {
                    VariantProjectionCase::Project {
                        project,
                        raw_projection,
                        omit_unrepresentable_enum_rows,
                        ..
                    } => {
                        projected = Self::update_map_project(
                            project,
                            *output_desc,
                            &RecordDeltas {
                                descriptor: delta.descriptor,
                                deltas: delta.deltas.clone(),
                            },
                            raw_projection.as_deref(),
                            *omit_unrepresentable_enum_rows,
                        )?;
                        &projected.deltas
                    }
                    VariantProjectionCase::Enum {
                        tag,
                        payload,
                        project,
                        raw_projection,
                        ..
                    } => {
                        projected = Self::update_variant_enum_project(
                            *tag,
                            *payload,
                            project,
                            *output_desc,
                            &RecordDeltas {
                                descriptor: delta.descriptor,
                                deltas: delta.deltas.clone(),
                            },
                            raw_projection.as_deref(),
                        )?;
                        &projected.deltas
                    }
                    VariantProjectionCase::Ignore { .. } => continue,
                }
            } else {
                if !delta.descriptor.registry_compatible_with(output_desc) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                &delta.deltas
            };
            for record_delta in source_deltas {
                if let Some(scan) = &input.scan {
                    let key = primary_key_value_bytes(
                        output_desc,
                        record_delta.raw(),
                        &primary_key_fields,
                    )?;
                    if !key_matches_static_scan(&key, scan)? {
                        continue;
                    }
                }
                deltas.push(record_delta.clone());
            }
        }
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    pub(super) async fn update_index_source(
        input: &IndexSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        table_deltas: &[TableDelta],
        storage: Option<&dyn OrderedKvStorage>,
        eval_mode: EvalMode,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if eval_mode == EvalMode::Hydrate {
            let storage = storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
            let max_items = scan_max_items(input.scan.as_ref());
            let scan =
                match persisted_index_scan_bounds(&input.table, &input.index, input.scan.as_ref())?
                {
                    StaticScanBounds::Prefix(prefix) => {
                        storage
                            .scan(ScanRequest {
                                cf: "indices".to_owned(),
                                bounds: ScanBounds::Prefix(prefix),
                                direction: ScanDirection::Forward,
                                max_items,
                            })
                            .await?
                    }
                    StaticScanBounds::Range { start, end } => {
                        if start >= end {
                            return Ok(RecordDeltas::empty(*output_desc));
                        }
                        storage
                            .scan(ScanRequest {
                                cf: "indices".to_owned(),
                                bounds: ScanBounds::Range { start, end },
                                direction: ScanDirection::Forward,
                                max_items,
                            })
                            .await?
                    }
                };
            let mut scan = scan;
            let mut deltas = Vec::new();
            while let Some(batch) = scan.next_batch().await? {
                deltas.extend(batch.into_iter().map(|(_, record)| RecordDelta {
                    record: Bytes::from(record),
                    weight: 1,
                }));
            }
            return Ok(RecordDeltas {
                descriptor: *output_desc,
                deltas,
            });
        }

        if let Some(row_projection) = &input.row_projection {
            let index_by = IndexByOp {
                key_expressions: Vec::new(),
                value_expressions: Vec::new(),
                explicit_index: None,
                key_fields: input.key_fields.clone(),
                value_fields: input.value_fields.clone(),
                unique: input.unique,
                append_value_to_key: input.append_value_to_key,
                store_value: input.store_value,
                scan: input.scan.clone(),
            };
            let mut matching = Vec::new();
            for table_delta in table_deltas
                .iter()
                .filter(|delta| delta.table == input.table)
            {
                for record_delta in &table_delta.deltas {
                    let one = TableDelta {
                        table: table_delta.table.clone(),
                        variant_tag: table_delta.variant_tag,
                        descriptor: table_delta.descriptor,
                        deltas: vec![record_delta.clone()],
                    };
                    let index_input = Self::update_table_source(
                        &TableSourceOp {
                            table: input.table.clone(),
                            scan: None,
                            variant_projection: input.variant_projection.clone(),
                        },
                        schema,
                        variant_projections,
                        &input.input_descriptor,
                        std::slice::from_ref(&one),
                    )?;
                    if apply_index_by(&index_by, &input.input_descriptor, &index_input.deltas)?
                        .is_empty()
                    {
                        continue;
                    }
                    matching.extend(
                        Self::update_table_source(
                            &TableSourceOp {
                                table: input.table.clone(),
                                scan: None,
                                variant_projection: Some(row_projection.clone()),
                            },
                            schema,
                            variant_projections,
                            output_desc,
                            std::slice::from_ref(&one),
                        )?
                        .deltas,
                    );
                }
            }
            return Ok(RecordDeltas {
                descriptor: *output_desc,
                deltas: matching,
            });
        }

        let index_by = IndexByOp {
            key_expressions: Vec::new(),
            value_expressions: Vec::new(),
            explicit_index: None,
            key_fields: input.key_fields.clone(),
            value_fields: input.value_fields.clone(),
            unique: input.unique,
            append_value_to_key: input.append_value_to_key,
            store_value: input.store_value,
            scan: input.scan.clone(),
        };
        let source = Self::update_table_source(
            &TableSourceOp {
                table: input.table.clone(),
                scan: None,
                variant_projection: input.variant_projection.clone(),
            },
            schema,
            variant_projections,
            &input.input_descriptor,
            table_deltas,
        )?;
        let deltas = apply_index_by(&index_by, &input.input_descriptor, &source.deltas)?;
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    pub(super) fn update_binding_source(
        input: &BindingSourceOp,
        output_desc: &RecordDescriptor,
        binding_deltas: &[BindingDelta],
        binding_snapshots: &HashMap<BindingSourceKey, RecordDeltas>,
        mode: ArrangementUpdateMode,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if mode == ArrangementUpdateMode::Replace {
            let Some(snapshot) = binding_snapshots.get(&input.key) else {
                return Ok(RecordDeltas::empty(*output_desc));
            };
            return project_binding_source_deltas(snapshot, output_desc);
        }
        let mut deltas = Vec::new();
        for delta in binding_deltas.iter().filter(|delta| delta.key == input.key) {
            deltas.extend(
                project_binding_source_deltas(
                    &RecordDeltas {
                        descriptor: delta.descriptor,
                        deltas: delta.deltas.clone(),
                    },
                    output_desc,
                )?
                .deltas,
            );
        }
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    pub(super) fn update_filter(
        filter: &FilterOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let predicate = &filter.predicate;
        let mut deltas = Vec::new();
        for delta in &input.deltas {
            if predicate.matches(delta.borrowed(&input.descriptor), filter.comparison)? {
                deltas.push(delta.clone());
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_map_project(
        project: &MapProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        raw_projection: Option<&[RawProjectionField]>,
        omit_unrepresentable_enum_rows: bool,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let omit_unrepresentable_enum_rows = omit_unrepresentable_enum_rows
            || project.expressions.iter().any(|expression| {
                matches!(
                    expression.expression,
                    ProjectExpr::RecursiveEnumRemap {
                        omit_unrepresentable: true,
                        ..
                    }
                )
            });
        let estimated_output_bytes = input
            .deltas
            .iter()
            .map(|delta| delta.record.len())
            .sum::<usize>();
        let mut output = BytesMut::with_capacity(estimated_output_bytes);
        let mut spans = Vec::with_capacity(input.deltas.len());
        let mut raw_projection_scratch = RawProjectionScratch::default();
        for delta in &input.deltas {
            let span = if let Some(fields) = raw_projection {
                output_desc
                    .project_raw_fields_into(
                        &input.descriptor,
                        delta.raw(),
                        fields,
                        &mut output,
                        &mut raw_projection_scratch,
                    )
                    .map_err(IvmRuntimeError::RecordEncoding)?
            } else {
                let start = output.len();
                let record = match project_record(
                    &project.expressions,
                    &project.mapping,
                    output_desc,
                    &input.descriptor,
                    delta.raw(),
                ) {
                    Ok(record) => record,
                    Err(
                        IvmRuntimeError::EnumTagProjectionAbsent { .. }
                        | IvmRuntimeError::EnumProjectionAbsent { .. },
                    ) if omit_unrepresentable_enum_rows => continue,
                    Err(error) => return Err(error),
                };
                output.extend_from_slice(&record);
                start..output.len()
            };
            spans.push((span, delta.weight));
        }
        let output = output.freeze();
        let deltas: Vec<_> = spans
            .into_iter()
            .map(|(span, weight)| RecordDelta {
                record: output.slice(span),
                weight,
            })
            .collect();
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_variant_enum_project(
        tag: u32,
        payload_desc: RecordDescriptor,
        project: &MapProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        raw_projection: Option<&[RawProjectionField]>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let payloads =
            Self::update_map_project(project, payload_desc, input, raw_projection, false)?;
        let mut deltas = Vec::with_capacity(payloads.deltas.len());
        for payload in payloads.deltas {
            let value = Value::Enum(EnumValue::new(
                tag,
                OwnedRecord::new(payload.record.to_vec(), payload_desc),
            ));
            deltas.push(RecordDelta {
                record: output_desc.create(&[value])?.into(),
                weight: payload.weight,
            });
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_variant_project(
        variant_project: &VariantProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for delta in &input.deltas {
            let value = delta
                .borrowed(&input.descriptor)
                .get_idx(variant_project.field_idx)?;
            let Value::Enum(value) = value else {
                return Err(IvmRuntimeError::VariantProjectFieldTypeMismatch {
                    field: variant_project.field.clone(),
                });
            };
            if value.tag() == variant_project.tag {
                if *value.record().descriptor() != output_desc {
                    return Err(IvmRuntimeError::VariantProjectPayloadMismatch {
                        field: variant_project.field.clone(),
                    });
                }
                deltas.push(RecordDelta {
                    record: Bytes::copy_from_slice(value.record().raw()),
                    weight: delta.weight,
                });
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_unwrap_nullable(
        unwrap: &UnwrapNullableOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let estimated_output_bytes = input
            .deltas
            .iter()
            .map(|delta| delta.record.len())
            .sum::<usize>();
        let mut output = BytesMut::with_capacity(estimated_output_bytes);
        let mut spans = Vec::new();
        let mut scratch = RawProjectionScratch::default();
        for delta in &input.deltas {
            if let Some(span) = output_desc
                .unwrap_nullable_field_into(
                    &input.descriptor,
                    delta.raw(),
                    unwrap.field_idx,
                    &mut output,
                    &mut scratch,
                )
                .map_err(IvmRuntimeError::RecordEncoding)?
            {
                spans.push((span, delta.weight));
            }
        }
        let output = output.freeze();
        let deltas = spans
            .into_iter()
            .map(|(span, weight)| RecordDelta {
                record: output.slice(span),
                weight,
            })
            .collect();
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_unnest(
        unnest: &UnnestOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for delta in &input.deltas {
            let values = delta
                .borrowed(&input.descriptor)
                .to_values()
                .map_err(IvmRuntimeError::RecordEncoding)?;
            let Some(value) = values.get(unnest.array_field_idx) else {
                return Err(IvmRuntimeError::GraphFieldIndexOutOfBounds(
                    unnest.array_field_idx,
                ));
            };
            let Value::Array(elements) = value else {
                return Err(IvmRuntimeError::UnsupportedOperator);
            };
            for element in elements {
                let mut output_values = values.clone();
                output_values.push(element.clone());
                deltas.push(RecordDelta {
                    record: output_desc.create(&output_values)?.into(),
                    weight: delta.weight,
                });
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_union(
        output_desc: RecordDescriptor,
        inputs: Vec<Arc<RecordDeltas>>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for input in inputs {
            if input.deltas.is_empty() {
                continue;
            }
            if !output_desc.registry_compatible_with(&input.descriptor) {
                return Err(IvmRuntimeError::GraphOutputMismatch);
            }
            deltas.extend(input.deltas.iter().cloned());
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    pub(super) fn update_index_by(
        index_by: &IndexByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let deltas = apply_index_by(index_by, &input.descriptor, &input.deltas)?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    #[cfg(any())]
    pub(super) fn update_persist(
        persist: &PersistOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        storage: &impl OrderedKvStorage,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let trace = std::env::var_os("GROOVE_TRACE_INDEX_BY").is_some() && !input.deltas.is_empty();
        let start = trace.then(std::time::Instant::now);
        let result = apply_persist_delta(
            storage,
            &persist.storage,
            &persist.key_fields,
            persist.unique,
            input,
        );
        if trace {
            eprintln!(
                "GROOVE_TRACE_PERSIST storage={} input={} unique={} key_fields={:?} elapsed_ms={:.3}",
                String::from_utf8_lossy(&persist.storage.key_prefix).replace('\0', "."),
                input.deltas.len(),
                persist.unique,
                persist.key_fields,
                start.expect("trace start").elapsed().as_secs_f64() * 1000.0
            );
        }
        result?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: input.deltas.clone(),
        })
    }
}
