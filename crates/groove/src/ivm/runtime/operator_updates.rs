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

    pub(super) fn update_index_source<S>(
        input: &IndexSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        table_deltas: &[TableDelta],
        storage: Option<&S>,
        eval_mode: EvalMode,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if eval_mode == EvalMode::Hydrate {
            let storage = storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
            let store = RecordStore::new(storage, "indices", output_desc);
            let mut deltas = Vec::new();
            let mut visit = |_: &[u8], record: &[u8]| {
                deltas.push(RecordDelta {
                    record: Bytes::copy_from_slice(record),
                    weight: 1,
                });
                Ok(())
            };
            match persisted_index_scan_bounds(&input.table, &input.index, input.scan.as_ref())? {
                StaticScanBounds::Prefix(prefix) => store.scan_prefix(&prefix, &mut visit)?,
                StaticScanBounds::Range { start, end } => {
                    if start < end {
                        store.scan_range(&start, &end, &mut visit)?;
                    }
                }
            }
            return Ok(RecordDeltas {
                descriptor: *output_desc,
                deltas,
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
        binding_snapshots: &HashMap<String, RecordDeltas>,
        mode: ArrangementUpdateMode,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if mode == ArrangementUpdateMode::Replace {
            let Some(snapshot) = binding_snapshots.get(&input.shape) else {
                return Ok(RecordDeltas::empty(*output_desc));
            };
            return project_binding_source_deltas(snapshot, output_desc);
        }
        let mut deltas = Vec::new();
        for delta in binding_deltas
            .iter()
            .filter(|delta| delta.shape == input.shape)
        {
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
                    PlanExpr::RecursiveEnumRemap {
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
