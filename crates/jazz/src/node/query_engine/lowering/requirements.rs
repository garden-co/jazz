//! Source-field and metadata requirements derived from analyzed plans.
//!
//! Requirements form the contract between query planning and source
//! resolution: they describe what every logical source must provide without
//! deciding how that source is physically implemented.

use super::*;

pub(super) fn source_requirements(
    request: &QueryProgramRequest,
    plan: &AnalyzedQueryPlan,
) -> CapabilityResult<BTreeMap<SourceId, SourceRequirements>> {
    let output = &request.output;
    let mut requirements = BTreeMap::<SourceId, SourceRequirements>::new();
    for source in program_sources(request, plan) {
        requirements.insert(source, SourceRequirements::default());
    }

    // Every closure hop consumes its parent reference as an executable join
    // key, even when that intermediate source contributes no application
    // payload.  Source projections are allowed to elide unrequested fields;
    // without this requirement a nested include such as `project.org` can
    // resolve `projects` with `user_org` replaced by its sparse projection
    // default, making the second hop look universally missing.
    for path in &request.input.shape.closure_paths {
        for segment in closure_path_segments(path) {
            let source_requirements = requirements.get_mut(&segment.parent).ok_or_else(|| {
                single_gap_report(UnsupportedReason::Runtime(format!(
                    "closure parent source {:?} was not initialized",
                    segment.parent
                )))
            })?;
            add_required_app_field(source_requirements, segment.source_field.clone());
        }
    }

    // A flat output carries an occurrence-addressed tuple. Keep source
    // version metadata available at the source boundary so joined-side
    // changes can be represented as maintained replacements.
    if !flat_join_payload_fields(plan).is_empty() {
        for requirements in requirements.values_mut() {
            requirements
                .metadata
                .insert(SourceMetadataRequirement::VersionWitnesses);
        }
    }

    if let Some(app_rows) = &output.app_rows {
        if !matches!(
            plan,
            AnalyzedQueryPlan::Linear(_)
                | AnalyzedQueryPlan::Union(_)
                | AnalyzedQueryPlan::CorrelatedPath(_)
        ) {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Operator(
                    "app row materialization for recursive relation projections is not lowered yet"
                        .to_owned(),
                )],
                explain: ExplainPlan {
                    capabilities: vec!["recursive relation app rows are not lowered".to_owned()],
                    ..ExplainPlan::default()
                },
            }));
        }
        let root_requirements = requirements
            .get_mut(plan.root_source())
            .expect("root source requirements were initialized");
        // The same lowered program also owns the storage-shaped graph used by
        // synchronous Rust materialization. Keep its provenance tuple complete
        // while the public Root collector below still publishes only the
        // explicitly selected magic fields.
        let projects_provenance = matches!(
            &app_rows.projection,
            PayloadProjection::Tree(AppProjectionTree {
                fields: FieldProjection::Fields(fields),
                ..
            }) if fields.iter().any(|field| matches!(
                field.as_str(),
                "$createdAt" | "$createdBy" | "$updatedAt" | "$updatedBy"
            ))
        );
        if app_rows.public_terminal && projects_provenance {
            root_requirements.metadata.extend([
                SourceMetadataRequirement::Provenance(ProvenanceField::CreatedAt),
                SourceMetadataRequirement::Provenance(ProvenanceField::CreatedBy),
                SourceMetadataRequirement::Provenance(ProvenanceField::UpdatedAt),
                SourceMetadataRequirement::Provenance(ProvenanceField::UpdatedBy),
            ]);
        }
        merge_field_requirement(
            &mut root_requirements.app_fields,
            match &app_rows.projection {
                PayloadProjection::ShapeDefault => FieldRequirement::All,
                PayloadProjection::Tree(tree) => tree.fields.clone().into(),
            },
        );
        if let PayloadProjection::Tree(tree) = &app_rows.projection {
            if let FieldProjection::Fields(fields) = &tree.fields {
                for field in fields {
                    let provenance = match field.as_str() {
                        "$createdAt" => Some(ProvenanceField::CreatedAt),
                        "$createdBy" => Some(ProvenanceField::CreatedBy),
                        "$updatedAt" => Some(ProvenanceField::UpdatedAt),
                        "$updatedBy" => Some(ProvenanceField::UpdatedBy),
                        _ => None,
                    };
                    if let Some(provenance) = provenance {
                        root_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::Provenance(provenance));
                    }
                }
            }
            collect_app_path_projection_requirements(&tree.paths, &mut requirements)?;
        }
    }

    for fact in &output.facts {
        match fact {
            ProgramFactKey::AuthorizedRows => {}
            ProgramFactKey::ResultMembership => {
                let root_requirements = requirements
                    .get_mut(plan.root_source())
                    .expect("root source requirements were initialized");
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::VersionWitnesses);
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::SettlePosition);
                for contribution in &request.input.shape.join_contributions {
                    if let Some(source_requirements) = requirements.get_mut(&contribution.source) {
                        source_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::VersionWitnesses);
                        source_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::SettlePosition);
                    }
                }
            }
            ProgramFactKey::VersionWitnesses | ProgramFactKey::ReplacementWitnesses => {
                for source_requirements in requirements.values_mut() {
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::VersionWitnesses);
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::VersionPayloads);
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::DeletionMarkers);
                }
            }
            ProgramFactKey::ProgramSourceCoverage(scope) => match scope {
                CoverageScope::Program => {
                    for source_requirements in requirements.values_mut() {
                        source_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::Coverage);
                    }
                }
                CoverageScope::Source(source) => {
                    let source_requirements = requirements.get_mut(source).ok_or_else(|| {
                        single_gap_report(UnsupportedReason::Source(SourceGap::Coverage))
                    })?;
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::Coverage);
                }
                CoverageScope::Path(_) => {
                    let root_requirements = requirements
                        .get_mut(plan.root_source())
                        .expect("root source requirements were initialized");
                    root_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::Coverage);
                }
            },
            ProgramFactKey::RelationEdges | ProgramFactKey::PathCorrelationCoverage => {
                for source_requirements in requirements.values_mut() {
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::VersionWitnesses);
                }
            }
            _ => {
                return Err(Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Output(Box::new(fact.clone()))],
                    explain: ExplainPlan {
                        capabilities: vec!["requested fact is not lowered yet".to_owned()],
                        ..ExplainPlan::default()
                    },
                }));
            }
        }
    }

    collect_plan_requirements(plan, &mut requirements)?;

    Ok(requirements)
}

#[cfg(test)]
pub(crate) fn source_requirements_for_test(
    request: &QueryProgramRequest,
) -> CapabilityResult<BTreeMap<SourceId, SourceRequirements>> {
    let plan = analyze_query_plan(request).map_err(|gaps| {
        Box::new(CapabilityReport {
            gaps,
            explain: ExplainPlan::default(),
        })
    })?;
    source_requirements(request, &plan)
}

fn collect_app_path_projection_requirements(
    paths: &[AppPathProjection],
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    for path in paths {
        let source_requirements = requirements.get_mut(&path.path.child).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "app projection path child source {:?} was not initialized",
                path.path.child
            )))
        })?;
        merge_field_requirement(
            &mut source_requirements.app_fields,
            path.fields.clone().into(),
        );
        collect_app_path_projection_requirements(&path.children, requirements)?;
    }
    Ok(())
}

fn merge_field_requirement(existing: &mut FieldRequirement, incoming: FieldRequirement) {
    match incoming {
        FieldRequirement::None => {}
        FieldRequirement::All => *existing = FieldRequirement::All,
        FieldRequirement::Fields(incoming) => match existing {
            FieldRequirement::None => *existing = FieldRequirement::Fields(incoming),
            FieldRequirement::All => {}
            FieldRequirement::Fields(existing) => existing.extend(incoming),
        },
    }
}

fn collect_plan_requirements(
    plan: &AnalyzedQueryPlan,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    let fragments = collect_plan_fragments(plan);
    for fragment in &fragments.linears {
        for step in fragment.steps {
            for (source, source_requirements) in requirements.iter_mut() {
                collect_step_requirements(step, source, source_requirements)?;
            }
        }
    }
    for correlation in fragments.correlations {
        collect_predicate_requirements_for_all_sources(correlation, requirements)?;
    }
    for relation in fragments.recursives {
        if !matches!(
            relation.frontier_key,
            NormalizedValueRef::FrontierColumn { .. }
                | NormalizedValueRef::RowId(RowIdRef::Frontier(_))
                | NormalizedValueRef::Param(_)
                | NormalizedValueRef::Literal(_)
        ) {
            collect_value_requirements_for_all_sources(&relation.frontier_key, requirements)?;
        }
        for key in &relation.dedupe_keys {
            if !matches!(
                key,
                NormalizedValueRef::FrontierColumn { .. }
                    | NormalizedValueRef::RowId(RowIdRef::Frontier(_))
                    | NormalizedValueRef::Param(_)
                    | NormalizedValueRef::Literal(_)
            ) {
                collect_value_requirements_for_all_sources(key, requirements)?;
            }
        }
    }
    Ok(())
}

fn collect_predicate_requirements_for_all_sources(
    predicate: &PredicateExpr,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    for (source, source_requirements) in requirements.iter_mut() {
        collect_predicate_requirements(predicate, source, source_requirements).map_err(|gap| {
            Box::new(CapabilityReport {
                gaps: vec![gap],
                explain: ExplainPlan {
                    capabilities: vec!["path correlation requirements are not lowered".to_owned()],
                    ..ExplainPlan::default()
                },
            })
        })?;
    }
    Ok(())
}

fn collect_value_requirements_for_all_sources(
    value: &NormalizedValueRef,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    for (source, source_requirements) in requirements.iter_mut() {
        collect_value_requirements(value, source, source_requirements).map_err(|gap| {
            Box::new(CapabilityReport {
                gaps: vec![gap],
                explain: ExplainPlan {
                    capabilities: vec!["relation key requirements are not lowered".to_owned()],
                    ..ExplainPlan::default()
                },
            })
        })?;
    }
    Ok(())
}

impl From<FieldProjection> for FieldRequirement {
    fn from(value: FieldProjection) -> Self {
        match value {
            FieldProjection::All => FieldRequirement::All,
            FieldProjection::Fields(fields) => FieldRequirement::Fields(fields),
        }
    }
}

fn collect_step_requirements(
    step: &LinearStep,
    source: &SourceId,
    requirements: &mut SourceRequirements,
) -> CapabilityResult<()> {
    let result: Result<(), UnsupportedReason> = match step {
        LinearStep::Filter(predicate) => {
            collect_predicate_requirements(predicate, source, requirements)
        }
        LinearStep::Join { on, .. } => (|| {
            collect_predicate_requirements(on, source, requirements)?;
            Ok(())
        })(),
        LinearStep::Project(columns) => (|| {
            for column in columns {
                collect_value_requirements(&column.value, source, requirements)?;
            }
            Ok(())
        })(),
        LinearStep::OrderBy(keys) => (|| {
            for key in keys {
                collect_value_requirements(&key.value, source, requirements)?;
            }
            Ok(())
        })(),
        LinearStep::Slice {
            partition_by,
            tie_breaker,
            ..
        } => (|| {
            for value in partition_by.iter().chain(tie_breaker) {
                collect_value_requirements(value, source, requirements)?;
            }
            Ok(())
        })(),
        LinearStep::Aggregate { group_by, outputs } => (|| {
            for value in group_by {
                collect_value_requirements(value, source, requirements)?;
            }
            for aggregate in outputs {
                if let Some(input) = &aggregate.input {
                    collect_value_requirements(input, source, requirements)?;
                }
            }
            Ok(())
        })(),
    };

    result.map_err(|gap| {
        Box::new(CapabilityReport {
            gaps: vec![gap],
            explain: ExplainPlan {
                capabilities: vec!["operator source requirements are not lowered".to_owned()],
                ..ExplainPlan::default()
            },
        })
    })
}

fn collect_predicate_requirements(
    predicate: &PredicateExpr,
    source: &SourceId,
    requirements: &mut SourceRequirements,
) -> Result<(), UnsupportedReason> {
    match predicate {
        PredicateExpr::True | PredicateExpr::False => Ok(()),
        PredicateExpr::Compare { left, right, .. } => {
            collect_value_requirements(left, source, requirements)?;
            collect_value_requirements(right, source, requirements)
        }
        PredicateExpr::In { value, options } => {
            collect_value_requirements(value, source, requirements)?;
            for option in options {
                collect_value_requirements(option, source, requirements)?;
            }
            Ok(())
        }
        PredicateExpr::ArrayContains { value, needle }
        | PredicateExpr::TextContains { value, needle } => {
            collect_value_requirements(value, source, requirements)?;
            collect_value_requirements(needle, source, requirements)
        }
        PredicateExpr::IsNull(value) | PredicateExpr::IsNotNull(value) => {
            collect_value_requirements(value, source, requirements)
        }
        PredicateExpr::And(predicates) | PredicateExpr::Or(predicates) => {
            for predicate in predicates {
                collect_predicate_requirements(predicate, source, requirements)?;
            }
            Ok(())
        }
        PredicateExpr::Not(predicate) => {
            collect_predicate_requirements(predicate, source, requirements)
        }
        PredicateExpr::EnumMatch { value, .. } => {
            collect_value_requirements(value, source, requirements)
        }
    }
}

fn collect_value_requirements(
    value: &NormalizedValueRef,
    source: &SourceId,
    requirements: &mut SourceRequirements,
) -> Result<(), UnsupportedReason> {
    match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } => {
            if value_source != source {
                return Ok(());
            }
            add_required_app_field(requirements, field.clone());
        }
        NormalizedValueRef::Provenance {
            source: value_source,
            field,
        } => {
            if value_source != source {
                return Ok(());
            }
            requirements
                .metadata
                .insert(SourceMetadataRequirement::Provenance(*field));
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) if value_source == source => {}
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) => {
            let _ = value_source;
        }
        NormalizedValueRef::Param(_)
        | NormalizedValueRef::Claim(_)
        | NormalizedValueRef::Literal(_) => {}
        NormalizedValueRef::FrontierColumn { .. }
        | NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => {}
    }
    Ok(())
}

fn add_required_app_field(requirements: &mut SourceRequirements, field: String) {
    match &mut requirements.app_fields {
        FieldRequirement::None => {
            requirements.app_fields = FieldRequirement::Fields(BTreeSet::from([field]));
        }
        FieldRequirement::Fields(fields) => {
            fields.insert(field);
        }
        FieldRequirement::All => {}
    }
}
