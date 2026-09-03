fn contribution_gset_element_descriptor(
    column_type: &records::ValueType,
) -> Result<records::RecordDescriptor, Error> {
    let records::ValueType::Array(element_type) = column_type else {
        return Err(Error::InvalidMergeableCommit(
            "g-set contribution column must be an array",
        ));
    };
    Ok(records::RecordDescriptor::new([(
        "element",
        element_type.as_ref().clone(),
    )]))
}

fn encode_contribution_gset_identity(
    column_type: &records::ValueType,
    element: &Value,
) -> Result<Vec<u8>, Error> {
    Ok(contribution_gset_element_descriptor(column_type)?
        .create(std::slice::from_ref(element))?)
}

fn decode_contribution_gset_identity(
    column_type: &records::ValueType,
    identity: &[u8],
) -> Result<Value, Error> {
    let descriptor = contribution_gset_element_descriptor(column_type)?;
    let malformed = || {
        Error::InvalidStoredValue("g-set contribution operation identity must be canonical")
    };
    let element = records::BorrowedRecord::new(identity, &descriptor)
        .get_idx(0)
        .map_err(|_| malformed())?;
    if descriptor
        .create(std::slice::from_ref(&element))
        .map_err(|_| malformed())?
        != identity
    {
        return Err(malformed());
    }
    Ok(element)
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// The one durable admission boundary for contribution provenance. Every
    /// path that can persist a transaction must pass through this before it
    /// can allocate aliases, stage large values, or mutate a batch.
    pub(super) fn admit_contribution_merge_for_storage(
        &self,
        tx: &Transaction,
    ) -> Result<Value, Error> {
        self.validate_contribution_merge_operation_identities(tx)?;
        self.contribution_merge_storage_value(tx.contribution_merge.as_ref())
    }

    /// Validate strategy-defined operation coordinates before a transaction can
    /// become durable.  Operation identity is not opaque provenance: its
    /// canonical spelling is part of merge deduplication, so a received or
    /// recovered record must be checked against the admitted schema rather
    /// than deferred until a later contribution calculation happens to read
    /// it.
    pub(super) fn validate_contribution_merge_operation_identities(
        &self,
        tx: &Transaction,
    ) -> Result<(), Error> {
        let Some(provenance) = &tx.contribution_merge else {
            return Ok(());
        };
        provenance.validate().map_err(|_| {
            Error::InvalidStoredValue("transaction contribution provenance must be canonical")
        })?;
        // Validate every branch key structurally against its *authored*
        // physical table before any durable codec calls `canonical_bytes`.
        // Raw input must become a malformed rejection, never an encoder panic.
        for intent in &provenance.branch_write_intents {
            let catalogue_schema = self
                .catalogue
                .catalogue_schemas
                .get(&intent.authored_schema)
                .ok_or(Error::InvalidStoredValue("branch write intent schema is unknown"))?;
            let mapping = self
                .catalogue
                .physical_mappings
                .get(&intent.authored_schema)
                .ok_or(Error::InvalidStoredValue("branch write intent physical mapping is missing"))?;
            let (table_name, _) = mapping
                .tables
                .iter()
                .find(|(_, table)| table.table_id == intent.physical_table_id)
                .ok_or(Error::InvalidStoredValue("branch write intent table is unknown"))?;
            let table = catalogue_schema
                .schema
                .tables
                .iter()
                .find(|table| &table.name == table_name)
                .ok_or(Error::InvalidStoredValue("branch write intent table schema is missing"))?;
            catalogue_schema
                .schema
                .validate_authored_branch_key(table, &intent.head)
                .map_err(Error::InvalidBranchKey)?;
            if let crate::tx::BranchWriteOperation::ViewUpdateCopy(evidence) = &intent.operation {
                if evidence.table != *table_name
                    || evidence.row_uuid != intent.row_uuid
                    || evidence.head != intent.head
                {
                    return Err(Error::InvalidStoredValue(
                        "branch write copy evidence is not bound to its intent",
                    ));
                }
                catalogue_schema
                    .schema
                    .validate_authored_branch_key(table, &evidence.head)
                    .map_err(Error::InvalidBranchKey)?;
                let base = match &evidence.base {
                    crate::tx::BranchViewCopyBase::Current(base) => base,
                    crate::tx::BranchViewCopyBase::Snapshot { branch, .. } => branch,
                };
                catalogue_schema
                    .schema
                    .validate_authored_branch_key(table, base)
                    .map_err(Error::InvalidBranchKey)?;
            }
        }
        let schema_version = self.catalogue.current_write_schema.schema;
        let coordinates = provenance.substitutions.iter().flat_map(|substitution| {
            std::iter::once(&substitution.target)
                .chain(substitution.sources.iter().map(|source| &source.coordinate))
        });
        for coordinate in coordinates {
            let ContributionComponent::Operation { column, identity } = &coordinate.component
            else {
                continue;
            };
            if coordinate.layer != MergeAspect::Content {
                return Err(Error::InvalidStoredValue(
                    "contribution operation must belong to the content layer",
                ));
            }
            let table = self.table_in_schema(&coordinate.table, schema_version)?;
            let column_schema = table
                .columns
                .iter()
                .find(|candidate| candidate.name == *column)
                .ok_or(Error::InvalidStoredValue(
                    "contribution operation column is absent from its table",
                ))?;
            match table.merge_strategy(column) {
                MergeStrategy::Counter if identity.is_empty() => {}
                MergeStrategy::Counter => {
                    return Err(Error::InvalidStoredValue(
                        "counter contribution operation identity must be empty",
                    ));
                }
                MergeStrategy::GSet => {
                    decode_contribution_gset_identity(&column_schema.column_type, identity)?;
                }
                MergeStrategy::Lww => {
                    return Err(Error::InvalidStoredValue(
                        "lww contribution column must not use an operation identity",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Calculate novel scalar, counter, grow-only-set, and deletion-register
    /// contributions between explicit branch views and commit the result as
    /// one ordinary atomic transaction. Unsupported or incomplete calculation
    /// fails before any output transaction is minted.
    pub async fn merge_branch_contributions(
        &mut self,
        request: ContributionMergeRequest,
    ) -> Result<Option<PublishedTransaction>, Error> {
        self.require_catalogue_ready()?;
        if !self.is_history_complete() {
            return Err(Error::InvalidMergeableCommit(
                "contribution merge requires complete history",
            ));
        }
        if request.rows.is_empty() {
            return Ok(None);
        }
        let schema_version = self.catalogue.current_write_schema.schema;
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("current write schema missing"))?
            .schema
            .clone();
        let full_key = |selector: &BranchSelector| -> Result<BranchKey, Error> {
            let branch_columns = schema
                .tables
                .iter()
                .flat_map(|table| {
                    table.branch_by.iter().map(|name| {
                        let column = table
                            .columns
                            .iter()
                            .find(|column| column.name == *name)
                            .expect("validated branch column");
                        (name.clone(), column.column_type.clone())
                    })
                })
                .collect::<BTreeMap<_, _>>();
            if selector.values.keys().collect::<BTreeSet<_>>()
                != branch_columns.keys().collect::<BTreeSet<_>>()
            {
                return Err(Error::InvalidBranchKey(
                    "contribution selector must bind every schema branch column".to_owned(),
                ));
            }
            let values = selector
                .values
                .iter()
                .map(|(name, encoded)| {
                    let value = encoded.decode().map_err(|_| {
                        Error::InvalidBranchKey(format!(
                            "invalid contribution branch column {name} encoding"
                        ))
                    })?;
                    let encoded = crate::protocol::BranchColumnValue::encode_typed(
                        &value,
                        &branch_columns[name],
                    )
                    .map_err(|_| {
                        Error::InvalidBranchKey(format!(
                            "invalid contribution branch column {name} value"
                        ))
                    })?;
                    Ok((name.clone(), encoded))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(BranchKey { values })
        };
        let source_full = full_key(&request.source)?;
        let target_full = full_key(&request.target)?;
        let mut index = ContributionSubstitutionIndex::default();
        let mut commits = Vec::new();
        let mut substitutions = Vec::new();

        for selected in &request.rows {
            let table = self.table_in_schema(&selected.table, schema_version)?;
            let source_shape = crate::query::Query::from(selected.table.as_str())
                .validate_with_schema_version(&schema, schema_version)?;
            let source_binding = source_shape.bind(BTreeMap::new())?;
            let source_view = crate::protocol::ReadViewSpec::branch_view(
                request.source.clone(),
                None,
            );
            let source_identity = request.permission_subject.unwrap_or(request.made_by);
            let mut source_is_readable = self
                .query_relation_snapshot_for_serving_in_read_view(
                    &source_shape,
                    &source_binding,
                    DurabilityTier::Local,
                    source_identity,
                    &source_view,
                )
                .await?
                .rows
                .into_iter()
                .any(|row| row.row_uuid() == selected.row_uuid);
            if !source_is_readable {
                source_is_readable = self
                    .query_rows_including_deleted_with_query_engine(
                        &source_shape,
                        &source_binding,
                        DurabilityTier::Local,
                        source_identity,
                        QueryAuthorizationMode::TrustedServing,
                        &source_view,
                    )
                    .await?
                    .into_iter()
                    .any(|row| row.row_uuid() == selected.row_uuid);
            }
            if !source_is_readable {
                return Err(Error::InvalidMergeableCommit(
                    "calculated merge source row is unreadable",
                ));
            }
            let (source_key, _) = schema
                .project_branch_view_selector(&table, &request.source)
                .map_err(Error::InvalidBranchKey)?;
            let (target_key, _) = schema
                .project_branch_view_selector(&table, &request.target)
                .map_err(Error::InvalidBranchKey)?;
            let versions = self.query_table_versions(&selected.table).await?;
            let all_row_versions = versions
                .iter()
                .filter(|version| version.row_uuid() == selected.row_uuid)
                .cloned()
                .collect::<Vec<_>>();
            let mut source_versions = Vec::new();
            let mut target_versions = Vec::new();
            let mut observed_transactions = BTreeSet::new();
            for version in versions {
                if version.row_uuid() != selected.row_uuid {
                    continue;
                }
                if schema.branch_key_matches(&table, version.branch_key(), &source_key) {
                    source_versions.push(version.clone());
                }
                if schema.branch_key_matches(&table, version.branch_key(), &target_key) {
                    target_versions.push(version.clone());
                }
                if schema.branch_key_matches(&table, version.branch_key(), &source_key)
                    || schema.branch_key_matches(&table, version.branch_key(), &target_key)
                {
                    let tx_id = self.version_tx_id(&version)?;
                    if observed_transactions.insert(tx_id)
                        && let Some(tx) = self.query_transaction(tx_id).await?
                        && let Some(provenance) = &tx.tx.contribution_merge
                    {
                        let mut provenance = provenance.clone();
                        for substitution in &mut provenance.substitutions {
                            let target_table = schema
                                .tables
                                .iter()
                                .find(|table| table.name == substitution.target.table)
                                .ok_or(Error::InvalidStoredValue(
                                    "contribution target table missing from current schema",
                                ))?;
                            substitution.target.branch_key = schema
                                .normalize_branch_key(
                                    target_table,
                                    &substitution.target.branch_key,
                                )
                                .map_err(Error::InvalidBranchKey)?;
                            for source in &mut substitution.sources {
                                let source_table = schema
                                    .tables
                                    .iter()
                                    .find(|table| table.name == source.coordinate.table)
                                    .ok_or(Error::InvalidStoredValue(
                                        "contribution source table missing from current schema",
                                    ))?;
                                source.coordinate.branch_key = schema
                                    .normalize_branch_key(
                                        source_table,
                                        &source.coordinate.branch_key,
                                    )
                                    .map_err(Error::InvalidBranchKey)?;
                            }
                        }
                        index
                            .observe(tx_id, &provenance)
                            .map_err(Error::InvalidMergeableCommit)?;
                    }
                }
            }

            let latest_dot = |node: &Self,
                              versions: &[VersionRow],
                              layer: VersionLayer,
                              component: ContributionComponent|
             -> Result<Option<ContributionDot>, Error> {
                let mut selected_dot: Option<ContributionDot> = None;
                for version in versions.iter().filter(|version| version.layer() == layer) {
                    if let Some(column) = match &component {
                        ContributionComponent::Column(column) => Some(column.as_str()),
                        ContributionComponent::Operation { column, .. } => Some(column.as_str()),
                        ContributionComponent::Register => None,
                    } {
                        let authored = node.authored_columns_for_version(version)?;
                        if !authored.as_ref().is_none_or(|columns| columns.contains(column))
                            || version.cell(&table, column)?.is_none()
                        {
                            continue;
                        }
                    }
                    let dot = ContributionDot {
                        tx_id: node.version_tx_id(version)?,
                        coordinate: ContributionCoordinate {
                            branch_key: schema
                                .normalize_branch_key(&table, version.branch_key())
                                .map_err(Error::InvalidBranchKey)?,
                            table: selected.table.clone(),
                            row_uuid: selected.row_uuid,
                            layer: match layer {
                                VersionLayer::Content => MergeAspect::Content,
                                VersionLayer::Deletion => MergeAspect::Deletion,
                            },
                            component: component.clone(),
                        },
                    };
                    if selected_dot.as_ref().is_none_or(|current| {
                        dot.tx_id.time.sort_key(dot.tx_id.node)
                            > current.tx_id.time.sort_key(current.tx_id.node)
                    }) {
                        selected_dot = Some(dot);
                    }
                }
                Ok(selected_dot)
            };

            let source_content = self
                .query_current_layer_winner_in_branch(
                    &selected.table,
                    &source_key,
                    selected.row_uuid,
                    VersionLayer::Content,
                )
                .await?;
            let mut cells = BTreeMap::new();
            let mut authored = BTreeSet::new();
            for column in table.columns.iter().filter(|column| {
                !table.branch_by.contains(&column.name)
            }) {
                let strategy = table.merge_strategy(&column.name);
                let component = match strategy {
                    MergeStrategy::Lww => ContributionComponent::Column(column.name.clone()),
                    MergeStrategy::Counter | MergeStrategy::GSet => {
                        ContributionComponent::Operation {
                            column: column.name.clone(),
                            identity: Vec::new(),
                        }
                    }
                };
                let novel = match strategy {
                    MergeStrategy::Lww => {
                        let Some(source_dot) = latest_dot(
                            self,
                            &source_versions,
                            VersionLayer::Content,
                            component.clone(),
                        )? else {
                            continue;
                        };
                        let target_dot = latest_dot(
                            self,
                            &target_versions,
                            VersionLayer::Content,
                            component.clone(),
                        )?;
                        index
                            .novel([source_dot], target_dot)
                            .map_err(Error::InvalidMergeableCommit)?
                    }
                    MergeStrategy::Counter => {
                        let dots = |versions: &[VersionRow]| -> Result<Vec<ContributionDot>, Error> {
                            let mut dots = Vec::new();
                            for version in versions
                                .iter()
                                .filter(|version| version.layer() == VersionLayer::Content)
                            {
                                let authored = self.authored_columns_for_version(version)?;
                                if !authored
                                    .as_ref()
                                    .is_none_or(|columns| columns.contains(&column.name))
                                {
                                    continue;
                                }
                                dots.push(ContributionDot {
                                    tx_id: self.version_tx_id(version)?,
                                    coordinate: ContributionCoordinate {
                                        branch_key: schema
                                            .normalize_branch_key(&table, version.branch_key())
                                            .map_err(Error::InvalidBranchKey)?,
                                        table: selected.table.clone(),
                                        row_uuid: selected.row_uuid,
                                        layer: MergeAspect::Content,
                                        component: component.clone(),
                                    },
                                });
                            }
                            Ok(dots)
                        };
                        index
                            .novel(dots(&source_versions)?, dots(&target_versions)?)
                            .map_err(Error::InvalidMergeableCommit)?
                    }
                    MergeStrategy::GSet => {
                        let dots = |versions: &[VersionRow]| -> Result<Vec<ContributionDot>, Error> {
                            let branch_versions = versions
                                .iter()
                                .filter(|version| version.layer() == VersionLayer::Content)
                                .map(|version| {
                                    self.version_tx_id(version)
                                        .map(|tx_id| (tx_id, version.clone()))
                                })
                                .collect::<Result<BTreeMap<_, _>, Error>>()?;
                            let mut dots = Vec::new();
                            for version in versions
                                .iter()
                                .filter(|version| version.layer() == VersionLayer::Content)
                            {
                                let authored = self.authored_columns_for_version(version)?;
                                if !authored
                                    .as_ref()
                                    .is_none_or(|columns| columns.contains(&column.name))
                                {
                                    continue;
                                }
                                let parent = ingest::gset_merge_value(
                                    &table,
                                    &column.name,
                                    &branch_versions,
                                    &version.parents(),
                                )?;
                                let Value::Array(parent) = parent else {
                                    return Err(Error::InvalidStoredValue(
                                        "g-set parent must materialize an array",
                                    ));
                                };
                                let parent = parent
                                    .iter()
                                    .map(|element| {
                                        encode_contribution_gset_identity(
                                            &column.column_type,
                                            element,
                                        )
                                    })
                                    .collect::<Result<BTreeSet<_>, _>>()?;
                                let Some(Value::Array(elements)) =
                                    version.cell(&table, &column.name)?
                                else {
                                    continue;
                                };
                                for element in elements {
                                    let identity = encode_contribution_gset_identity(
                                        &column.column_type,
                                        &element,
                                    )?;
                                    if parent.contains(&identity) {
                                        continue;
                                    }
                                    dots.push(ContributionDot {
                                        tx_id: self.version_tx_id(version)?,
                                        coordinate: ContributionCoordinate {
                                            branch_key: schema
                                                .normalize_branch_key(&table, version.branch_key())
                                                .map_err(Error::InvalidBranchKey)?,
                                            table: selected.table.clone(),
                                            row_uuid: selected.row_uuid,
                                            layer: MergeAspect::Content,
                                            component: ContributionComponent::Operation {
                                                column: column.name.clone(),
                                                identity,
                                            },
                                        },
                                    });
                                }
                            }
                            Ok(dots)
                        };
                        index
                            .novel(dots(&source_versions)?, dots(&target_versions)?)
                            .map_err(Error::InvalidMergeableCommit)?
                    }
                };
                if novel.is_empty() {
                    continue;
                }
                let value = match strategy {
                    MergeStrategy::Lww => {
                        let Some(value) = source_content
                            .as_ref()
                            .map(|version| version.cell(&table, &column.name))
                            .transpose()?
                            .flatten()
                        else {
                            continue;
                        };
                        value
                    }
                    MergeStrategy::Counter => {
                        let target_value = self
                            .query_current_layer_winner_in_branch(
                                &selected.table,
                                &target_key,
                                selected.row_uuid,
                                VersionLayer::Content,
                            )
                            .await?
                            .as_ref()
                            .map(|version| version.cell(&table, &column.name))
                            .transpose()?
                            .flatten()
                            .as_ref()
                            .map(ingest::counter_value_to_i128)
                            .transpose()?
                            .unwrap_or(0);
                        let mut imported = 0i128;
                        for root in &novel {
                            let version = all_row_versions
                                .iter()
                                .find(|version| {
                                    schema.branch_key_matches(
                                        &table,
                                        version.branch_key(),
                                        &root.coordinate.branch_key,
                                    )
                                        && self.version_tx_id(version).ok() == Some(root.tx_id)
                                        && version.layer() == VersionLayer::Content
                                })
                                .ok_or(Error::MissingTransaction(root.tx_id))?;
                            let branch_versions = all_row_versions
                                .iter()
                                .filter(|candidate| {
                                    schema.branch_key_matches(
                                        &table,
                                        candidate.branch_key(),
                                        &root.coordinate.branch_key,
                                    )
                                        && candidate.layer() == VersionLayer::Content
                                })
                                .map(|candidate| {
                                    self.version_tx_id(candidate)
                                        .map(|tx_id| (tx_id, candidate.clone()))
                                })
                                .collect::<Result<BTreeMap<_, _>, Error>>()?;
                            let authored_value = version
                                .cell(&table, &column.name)?
                                .ok_or(Error::InvalidStoredValue(
                                    "counter contribution value missing",
                                ))?;
                            let parent_value = ingest::counter_merge_value(
                                &table,
                                &column.name,
                                &branch_versions,
                                &version.parents(),
                                &mut BTreeMap::new(),
                            )?;
                            imported += ingest::counter_value_to_i128(&authored_value)? - parent_value;
                        }
                        ingest::counter_value_from_i128(
                            &column.column_type,
                            target_value + imported,
                        )?
                    }
                    MergeStrategy::GSet => {
                        let mut elements = BTreeMap::<Vec<u8>, Value>::new();
                        if let Some(Value::Array(current)) = self
                            .query_current_layer_winner_in_branch(
                                &selected.table,
                                &target_key,
                                selected.row_uuid,
                                VersionLayer::Content,
                            )
                            .await?
                            .as_ref()
                            .map(|version| version.cell(&table, &column.name))
                            .transpose()?
                            .flatten()
                        {
                            for element in current {
                                elements.insert(
                                    encode_contribution_gset_identity(
                                        &column.column_type,
                                        &element,
                                    )?,
                                    element,
                                );
                            }
                        }
                        for root in &novel {
                            let ContributionComponent::Operation {
                                column: operation_column,
                                identity,
                            } = &root.coordinate.component
                            else {
                                return Err(Error::InvalidStoredValue(
                                    "g-set contribution operation missing",
                                ));
                            };
                            if operation_column != &column.name {
                                return Err(Error::InvalidStoredValue(
                                    "g-set contribution operation column mismatch",
                                ));
                            }
                            let element = decode_contribution_gset_identity(
                                &column.column_type,
                                identity,
                            )?;
                            elements.insert(identity.clone(), element);
                        }
                        Value::Array(elements.into_values().collect())
                    }
                };
                cells.insert(column.name.clone(), value);
                authored.insert(column.name.clone());
                if strategy == MergeStrategy::GSet {
                    let mut by_operation = BTreeMap::<Vec<u8>, Vec<ContributionDot>>::new();
                    for root in novel {
                        let ContributionComponent::Operation {
                            column: operation_column,
                            identity,
                        } = &root.coordinate.component
                        else {
                            return Err(Error::InvalidStoredValue(
                                "g-set contribution operation missing",
                            ));
                        };
                        if operation_column != &column.name {
                            return Err(Error::InvalidStoredValue(
                                "g-set contribution operation column mismatch",
                            ));
                        }
                        by_operation.entry(identity.clone()).or_default().push(root);
                    }
                    substitutions.extend(by_operation.into_iter().map(
                        |(identity, sources)| ContributionSubstitution {
                            target: ContributionCoordinate {
                                branch_key: target_key.clone(),
                                table: selected.table.clone(),
                                row_uuid: selected.row_uuid,
                                layer: MergeAspect::Content,
                                component: ContributionComponent::Operation {
                                    column: column.name.clone(),
                                    identity,
                                },
                            },
                            sources,
                        },
                    ));
                } else {
                    substitutions.push(ContributionSubstitution {
                        target: ContributionCoordinate {
                            branch_key: target_key.clone(),
                            table: selected.table.clone(),
                            row_uuid: selected.row_uuid,
                            layer: MergeAspect::Content,
                            component,
                        },
                        sources: novel.into_iter().collect(),
                    });
                }
            }
            if !cells.is_empty() {
                let parents = self
                    .query_current_layer_winner_in_branch(
                        &selected.table,
                        &target_key,
                        selected.row_uuid,
                        VersionLayer::Content,
                    )
                    .await?
                    .as_ref()
                    .map(|version| self.version_tx_id(version))
                    .transpose()?
                    .into_iter()
                    .collect();
                commits.push(
                    MergeableCommit::new(&selected.table, selected.row_uuid, request.now_ms)
                        .branch(request.target.clone())
                        .made_by(request.made_by)
                        .cells(cells)
                        .authored_columns(authored)
                        .parents(parents),
                );
            }

            let source_deletion = latest_dot(
                self,
                &source_versions,
                VersionLayer::Deletion,
                ContributionComponent::Register,
            )?;
            if let Some(source_dot) = source_deletion {
                let target_dot = latest_dot(
                    self,
                    &target_versions,
                    VersionLayer::Deletion,
                    ContributionComponent::Register,
                )?;
                let novel = index
                    .novel([source_dot], target_dot)
                    .map_err(Error::InvalidMergeableCommit)?;
                if !novel.is_empty() {
                    let source_register = self
                        .query_current_layer_winner_in_branch(
                            &selected.table,
                            &source_key,
                            selected.row_uuid,
                            VersionLayer::Deletion,
                        )
                        .await?
                        .ok_or(Error::InvalidStoredValue(
                            "source deletion contribution has no current register",
                        ))?;
                    let parents = self
                        .query_current_layer_winner_in_branch(
                            &selected.table,
                            &target_key,
                            selected.row_uuid,
                            VersionLayer::Deletion,
                        )
                        .await?
                        .as_ref()
                        .map(|version| self.version_tx_id(version))
                        .transpose()?
                        .into_iter()
                        .collect();
                    let mut commit =
                        MergeableCommit::new(&selected.table, selected.row_uuid, request.now_ms)
                            .branch(request.target.clone())
                            .made_by(request.made_by)
                            .parents(parents)
                            .deletion(source_register.deletion().ok_or(
                                Error::InvalidStoredValue("source register event missing"),
                            )?);
                    if let Some(subject) = request.permission_subject {
                        commit = commit.permission_subject(subject);
                    }
                    commits.push(commit);
                    substitutions.push(ContributionSubstitution {
                        target: ContributionCoordinate {
                            branch_key: target_key,
                            table: selected.table.clone(),
                            row_uuid: selected.row_uuid,
                            layer: MergeAspect::Deletion,
                            component: ContributionComponent::Register,
                        },
                        sources: novel.into_iter().collect(),
                    });
                }
            }
        }
        if commits.is_empty() {
            return Ok(None);
        }
        if let Some(subject) = request.permission_subject {
            for commit in &mut commits {
                commit.permission_subject = Some(subject);
            }
        }
        for commit in &commits {
            if !self.dry_run_mergeable_write_allows_in_schema(
                schema_version,
                commit.clone(),
            )
            .await?
            {
                return Err(Error::InvalidMergeableCommit(
                    "calculated merge target write is unauthorized",
                ));
            }
        }
        let provenance = ContributionMergeProvenance::canonical(
            source_full,
            target_full,
            substitutions,
        )
        .map_err(Error::InvalidMergeableCommit)?;
        self.commit_calculated_merge_many(commits, provenance)
            .await
            .map(Some)
    }
}
