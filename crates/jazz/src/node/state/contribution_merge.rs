impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Calculate novel LWW field/register contributions between explicit branch
    /// views and commit the result as one ordinary atomic transaction.
    ///
    /// Counter and grow-only-set encoding are deliberately rejected until
    /// their native-operation extractors are available; partial calculation is
    /// never committed.
    pub fn merge_branch_contributions(
        &mut self,
        request: ContributionMergeRequest,
    ) -> Result<Option<TxId>, Error> {
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
            if selector.dimensions.len() != schema.branch_dimensions.len() {
                return Err(Error::InvalidBranchKey(
                    "contribution selector must bind every schema dimension".to_owned(),
                ));
            }
            let mut dimensions = schema
                .branch_dimensions
                .iter()
                .map(|dimension| {
                    selector
                        .dimensions
                        .get(&dimension.name)
                        .cloned()
                        .map(|value| (dimension.id, value))
                        .ok_or_else(|| {
                            Error::InvalidBranchKey(format!(
                                "missing branch dimension {}",
                                dimension.name
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            dimensions.sort_by_key(|(dimension, _)| *dimension);
            Ok(BranchKey { dimensions })
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
                )?
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
                    )?
                    .into_iter()
                    .any(|row| row.row_uuid() == selected.row_uuid);
            }
            if !source_is_readable {
                return Err(Error::InvalidMergeableCommit(
                    "calculated merge source row is unreadable",
                ));
            }
            if table
                .columns
                .iter()
                .filter(|column| {
                    !table
                        .branch_by
                        .iter()
                        .any(|binding| binding.column == column.name)
                })
                .any(|column| {
                    table
                        .merge_strategies
                        .get(&column.name)
                        .copied()
                        .unwrap_or_default()
                        != MergeStrategy::Lww
                })
            {
                return Err(Error::InvalidMergeableCommit(
                    "contribution merge strategy is not yet supported",
                ));
            }
            let (source_key, _) = schema
                .project_branch_view_selector(&table, &request.source)
                .map_err(Error::InvalidBranchKey)?;
            let (target_key, _) = schema
                .project_branch_view_selector(&table, &request.target)
                .map_err(Error::InvalidBranchKey)?;
            let versions = self.query_table_versions(&selected.table)?;
            let mut source_versions = Vec::new();
            let mut target_versions = Vec::new();
            let mut observed_transactions = BTreeSet::new();
            for version in versions {
                if version.row_uuid() != selected.row_uuid {
                    continue;
                }
                if version.branch_key() == &source_key {
                    source_versions.push(version.clone());
                }
                if version.branch_key() == &target_key {
                    target_versions.push(version.clone());
                }
                if version.branch_key() == &source_key || version.branch_key() == &target_key {
                    let tx_id = self.version_tx_id(&version)?;
                    if observed_transactions.insert(tx_id)
                        && let Some(tx) = self.query_transaction(tx_id)?
                        && let Some(provenance) = &tx.tx.contribution_merge
                    {
                        index
                            .observe(tx_id, provenance)
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
                    if let ContributionComponent::Column(column) = &component {
                        let authored = version.authored_columns(&table)?;
                        if !authored.as_ref().is_none_or(|columns| columns.contains(column))
                            || version.cell(&table, column)?.is_none()
                        {
                            continue;
                        }
                    }
                    let dot = ContributionDot {
                        tx_id: node.version_tx_id(version)?,
                        coordinate: ContributionCoordinate {
                            branch_key: version.branch_key().clone(),
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
                .query_local_layer_winner_in_branch(
                    &selected.table,
                    &source_key,
                    selected.row_uuid,
                    VersionLayer::Content,
                )?
                .or(self.query_global_layer_winner_in_branch(
                    &selected.table,
                    &source_key,
                    selected.row_uuid,
                    VersionLayer::Content,
                )?);
            let mut cells = BTreeMap::new();
            let mut authored = BTreeSet::new();
            for column in table.columns.iter().filter(|column| {
                !table
                    .branch_by
                    .iter()
                    .any(|binding| binding.column == column.name)
            }) {
                let Some(source_dot) = latest_dot(
                    self,
                    &source_versions,
                    VersionLayer::Content,
                    ContributionComponent::Column(column.name.clone()),
                )? else {
                    continue;
                };
                let target_dot = latest_dot(
                    self,
                    &target_versions,
                    VersionLayer::Content,
                    ContributionComponent::Column(column.name.clone()),
                )?;
                let novel = index
                    .novel([source_dot], target_dot)
                    .map_err(Error::InvalidMergeableCommit)?;
                if novel.is_empty() {
                    continue;
                }
                let Some(value) = source_content
                    .as_ref()
                    .map(|version| version.cell(&table, &column.name))
                    .transpose()?
                    .flatten()
                else {
                    continue;
                };
                cells.insert(column.name.clone(), value);
                authored.insert(column.name.clone());
                substitutions.push(ContributionSubstitution {
                    target: ContributionCoordinate {
                        branch_key: target_key.clone(),
                        table: selected.table.clone(),
                        row_uuid: selected.row_uuid,
                        layer: MergeAspect::Content,
                        component: ContributionComponent::Column(column.name.clone()),
                    },
                    sources: novel.into_iter().collect(),
                });
            }
            if !cells.is_empty() {
                let parents = self
                    .query_local_layer_winner_in_branch(
                        &selected.table,
                        &target_key,
                        selected.row_uuid,
                        VersionLayer::Content,
                    )?
                    .or(self.query_global_layer_winner_in_branch(
                        &selected.table,
                        &target_key,
                        selected.row_uuid,
                        VersionLayer::Content,
                    )?)
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
                        .query_local_layer_winner_in_branch(
                            &selected.table,
                            &source_key,
                            selected.row_uuid,
                            VersionLayer::Deletion,
                        )?
                        .or(self.query_global_layer_winner_in_branch(
                            &selected.table,
                            &source_key,
                            selected.row_uuid,
                            VersionLayer::Deletion,
                        )?)
                        .ok_or(Error::InvalidStoredValue(
                            "source deletion contribution has no current register",
                        ))?;
                    let parents = self
                        .query_local_layer_winner_in_branch(
                            &selected.table,
                            &target_key,
                            selected.row_uuid,
                            VersionLayer::Deletion,
                        )?
                        .or(self.query_global_layer_winner_in_branch(
                            &selected.table,
                            &target_key,
                            selected.row_uuid,
                            VersionLayer::Deletion,
                        )?)
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
            )? {
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
            .map(Some)
    }
}
