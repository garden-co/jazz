impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn physical_scalar_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table.scalar_enum_cases.get(&column_id) {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical scalar enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_scalar_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    fn physical_payload_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
    ) -> Result<Vec<GlobalEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table.payload_enum_cases.get(&column_id) {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical payload enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_global_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    fn physical_nested_scalar_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: &str,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table
                    .nested_scalar_enum_cases
                    .get(&column_id)
                    .and_then(|paths| paths.get(path))
                {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical nested scalar enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_scalar_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    fn physical_nested_payload_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: &str,
    ) -> Result<Vec<GlobalEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table
                    .nested_payload_enum_cases
                    .get(&column_id)
                    .and_then(|paths| paths.get(path))
                {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical nested payload enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_global_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    /// Construct the physical-to-authored side of the enum interning boundary
    /// for one user cell. Entries are keyed by structural occurrence; absent
    /// target cases stay `None`, so older schema views fail rather than
    /// fabricating a value. Payload values also receive a `root/nullable`
    /// alias for the current-table's synthetic nullable cell carrier.
    fn physical_to_authored_enum_remaps(
        &self,
        target_mapping: &TablePhysicalMapping,
        column_id: PhysicalColumnId,
    ) -> Result<EnumOccurrenceRemaps, Error> {
        let mut remaps = EnumOccurrenceRemaps::default();
        if let Some(target_cases) = target_mapping.scalar_enum_cases.get(&column_id) {
            // Bootstrap defines the physical table before its freshly
            // hydrated catalogue mapping is durable.  In that one state the
            // target's own registry is necessarily the complete physical
            // registry; later states must use the lineage union below.
            let physical_cases = self
                .physical_scalar_enum_cases(target_mapping.table_id, column_id)
                .unwrap_or_else(|_| target_cases.clone());
            let tags = physical_cases
                .iter()
                .map(|identity| {
                    target_cases
                        .iter()
                        .position(|candidate| candidate == identity)
                        .map(|tag| {
                            u8::try_from(tag).map_err(|_| {
                                Error::InvalidStoredValue("target scalar enum tag exhausted")
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            remaps.scalar.insert("root".to_owned(), tags);
        }
        if let Some(target_cases) = target_mapping.payload_enum_cases.get(&column_id) {
            let physical_cases = self
                .physical_payload_enum_cases(target_mapping.table_id, column_id)
                .unwrap_or_else(|_| target_cases.clone());
            let tags = physical_cases
                .iter()
                .map(|identity| {
                    target_cases
                        .iter()
                        .position(|candidate| candidate == identity)
                        .map(|tag| {
                            u32::try_from(tag).map_err(|_| {
                                Error::InvalidStoredValue("target payload enum tag exhausted")
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let children = physical_cases
                .iter()
                .map(|identity| Some(global_case_path("root", identity)))
                .collect::<Vec<_>>();
            remaps.payload.insert("root".to_owned(), tags.clone());
            remaps
                .payload_children
                .insert("root".to_owned(), children.clone());
            remaps.payload.insert("root/nullable".to_owned(), tags);
            remaps
                .payload_children
                .insert("root/nullable".to_owned(), children);
        }
        if let Some(paths) = target_mapping.nested_scalar_enum_cases.get(&column_id) {
            for (path, target_cases) in paths {
                let physical_cases = self
                    .physical_nested_scalar_enum_cases(target_mapping.table_id, column_id, path)
                    .unwrap_or_else(|_| target_cases.clone());
                remaps.scalar.insert(
                    path.clone(),
                    physical_cases
                        .iter()
                        .map(|identity| {
                            target_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u8::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "target nested scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
        }
        if let Some(paths) = target_mapping.nested_payload_enum_cases.get(&column_id) {
            for (path, target_cases) in paths {
                let physical_cases = self
                    .physical_nested_payload_enum_cases(target_mapping.table_id, column_id, path)
                    .unwrap_or_else(|_| target_cases.clone());
                remaps.payload.insert(
                    path.clone(),
                    physical_cases
                        .iter()
                        .map(|identity| {
                            target_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u32::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "target nested payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    path.clone(),
                    physical_cases
                        .iter()
                        .map(|identity| Some(global_case_path(path, identity)))
                        .collect(),
                );
            }
        }
        Ok(remaps)
    }

    /// Re-encode a source physical enum occurrence into a distinct copied
    /// column's physical registry. Copying an enum's raw tag is invalid: each
    /// physical column owns an independent durable registry, even when the
    /// authored enum layouts are identical. The lens validates compatible
    /// layouts, so ordinal correspondence is the authored copy relation while
    /// this remap translates both sides' physical tags and nested payload
    /// paths.
    fn physical_copy_enum_remaps(
        &self,
        source_mapping: &TablePhysicalMapping,
        source_column_id: PhysicalColumnId,
        target_mapping: &TablePhysicalMapping,
        target_column_id: PhysicalColumnId,
        source_column_type: &records::ValueType,
        target_column_type: &records::ValueType,
    ) -> Result<EnumOccurrenceRemaps, Error> {
        let mut remaps = EnumOccurrenceRemaps::default();
        if let Some(source_cases) = source_mapping.scalar_enum_cases.get(&source_column_id) {
            let source_cases = self
                .physical_scalar_enum_cases(source_mapping.table_id, source_column_id)
                .unwrap_or_else(|_| source_cases.clone());
            let target_cases = target_mapping
                .scalar_enum_cases
                .get(&target_column_id)
                .map(|fallback| {
                    self.physical_scalar_enum_cases(target_mapping.table_id, target_column_id)
                        .unwrap_or_else(|_| fallback.clone())
                })
                .unwrap_or_default();
            remaps.scalar.insert(
                "root".to_owned(),
                source_cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target_cases
                            .get(ordinal)
                            .map(|_| {
                                u8::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue("copied scalar enum tag exhausted")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
        }
        if let Some(source_cases) = source_mapping.payload_enum_cases.get(&source_column_id) {
            let source_cases = self
                .physical_payload_enum_cases(source_mapping.table_id, source_column_id)
                .unwrap_or_else(|_| source_cases.clone());
            let target_cases = target_mapping
                .payload_enum_cases
                .get(&target_column_id)
                .map(|fallback| {
                    self.physical_payload_enum_cases(target_mapping.table_id, target_column_id)
                        .unwrap_or_else(|_| fallback.clone())
                })
                .unwrap_or_default();
            remaps.payload.insert(
                "root".to_owned(),
                source_cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target_cases
                            .get(ordinal)
                            .map(|_| {
                                u32::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue("copied payload enum tag exhausted")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
            remaps.payload_children.insert(
                "root".to_owned(),
                source_cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target_cases
                            .get(ordinal)
                            .map(|identity| global_case_path("root", identity))
                    })
                    .collect(),
            );
        }
        if let Some(source_paths) = source_mapping
            .nested_scalar_enum_cases
            .get(&source_column_id)
        {
            for (path, source_cases) in source_paths {
                if source_cases.is_empty() {
                    continue;
                }
                let source_cases = self
                    .physical_nested_scalar_enum_cases(
                        source_mapping.table_id,
                        source_column_id,
                        path,
                    )
                    .unwrap_or_else(|_| source_cases.clone());
                let target_cases = target_mapping
                    .nested_scalar_enum_cases
                    .get(&target_column_id)
                    .and_then(|paths| paths.get(path))
                    .map(|fallback| {
                        self.physical_nested_scalar_enum_cases(
                            target_mapping.table_id,
                            target_column_id,
                            path,
                        )
                        .unwrap_or_else(|_| fallback.clone())
                    })
                    .unwrap_or_default();
                remaps.scalar.insert(
                    path.clone(),
                    source_cases
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| {
                            target_cases
                                .get(ordinal)
                                .map(|_| {
                                    u8::try_from(ordinal).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "copied nested scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
        }
        if let Some(source_paths) = source_mapping
            .nested_payload_enum_cases
            .get(&source_column_id)
        {
            for (path, source_cases) in source_paths {
                if source_cases.is_empty() {
                    continue;
                }
                let source_cases = self
                    .physical_nested_payload_enum_cases(
                        source_mapping.table_id,
                        source_column_id,
                        path,
                    )
                    .unwrap_or_else(|_| source_cases.clone());
                let target_cases = target_mapping
                    .nested_payload_enum_cases
                    .get(&target_column_id)
                    .and_then(|paths| paths.get(path))
                    .map(|fallback| {
                        self.physical_nested_payload_enum_cases(
                            target_mapping.table_id,
                            target_column_id,
                            path,
                        )
                        .unwrap_or_else(|_| fallback.clone())
                    })
                    .unwrap_or_default();
                remaps.payload.insert(
                    path.clone(),
                    source_cases
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| {
                            target_cases
                                .get(ordinal)
                                .map(|_| {
                                    u32::try_from(ordinal).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "copied nested payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    path.clone(),
                    source_cases
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| {
                            target_cases
                                .get(ordinal)
                                .map(|identity| global_case_path(path, identity))
                        })
                        .collect(),
                );
            }
        }
        bootstrap_copy_enum_remaps(source_column_type, target_column_type, "root", &mut remaps)?;
        Ok(remaps)
    }
}
