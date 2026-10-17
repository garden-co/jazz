/// One kind of durable enum case identity: compact scalar tags (`u8`) or
/// payload enum tags (`u32`). Each kind has a root registry and a nested
/// per-path registry in [`TablePhysicalMapping`]; the lookups and remaps below
/// are written once over this trait. Error texts are `[root, nested]` pairs.
trait EnumCase: Clone + Ord + Sized {
    type Tag: TryFrom<usize>;
    const MISSING: [&'static str; 2];
    const TARGET_TAG_EXHAUSTED: [&'static str; 2];
    const COPIED_TAG_EXHAUSTED: [&'static str; 2];

    fn root(table: &TablePhysicalMapping) -> &BTreeMap<PhysicalColumnId, Vec<Self>>;
    fn nested(
        table: &TablePhysicalMapping,
    ) -> &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<Self>>>;
    fn compare(
        aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
        left: &Self,
        right: &Self,
    ) -> std::cmp::Ordering;
    /// Record one occurrence's tags under `path`; payload kinds also record
    /// the child path rooted at each case in `children`.
    fn install<'a>(
        remaps: &mut EnumOccurrenceRemaps,
        path: &str,
        tags: Vec<Option<Self::Tag>>,
        children: impl Iterator<Item = Option<&'a Self>>,
    ) where
        Self: 'a;
    /// Hook run after the root occurrence of a physical-to-authored remap.
    fn after_authored_root(_remaps: &mut EnumOccurrenceRemaps) {}

    /// The registry at the root (`None`) or a nested structural path.
    fn occurrence<'a>(
        table: &'a TablePhysicalMapping,
        column_id: PhysicalColumnId,
        path: Option<&str>,
    ) -> Option<&'a Vec<Self>> {
        match path {
            None => Self::root(table).get(&column_id),
            Some(path) => Self::nested(table).get(&column_id)?.get(path),
        }
    }
}

fn enum_error(messages: [&'static str; 2], path: Option<&str>) -> Error {
    Error::InvalidStoredValue(messages[usize::from(path.is_some())])
}

impl EnumCase for GlobalScalarEnumCaseId {
    type Tag = u8;
    const MISSING: [&'static str; 2] = [
        "physical scalar enum registry identity mapping missing",
        "physical nested scalar enum registry identity mapping missing",
    ];
    const TARGET_TAG_EXHAUSTED: [&'static str; 2] = [
        "target scalar enum tag exhausted",
        "target nested scalar enum tag exhausted",
    ];
    const COPIED_TAG_EXHAUSTED: [&'static str; 2] = [
        "copied scalar enum tag exhausted",
        "copied nested scalar enum tag exhausted",
    ];

    fn root(table: &TablePhysicalMapping) -> &BTreeMap<PhysicalColumnId, Vec<Self>> {
        &table.scalar_enum_cases
    }
    fn nested(
        table: &TablePhysicalMapping,
    ) -> &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<Self>>> {
        &table.nested_scalar_enum_cases
    }
    fn compare(
        aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
        left: &Self,
        right: &Self,
    ) -> std::cmp::Ordering {
        compare_scalar_enum_cases(aliases, left, right)
    }
    fn install<'a>(
        remaps: &mut EnumOccurrenceRemaps,
        path: &str,
        tags: Vec<Option<u8>>,
        _children: impl Iterator<Item = Option<&'a Self>>,
    ) {
        remaps.scalar.insert(path.to_owned(), tags);
    }
}

impl EnumCase for GlobalEnumCaseId {
    type Tag = u32;
    const MISSING: [&'static str; 2] = [
        "physical payload enum registry identity mapping missing",
        "physical nested payload enum registry identity mapping missing",
    ];
    const TARGET_TAG_EXHAUSTED: [&'static str; 2] = [
        "target payload enum tag exhausted",
        "target nested payload enum tag exhausted",
    ];
    const COPIED_TAG_EXHAUSTED: [&'static str; 2] = [
        "copied payload enum tag exhausted",
        "copied nested payload enum tag exhausted",
    ];

    fn root(table: &TablePhysicalMapping) -> &BTreeMap<PhysicalColumnId, Vec<Self>> {
        &table.payload_enum_cases
    }
    fn nested(
        table: &TablePhysicalMapping,
    ) -> &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<Self>>> {
        &table.nested_payload_enum_cases
    }
    fn compare(
        aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
        left: &Self,
        right: &Self,
    ) -> std::cmp::Ordering {
        compare_global_enum_cases(aliases, left, right)
    }
    fn install<'a>(
        remaps: &mut EnumOccurrenceRemaps,
        path: &str,
        tags: Vec<Option<u32>>,
        children: impl Iterator<Item = Option<&'a Self>>,
    ) {
        remaps.payload.insert(path.to_owned(), tags);
        remaps.payload_children.insert(
            path.to_owned(),
            children
                .map(|identity| identity.map(|identity| global_case_path(path, identity)))
                .collect(),
        );
    }
    /// Payload values also receive a `root/nullable` alias for the
    /// current-table's synthetic nullable cell carrier.
    fn after_authored_root(remaps: &mut EnumOccurrenceRemaps) {
        if let Some(tags) = remaps.payload.get("root").cloned() {
            remaps.payload.insert("root/nullable".to_owned(), tags);
        }
        if let Some(children) = remaps.payload_children.get("root").cloned() {
            remaps
                .payload_children
                .insert("root/nullable".to_owned(), children);
        }
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn physical_scalar_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        self.physical_enum_cases(table_id, column_id, None)
    }

    fn physical_payload_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
    ) -> Result<Vec<GlobalEnumCaseId>, Error> {
        self.physical_enum_cases(table_id, column_id, None)
    }

    fn physical_nested_scalar_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: &str,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        self.physical_enum_cases(table_id, column_id, Some(path))
    }

    fn physical_nested_payload_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: &str,
    ) -> Result<Vec<GlobalEnumCaseId>, Error> {
        self.physical_enum_cases(table_id, column_id, Some(path))
    }

    /// The union of one enum occurrence's registry across every mapping of
    /// the physical table, in durable introduction order.
    fn physical_enum_cases<C: EnumCase>(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: Option<&str>,
    ) -> Result<Vec<C>, Error> {
        let mut cases = BTreeSet::new();
        for table in self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values())
            .filter(|table| table.table_id == table_id)
        {
            if let Some(column_cases) = C::occurrence(table, column_id, path) {
                cases.extend(column_cases.iter().cloned());
            }
        }
        if cases.is_empty() {
            return Err(enum_error(C::MISSING, path));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases
            .sort_by(|left, right| C::compare(&self.catalogue.schema_version_aliases, left, right));
        Ok(cases)
    }

    /// The lineage union for an occurrence, or `fallback` when the union is
    /// not yet durable.
    fn physical_enum_cases_or<C: EnumCase>(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: Option<&str>,
        fallback: &[C],
    ) -> Vec<C> {
        self.physical_enum_cases(table_id, column_id, path)
            .unwrap_or_else(|_| fallback.to_vec())
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
        self.install_authored_enum_remaps::<GlobalScalarEnumCaseId>(
            &mut remaps,
            target_mapping,
            column_id,
        )?;
        self.install_authored_enum_remaps::<GlobalEnumCaseId>(
            &mut remaps,
            target_mapping,
            column_id,
        )?;
        Ok(remaps)
    }

    fn install_authored_enum_remaps<C: EnumCase>(
        &self,
        remaps: &mut EnumOccurrenceRemaps,
        target_mapping: &TablePhysicalMapping,
        column_id: PhysicalColumnId,
    ) -> Result<(), Error> {
        let root = C::root(target_mapping)
            .get(&column_id)
            .map(|cases| (None, cases));
        let nested = C::nested(target_mapping)
            .get(&column_id)
            .into_iter()
            .flatten()
            .map(|(path, cases)| (Some(path.as_str()), cases));
        for (path, target_cases) in root.into_iter().chain(nested) {
            // Bootstrap defines the physical table before its freshly
            // hydrated catalogue mapping is durable.  In that one state the
            // target's own registry is necessarily the complete physical
            // registry; later states must use the lineage union.
            let physical_cases =
                self.physical_enum_cases_or(target_mapping.table_id, column_id, path, target_cases);
            let tags = physical_cases
                .iter()
                .map(|identity| {
                    target_cases
                        .iter()
                        .position(|candidate| candidate == identity)
                        .map(|tag| {
                            C::Tag::try_from(tag)
                                .map_err(|_| enum_error(C::TARGET_TAG_EXHAUSTED, path))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            C::install(
                remaps,
                path.unwrap_or("root"),
                tags,
                physical_cases.iter().map(Some),
            );
            if path.is_none() {
                C::after_authored_root(remaps);
            }
        }
        Ok(())
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
        let (source, target) = (
            (source_mapping, source_column_id),
            (target_mapping, target_column_id),
        );
        self.install_copied_enum_remaps::<GlobalScalarEnumCaseId>(&mut remaps, source, target)?;
        self.install_copied_enum_remaps::<GlobalEnumCaseId>(&mut remaps, source, target)?;
        bootstrap_copy_enum_remaps(source_column_type, target_column_type, "root", &mut remaps)?;
        Ok(remaps)
    }

    fn install_copied_enum_remaps<C: EnumCase>(
        &self,
        remaps: &mut EnumOccurrenceRemaps,
        (source_mapping, source_column_id): (&TablePhysicalMapping, PhysicalColumnId),
        (target_mapping, target_column_id): (&TablePhysicalMapping, PhysicalColumnId),
    ) -> Result<(), Error> {
        let root = C::root(source_mapping)
            .get(&source_column_id)
            .map(|cases| (None, cases));
        let nested = C::nested(source_mapping)
            .get(&source_column_id)
            .into_iter()
            .flatten()
            .filter(|(_, cases)| !cases.is_empty())
            .map(|(path, cases)| (Some(path.as_str()), cases));
        for (path, source_cases) in root.into_iter().chain(nested) {
            let source_cases = self.physical_enum_cases_or(
                source_mapping.table_id,
                source_column_id,
                path,
                source_cases,
            );
            let target_cases = C::occurrence(target_mapping, target_column_id, path)
                .map(|fallback| {
                    self.physical_enum_cases_or(
                        target_mapping.table_id,
                        target_column_id,
                        path,
                        fallback,
                    )
                })
                .unwrap_or_default();
            let tags = (0..source_cases.len())
                .map(|ordinal| {
                    target_cases
                        .get(ordinal)
                        .map(|_| {
                            C::Tag::try_from(ordinal)
                                .map_err(|_| enum_error(C::COPIED_TAG_EXHAUSTED, path))
                        })
                        .transpose()
                })
                .collect::<Result<_, _>>()?;
            C::install(
                remaps,
                path.unwrap_or("root"),
                tags,
                (0..source_cases.len()).map(|ordinal| target_cases.get(ordinal)),
            );
        }
        Ok(())
    }
}
