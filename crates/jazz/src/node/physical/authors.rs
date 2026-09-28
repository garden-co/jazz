// Row-author aliasing at the physical storage boundary.
//
// Logical row images (history/current descriptors, `VersionRow`, query
// graphs, policies, wire records) carry the full `RowAuthor` record in
// `created_by` / `updated_by`. The physical tables below store a node-local
// `AuthorAlias` (`U32`, 4-byte little-endian) there instead. Writes translate record -> alias
// (allocating a durable `jazz_authors` row in the same batch on first use);
// reads translate alias -> exact record bytes, through the shared author
// dictionary for Groove projections and through `expand_physical_row_authors`
// for direct storage reads.

/// Field indices of `created_by` / `updated_by`. The history and current
/// layouts share this system prefix, so one pair serves both.
pub(super) const ROW_AUTHOR_FIELDS: [usize; 2] = [
    HistoryRowRecord::FIELD_CREATED_BY_IDX,
    HistoryRowRecord::FIELD_UPDATED_BY_IDX,
];

const _: () = assert!(
    HistoryRowRecord::FIELD_CREATED_BY_IDX == GlobalCurrentRowRecord::FIELD_CREATED_BY_IDX
        && HistoryRowRecord::FIELD_UPDATED_BY_IDX == GlobalCurrentRowRecord::FIELD_UPDATED_BY_IDX
);

/// Whether physical tables of `shape` store author aliases.
fn physical_row_authors_aliased(shape: ContentProjectionShape) -> bool {
    match shape {
        ContentProjectionShape::History | ContentProjectionShape::Current => true,
    }
}

fn is_row_author_column(name: &str) -> bool {
    name == "created_by" || name == "updated_by"
}

/// Physical declaration of one logical system column of `shape`.
fn physical_system_column(
    column: GrooveColumnSchema,
    shape: ContentProjectionShape,
) -> GrooveColumnSchema {
    if physical_row_authors_aliased(shape) && is_row_author_column(&column.name) {
        GrooveColumnSchema::new(column.name, records::ValueType::U32)
    } else {
        column
    }
}

/// Physical value type of one logical system field of `shape`.
fn physical_system_value_type(
    name: &str,
    logical: &records::ValueType,
    shape: ContentProjectionShape,
) -> records::ValueType {
    if physical_row_authors_aliased(shape) && is_row_author_column(name) {
        records::ValueType::U32
    } else {
        logical.clone()
    }
}

/// Whether `descriptor` stores author aliases in its author fields.
pub(super) fn descriptor_has_author_aliases(descriptor: &records::RecordDescriptor) -> bool {
    ROW_AUTHOR_FIELDS.iter().all(|index| {
        descriptor
            .fields()
            .get(*index)
            .is_some_and(|field| field.value_type == records::ValueType::U32)
    })
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Load every durable author alias into the resident catalogue.
    pub(super) async fn load_author_aliases(&mut self) -> Result<(), Error> {
        for raw in self
            .database
            .primary_key_scan_raw("jazz_authors", &[])
            .await?
        {
            let record = raw.record();
            let alias = AuthorAlias(record.get_u32(AuthorAliasRowRecord::FIELD_ID_IDX)?);
            let span = record
                .descriptor()
                .field_span(record.raw(), AuthorAliasRowRecord::FIELD_AUTHOR_IDX)?;
            let author = &record.raw()[span];
            // Validate the durable record before it can reach any reader.
            RowAuthor::from_record(record.get_record(AuthorAliasRowRecord::FIELD_AUTHOR_IDX)?)
                .map_err(|_| Error::InvalidStoredValue("invalid durable row author"))?;
            self.author_aliases
                .install_durable(alias, author)
                .map_err(|_| {
                    Error::InvalidStoredValue("row author alias maps to conflicting authors")
                })?;
        }
        Ok(())
    }

    /// Promote provisional aliases whose author row has become resident.
    /// Cheap when nothing is provisional, which is the steady state.
    pub(super) async fn settle_provisional_author_aliases(&mut self) -> Result<(), Error> {
        if !self.author_aliases.has_provisional() {
            return Ok(());
        }
        let provisional = self.author_aliases.provisional().collect::<Vec<_>>();
        for alias in provisional {
            if self
                .database
                .primary_key_get_raw("jazz_authors", &[Value::U32(alias.0)])
                .await?
                .is_some()
            {
                self.author_aliases.confirm(alias);
            }
        }
        Ok(())
    }

    /// Ensure both authors of `version` have aliases before it is encoded
    /// into a physical table. A new or still-provisional alias also puts its
    /// `jazz_authors` row into `batch`, so the mapping becomes durable
    /// atomically with the first row that uses it.
    pub(super) fn stage_row_author_aliases(
        &mut self,
        version: &VersionRow,
        batch: &mut DatabaseBatch,
    ) -> Result<(), Error> {
        let record = version.record.borrowed();
        let mut staged = None;
        for index in ROW_AUTHOR_FIELDS {
            if record.descriptor().fields()[index].value_type == records::ValueType::U32 {
                continue;
            }
            let span = record.descriptor().field_span(record.raw(), index)?;
            let author = &record.raw()[span];
            let (alias, needs_row) = self
                .author_aliases
                .stage(author)
                .map_err(|_| Error::AuthorAliasSpaceExhausted)?;
            if needs_row && staged != Some(alias) {
                batch.update(
                    "jazz_authors",
                    vec![
                        Value::U32(alias.0),
                        Value::Record(OwnedRecord::new(author.to_vec(), {
                            let records::ValueType::Record(descriptor) = RowAuthor::value_type()
                            else {
                                unreachable!("row author is a record type")
                            };
                            *descriptor
                        })),
                    ],
                );
                staged = Some(alias);
            }
        }
        Ok(())
    }

    /// Alias of a staged author record for physical encoding.
    fn staged_author_alias(&self, author: &[u8]) -> Result<AuthorAlias, Error> {
        self.author_aliases
            .alias_for(author)
            .ok_or(Error::InvalidStoredValue("row author alias was not staged"))
    }

    /// Replace the author aliases of a physical row read directly from
    /// storage with their exact author records, so the row reads like any
    /// logical row image. Rows without aliases are returned unchanged.
    pub(super) fn expand_physical_row_authors(
        &mut self,
        record: OwnedRecord,
    ) -> Result<OwnedRecord, Error> {
        let physical = *record.descriptor();
        if !descriptor_has_author_aliases(&physical) {
            return Ok(record);
        }
        let expanded = self
            .author_aliases
            .expanded_descriptor(physical, &ROW_AUTHOR_FIELDS);
        let input = record.borrowed();
        let aliases = &self.author_aliases;
        let raw = expanded.create_with_encoded_fields::<Error>(
            input.raw().len() + 128,
            |index, output| {
                if ROW_AUTHOR_FIELDS.contains(&index) {
                    let alias = AuthorAlias(input.get_u32(index)?);
                    let author = aliases.author_record(alias).ok_or(
                        Error::InvalidStoredValue("stored row author alias is not in jazz_authors"),
                    )?;
                    output.extend_from_slice(&author);
                    return Ok(());
                }
                let span = physical.field_span(input.raw(), index)?;
                output.extend_from_slice(&input.raw()[span]);
                Ok(())
            },
        )?;
        Ok(OwnedRecord::new(raw, expanded))
    }
}
