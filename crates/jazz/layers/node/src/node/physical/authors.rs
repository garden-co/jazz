// Row- and transaction-author aliasing at the physical storage boundary.
//
// Logical row images (history/current descriptors, `VersionRow`, query
// graphs, policies, wire records) carry the full `RowAuthor` record in
// `created_by` / `updated_by`, and in-memory / wire transactions carry the
// full `made_by` subject. The physical row tables below and
// `jazz_transactions.made_by` store a node-local `AuthorAlias` (`U32`, 4-byte
// little-endian) there instead. Writes translate record -> alias
// (allocating a durable `jazz_authors` row in the same batch on first use);
// reads translate alias -> exact record bytes, through the shared author
// dictionary for Groove projections, through `expand_physical_row_authors`
// for direct row reads and through `stored_transaction_made_by` for
// transaction rows.

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

fn row_author_descriptor() -> records::RecordDescriptor {
    let records::ValueType::Record(descriptor) = RowAuthor::value_type() else {
        unreachable!("row author is a record type")
    };
    *descriptor
}

fn is_row_author_column(name: &str) -> bool {
    name == "created_by" || name == "updated_by"
}

/// Alias type storing a logical author type: `U32`, or `Nullable(U32)` for
/// a nullable author (history `updated_by`, which is null when it is the
/// author of the image's own transaction).
fn author_alias_value_type(logical: &records::ValueType) -> records::ValueType {
    match logical {
        records::ValueType::Nullable(_) => records::ValueType::U32.nullable(),
        _ => records::ValueType::U32,
    }
}

/// Whether `value_type` is an alias author type (`U32` or `Nullable(U32)`).
fn is_author_alias_value_type(value_type: &records::ValueType) -> bool {
    match value_type {
        records::ValueType::U32 => true,
        records::ValueType::Nullable(inner) => inner.as_ref() == &records::ValueType::U32,
        _ => false,
    }
}

/// Physical declaration of one logical system column of `shape`.
fn physical_system_column(
    column: GrooveColumnSchema,
    shape: ContentProjectionShape,
) -> GrooveColumnSchema {
    if physical_row_authors_aliased(shape) && is_row_author_column(&column.name) {
        let value_type = author_alias_value_type(&column.column_type);
        GrooveColumnSchema::new(column.name, value_type)
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
        author_alias_value_type(logical)
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
            .is_some_and(|field| is_author_alias_value_type(&field.value_type))
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
    /// into a current physical table. A new or still-provisional alias also
    /// puts its `jazz_authors` row into `batch`, so the mapping becomes
    /// durable atomically with the first row that uses it.
    pub(super) fn stage_row_author_aliases(
        &mut self,
        version: &VersionRow,
        batch: &mut DatabaseBatch,
    ) -> Result<(), Error> {
        self.stage_created_by_alias(version, batch)?;
        let updated_by = version.updated_by();
        if updated_by != version.created_by() {
            self.stage_row_author_subject_alias(updated_by, batch)?;
        }
        Ok(())
    }

    /// Like [`Self::stage_row_author_aliases`] for a history image written by
    /// a transaction authored by `tx_author`. The image stores `updated_by`
    /// only when it differs from `tx_author`, so only then is it staged; the
    /// transaction row stages its own author alias.
    pub(super) fn stage_history_row_author_aliases(
        &mut self,
        version: &VersionRow,
        tx_author: AuthorSubject,
        batch: &mut DatabaseBatch,
    ) -> Result<(), Error> {
        self.stage_created_by_alias(version, batch)?;
        let updated_by = version.updated_by();
        if updated_by != tx_author && updated_by != version.created_by() {
            self.stage_row_author_subject_alias(updated_by, batch)?;
        }
        Ok(())
    }

    fn stage_created_by_alias(
        &mut self,
        version: &VersionRow,
        batch: &mut DatabaseBatch,
    ) -> Result<(), Error> {
        let record = version.record.borrowed();
        let index = HistoryRowRecord::FIELD_CREATED_BY_IDX;
        if is_author_alias_value_type(&record.descriptor().fields()[index].value_type) {
            return Ok(());
        }
        let span = record.descriptor().field_span(record.raw(), index)?;
        self.stage_author_alias(&record.raw()[span], batch)?;
        Ok(())
    }

    fn stage_row_author_subject_alias(
        &mut self,
        author: AuthorSubject,
        batch: &mut DatabaseBatch,
    ) -> Result<AuthorAlias, Error> {
        let author =
            RowAuthor::from_persisted_subject(author).map_err(|_| Error::UnadmittedWriteAuthor)?;
        self.stage_author_alias(author.encoded_record().raw(), batch)
    }

    /// Alias for storing one exact encoded author record. A new or
    /// still-provisional alias also upserts its `jazz_authors` row into
    /// `batch`, so the mapping is durable atomically with its first use.
    fn stage_author_alias(
        &mut self,
        author: &[u8],
        batch: &mut DatabaseBatch,
    ) -> Result<AuthorAlias, Error> {
        let (alias, needs_row) = self
            .author_aliases
            .stage(author)
            .map_err(|_| Error::AuthorAliasSpaceExhausted)?;
        if needs_row {
            batch.update(
                "jazz_authors",
                vec![
                    Value::U32(alias.0),
                    Value::Record(OwnedRecord::new(author.to_vec(), row_author_descriptor())),
                ],
            );
        }
        Ok(alias)
    }

    /// Alias stored in `jazz_transactions.made_by` for a transaction authored
    /// by `made_by`, staged into `batch` exactly like a row author. Callers
    /// put the transaction row into the same `batch`.
    pub(super) fn stage_transaction_author_alias(
        &mut self,
        made_by: AuthorSubject,
        batch: &mut DatabaseBatch,
    ) -> Result<AuthorAlias, Error> {
        let author =
            RowAuthor::from_persisted_subject(made_by).map_err(|_| Error::UnadmittedWriteAuthor)?;
        self.stage_author_alias(author.encoded_record().raw(), batch)
    }

    /// Resident alias of `made_by`, without allocating. `None` when no row
    /// or transaction stored on this node has that exact author.
    pub(super) fn resident_transaction_author_alias(
        &self,
        made_by: AuthorSubject,
    ) -> Option<AuthorAlias> {
        let author = RowAuthor::from_persisted_subject(made_by).ok()?;
        self.author_aliases
            .alias_for(author.encoded_record().raw())
    }

    /// Decode `jazz_transactions.made_by` (a stored alias) back to the
    /// transaction's author subject.
    pub(super) fn stored_transaction_made_by(
        &self,
        record: records::BorrowedRecord<'_>,
    ) -> Result<AuthorSubject, Error> {
        let alias = AuthorAlias(record.get_u32(TransactionRowRecord::FIELD_MADE_BY_IDX)?);
        let author = self.author_aliases.author_record(alias).ok_or(
            Error::InvalidStoredValue("stored transaction author alias is not in jazz_authors"),
        )?;
        let descriptor = row_author_descriptor();
        Ok(RowAuthor::from_record(descriptor.bind(&author))
            .map_err(|_| Error::InvalidStoredValue("invalid durable transaction author"))?
            .as_author_subject())
    }

    /// Alias of a staged author record for physical encoding.
    fn staged_author_alias(&self, author: &[u8]) -> Result<AuthorAlias, Error> {
        self.author_aliases
            .alias_for(author)
            .ok_or(Error::InvalidStoredValue("row author alias was not staged"))
    }

    /// Physical value of a logical author `value` (a `RowAuthor` record, or
    /// a nullable one) for a field of `physical` type: its alias when the
    /// field stores aliases, otherwise `value` unchanged.
    pub(super) fn physical_author_value(
        &self,
        value: Value,
        physical: &records::ValueType,
    ) -> Result<Value, Error> {
        if !is_author_alias_value_type(physical) {
            return Ok(value);
        }
        Ok(match value {
            Value::Record(author) => Value::U32(self.staged_author_alias(author.raw())?.0),
            Value::Nullable(Some(inner)) => match *inner {
                Value::Record(author) => Value::Nullable(Some(Box::new(Value::U32(
                    self.staged_author_alias(author.raw())?.0,
                )))),
                other => Value::Nullable(Some(Box::new(other))),
            },
            other => other,
        })
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
                    let alias = match input.get_idx(index)? {
                        Value::U32(alias) => AuthorAlias(alias),
                        // History `updated_by` is null when it is the author
                        // of the image's own transaction; a read fills it in.
                        Value::Nullable(None) => {
                            expanded.encode_field_into(index, &Value::Nullable(None), output)?;
                            return Ok(());
                        }
                        Value::Nullable(Some(inner)) => match *inner {
                            Value::U32(alias) => AuthorAlias(alias),
                            _ => {
                                return Err(Error::InvalidStoredValue(
                                    "stored row author alias is not a U32",
                                ));
                            }
                        },
                        _ => {
                            return Err(Error::InvalidStoredValue(
                                "stored row author alias is not a U32",
                            ));
                        }
                    };
                    let author = aliases.author_record(alias).ok_or(
                        Error::InvalidStoredValue("stored row author alias is not in jazz_authors"),
                    )?;
                    if matches!(
                        expanded.fields()[index].value_type,
                        records::ValueType::Nullable(_)
                    ) {
                        expanded.encode_field_into(
                            index,
                            &Value::Nullable(Some(Box::new(Value::Record(OwnedRecord::new(
                                author.to_vec(),
                                row_author_descriptor(),
                            ))))),
                            output,
                        )?;
                    } else {
                        output.extend_from_slice(&author);
                    }
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
