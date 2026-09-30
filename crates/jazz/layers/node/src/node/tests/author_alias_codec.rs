/// Semantic-to-exact-byte receipt for the `jazz.author-alias.v1` storage
/// codec family (SPEC 2 §2.2, "Row authors follow the same discipline").
///
/// One ordinary local commit by `system@node(1)` must leave exactly these
/// durable bytes:
///
/// ```text
/// jazz_authors  { id: U32 = 1, author: RowAuthor record }  one row, pinned below
/// jazz_transactions.made_by                  = 01 00 00 00   (alias 1, U32 LE)
/// history created_by                        = 01 00 00 00   (alias 1, U32 LE)
/// history updated_by                        = null          (Nullable(U32): it is
///                                                            the tx's own author)
/// ```
///
/// The alias is physical shorthand only: the stored transaction and the
/// row's provenance still decode to the full author.
#[test]
fn author_alias_codec_v1_pins_physical_bytes() {
    let (_temp_dir, mut core) = open_node();
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xa1), 10).cells(title_cells("aliased")),
        )
        .unwrap();
    let author = RowAuthor::system_at(node(1));
    const ALIAS_1: [u8; 4] = [0x01, 0x00, 0x00, 0x00];

    // jazz_authors: fixed-width `U32` id, then the exact author record bytes.
    let authors = core
        .database
        .primary_key_scan_raw("jazz_authors", &[])
        .unwrap();
    assert_eq!(authors.len(), 1, "one author, one alias");
    let record = authors[0].record();
    let descriptor = record.descriptor();
    let fields = descriptor.fields();
    assert_eq!(
        fields[AuthorAliasRowRecord::FIELD_ID_IDX].value_type,
        records::ValueType::U32
    );
    assert_eq!(
        fields[AuthorAliasRowRecord::FIELD_AUTHOR_IDX].value_type,
        RowAuthor::value_type()
    );
    let author_span = record
        .descriptor()
        .field_span(record.raw(), AuthorAliasRowRecord::FIELD_AUTHOR_IDX)
        .unwrap();
    assert_eq!(&record.raw()[author_span], author.encoded_record().raw());
    assert_eq!(
        hex::encode(record.raw()),
        AUTHOR_ALIAS_V1_JAZZ_AUTHORS_ROW_HEX,
        "jazz_authors row bytes"
    );
    assert!(
        core.database
            .primary_key_get_raw("jazz_authors", &[Value::U32(1)])
            .unwrap()
            .is_some()
    );

    // jazz_transactions.made_by: the 4-byte alias, not the author record.
    let transactions = core
        .database
        .primary_key_scan_raw("jazz_transactions", &[])
        .unwrap();
    assert_eq!(transactions.len(), 1);
    let record = transactions[0].record();
    assert_eq!(
        record.descriptor().fields()[TransactionRowRecord::FIELD_MADE_BY_IDX].value_type,
        records::ValueType::U32
    );
    let span = record
        .descriptor()
        .field_span(record.raw(), TransactionRowRecord::FIELD_MADE_BY_IDX)
        .unwrap();
    assert_eq!(&record.raw()[span], ALIAS_1);

    // Physical history row: `created_by` holds the alias; `updated_by` is a
    // nullable alias, null because it is the transaction's own author.
    let table_id = core
        .physical_table_id_for_schema(core.catalogue.local_schema_version_id, "todos")
        .unwrap();
    let history = core
        .database
        .primary_key_scan_raw(&physical_history_table_name(table_id), &[])
        .unwrap();
    assert_eq!(history.len(), 1);
    let record = history[0].record();
    let created_by = HistoryRowRecord::FIELD_CREATED_BY_IDX;
    assert_eq!(
        record.descriptor().fields()[created_by].value_type,
        records::ValueType::U32
    );
    let span = record
        .descriptor()
        .field_span(record.raw(), created_by)
        .unwrap();
    assert_eq!(&record.raw()[span], ALIAS_1);
    let updated_by = HistoryRowRecord::FIELD_UPDATED_BY_IDX;
    assert_eq!(
        record.descriptor().fields()[updated_by].value_type,
        records::ValueType::U32.nullable()
    );
    assert_eq!(record.get_idx(updated_by).unwrap(), Value::Nullable(None));

    // Decoding resolves the alias back to the full author.
    assert_eq!(
        core.query_transaction(tx_id).unwrap().unwrap().tx.made_by,
        author.as_author_subject()
    );
}

/// `jazz_authors` row for alias 1 -> `RowAuthor::system_at(node(1))`: the
/// fixed-width `U32` id, then the unchanged Groove record encoding of the
/// author (asserted byte-equal to `RowAuthor::encoded_record` above; Groove
/// SPEC 2 §2.7). Each record puts fixed fields first and stores the end offset
/// of every variable field but the last.
const AUTHOR_ALIAS_V1_JAZZ_AUTHORS_ROW_HEX: &str = concat!(
    // jazz_authors row: id (fixed), then author (its only variable field).
    "01000000", // id: AuthorAlias(1), U32 little-endian
    // author record: account (fixed Uuid), then identity (only variable field).
    "00000000000000000000000000000000", // account: SYSTEM_ACCOUNT_ID
    // identity record: issuer and subject, both variable-width strings.
    "14000000",                       // issuer ends at identity-relative byte 20
    "02",                             // String StoredScalar arm tag
    "75726e3a6a617a7a3a73797374656d", // issuer: "urn:jazz:system"
    "02",                             // String StoredScalar arm tag
    // subject: node(1) UUID "01010101-0101-0101-0101-010101010101"
    "30313031303130312d303130312d303130312d303130312d303130313031303130313031",
);
