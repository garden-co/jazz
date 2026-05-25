#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRowRead {
    pub table: String,
    pub row_id: String,
    pub visible_tx_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedAbsenceRead {
    pub table: String,
    pub row_id: String,
    pub reason: String,
}

pub fn encode_row_read(read: &EncodedRowRead) -> String {
    encode_row_reads(std::slice::from_ref(read))
}

pub fn encode_row_reads(reads: &[EncodedRowRead]) -> String {
    let entries = reads
        .iter()
        .map(|read| {
            format!(
                r#"{{"kind":"row","table":"{}","rowId":"{}","visibleTxId":"{}","reason":"{}"}}"#,
                read.table, read.row_id, read.visible_tx_id, read.reason
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{entries}]")
}

pub fn encode_mixed_reads(
    row_reads: &[EncodedRowRead],
    absence_reads: &[EncodedAbsenceRead],
) -> String {
    let mut entries = Vec::new();
    for read in row_reads {
        entries.push(format!(
            r#"{{"kind":"row","table":"{}","rowId":"{}","visibleTxId":"{}","reason":"{}"}}"#,
            read.table, read.row_id, read.visible_tx_id, read.reason
        ));
    }
    for read in absence_reads {
        entries.push(format!(
            r#"{{"kind":"range","table":"{}","index":"{}_by_row_id_deleted","predicate":{{"rowId":"{}","isDeleted":false}},"reason":"{}"}}"#,
            read.table, read.table, read.row_id, read.reason
        ));
    }
    format!("[{}]", entries.join(","))
}

pub fn encode_absence_read(read: &EncodedAbsenceRead) -> String {
    format!(
        "[{}]",
        encode_mixed_reads(&[], std::slice::from_ref(read))
            .trim_start_matches('[')
            .trim_end_matches(']')
    )
}

pub fn decode_first_row_read(input: &str) -> Option<EncodedRowRead> {
    decode_row_reads(input).into_iter().next()
}

pub fn decode_row_reads(input: &str) -> Vec<EncodedRowRead> {
    input
        .split(r#"{"kind":"row""#)
        .skip(1)
        .filter_map(|entry| {
            let entry = format!(r#"{{"kind":"row"{entry}"#);
            Some(EncodedRowRead {
                table: value_after(&entry, r#""table":""#)?,
                row_id: value_after(&entry, r#""rowId":""#)?,
                visible_tx_id: value_after(&entry, r#""visibleTxId":""#)?,
                reason: value_after(&entry, r#""reason":""#)?,
            })
        })
        .collect()
}

pub fn decode_first_absence_read(input: &str) -> Option<EncodedAbsenceRead> {
    decode_absence_reads(input).into_iter().next()
}

pub fn decode_absence_reads(input: &str) -> Vec<EncodedAbsenceRead> {
    input
        .split(r#"{"kind":"range""#)
        .skip(1)
        .filter(|entry| entry.contains(r#""isDeleted":false"#))
        .filter_map(|entry| {
            let entry = format!(r#"{{"kind":"range"{entry}"#);
            Some(EncodedAbsenceRead {
                table: value_after(&entry, r#""table":""#)?,
                row_id: value_after(&entry, r#""rowId":""#)?,
                reason: value_after(&entry, r#""reason":""#)?,
            })
        })
        .collect()
}

fn value_after(input: &str, marker: &str) -> Option<String> {
    let start = input.find(marker)? + marker.len();
    let rest = &input[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_read_round_trips_through_the_tiny_codec() {
        let read = EncodedRowRead {
            table: "todos".into(),
            row_id: "todo-1".into(),
            visible_tx_id: "tx-base".into(),
            reason: "write_base".into(),
        };

        assert_eq!(decode_first_row_read(&encode_row_read(&read)), Some(read));
    }

    #[test]
    fn multiple_row_reads_round_trip_through_the_tiny_codec() {
        let reads = vec![
            EncodedRowRead {
                table: "todos".into(),
                row_id: "todo-1".into(),
                visible_tx_id: "tx-base-1".into(),
                reason: "write_base".into(),
            },
            EncodedRowRead {
                table: "projects".into(),
                row_id: "project-1".into(),
                visible_tx_id: "tx-base-2".into(),
                reason: "policy_dependency".into(),
            },
        ];

        assert_eq!(decode_row_reads(&encode_row_reads(&reads)), reads);
    }

    #[test]
    fn absence_read_round_trips_through_the_tiny_codec() {
        let read = EncodedAbsenceRead {
            table: "projects".into(),
            row_id: "project-missing".into(),
            reason: "optional_join_absence".into(),
        };

        assert_eq!(
            decode_first_absence_read(&encode_absence_read(&read)),
            Some(read)
        );
    }
}
