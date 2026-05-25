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
    format!(
        r#"[{{"kind":"row","table":"{}","rowId":"{}","visibleTxId":"{}","reason":"{}"}}]"#,
        read.table, read.row_id, read.visible_tx_id, read.reason
    )
}

pub fn encode_absence_read(read: &EncodedAbsenceRead) -> String {
    format!(
        r#"[{{"kind":"range","table":"{}","index":"{}_by_row_id_deleted","predicate":{{"rowId":"{}","isDeleted":false}},"reason":"{}"}}]"#,
        read.table, read.table, read.row_id, read.reason
    )
}

pub fn decode_first_row_read(input: &str) -> Option<EncodedRowRead> {
    if !input.contains(r#""kind":"row""#) {
        return None;
    }
    Some(EncodedRowRead {
        table: value_after(input, r#""table":""#)?,
        row_id: value_after(input, r#""rowId":""#)?,
        visible_tx_id: value_after(input, r#""visibleTxId":""#)?,
        reason: value_after(input, r#""reason":""#)?,
    })
}

pub fn decode_first_absence_read(input: &str) -> Option<EncodedAbsenceRead> {
    if !input.contains(r#""kind":"range""#) || !input.contains(r#""isDeleted":false"#) {
        return None;
    }
    Some(EncodedAbsenceRead {
        table: value_after(input, r#""table":""#)?,
        row_id: value_after(input, r#""rowId":""#)?,
        reason: value_after(input, r#""reason":""#)?,
    })
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
