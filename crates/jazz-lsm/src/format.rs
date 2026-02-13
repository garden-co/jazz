use crate::error::LsmError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpKind {
    Put,
    Delete,
    Merge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionedRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) seq: u64,
    pub(crate) kind: OpKind,
    pub(crate) merge_op_id: u32,
    pub(crate) value: Vec<u8>,
}

impl VersionedRecord {
    pub(crate) fn put(key: Vec<u8>, seq: u64, value: Vec<u8>) -> Self {
        Self {
            key,
            seq,
            kind: OpKind::Put,
            merge_op_id: 0,
            value,
        }
    }

    pub(crate) fn delete(key: Vec<u8>, seq: u64) -> Self {
        Self {
            key,
            seq,
            kind: OpKind::Delete,
            merge_op_id: 0,
            value: Vec::new(),
        }
    }

    pub(crate) fn merge(key: Vec<u8>, seq: u64, merge_op_id: u32, operand: Vec<u8>) -> Self {
        Self {
            key,
            seq,
            kind: OpKind::Merge,
            merge_op_id,
            value: operand,
        }
    }
}

pub(crate) fn encode_record(record: &VersionedRecord) -> Vec<u8> {
    let mut payload = Vec::new();
    let kind = match record.kind {
        OpKind::Put => 0u8,
        OpKind::Delete => 1u8,
        OpKind::Merge => 2u8,
    };
    payload.push(kind);
    payload.extend_from_slice(&record.seq.to_le_bytes());
    payload.extend_from_slice(&record.merge_op_id.to_le_bytes());
    payload.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(record.value.len() as u32).to_le_bytes());
    payload.extend_from_slice(&record.key);
    payload.extend_from_slice(&record.value);

    let checksum = crc32fast::hash(&payload);
    payload.extend_from_slice(&checksum.to_le_bytes());

    let mut encoded = Vec::with_capacity(4 + payload.len());
    encoded.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&payload);
    encoded
}

pub(crate) fn decode_records(
    data: &[u8],
    path: &str,
    allow_truncated_tail: bool,
) -> Result<Vec<VersionedRecord>, LsmError> {
    let mut out = Vec::new();
    let mut offset = 0usize;

    while offset < data.len() {
        if offset + 4 > data.len() {
            if allow_truncated_tail {
                break;
            }
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        let record_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .expect("length bytes are present"),
        ) as usize;
        if record_len == 0 {
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        let record_start = offset + 4;
        let record_end = record_start + record_len;
        if record_end > data.len() {
            if allow_truncated_tail {
                break;
            }
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        let payload = &data[record_start..record_end];
        if payload.len() < 1 + 8 + 4 + 4 + 4 + 4 {
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        let crc_offset = payload.len() - 4;
        let expected_crc = u32::from_le_bytes(
            payload[crc_offset..]
                .try_into()
                .expect("crc bytes are present"),
        );
        let actual_crc = crc32fast::hash(&payload[..crc_offset]);
        if expected_crc != actual_crc {
            if allow_truncated_tail {
                break;
            }
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        let mut cursor = 0usize;
        let kind = match payload[cursor] {
            0 => OpKind::Put,
            1 => OpKind::Delete,
            2 => OpKind::Merge,
            _ => {
                return Err(LsmError::CorruptRecord {
                    path: path.to_string(),
                    offset: offset as u64,
                });
            }
        };
        cursor += 1;

        let seq = u64::from_le_bytes(
            payload[cursor..cursor + 8]
                .try_into()
                .expect("seq bytes are present"),
        );
        cursor += 8;

        let merge_op_id = u32::from_le_bytes(
            payload[cursor..cursor + 4]
                .try_into()
                .expect("merge op bytes are present"),
        );
        cursor += 4;

        let key_len = u32::from_le_bytes(
            payload[cursor..cursor + 4]
                .try_into()
                .expect("key length bytes are present"),
        ) as usize;
        cursor += 4;

        let value_len = u32::from_le_bytes(
            payload[cursor..cursor + 4]
                .try_into()
                .expect("value length bytes are present"),
        ) as usize;
        cursor += 4;

        let data_end = cursor + key_len + value_len;
        if data_end != crc_offset {
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        let key = payload[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let value = payload[cursor..cursor + value_len].to_vec();

        if kind == OpKind::Delete && !value.is_empty() {
            return Err(LsmError::CorruptRecord {
                path: path.to_string(),
                offset: offset as u64,
            });
        }

        out.push(VersionedRecord {
            key,
            seq,
            kind,
            merge_op_id,
            value,
        });

        offset = record_end;
    }

    Ok(out)
}
