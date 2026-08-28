use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use jazz::tools::ObjectId;
use jazz::tools::metadata::MetadataKey;
const CATALOGUE_ENTRY_MAGIC: &[u8; 4] = b"JCAT";
const CATALOGUE_ENTRY_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogueEntry {
    pub object_id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub content: Vec<u8>,
}

impl CatalogueEntry {
    pub fn object_type(&self) -> Option<&str> {
        self.metadata
            .get(MetadataKey::Type.as_str())
            .map(String::as_str)
    }

    pub(crate) fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        // The catalogue is restart authority.  Do not let HashMap iteration or
        // serde's object representation select physical bytes here.
        let mut metadata = self.metadata.iter().collect::<Vec<_>>();
        metadata.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));

        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOGUE_ENTRY_MAGIC);
        bytes.push(CATALOGUE_ENTRY_VERSION);
        bytes.extend_from_slice(self.object_id.uuid().as_bytes());
        put_u16(&mut bytes, metadata.len(), "metadata count")?;
        for (key, value) in metadata {
            put_string(&mut bytes, key, "metadata key")?;
            put_string(&mut bytes, value, "metadata value")?;
        }
        put_u32(&mut bytes, self.content.len(), "content")?;
        bytes.extend_from_slice(&self.content);
        Ok(bytes)
    }

    pub(crate) fn decode_storage_row(object_id: ObjectId, bytes: &[u8]) -> Result<Self, String> {
        let mut input = bytes;
        if take(&mut input, 4)? != CATALOGUE_ENTRY_MAGIC {
            return Err("catalogue entry magic is invalid".to_owned());
        }
        let version = take_u8(&mut input)?;
        if version != CATALOGUE_ENTRY_VERSION {
            return Err(format!(
                "unsupported catalogue entry version {version}; expected {CATALOGUE_ENTRY_VERSION}"
            ));
        }
        let stored_object_id = ObjectId::from_uuid(uuid::Uuid::from_bytes(
            take(&mut input, 16)?
                .try_into()
                .expect("catalogue entry UUID is exactly sixteen bytes"),
        ));
        if stored_object_id != object_id {
            return Err("catalogue entry key does not match embedded object id".to_owned());
        }
        let metadata_count = take_u16(&mut input)? as usize;
        let mut metadata = HashMap::with_capacity(metadata_count);
        let mut previous_key = None::<Vec<u8>>;
        for _ in 0..metadata_count {
            let key = take_string(&mut input, "metadata key")?;
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_bytes())
            {
                return Err("catalogue metadata keys are not strictly canonical".to_owned());
            }
            previous_key = Some(key.as_bytes().to_vec());
            let value = take_string(&mut input, "metadata value")?;
            metadata.insert(key, value);
        }
        let content_len = take_u32(&mut input)? as usize;
        let content = take(&mut input, content_len)?.to_vec();
        if !input.is_empty() {
            return Err("catalogue entry has trailing bytes".to_owned());
        }

        let entry = Self {
            object_id,
            metadata,
            content,
        };
        if entry.encode_storage_row()? != bytes {
            return Err("catalogue entry bytes are noncanonical".to_owned());
        }
        Ok(entry)
    }
}

fn put_u16(bytes: &mut Vec<u8>, value: usize, field: &str) -> Result<(), String> {
    let value = u16::try_from(value).map_err(|_| format!("catalogue entry {field} is too long"))?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_u32(bytes: &mut Vec<u8>, value: usize, field: &str) -> Result<(), String> {
    let value = u32::try_from(value).map_err(|_| format!("catalogue entry {field} is too long"))?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_string(bytes: &mut Vec<u8>, value: &str, field: &str) -> Result<(), String> {
    put_u16(bytes, value.len(), field)?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], String> {
    if input.len() < count {
        return Err("catalogue entry is truncated".to_owned());
    }
    let (taken, remaining) = input.split_at(count);
    *input = remaining;
    Ok(taken)
}

fn take_u8(input: &mut &[u8]) -> Result<u8, String> {
    Ok(take(input, 1)?[0])
}

fn take_u16(input: &mut &[u8]) -> Result<u16, String> {
    let bytes: [u8; 2] = take(input, 2)?
        .try_into()
        .expect("catalogue entry reader returned two bytes");
    Ok(u16::from_be_bytes(bytes))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, String> {
    let bytes: [u8; 4] = take(input, 4)?
        .try_into()
        .expect("catalogue entry reader returned four bytes");
    Ok(u32::from_be_bytes(bytes))
}

fn take_string(input: &mut &[u8], field: &str) -> Result<String, String> {
    let length = take_u16(input)? as usize;
    std::str::from_utf8(take(input, length)?)
        .map_err(|_| format!("catalogue entry {field} is not UTF-8"))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_entry() -> CatalogueEntry {
        CatalogueEntry {
            object_id: ObjectId::from_uuid(uuid::Uuid::from_bytes([0x11; 16])),
            metadata: HashMap::from([
                ("type".to_owned(), "table".to_owned()),
                ("app_id".to_owned(), "a".to_owned()),
            ]),
            content: vec![0xde, 0xad],
        }
    }

    #[test]
    fn storage_row_v1_golden_is_exact_and_rejects_alternates() {
        let entry = fixture_entry();
        let golden = b"JCAT\x01\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\
            \x00\x02\x00\x06app_id\x00\x01a\x00\x04type\x00\x05table\x00\x00\x00\x02\xde\xad";
        assert_eq!(entry.encode_storage_row().unwrap(), golden);
        assert_eq!(
            CatalogueEntry::decode_storage_row(entry.object_id, golden).unwrap(),
            entry
        );

        let reverse_metadata =
            b"JCAT\x01\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\
            \x00\x02\x00\x04type\x00\x05table\x00\x06app_id\x00\x01a\x00\x00\x00\x02\xde\xad";
        let mut unknown_version = golden.to_vec();
        unknown_version[4] = 2;
        for malformed in [
            Vec::new(),
            b"JCAX\x01".to_vec(),
            unknown_version,
            [golden.as_slice(), &[0]].concat(),
            reverse_metadata.to_vec(),
        ] {
            assert!(
                CatalogueEntry::decode_storage_row(entry.object_id, &malformed).is_err(),
                "{malformed:?}"
            );
        }
        assert!(
            CatalogueEntry::decode_storage_row(
                ObjectId::from_uuid(uuid::Uuid::from_bytes([0x22; 16])),
                golden,
            )
            .is_err()
        );
    }
}
