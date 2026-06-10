const STORAGE_BUNDLE_MAGIC: &[u8] = b"JAZZOPFSBUNDLE1";
const STORAGE_BUNDLE_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageBundleFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageBundle {
    pub metadata_text: String,
    pub files: Vec<StorageBundleFile>,
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_bytes(&mut self, len: usize, label: &str) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| format!("Invalid storage bundle: {label} length overflows"))?;
        if end > self.bytes.len() {
            return Err(format!("Invalid storage bundle: truncated {label}"));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn read_u32(&mut self, label: &str) -> Result<u32, String> {
        let bytes = self.read_bytes(4, label)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
    }

    fn read_u64(&mut self, label: &str) -> Result<u64, String> {
        let bytes = self.read_bytes(8, label)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("u64 bytes")))
    }

    fn assert_done(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("Invalid storage bundle: trailing bytes".to_string())
        }
    }
}

pub fn decode_storage_bundle(bytes: &[u8]) -> Result<StorageBundle, String> {
    let mut reader = Reader::new(bytes);
    let magic = reader.read_bytes(STORAGE_BUNDLE_MAGIC.len(), "magic")?;
    if magic != STORAGE_BUNDLE_MAGIC {
        return Err("Invalid storage bundle: bad magic".to_string());
    }

    let version = reader.read_u32("version")?;
    if version != STORAGE_BUNDLE_VERSION {
        return Err(format!("Unsupported storage bundle version: {version}"));
    }

    let metadata_len = reader.read_u32("metadata length")? as usize;
    let metadata_bytes = reader.read_bytes(metadata_len, "metadata")?;
    let metadata_value = if metadata_bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(metadata_bytes)
            .map_err(|err| format!("Invalid storage bundle: metadata is not JSON: {err}"))?
    };
    let metadata_text = serde_json::to_string_pretty(&metadata_value)
        .map_err(|err| format!("Invalid storage bundle: metadata could not format: {err}"))?;

    let file_count = reader.read_u32("file count")? as usize;
    let mut files = Vec::with_capacity(file_count);
    for index in 0..file_count {
        let path_len = reader.read_u32(&format!("file {index} path length"))? as usize;
        let path_bytes = reader.read_bytes(path_len, &format!("file {index} path"))?;
        let path = String::from_utf8(path_bytes.to_vec()).map_err(|err| {
            format!("Invalid storage bundle: file {index} path is not UTF-8: {err}")
        })?;
        let byte_len = reader.read_u64(&format!("file {index} byte length"))?;
        let byte_len = usize::try_from(byte_len)
            .map_err(|_| format!("Invalid storage bundle: file {index} is too large"))?;
        let file_bytes = reader
            .read_bytes(byte_len, &format!("file {index} bytes"))?
            .to_vec();
        files.push(StorageBundleFile {
            path,
            bytes: file_bytes,
        });
    }

    reader.assert_done()?;
    Ok(StorageBundle {
        metadata_text,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u32_bytes(value: u32) -> [u8; 4] {
        value.to_le_bytes()
    }

    fn u64_bytes(value: u64) -> [u8; 8] {
        value.to_le_bytes()
    }

    fn encode_test_bundle() -> Vec<u8> {
        let metadata = br#"{"dbName":"debug-db"}"#;
        let path = b"debug-db.opfsbtree";
        let file = [1u8, 2, 3];
        let mut out = Vec::new();
        out.extend_from_slice(STORAGE_BUNDLE_MAGIC);
        out.extend_from_slice(&u32_bytes(STORAGE_BUNDLE_VERSION));
        out.extend_from_slice(&u32_bytes(metadata.len() as u32));
        out.extend_from_slice(metadata);
        out.extend_from_slice(&u32_bytes(1));
        out.extend_from_slice(&u32_bytes(path.len() as u32));
        out.extend_from_slice(path);
        out.extend_from_slice(&u64_bytes(file.len() as u64));
        out.extend_from_slice(&file);
        out
    }

    #[test]
    fn decodes_storage_bundle() {
        let bundle = decode_storage_bundle(&encode_test_bundle()).expect("decode bundle");
        assert_eq!(bundle.files.len(), 1);
        assert_eq!(bundle.files[0].path, "debug-db.opfsbtree");
        assert_eq!(bundle.files[0].bytes, vec![1, 2, 3]);
        assert!(bundle.metadata_text.contains("debug-db"));
    }

    #[test]
    fn rejects_bad_magic() {
        let err = decode_storage_bundle(&[0; STORAGE_BUNDLE_MAGIC.len()]).expect_err("must reject");
        assert_eq!(err, "Invalid storage bundle: bad magic");
    }
}
