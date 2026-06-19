use anyhow::Result;
use opfs_btree::bench_dataset::ValueEncoding;
use serde::Serialize;

/// Serialize a record to value bytes in the requested encoding.
pub fn encode_value<T: Serialize>(record: &T, encoding: ValueEncoding) -> Result<Vec<u8>> {
    match encoding {
        ValueEncoding::Cbor => {
            let mut buf = Vec::new();
            ciborium::into_writer(record, &mut buf)?;
            Ok(buf)
        }
        ValueEncoding::Json => Ok(serde_json::to_vec(record)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Sample {
        a: u32,
        b: String,
    }

    #[test]
    fn cbor_is_smaller_than_json_for_structured_record() {
        let r = Sample {
            a: 7,
            b: "hello world".into(),
        };
        let cbor = encode_value(&r, ValueEncoding::Cbor).unwrap();
        let json = encode_value(&r, ValueEncoding::Json).unwrap();
        assert!(!cbor.is_empty() && !json.is_empty());
        assert!(cbor.len() <= json.len());
    }
}
