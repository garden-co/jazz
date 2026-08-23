use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

pub type PageId = u64;

const MAGIC: &[u8; 8] = b"IDBTREE\0";
const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 1 + 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Page {
    Leaf {
        entries: Vec<(Vec<u8>, ValueCell)>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
    Overflow {
        next: Option<PageId>,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueCell {
    Inline(Vec<u8>),
    Overflow { head: PageId, len: usize },
}

impl Page {
    pub fn leaf() -> Self {
        Self::Leaf {
            entries: Vec::new(),
        }
    }
}

pub fn encode_page(page: &Page) -> Result<Vec<u8>, String> {
    let payload = postcard::to_allocvec(page).map_err(|error| error.to_string())?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.push(FORMAT_VERSION);
    bytes.extend_from_slice(&xxh3_64(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_page(bytes: &[u8]) -> Result<Page, String> {
    if bytes.len() < HEADER_LEN || &bytes[..MAGIC.len()] != MAGIC {
        return Err("bad magic".to_owned());
    }
    if bytes[MAGIC.len()] != FORMAT_VERSION {
        return Err(format!("unsupported format version {}", bytes[MAGIC.len()]));
    }
    let checksum_offset = MAGIC.len() + 1;
    let expected = u64::from_le_bytes(
        bytes[checksum_offset..HEADER_LEN]
            .try_into()
            .expect("checksum has fixed width"),
    );
    let payload = &bytes[HEADER_LEN..];
    if xxh3_64(payload) != expected {
        return Err("checksum mismatch".to_owned());
    }
    postcard::from_bytes(payload).map_err(|error| error.to_string())
}
