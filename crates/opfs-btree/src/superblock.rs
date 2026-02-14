use crate::BTreeError;

const MAGIC: [u8; 8] = *b"OPFSBT01";
const FORMAT_VERSION: u32 = 1;
const RESERVED_BYTES: usize = 32;

const OFFSET_MAGIC: usize = 0;
const OFFSET_VERSION: usize = OFFSET_MAGIC + 8;
const OFFSET_PAGE_SIZE: usize = OFFSET_VERSION + 4;
const OFFSET_GENERATION: usize = OFFSET_PAGE_SIZE + 4;
const OFFSET_ROOT_PAGE_ID: usize = OFFSET_GENERATION + 8;
const OFFSET_FREELIST_HEAD_PAGE_ID: usize = OFFSET_ROOT_PAGE_ID + 8;
const OFFSET_TOTAL_PAGES: usize = OFFSET_FREELIST_HEAD_PAGE_ID + 8;
const OFFSET_RESERVED: usize = OFFSET_TOTAL_PAGES + 8;
const OFFSET_CHECKSUM: usize = OFFSET_RESERVED + RESERVED_BYTES;

pub(crate) const SUPERBLOCK_ENCODED_BYTES: usize = OFFSET_CHECKSUM + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SuperblockSlot {
    A,
    B,
}

impl SuperblockSlot {
    pub(crate) fn inactive(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    pub(crate) fn page_index(self) -> u64 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    pub(crate) fn byte_offset(self, page_size: usize) -> u64 {
        self.page_index() * page_size as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Superblock {
    pub(crate) page_size: u32,
    pub(crate) generation: u64,
    pub(crate) root_page_id: u64,
    pub(crate) freelist_head_page_id: u64,
    pub(crate) total_pages: u64,
}

impl Superblock {
    pub(crate) fn new(
        page_size: u32,
        generation: u64,
        root_page_id: u64,
        freelist_head_page_id: u64,
        total_pages: u64,
    ) -> Self {
        Self {
            page_size,
            generation,
            root_page_id,
            freelist_head_page_id,
            total_pages,
        }
    }

    pub(crate) fn encode_into_page(self, page: &mut [u8]) -> Result<(), BTreeError> {
        if page.len() < SUPERBLOCK_ENCODED_BYTES {
            return Err(BTreeError::Corrupt(format!(
                "superblock page too small: {}",
                page.len()
            )));
        }
        page.fill(0);

        page[OFFSET_MAGIC..OFFSET_MAGIC + 8].copy_from_slice(&MAGIC);
        page[OFFSET_VERSION..OFFSET_VERSION + 4].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        page[OFFSET_PAGE_SIZE..OFFSET_PAGE_SIZE + 4].copy_from_slice(&self.page_size.to_le_bytes());
        page[OFFSET_GENERATION..OFFSET_GENERATION + 8]
            .copy_from_slice(&self.generation.to_le_bytes());
        page[OFFSET_ROOT_PAGE_ID..OFFSET_ROOT_PAGE_ID + 8]
            .copy_from_slice(&self.root_page_id.to_le_bytes());
        page[OFFSET_FREELIST_HEAD_PAGE_ID..OFFSET_FREELIST_HEAD_PAGE_ID + 8]
            .copy_from_slice(&self.freelist_head_page_id.to_le_bytes());
        page[OFFSET_TOTAL_PAGES..OFFSET_TOTAL_PAGES + 8]
            .copy_from_slice(&self.total_pages.to_le_bytes());

        let checksum = crc32fast::hash(&page[..OFFSET_CHECKSUM]);
        page[OFFSET_CHECKSUM..OFFSET_CHECKSUM + 4].copy_from_slice(&checksum.to_le_bytes());
        Ok(())
    }

    pub(crate) fn decode_from_page(
        page: &[u8],
        expected_page_size: usize,
    ) -> Result<Self, BTreeError> {
        if page.len() < SUPERBLOCK_ENCODED_BYTES {
            return Err(BTreeError::Corrupt(format!(
                "superblock page too small: {}",
                page.len()
            )));
        }

        if page[OFFSET_MAGIC..OFFSET_MAGIC + 8] != MAGIC {
            return Err(BTreeError::Corrupt("superblock magic mismatch".to_string()));
        }

        let version = u32::from_le_bytes(
            page[OFFSET_VERSION..OFFSET_VERSION + 4]
                .try_into()
                .expect("superblock version slice"),
        );
        if version != FORMAT_VERSION {
            return Err(BTreeError::Corrupt(format!(
                "unsupported superblock version {}",
                version
            )));
        }

        let page_size = u32::from_le_bytes(
            page[OFFSET_PAGE_SIZE..OFFSET_PAGE_SIZE + 4]
                .try_into()
                .expect("superblock page size slice"),
        );
        if page_size as usize != expected_page_size {
            return Err(BTreeError::Corrupt(format!(
                "page size mismatch: found {}, expected {}",
                page_size, expected_page_size
            )));
        }

        let expected_checksum = u32::from_le_bytes(
            page[OFFSET_CHECKSUM..OFFSET_CHECKSUM + 4]
                .try_into()
                .expect("superblock checksum slice"),
        );
        let actual_checksum = crc32fast::hash(&page[..OFFSET_CHECKSUM]);
        if expected_checksum != actual_checksum {
            return Err(BTreeError::Corrupt(format!(
                "superblock checksum mismatch: expected {}, got {}",
                expected_checksum, actual_checksum
            )));
        }

        let generation = u64::from_le_bytes(
            page[OFFSET_GENERATION..OFFSET_GENERATION + 8]
                .try_into()
                .expect("superblock generation slice"),
        );
        let root_page_id = u64::from_le_bytes(
            page[OFFSET_ROOT_PAGE_ID..OFFSET_ROOT_PAGE_ID + 8]
                .try_into()
                .expect("superblock root page id slice"),
        );
        let freelist_head_page_id = u64::from_le_bytes(
            page[OFFSET_FREELIST_HEAD_PAGE_ID..OFFSET_FREELIST_HEAD_PAGE_ID + 8]
                .try_into()
                .expect("superblock freelist head page id slice"),
        );
        let total_pages = u64::from_le_bytes(
            page[OFFSET_TOTAL_PAGES..OFFSET_TOTAL_PAGES + 8]
                .try_into()
                .expect("superblock total pages slice"),
        );

        Ok(Self {
            page_size,
            generation,
            root_page_id,
            freelist_head_page_id,
            total_pages,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_round_trip() {
        let sb = Superblock::new(16 * 1024, 7, 123, 456, 789);
        let mut page = vec![0u8; 16 * 1024];
        sb.encode_into_page(&mut page).expect("encode superblock");

        let decoded = Superblock::decode_from_page(&page, 16 * 1024).expect("decode superblock");
        assert_eq!(decoded, sb);
    }

    #[test]
    fn superblock_detects_checksum_corruption() {
        let sb = Superblock::new(16 * 1024, 1, 2, 3, 4);
        let mut page = vec![0u8; 16 * 1024];
        sb.encode_into_page(&mut page).expect("encode superblock");

        page[OFFSET_ROOT_PAGE_ID] ^= 0xAA;
        let err = Superblock::decode_from_page(&page, 16 * 1024).expect_err("must fail");
        assert!(matches!(err, BTreeError::Corrupt(_)));
    }
}
