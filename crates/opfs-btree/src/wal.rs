use std::borrow::Cow;

use crate::file::SyncFile;
use crate::page::PageId;
use crate::{BTreeError, checksum};

const WAL_HEADER_MAGIC: [u8; 8] = *b"OPFSWJ01";
const WAL_FRAME_MAGIC: [u8; 8] = *b"OPFSWF01";
const WAL_COMMIT_MAGIC: [u8; 8] = *b"OPFSWC01";
const WAL_FORMAT_VERSION: u32 = 1;
const WAL_FRAME_FLAG_BLOB: u32 = 1;
const WAL_FRAME_FLAG_FREELIST: u32 = 1 << 1;

const WAL_HEADER_CHECKSUM_OFFSET: usize = 56;
const WAL_FRAME_CHECKSUM_OFFSET: usize = 32;
const WAL_COMMIT_CHECKSUM_OFFSET: usize = 28;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WalHeader {
    pub(crate) generation: u64,
    pub(crate) root_page_id: PageId,
    pub(crate) freelist_head_page_id: PageId,
    pub(crate) total_pages: u64,
    frame_count: u64,
}

impl WalHeader {
    pub(crate) fn new(
        generation: u64,
        root_page_id: PageId,
        freelist_head_page_id: PageId,
        total_pages: u64,
    ) -> Self {
        Self {
            generation,
            root_page_id,
            freelist_head_page_id,
            total_pages,
            frame_count: 0,
        }
    }

    fn with_frame_count(self, frame_count: u64) -> Self {
        Self {
            frame_count,
            ..self
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WalFrame {
    pub(crate) page_id: PageId,
    pub(crate) is_blob: bool,
    pub(crate) is_freelist: bool,
    pub(crate) raw: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WalFrameRef<'a> {
    pub(crate) page_id: PageId,
    pub(crate) is_blob: bool,
    pub(crate) is_freelist: bool,
    pub(crate) raw: &'a [u8],
}

pub(crate) fn append_commit<F: SyncFile>(
    file: &F,
    page_size: usize,
    start_page_id: PageId,
    header: WalHeader,
    frames: &[WalFrameRef<'_>],
) -> Result<u64, BTreeError> {
    let frame_count = u64::try_from(frames.len())
        .map_err(|_| BTreeError::Io("WAL frame count overflow".to_string()))?;
    let wal_pages = commit_page_count(frame_count, "WAL page count overflow", BTreeError::Io)?;
    let required_pages = start_page_id
        .checked_add(wal_pages)
        .ok_or_else(|| BTreeError::Io("WAL file length overflow".to_string()))?;
    let required_len = required_pages
        .checked_mul(page_size as u64)
        .ok_or_else(|| BTreeError::Io("WAL file byte length overflow".to_string()))?;
    file.truncate(required_len)?;

    let header = header.with_frame_count(frame_count);
    write_page(
        file,
        page_size,
        start_page_id,
        &encode_wal_header_page(page_size, header)?,
    )?;

    let mut cursor = start_page_id + 1;
    for frame in frames {
        let meta = encode_wal_frame_meta_page(
            page_size,
            frame.page_id,
            frame.is_blob,
            frame.is_freelist,
            frame.raw,
        )?;
        write_page(file, page_size, cursor, &meta)?;
        write_page(file, page_size, cursor + 1, frame.raw)?;
        cursor += 2;
    }

    let commit = encode_wal_commit_page(page_size, header.generation, frame_count)?;
    write_page(file, page_size, cursor, &commit)?;
    Ok(wal_pages)
}

pub(crate) fn read_commit<F: SyncFile>(
    file: &F,
    page_size: usize,
    start_page_id: PageId,
    persisted_pages: u64,
) -> Result<Option<(WalHeader, Vec<WalFrame>, PageId)>, BTreeError> {
    read_commit_with_mode(
        file,
        page_size,
        start_page_id,
        persisted_pages,
        batch_wal_replay_reads(),
    )
}

// `batched` is cfg-selected in production (see `batch_wal_replay_reads`) but
// kept as a parameter so native tests can exercise the batched reader too.
fn read_commit_with_mode<F: SyncFile>(
    file: &F,
    page_size: usize,
    start_page_id: PageId,
    persisted_pages: u64,
    batched: bool,
) -> Result<Option<(WalHeader, Vec<WalFrame>, PageId)>, BTreeError> {
    let mut reader = if batched {
        WalPageReader::Batched(PageRunReader::new(file, page_size, persisted_pages)?)
    } else {
        WalPageReader::PerPage { file, page_size }
    };
    reader.limit_to(start_page_id + 1);
    let Some(header) = decode_wal_header_page(&reader.page(start_page_id)?, page_size)? else {
        truncate_tail(file, page_size, start_page_id)?;
        return Ok(None);
    };
    let frame_count = usize::try_from(header.frame_count)
        .map_err(|_| BTreeError::Corrupt("WAL frame count too large".to_string()))?;
    let wal_pages = commit_page_count(
        header.frame_count,
        "WAL page count overflow",
        BTreeError::Corrupt,
    )?;
    let next_cursor = start_page_id
        .checked_add(wal_pages)
        .ok_or_else(|| BTreeError::Corrupt("WAL cursor overflow".to_string()))?;
    if next_cursor > persisted_pages {
        truncate_tail(file, page_size, start_page_id)?;
        return Ok(None);
    }
    reader.limit_to(next_cursor);

    let mut frames = Vec::with_capacity(frame_count);
    let mut cursor = start_page_id + 1;
    for _ in 0..frame_count {
        let Some((page_id, is_blob, is_freelist, expected_checksum)) =
            decode_wal_frame_meta_page(&reader.page(cursor)?, page_size)?
        else {
            truncate_tail(file, page_size, start_page_id)?;
            return Ok(None);
        };
        let raw = reader.page(cursor + 1)?.into_owned();
        if checksum::hash(&raw) != expected_checksum {
            truncate_tail(file, page_size, start_page_id)?;
            return Ok(None);
        }
        frames.push(WalFrame {
            page_id,
            is_blob,
            is_freelist,
            raw,
        });
        cursor += 2;
    }

    if !decode_wal_commit_page(&reader.page(cursor)?, header.generation, header.frame_count)? {
        truncate_tail(file, page_size, start_page_id)?;
        return Ok(None);
    }
    Ok(Some((header, frames, next_cursor)))
}

fn commit_page_count(
    frame_count: u64,
    overflow_message: &'static str,
    err: fn(String) -> BTreeError,
) -> Result<u64, BTreeError> {
    frame_count
        .checked_mul(2)
        .and_then(|pages| pages.checked_add(2))
        .ok_or_else(|| err(overflow_message.to_string()))
}

/// Page source for `read_commit_with_mode`: either the run-buffered reader or
/// direct per-page reads, behind one `Cow`-returning interface so the replay
/// loop has a single control flow.
enum WalPageReader<'a, F: SyncFile> {
    Batched(PageRunReader<'a, F>),
    PerPage { file: &'a F, page_size: usize },
}

impl<'a, F: SyncFile> WalPageReader<'a, F> {
    /// Caps batched reads at `end_page_id` so the run prefetch never reads
    /// past what the caller has validated. Per-page reads need no cap; they
    /// only ever touch the requested page.
    fn limit_to(&mut self, end_page_id: PageId) {
        if let WalPageReader::Batched(reader) = self {
            reader.set_end_page_id(end_page_id);
        }
    }

    fn page(&mut self, page_id: PageId) -> Result<Cow<'_, [u8]>, BTreeError> {
        match self {
            WalPageReader::Batched(reader) => reader.page(page_id).map(Cow::Borrowed),
            WalPageReader::PerPage { file, page_size } => {
                read_raw_page_at(*file, *page_size, page_id).map(Cow::Owned)
            }
        }
    }
}

fn read_raw_page_at<F: SyncFile>(
    file: &F,
    page_size: usize,
    page_id: PageId,
) -> Result<Vec<u8>, BTreeError> {
    let offset = page_offset(page_id, page_size, "raw page read offset overflow")?;
    let mut raw = vec![0u8; page_size];
    file.read_exact_at(offset, &mut raw)?;
    Ok(raw)
}

#[cfg(target_arch = "wasm32")]
fn batch_wal_replay_reads() -> bool {
    true
}

#[cfg(not(target_arch = "wasm32"))]
fn batch_wal_replay_reads() -> bool {
    // Native files and MemoryFile can fill owned page buffers cheaply; run
    // buffering would copy frame data twice. OPFS pays enough per read call for
    // batched replay to win there.
    false
}

const READ_RUN_PAGES: u64 = 64;

/// Serves WAL page reads from a buffered run of up to READ_RUN_PAGES pages,
/// so sequential replay does one backing-file read per run instead of one
/// (freshly allocated) read per page.
struct PageRunReader<'a, F: SyncFile> {
    file: &'a F,
    page_size: usize,
    file_end_page_id: PageId,
    /// First page id past the readable region: min(persisted pages, file length).
    end_page_id: PageId,
    buf: Vec<u8>,
    buf_first_page: PageId,
    buf_page_count: u64,
}

impl<'a, F: SyncFile> PageRunReader<'a, F> {
    fn new(file: &'a F, page_size: usize, persisted_pages: u64) -> Result<Self, BTreeError> {
        let file_pages = file.len()? / page_size as u64;
        let file_end_page_id = persisted_pages.min(file_pages);
        Ok(Self {
            file,
            page_size,
            file_end_page_id,
            end_page_id: file_end_page_id,
            buf: Vec::new(),
            buf_first_page: 0,
            buf_page_count: 0,
        })
    }

    fn set_end_page_id(&mut self, end_page_id: PageId) {
        self.end_page_id = end_page_id.min(self.file_end_page_id);
    }

    fn page(&mut self, page_id: PageId) -> Result<&[u8], BTreeError> {
        if page_id >= self.end_page_id {
            return Err(BTreeError::Io(format!(
                "unexpected eof: WAL page {} beyond readable end {}",
                page_id, self.end_page_id
            )));
        }
        if page_id < self.buf_first_page || page_id >= self.buf_first_page + self.buf_page_count {
            self.fill(page_id)?;
        }
        let start = (page_id - self.buf_first_page) as usize * self.page_size;
        Ok(&self.buf[start..start + self.page_size])
    }

    fn fill(&mut self, page_id: PageId) -> Result<(), BTreeError> {
        if page_id >= self.end_page_id {
            return Err(BTreeError::Io(format!(
                "unexpected eof: WAL page {} beyond readable end {}",
                page_id, self.end_page_id
            )));
        }
        let run = READ_RUN_PAGES.min(self.end_page_id - page_id);
        let len = run as usize * self.page_size;
        self.buf.resize(len, 0);
        let offset = page_offset(page_id, self.page_size, "raw page read offset overflow")?;
        self.file.read_exact_at(offset, &mut self.buf[..len])?;
        self.buf_first_page = page_id;
        self.buf_page_count = run;
        Ok(())
    }
}

fn write_page<F: SyncFile>(
    file: &F,
    page_size: usize,
    page_id: PageId,
    raw: &[u8],
) -> Result<(), BTreeError> {
    if raw.len() != page_size {
        return Err(BTreeError::Corrupt(format!(
            "WAL page raw length {} does not match page size {}",
            raw.len(),
            page_size
        )));
    }
    let offset = page_offset(page_id, page_size, "WAL write offset overflow")?;
    file.write_all_at(offset, raw)
}

fn truncate_tail<F: SyncFile>(
    file: &F,
    page_size: usize,
    start_page_id: PageId,
) -> Result<(), BTreeError> {
    let len = start_page_id
        .checked_mul(page_size as u64)
        .ok_or_else(|| BTreeError::Io("WAL truncate length overflow".to_string()))?;
    file.truncate(len)
}

fn page_offset(
    page_id: PageId,
    page_size: usize,
    overflow_message: &'static str,
) -> Result<u64, BTreeError> {
    page_id
        .checked_mul(page_size as u64)
        .ok_or_else(|| BTreeError::Io(overflow_message.to_string()))
}

fn encode_wal_header_page(page_size: usize, header: WalHeader) -> Result<Vec<u8>, BTreeError> {
    let mut page = vec![0u8; page_size];
    ensure_wal_page_capacity(&page, WAL_HEADER_CHECKSUM_OFFSET + 4)?;
    page[0..8].copy_from_slice(&WAL_HEADER_MAGIC);
    write_u32_at(&mut page, 8, WAL_FORMAT_VERSION)?;
    write_u32_at(&mut page, 12, page_size_u32(page_size)?)?;
    write_u64_at(&mut page, 16, header.generation)?;
    write_u64_at(&mut page, 24, header.root_page_id)?;
    write_u64_at(&mut page, 32, header.freelist_head_page_id)?;
    write_u64_at(&mut page, 40, header.total_pages)?;
    write_u64_at(&mut page, 48, header.frame_count)?;
    let checksum = checksum::hash(&page[..WAL_HEADER_CHECKSUM_OFFSET]);
    write_u32_at(&mut page, WAL_HEADER_CHECKSUM_OFFSET, checksum)?;
    Ok(page)
}

fn decode_wal_header_page(raw: &[u8], page_size: usize) -> Result<Option<WalHeader>, BTreeError> {
    ensure_wal_page_capacity(raw, WAL_HEADER_CHECKSUM_OFFSET + 4)?;
    if raw[0..8] != WAL_HEADER_MAGIC {
        return Ok(None);
    }
    if read_u32_at(raw, 8)? != WAL_FORMAT_VERSION
        || read_u32_at(raw, 12)? != page_size_u32(page_size)?
    {
        return Ok(None);
    }
    if read_u32_at(raw, WAL_HEADER_CHECKSUM_OFFSET)?
        != checksum::hash(&raw[..WAL_HEADER_CHECKSUM_OFFSET])
    {
        return Ok(None);
    }
    Ok(Some(WalHeader {
        generation: read_u64_at(raw, 16)?,
        root_page_id: read_u64_at(raw, 24)?,
        freelist_head_page_id: read_u64_at(raw, 32)?,
        total_pages: read_u64_at(raw, 40)?,
        frame_count: read_u64_at(raw, 48)?,
    }))
}

fn encode_wal_frame_meta_page(
    page_size: usize,
    page_id: PageId,
    is_blob: bool,
    is_freelist: bool,
    raw_page: &[u8],
) -> Result<Vec<u8>, BTreeError> {
    let mut page = vec![0u8; page_size];
    ensure_wal_page_capacity(&page, WAL_FRAME_CHECKSUM_OFFSET + 4)?;
    page[0..8].copy_from_slice(&WAL_FRAME_MAGIC);
    write_u32_at(&mut page, 8, WAL_FORMAT_VERSION)?;
    write_u32_at(&mut page, 12, page_size_u32(page_size)?)?;
    write_u64_at(&mut page, 16, page_id)?;
    let mut flags = 0;
    if is_blob {
        flags |= WAL_FRAME_FLAG_BLOB;
    }
    if is_freelist {
        flags |= WAL_FRAME_FLAG_FREELIST;
    }
    write_u32_at(&mut page, 24, flags)?;
    write_u32_at(&mut page, 28, checksum::hash(raw_page))?;
    let checksum = checksum::hash(&page[..WAL_FRAME_CHECKSUM_OFFSET]);
    write_u32_at(&mut page, WAL_FRAME_CHECKSUM_OFFSET, checksum)?;
    Ok(page)
}

fn decode_wal_frame_meta_page(
    raw: &[u8],
    page_size: usize,
) -> Result<Option<(PageId, bool, bool, u32)>, BTreeError> {
    ensure_wal_page_capacity(raw, WAL_FRAME_CHECKSUM_OFFSET + 4)?;
    if raw[0..8] != WAL_FRAME_MAGIC {
        return Ok(None);
    }
    if read_u32_at(raw, 8)? != WAL_FORMAT_VERSION
        || read_u32_at(raw, 12)? != page_size_u32(page_size)?
    {
        return Ok(None);
    }
    if read_u32_at(raw, WAL_FRAME_CHECKSUM_OFFSET)?
        != checksum::hash(&raw[..WAL_FRAME_CHECKSUM_OFFSET])
    {
        return Ok(None);
    }
    let flags = read_u32_at(raw, 24)?;
    if flags & !(WAL_FRAME_FLAG_BLOB | WAL_FRAME_FLAG_FREELIST) != 0 {
        return Ok(None);
    }
    Ok(Some((
        read_u64_at(raw, 16)?,
        flags & WAL_FRAME_FLAG_BLOB != 0,
        flags & WAL_FRAME_FLAG_FREELIST != 0,
        read_u32_at(raw, 28)?,
    )))
}

fn encode_wal_commit_page(
    page_size: usize,
    generation: u64,
    frame_count: u64,
) -> Result<Vec<u8>, BTreeError> {
    let mut page = vec![0u8; page_size];
    ensure_wal_page_capacity(&page, WAL_COMMIT_CHECKSUM_OFFSET + 4)?;
    page[0..8].copy_from_slice(&WAL_COMMIT_MAGIC);
    write_u32_at(&mut page, 8, WAL_FORMAT_VERSION)?;
    write_u64_at(&mut page, 12, generation)?;
    write_u64_at(&mut page, 20, frame_count)?;
    let checksum = checksum::hash(&page[..WAL_COMMIT_CHECKSUM_OFFSET]);
    write_u32_at(&mut page, WAL_COMMIT_CHECKSUM_OFFSET, checksum)?;
    Ok(page)
}

fn decode_wal_commit_page(
    raw: &[u8],
    generation: u64,
    frame_count: u64,
) -> Result<bool, BTreeError> {
    ensure_wal_page_capacity(raw, WAL_COMMIT_CHECKSUM_OFFSET + 4)?;
    if raw[0..8] != WAL_COMMIT_MAGIC {
        return Ok(false);
    }
    if read_u32_at(raw, 8)? != WAL_FORMAT_VERSION {
        return Ok(false);
    }
    if read_u64_at(raw, 12)? != generation || read_u64_at(raw, 20)? != frame_count {
        return Ok(false);
    }
    Ok(read_u32_at(raw, WAL_COMMIT_CHECKSUM_OFFSET)?
        == checksum::hash(&raw[..WAL_COMMIT_CHECKSUM_OFFSET]))
}

fn ensure_wal_page_capacity(raw: &[u8], needed: usize) -> Result<(), BTreeError> {
    if raw.len() < needed {
        return Err(BTreeError::Corrupt(format!(
            "WAL page too small: {} < {}",
            raw.len(),
            needed
        )));
    }
    Ok(())
}

fn page_size_u32(page_size: usize) -> Result<u32, BTreeError> {
    u32::try_from(page_size)
        .map_err(|_| BTreeError::InvalidOptions("page size too large".to_string()))
}

fn read_u32_at(raw: &[u8], offset: usize) -> Result<u32, BTreeError> {
    let bytes = raw
        .get(offset..offset + 4)
        .ok_or_else(|| BTreeError::Corrupt("u32 read out of bounds".to_string()))?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("slice len")))
}

fn read_u64_at(raw: &[u8], offset: usize) -> Result<u64, BTreeError> {
    let bytes = raw
        .get(offset..offset + 8)
        .ok_or_else(|| BTreeError::Corrupt("u64 read out of bounds".to_string()))?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("slice len")))
}

fn write_u32_at(raw: &mut [u8], offset: usize, value: u32) -> Result<(), BTreeError> {
    let slot = raw
        .get_mut(offset..offset + 4)
        .ok_or_else(|| BTreeError::Corrupt("u32 write out of bounds".to_string()))?;
    slot.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64_at(raw: &mut [u8], offset: usize, value: u64) -> Result<(), BTreeError> {
    let slot = raw
        .get_mut(offset..offset + 8)
        .ok_or_else(|| BTreeError::Corrupt("u64 write out of bounds".to_string()))?;
    slot.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::MemoryFile;

    const PAGE_SIZE: usize = 4 * 1024;
    const START_PAGE_ID: PageId = 2;

    fn header() -> WalHeader {
        WalHeader::new(7, 3, 4, 9)
    }

    #[test]
    fn append_and_read_commit_round_trip() {
        let file = MemoryFile::new();
        let page_a = vec![1u8; PAGE_SIZE];
        let page_b = vec![2u8; PAGE_SIZE];
        let frames = [
            WalFrameRef {
                page_id: 3,
                is_blob: false,
                is_freelist: false,
                raw: &page_a,
            },
            WalFrameRef {
                page_id: 4,
                is_blob: true,
                is_freelist: false,
                raw: &page_b,
            },
        ];

        let pages_written =
            append_commit(&file, PAGE_SIZE, START_PAGE_ID, header(), &frames).expect("append WAL");

        assert_eq!(pages_written, 6);
        let persisted_pages = START_PAGE_ID + pages_written;
        let (read_header, read_frames, next_cursor) =
            read_commit(&file, PAGE_SIZE, START_PAGE_ID, persisted_pages)
                .expect("read WAL")
                .expect("commit present");

        assert_eq!(read_header.generation, 7);
        assert_eq!(read_header.root_page_id, 3);
        assert_eq!(read_header.freelist_head_page_id, 4);
        assert_eq!(read_header.total_pages, 9);
        assert_eq!(read_frames.len(), 2);
        assert_eq!(read_frames[0].raw, page_a);
        assert_eq!(read_frames[1].raw, page_b);
        assert_eq!(next_cursor, persisted_pages);
    }

    #[test]
    fn read_commit_round_trips_commits_larger_than_one_read_run() {
        let file = MemoryFile::new();
        // 40 frames -> 2 + 40 * 2 = 82 WAL pages, more than one 64-page run.
        let frame_pages: Vec<Vec<u8>> = (0..40u8)
            .map(|i| vec![i.wrapping_add(1); PAGE_SIZE])
            .collect();
        let frames: Vec<WalFrameRef<'_>> = frame_pages
            .iter()
            .enumerate()
            .map(|(i, raw)| WalFrameRef {
                page_id: 100 + i as PageId,
                is_blob: i % 3 == 0,
                is_freelist: i % 5 == 0,
                raw,
            })
            .collect();

        let pages_written =
            append_commit(&file, PAGE_SIZE, START_PAGE_ID, header(), &frames).expect("append WAL");
        assert_eq!(pages_written, 82);

        let persisted_pages = START_PAGE_ID + pages_written;
        for batched in [false, true] {
            let (_, read_frames, next_cursor) =
                read_commit_with_mode(&file, PAGE_SIZE, START_PAGE_ID, persisted_pages, batched)
                    .expect("read WAL")
                    .expect("commit present");

            assert_eq!(next_cursor, persisted_pages);
            assert_eq!(read_frames.len(), 40);
            for (i, frame) in read_frames.iter().enumerate() {
                assert_eq!(frame.page_id, 100 + i as PageId);
                assert_eq!(frame.is_blob, i % 3 == 0);
                assert_eq!(frame.is_freelist, i % 5 == 0);
                assert_eq!(frame.raw, frame_pages[i]);
            }
        }
    }

    #[test]
    fn read_commit_returns_none_and_truncates_truncated_tail() {
        let file = MemoryFile::new();
        let page = vec![1u8; PAGE_SIZE];
        let frames = [WalFrameRef {
            page_id: 3,
            is_blob: false,
            is_freelist: false,
            raw: &page,
        }];
        let pages_written =
            append_commit(&file, PAGE_SIZE, START_PAGE_ID, header(), &frames).expect("append WAL");
        let truncated_pages = START_PAGE_ID + pages_written - 1;
        file.truncate(truncated_pages * PAGE_SIZE as u64)
            .expect("truncate commit page");

        let result =
            read_commit(&file, PAGE_SIZE, START_PAGE_ID, truncated_pages).expect("read torn WAL");

        assert!(result.is_none());
        assert_eq!(
            file.len().expect("file len"),
            START_PAGE_ID * PAGE_SIZE as u64
        );
    }

    #[test]
    fn read_commit_returns_none_and_truncates_corrupt_frame() {
        let file = MemoryFile::new();
        let page = vec![1u8; PAGE_SIZE];
        let frames = [WalFrameRef {
            page_id: 3,
            is_blob: false,
            is_freelist: false,
            raw: &page,
        }];
        let pages_written =
            append_commit(&file, PAGE_SIZE, START_PAGE_ID, header(), &frames).expect("append WAL");
        let persisted_pages = START_PAGE_ID + pages_written;
        let frame_page_offset = (START_PAGE_ID + 2) * PAGE_SIZE as u64;
        file.write_all_at(frame_page_offset, &[9])
            .expect("corrupt frame data");

        let result = read_commit(&file, PAGE_SIZE, START_PAGE_ID, persisted_pages)
            .expect("read corrupt WAL");

        assert!(result.is_none());
        assert_eq!(
            file.len().expect("file len"),
            START_PAGE_ID * PAGE_SIZE as u64
        );
    }
}
