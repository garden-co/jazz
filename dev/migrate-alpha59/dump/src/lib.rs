//! The dump that carries one alpha.59 server store to the SQLite importer.
//!
//! A dump is a sequence of frames: a little-endian `u32` length, then one
//! CBOR-encoded [`Record`]. It starts with [`Record::Header`] and ends with
//! [`Record::Footer`], whose digest is BLAKE3 over every frame before it. The
//! types here mirror Jazz's own but depend on neither version, so the
//! exporter (linked against alpha.59) and the importer (linked against the
//! SQLite engine) can share them.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

/// Bumped when a record changes shape.
pub const DUMP_FORMAT: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Record {
    Header(Header),
    /// One schema version from the store's runtime catalogue.
    Schema(Schema),
    /// One admitted migration lens.
    Lens(Lens),
    /// Which schema the store wrote under last.
    ActiveSchema {
        schema: [u8; 16],
    },
    /// One entry of the admin catalogue (`catalogue.rocksdb`).
    AdminEntry(AdminEntry),
    /// One command of the account journal (`accounts.rocksdb`), in order.
    Account(Account),
    /// What alpha.59 answers when this principal logs in after the whole
    /// journal: `{"Ok": assignment}` or `{"Err": reason}`. For verification.
    AccountLogin {
        principal_json: String,
        login_json: String,
    },
    /// One stored row version.
    Version(Version),
    Footer(Footer),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub format: u32,
    /// Exporter build: Jazz tag and commit it was linked against.
    pub source: String,
    /// The store's own node id.
    pub node: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// alpha.59 schema version id.
    pub id: [u8; 16],
    /// The public schema as JSON (tables, columns, policies), exactly as
    /// alpha.59's `SchemaVersion` serializes it.
    pub public_schema_json: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Lens {
    pub id: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
    pub tables: Vec<TableLens>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableLens {
    pub source_table: String,
    pub target_table: String,
    /// Each op as alpha.59's serde JSON of `protocol::LensOp`.
    pub ops_json: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdminEntry {
    pub object_id: [u8; 16],
    pub metadata: BTreeMap<String, String>,
    /// `catalogue_schema` | `catalogue_lens` | `catalogue_permissions_bundle`
    /// | `catalogue_permissions_head`.
    pub object_type: String,
    /// The decoded payload as JSON of alpha.59's public types.
    pub payload_json: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub revision: u64,
    /// alpha.59's `AccountCommand` as JSON.
    pub command_json: String,
}

/// An author as stored: the system capability, a node's system attribution,
/// or an authenticated principal with its account.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Author {
    System,
    SystemAt {
        account: Option<[u8; 16]>,
        issuer: String,
        subject: String,
    },
    Authenticated {
        account: Option<[u8; 16]>,
        issuer: String,
        subject: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Deletion {
    Deleted,
    Restored,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub table: String,
    /// alpha.59 schema version the version was written under.
    pub schema: [u8; 16],
    /// Branch column values (name, canonical value bytes); empty on the root.
    pub branch: Vec<(String, serde_bytes::ByteBuf)>,
    pub row: [u8; 16],
    pub tx_node: [u8; 16],
    /// Packed HLC: physical milliseconds << 18 | counter.
    pub tx_time: u64,
    pub global_time: Option<u64>,
    /// `accepted`, `pending` or `rejected:<reason>`.
    pub fate: String,
    pub made_by: Author,
    pub created_by: Author,
    pub created_at: u64,
    pub updated_by: Author,
    pub updated_at: u64,
    pub deletion: Option<Deletion>,
    pub cells: BTreeMap<String, Value>,
    pub user_metadata_json: Option<String>,
}

impl Version {
    /// Physical milliseconds of a packed HLC time.
    pub fn millis(hlc: u64) -> u64 {
        hlc >> 18
    }
}

/// A stored cell, mirroring the engine's record value without its internal
/// large-value and nested-record arms (large values arrive inline).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F64(f64),
    Bool(bool),
    String(String),
    Bytes(serde_bytes::ByteBuf),
    Uuid([u8; 16]),
    EnumTag(u8),
    Tuple(Vec<Value>),
    Array(Vec<Value>),
    Nullable(Option<Box<Value>>),
    I64(i64),
    I32(i32),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Footer {
    pub records: u64,
    pub versions: u64,
    /// Versions per table.
    pub tables: BTreeMap<String, u64>,
    /// BLAKE3 over every frame before the footer.
    pub digest: [u8; 32],
}

/// Writes frames and keeps the running digest and counts.
pub struct Writer<W: Write> {
    out: W,
    hasher: blake3::Hasher,
    footer: Footer,
}

impl<W: Write> Writer<W> {
    pub fn new(out: W, header: Header) -> io::Result<Self> {
        let mut writer = Self {
            out,
            hasher: blake3::Hasher::new(),
            footer: Footer::default(),
        };
        writer.write(&Record::Header(header))?;
        Ok(writer)
    }

    pub fn write(&mut self, record: &Record) -> io::Result<()> {
        if let Record::Version(version) = record {
            self.footer.versions += 1;
            *self.footer.tables.entry(version.table.clone()).or_default() += 1;
        }
        self.footer.records += 1;
        let frame = frame(record)?;
        self.hasher.update(&frame);
        self.out.write_all(&frame)
    }

    pub fn finish(mut self) -> io::Result<(W, Footer)> {
        self.footer.digest = *self.hasher.finalize().as_bytes();
        let footer = self.footer.clone();
        self.out
            .write_all(&frame(&Record::Footer(footer.clone()))?)?;
        self.out.flush()?;
        Ok((self.out, footer))
    }
}

fn frame(record: &Record) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    ciborium::into_writer(record, &mut body).map_err(io::Error::other)?;
    let len = u32::try_from(body.len()).map_err(|_| io::Error::other("record over 4 GiB"))?;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Reads frames and checks the footer's digest and counts when it arrives.
pub struct Reader<R: Read> {
    input: R,
    hasher: blake3::Hasher,
    seen: Footer,
    done: bool,
}

impl<R: Read> Reader<R> {
    /// Opens a dump and returns its header.
    pub fn open(input: R) -> io::Result<(Self, Header)> {
        let mut reader = Self {
            input,
            hasher: blake3::Hasher::new(),
            seen: Footer::default(),
            done: false,
        };
        match reader.next_record()? {
            Some(Record::Header(header)) if header.format == DUMP_FORMAT => Ok((reader, header)),
            Some(Record::Header(header)) => Err(io::Error::other(format!(
                "dump format {} is not {DUMP_FORMAT}",
                header.format
            ))),
            _ => Err(io::Error::other("dump does not start with a header")),
        }
    }

    /// The next record before the footer; `None` once the footer checked out.
    pub fn next_record(&mut self) -> io::Result<Option<Record>> {
        if self.done {
            return Ok(None);
        }
        let mut len = [0u8; 4];
        self.input.read_exact(&mut len).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::other("dump ends without a footer")
            } else {
                error
            }
        })?;
        let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
        self.input.read_exact(&mut body)?;
        let record: Record = ciborium::from_reader(body.as_slice()).map_err(io::Error::other)?;
        if let Record::Footer(footer) = &record {
            self.seen.digest = *self.hasher.finalize().as_bytes();
            if *footer != self.seen {
                return Err(io::Error::other(format!(
                    "dump footer does not match its contents: footer {footer:?}, read {:?}",
                    self.seen
                )));
            }
            self.done = true;
            return Ok(None);
        }
        self.hasher.update(&len);
        self.hasher.update(&body);
        if let Record::Version(version) = &record {
            self.seen.versions += 1;
            *self.seen.tables.entry(version.table.clone()).or_default() += 1;
        }
        self.seen.records += 1;
        Ok(Some(record))
    }

    /// Counts read so far (the footer's, once it checked out).
    pub fn footer(&self) -> &Footer {
        &self.seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header {
        Header {
            format: DUMP_FORMAT,
            source: "test".into(),
            node: [1; 16],
        }
    }

    fn version(table: &str) -> Record {
        Record::Version(Version {
            table: table.into(),
            schema: [2; 16],
            branch: vec![],
            row: [3; 16],
            tx_node: [4; 16],
            tx_time: 5 << 18,
            global_time: Some(6),
            fate: "accepted".into(),
            made_by: Author::System,
            created_by: Author::System,
            created_at: 5 << 18,
            updated_by: Author::System,
            updated_at: 5 << 18,
            deletion: None,
            cells: BTreeMap::from([("title".to_owned(), Value::String("x".into()))]),
            user_metadata_json: None,
        })
    }

    #[test]
    fn round_trips_and_checks_the_footer() {
        let mut writer = Writer::new(Vec::new(), header()).unwrap();
        writer.write(&version("todos")).unwrap();
        writer.write(&version("todos")).unwrap();
        let (bytes, footer) = writer.finish().unwrap();
        assert_eq!(footer.versions, 2);
        let (mut reader, read_header) = Reader::open(bytes.as_slice()).unwrap();
        assert_eq!(read_header, header());
        let mut records = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            records.push(record);
        }
        assert_eq!(records, vec![version("todos"), version("todos")]);
        assert_eq!(reader.footer().digest, footer.digest);
    }

    #[test]
    fn a_changed_byte_or_a_missing_footer_is_refused() {
        let mut writer = Writer::new(Vec::new(), header()).unwrap();
        writer.write(&version("todos")).unwrap();
        let (bytes, _) = writer.finish().unwrap();

        let mut corrupt = bytes.clone();
        let at = corrupt.len() / 2;
        corrupt[at] ^= 1;
        let read_all = |input: &[u8]| -> io::Result<()> {
            let (mut reader, _) = Reader::open(input)?;
            while reader.next_record()?.is_some() {}
            Ok(())
        };
        assert!(read_all(&corrupt).is_err());

        let (header_len, _) = (u32::from_le_bytes(bytes[..4].try_into().unwrap()), ());
        let truncated = &bytes[..4 + header_len as usize];
        let error = read_all(truncated).unwrap_err();
        assert!(error.to_string().contains("without a footer"), "{error}");
    }
}
