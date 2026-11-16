//! Stored encrypted values: the cell and stream records Jazz writes into an
//! encrypted column, and the contexts that bind them to their coordinates.
//!
//! A stored cell is the `jazz.e2ee.cell-record` v1 envelope with payload
//! `epoch ID (36 ASCII bytes) || cell mechanism envelope`. A stored stream is
//! the `jazz.e2ee.stream-record` v1 envelope with payload
//! `epoch ID || stream mechanism encoding`; that encoding begins with the
//! stream mechanism's header, which is carried once.
//!
//! The logical column type and value encodings are the caller's: this module
//! takes the canonical cell-type bytes (`{"column_type":…,"nullable":…}`) and
//! the encoded value bytes as given.

use zeroize::Zeroizing;

use crate::agility::{CellCipher, EqualityTagger, StreamCipher, StreamWriter};
use crate::context::{CryptoContext, frame_record};
use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::protocol::ids::{self, IdForm};
use crate::random::RandomSource;
use crate::registry::Registry;

pub const CELL_RECORD: Mechanism<'static> = Mechanism::new("jazz.e2ee.cell-record", 1);
pub const STREAM_RECORD: Mechanism<'static> = Mechanism::new("jazz.e2ee.stream-record", 1);
const CELL_POLICY: &str = "jazz.e2ee.cell-record.v1";
const STREAM_POLICY: &str = "jazz.e2ee.stream-record.v1";
const EQUALITY_POLICY: &str = "jazz.e2ee.equality.v1";
const EPOCH_BYTES: usize = 36;
/// The epoch ID plus the shortest possible envelope.
const MIN_RECORD_PAYLOAD: usize = EPOCH_BYTES + 11;

/// Stable catalogue identities of one encrypted cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellIdentity<'a> {
    pub application: &'a str,
    pub scope_id: &'a str,
    pub identifier: &'a str,
    pub table_id: &'a str,
    /// A lowercase hyphenated UUID.
    pub row_id: &'a str,
    pub column_id: &'a str,
    /// A lowercase hyphenated UUID.
    pub epoch_id: &'a str,
}

/// Stable identities of one encrypted, equality-indexed column. Tokens do not
/// bind a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EqualityIdentity<'a> {
    pub application: &'a str,
    pub scope_id: &'a str,
    pub identifier: &'a str,
    pub table_id: &'a str,
    pub column_id: &'a str,
    pub epoch_id: &'a str,
}

/// Whether a stored value is a cell or a stream record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Cell,
    Stream,
}

/// A stored record's routing, before any authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredRecord<'a> {
    pub kind: RecordKind,
    pub epoch_id: &'a str,
    /// For a cell, the cell mechanism envelope; for a stream, the stream
    /// mechanism's encoding, beginning with its header.
    pub ciphertext: &'a [u8],
}

/// A lowercase hyphenated UUID with the RFC variant, of any version: the
/// form of application row IDs (`E2EE_PROTOCOL.md` §0, "Identifiers").
pub fn is_uuid(value: &[u8]) -> bool {
    core::str::from_utf8(value).is_ok_and(|value| ids::is_id(value, IdForm::Any))
}

/// A space epoch ID: a lowercase UUID of version 4 or 7, as the protocol
/// generates them (§0).
pub fn is_epoch_id(value: &[u8]) -> bool {
    value.len() == EPOCH_BYTES
        && core::str::from_utf8(value).is_ok_and(|value| ids::is_id(value, IdForm::Random))
}

impl CellIdentity<'_> {
    fn check(&self) -> Result<()> {
        if is_uuid(self.row_id.as_bytes())
            && is_epoch_id(self.epoch_id.as_bytes())
            && !self.table_id.is_empty()
            && !self.column_id.is_empty()
        {
            Ok(())
        } else {
            Err(Error::InvalidInput)
        }
    }

    fn context(&self, policy: &str) -> Result<Vec<u8>> {
        CryptoContext {
            application: self.application,
            policy,
            scope: self.scope_id,
            identifier: self.identifier,
            table: self.table_id,
            row: self.row_id,
            column: self.column_id,
            epoch: self.epoch_id,
            recipient: "",
        }
        .encode()
    }
}

/// The cell-record v1 context: `frame(canonical context, cell type)`.
pub fn cell_context(identity: &CellIdentity<'_>, cell_type: &[u8]) -> Result<Vec<u8>> {
    identity.check()?;
    frame_record(&[&identity.context(CELL_POLICY)?, cell_type])
}

/// The stream-record v1 context: `frame(canonical context, cell type)`, as
/// for a cell but with the stream policy. The stream mechanism binds its own
/// header, once, as the cell mechanism does.
pub fn stream_context(identity: &CellIdentity<'_>, cell_type: &[u8]) -> Result<Vec<u8>> {
    identity.check()?;
    frame_record(&[&identity.context(STREAM_POLICY)?, cell_type])
}

impl EqualityIdentity<'_> {
    fn check(&self) -> Result<()> {
        if is_epoch_id(self.epoch_id.as_bytes())
            && !self.table_id.is_empty()
            && !self.column_id.is_empty()
        {
            Ok(())
        } else {
            Err(Error::InvalidInput)
        }
    }
}

/// The equality v1 context: `frame(canonical context without row, cell type)`.
pub fn equality_context(identity: &EqualityIdentity<'_>, cell_type: &[u8]) -> Result<Vec<u8>> {
    identity.check()?;
    let context = CryptoContext {
        application: identity.application,
        policy: EQUALITY_POLICY,
        scope: identity.scope_id,
        identifier: identity.identifier,
        table: identity.table_id,
        column: identity.column_id,
        epoch: identity.epoch_id,
        ..CryptoContext::default()
    }
    .encode()?;
    frame_record(&[&context, cell_type])
}

fn routed(record: Mechanism<'_>, epoch_id: &str, inner: &[u8]) -> Result<Vec<u8>> {
    if !is_epoch_id(epoch_id.as_bytes()) {
        return Err(Error::InvalidInput);
    }
    let mut payload = Vec::with_capacity(EPOCH_BYTES + inner.len());
    payload.extend_from_slice(epoch_id.as_bytes());
    payload.extend_from_slice(inner);
    envelope::encode(record, &payload)
}

fn read_routed<'b>(record: Mechanism<'_>, value: &'b [u8]) -> Result<(&'b str, &'b [u8])> {
    let payload = envelope::decode(record, value)?;
    if payload.len() < MIN_RECORD_PAYLOAD || !is_epoch_id(&payload[..EPOCH_BYTES]) {
        return Err(Error::InvalidCiphertext);
    }
    let epoch_id = core::str::from_utf8(&payload[..EPOCH_BYTES]).expect("checked ASCII");
    Ok((epoch_id, &payload[EPOCH_BYTES..]))
}

/// Frames a cell mechanism envelope as a stored cell record.
pub fn encode_cell_record(epoch_id: &str, ciphertext: &[u8]) -> Result<Vec<u8>> {
    routed(CELL_RECORD, epoch_id, ciphertext)
}

/// The routing that begins a stored stream record: its header and epoch.
/// The stream mechanism's encoding, header included, follows directly.
pub fn stream_record_prefix(epoch_id: &str) -> Result<Vec<u8>> {
    routed(STREAM_RECORD, epoch_id, &[])
}

/// Reads a stored value's routing. A cell record for another version is
/// [`Error::UnsupportedFormat`] only if it is not a stream record either.
pub fn read_stored_record(value: &[u8]) -> Result<StoredRecord<'_>> {
    match read_routed(CELL_RECORD, value) {
        Ok((epoch_id, ciphertext)) => Ok(StoredRecord {
            kind: RecordKind::Cell,
            epoch_id,
            ciphertext,
        }),
        Err(Error::UnsupportedFormat) => {
            let (epoch_id, ciphertext) = read_routed(STREAM_RECORD, value)?;
            Ok(StoredRecord {
                kind: RecordKind::Stream,
                epoch_id,
                ciphertext,
            })
        }
        Err(error) => Err(error),
    }
}

/// Encrypts an encoded cell value as a stored cell record with `cipher`,
/// the mechanism the writer selected.
pub fn encrypt_cell_value(
    cipher: &dyn CellCipher,
    key: &[u8; 32],
    identity: &CellIdentity<'_>,
    cell_type: &[u8],
    value: &[u8],
    rng: &mut dyn RandomSource,
) -> Result<Vec<u8>> {
    let context = cell_context(identity, cell_type)?;
    let ciphertext = cipher.encrypt(key, &context, value, rng)?;
    encode_cell_record(identity.epoch_id, &ciphertext)
}

/// Decrypts a stored cell record written for `identity` with whichever
/// registered cell cipher its envelope names, returning the encoded value.
/// An unregistered mechanism is [`Error::UnsupportedFormat`]; a record from
/// another epoch, malformed bytes or an authentication failure is
/// [`Error::InvalidCiphertext`]; a provider failure is
/// [`Error::CryptoFailure`].
pub fn decrypt_cell_value(
    registry: &Registry,
    key: &[u8; 32],
    identity: &CellIdentity<'_>,
    cell_type: &[u8],
    stored: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let context = cell_context(identity, cell_type)?;
    let (epoch_id, ciphertext) = read_routed(CELL_RECORD, stored)?;
    if epoch_id != identity.epoch_id {
        return Err(Error::InvalidCiphertext);
    }
    let cipher = registry.cell_cipher_for(ciphertext)?;
    cipher
        .decrypt(key, &context, ciphertext)
        .map_err(Error::after_routing)
}

/// Starts a streamed stored value with `cipher`. Write the returned prefix,
/// then the writer's output.
pub fn begin_stream_value(
    cipher: &dyn StreamCipher,
    key: &[u8; 32],
    identity: &CellIdentity<'_>,
    cell_type: &[u8],
    rng: &mut dyn RandomSource,
) -> Result<(Box<dyn StreamWriter>, Vec<u8>)> {
    let context = stream_context(identity, cell_type)?;
    let mut prefix = stream_record_prefix(identity.epoch_id)?;
    let (writer, header) = cipher.encryptor(key, &context, rng)?;
    prefix.extend_from_slice(&header);
    Ok((writer, prefix))
}

/// Encrypts a whole value as a stored stream record with `cipher`.
pub fn encrypt_stream_value(
    cipher: &dyn StreamCipher,
    key: &[u8; 32],
    identity: &CellIdentity<'_>,
    cell_type: &[u8],
    value: &[u8],
    rng: &mut dyn RandomSource,
) -> Result<Vec<u8>> {
    let (mut writer, mut out) = begin_stream_value(cipher, key, identity, cell_type, rng)?;
    writer.update(value, &mut out)?;
    writer.finish(&mut out)?;
    Ok(out)
}

/// Decrypts a whole stored stream record written for `identity` with
/// whichever registered stream cipher its encoding names. An unregistered
/// mechanism is [`Error::UnsupportedFormat`]; malformed bytes or an
/// authentication failure is [`Error::InvalidCiphertext`]; a provider
/// failure is [`Error::CryptoFailure`].
pub fn decrypt_stream_value(
    registry: &Registry,
    key: &[u8; 32],
    identity: &CellIdentity<'_>,
    cell_type: &[u8],
    stored: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let (epoch_id, routed) = read_routed(STREAM_RECORD, stored)?;
    if epoch_id != identity.epoch_id {
        return Err(Error::InvalidCiphertext);
    }
    let cipher = registry.stream_cipher_for(routed)?;
    let context = stream_context(identity, cell_type)?;
    let mut reader = cipher.decryptor(key, &context)?;
    let mut out = Zeroizing::new(Vec::with_capacity(routed.len()));
    reader
        .update(routed, &mut out)
        .and_then(|()| reader.finish())
        .map_err(Error::after_routing)?;
    Ok(out)
}

/// A logical value as an equality index encodes it (`E2EE_EQUALITY_FORMAT.md`,
/// "Value encoding"). The column's type is bound by the context, so values
/// of different types are never compared.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EqualityValue<'a> {
    Null,
    Bool(bool),
    /// 32- and 64-bit integers.
    Int(i64),
    Timestamp(u64),
    Float(f64),
    /// Compared by exact UTF-8 bytes, with no Unicode normalisation.
    Text(&'a str),
    Bytes(&'a [u8]),
    Uuid([u8; 16]),
}

/// The canonical bit pattern of an equality-indexed float: every NaN is
/// `0x7ff8000000000000` and `-0.0` is `0.0`, so tokens match Jazz's
/// plaintext equality, where `-0.0 == 0.0`.
const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

/// `V`, the encoded value an equality token hashes: `00` for null;
/// otherwise `01` followed by the value: one byte `00`/`01` for a boolean,
/// eight big-endian bytes for an integer (two's complement), a timestamp or
/// a float (its canonical bit pattern), the exact UTF-8 bytes of a text,
/// the bytes of a byte string, the 16 bytes of a UUID.
pub fn equality_value(value: &EqualityValue<'_>) -> Vec<u8> {
    let mut out = vec![1u8];
    match *value {
        EqualityValue::Null => return vec![0],
        EqualityValue::Bool(value) => out.push(u8::from(value)),
        EqualityValue::Int(value) => out.extend_from_slice(&value.to_be_bytes()),
        EqualityValue::Timestamp(value) => out.extend_from_slice(&value.to_be_bytes()),
        EqualityValue::Float(value) => {
            let bits = if value.is_nan() {
                CANONICAL_NAN
            } else if value == 0.0 {
                0
            } else {
                value.to_bits()
            };
            out.extend_from_slice(&bits.to_be_bytes());
        }
        EqualityValue::Text(value) => out.extend_from_slice(value.as_bytes()),
        EqualityValue::Bytes(value) => out.extend_from_slice(value),
        EqualityValue::Uuid(value) => out.extend_from_slice(&value),
    }
    out
}

/// The equality token of a logical value: [`equality_token`] of
/// [`equality_value`].
pub fn equality_token_of(
    tagger: &dyn EqualityTagger,
    key: &[u8; 32],
    identity: &EqualityIdentity<'_>,
    cell_type: &[u8],
    value: &EqualityValue<'_>,
) -> Result<Vec<u8>> {
    equality_token(tagger, key, identity, cell_type, &equality_value(value))
}

/// The equality token for an encoded value, with `tagger`.
pub fn equality_token(
    tagger: &dyn EqualityTagger,
    key: &[u8; 32],
    identity: &EqualityIdentity<'_>,
    cell_type: &[u8],
    value: &[u8],
) -> Result<Vec<u8>> {
    tagger.token(key, &equality_context(identity, cell_type)?, value)
}
