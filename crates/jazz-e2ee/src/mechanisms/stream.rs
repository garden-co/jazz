//! Large-value stream mechanism `jazz.stream.xchacha20poly1305`, version 1
//! (`E2EE_LARGE_VALUE_FORMAT.md`).
//!
//! The STREAM construction (Hoang, Reyhanitabar, Rogaway and Vizár, "Online
//! Authenticated-Encryption and its Nonce-Reuse Misuse-Resistance") over
//! XChaCha20-Poly1305, as RustCrypto's `aead-stream` implements it in
//! `StreamBE32`: each record's 24-byte nonce is a 19-byte per-value nonce
//! prefix, the record's u32be index and a last-record flag byte (0 or 1).
//!
//! With `H` this mechanism's header and `C` the context, let
//! `A = len(H) || H || len(C) || C` (u32be lengths). The stream key is
//! `BLAKE2b-256(key = epoch key, A)` and every record uses `A` as associated
//! data. The encoding is `H || 19-byte random nonce prefix`, then data
//! records (`u32be length || ciphertext`, 1–65,536 plaintext bytes, flag 0),
//! then exactly one empty last record (length 16, flag 1). A length of 16
//! marks the last record; data records are never empty, so they are longer.
//!
//! Both directions are incremental and do no I/O: callers feed bytes in any
//! chunking and receive output as soon as a record is complete. Memory is
//! bounded by one record. A decrypted record is released only after it
//! authenticates, but whole-value success is established only by
//! [`StreamDecryptor::finish`]: the last flag in the nonce is what makes a
//! truncated stream fail.

use aead_stream::{DecryptorBE32, EncryptorBE32};
use chacha20poly1305::XChaCha20Poly1305;
use zeroize::Zeroizing;

use crate::agility::{MechanismImpl, StreamCipher, StreamReader, StreamWriter};
use crate::context::frame_record;
use crate::envelope::Mechanism;
use crate::error::{Error, Result};
use crate::primitives::hash;
use crate::random::{RandomSource, random_array};

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.stream.xchacha20poly1305", 1);
/// The largest data record's plaintext; writers fill records to this size.
pub const RECORD_BYTES: usize = 65_536;
/// The per-value nonce prefix carried after the header.
pub const NONCE_PREFIX_BYTES: usize = 19;
/// Poly1305 tag bytes per record.
pub const TAG_BYTES: usize = 16;
/// The last record's length: an empty plaintext and its tag.
const LAST_RECORD: usize = TAG_BYTES;
const MAX_CIPHERTEXT: usize = RECORD_BYTES + TAG_BYTES;

/// What both directions derive from the key and context.
struct Prepared {
    aad: Vec<u8>,
    key: Zeroizing<[u8; 32]>,
    header: Vec<u8>,
}

fn prepare(key: &[u8; 32], context: &[u8]) -> Result<Prepared> {
    let header = MECHANISM.header()?;
    let aad = frame_record(&[&header, context])?;
    let key = hash::keyed_hash(key, &aad);
    Ok(Prepared { aad, key, header })
}

fn put_record(out: &mut Vec<u8>, ciphertext: &[u8]) {
    out.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
    out.extend_from_slice(ciphertext);
}

/// Encrypts a value incrementally, coalescing input into full records.
pub struct StreamEncryptor {
    stream: EncryptorBE32<XChaCha20Poly1305>,
    aad: Vec<u8>,
    pending: Zeroizing<Vec<u8>>,
}

impl StreamEncryptor {
    /// Starts a stream under a fresh random nonce prefix. The returned bytes
    /// (`H || nonce prefix`) begin the encoding.
    pub fn new(
        key: &[u8; 32],
        context: &[u8],
        rng: &mut (impl RandomSource + ?Sized),
    ) -> Result<(Self, Vec<u8>)> {
        let Prepared {
            aad,
            key,
            header: mut prefix,
        } = prepare(key, context)?;
        let nonce = random_array::<NONCE_PREFIX_BYTES>(rng)?;
        prefix.extend_from_slice(&nonce);
        let encryptor = Self {
            stream: EncryptorBE32::new((&*key).into(), (&nonce).into()),
            aad,
            pending: Zeroizing::new(Vec::with_capacity(RECORD_BYTES + TAG_BYTES)),
        };
        Ok((encryptor, prefix))
    }

    /// Appends every record completed by `input` to `out`.
    pub fn update(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        while !input.is_empty() {
            let take = (RECORD_BYTES - self.pending.len()).min(input.len());
            self.pending.extend_from_slice(&input[..take]);
            input = &input[take..];
            if self.pending.len() == RECORD_BYTES {
                self.flush(out)?;
            }
        }
        Ok(())
    }

    /// Appends the last data record, if any, and the empty last record.
    pub fn finish(mut self, out: &mut Vec<u8>) -> Result<()> {
        if !self.pending.is_empty() {
            self.flush(out)?;
        }
        let mut last = Vec::with_capacity(TAG_BYTES);
        self.stream
            .encrypt_last_in_place(&self.aad, &mut last)
            .map_err(|_| Error::CryptoFailure)?;
        put_record(out, &last);
        Ok(())
    }

    fn flush(&mut self, out: &mut Vec<u8>) -> Result<()> {
        // Encrypted in place, so the buffer then holds only ciphertext.
        self.stream
            .encrypt_next_in_place(&self.aad, &mut *self.pending)
            .map_err(|_| Error::CryptoFailure)?;
        put_record(out, &self.pending);
        self.pending.clear();
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Header,
    Records,
    Done,
    Failed,
}

/// Decrypts a value incrementally. After any error the decryptor is spent:
/// its key state is dropped and every later call fails.
pub struct StreamDecryptor {
    key: Zeroizing<[u8; 32]>,
    aad: Vec<u8>,
    header: Vec<u8>,
    stream: Option<DecryptorBE32<XChaCha20Poly1305>>,
    buffer: Zeroizing<Vec<u8>>,
    phase: Phase,
}

impl StreamDecryptor {
    pub fn new(key: &[u8; 32], context: &[u8]) -> Result<Self> {
        let Prepared { aad, key, header } = prepare(key, context)?;
        Ok(Self {
            key,
            aad,
            header,
            stream: None,
            buffer: Zeroizing::new(Vec::new()),
            phase: Phase::Header,
        })
    }

    /// Consumes `input`, appending each record's plaintext to `out` once
    /// that record authenticates.
    pub fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        let result = self.consume(input, out);
        if result.is_err() {
            self.fail();
        }
        result
    }

    /// Bytes held while waiting for the rest of a header, length or record:
    /// at most one record and its length.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// Succeeds only if the last record authenticated and nothing followed.
    pub fn finish(mut self) -> Result<()> {
        if self.phase == Phase::Done && self.buffer.is_empty() {
            Ok(())
        } else {
            self.fail();
            Err(Error::InvalidCiphertext)
        }
    }

    fn fail(&mut self) {
        self.phase = Phase::Failed;
        self.stream = None;
        self.buffer.clear();
    }

    /// Bytes the next unit needs in total: the header, a length, or a record.
    fn needed(&self) -> Result<usize> {
        match self.phase {
            Phase::Header => Ok(self.header.len() + NONCE_PREFIX_BYTES),
            Phase::Records if self.buffer.len() < 4 => Ok(4),
            Phase::Records => {
                let length =
                    u32::from_be_bytes(self.buffer[..4].try_into().expect("4 bytes")) as usize;
                if length == LAST_RECORD || (TAG_BYTES + 1..=MAX_CIPHERTEXT).contains(&length) {
                    Ok(4 + length)
                } else {
                    Err(Error::InvalidCiphertext)
                }
            }
            Phase::Done | Phase::Failed => Err(Error::InvalidCiphertext),
        }
    }

    fn consume(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        if self.phase == Phase::Failed {
            return Err(Error::InvalidCiphertext);
        }
        while !input.is_empty() {
            let needed = self.needed()?;
            let take = (needed - self.buffer.len()).min(input.len());
            self.buffer.extend_from_slice(&input[..take]);
            input = &input[take..];
            if self.buffer.len() < needed {
                break;
            }
            match self.phase {
                Phase::Header => self.read_header()?,
                Phase::Records if needed == 4 => {
                    // Reject an out-of-range length as soon as it arrives.
                    self.needed()?;
                    continue;
                }
                Phase::Records => self.read_record(out)?,
                Phase::Done | Phase::Failed => unreachable!("needed() rejects these"),
            }
            self.buffer.clear();
        }
        Ok(())
    }

    fn read_header(&mut self) -> Result<()> {
        let (envelope, nonce) = self.buffer.split_at(self.header.len());
        if envelope != self.header {
            return Err(Error::InvalidCiphertext);
        }
        let nonce: &[u8; NONCE_PREFIX_BYTES] = nonce.try_into().expect("19 nonce bytes");
        self.stream = Some(DecryptorBE32::new((&*self.key).into(), nonce.into()));
        self.phase = Phase::Records;
        Ok(())
    }

    fn read_record(&mut self, out: &mut Vec<u8>) -> Result<()> {
        let mut record = Zeroizing::new(self.buffer[4..].to_vec());
        if record.len() == LAST_RECORD {
            let stream = self.stream.take().ok_or(Error::InvalidCiphertext)?;
            stream
                .decrypt_last_in_place(&self.aad, &mut *record)
                .map_err(|_| Error::InvalidCiphertext)?;
            self.phase = Phase::Done;
        } else {
            let stream = self.stream.as_mut().ok_or(Error::InvalidCiphertext)?;
            stream
                .decrypt_next_in_place(&self.aad, &mut *record)
                .map_err(|_| Error::InvalidCiphertext)?;
            out.extend_from_slice(&record);
        }
        Ok(())
    }
}

/// Encrypts a whole value.
pub fn encrypt(
    key: &[u8; 32],
    context: &[u8],
    plaintext: &[u8],
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<Vec<u8>> {
    let (mut encryptor, mut out) = StreamEncryptor::new(key, context, rng)?;
    let records = plaintext.len().div_ceil(RECORD_BYTES) + 1;
    out.reserve(plaintext.len() + records * (4 + TAG_BYTES));
    encryptor.update(plaintext, &mut out)?;
    encryptor.finish(&mut out)?;
    Ok(out)
}

/// Decrypts a whole value; returns plaintext only if the entire stream
/// authenticates.
pub fn decrypt(key: &[u8; 32], context: &[u8], ciphertext: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let mut decryptor = StreamDecryptor::new(key, context)?;
    let mut out = Zeroizing::new(Vec::with_capacity(ciphertext.len()));
    decryptor.update(ciphertext, &mut out)?;
    decryptor.finish()?;
    Ok(out)
}

/// The built-in stream mechanism as a [`StreamCipher`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtin;

impl MechanismImpl for Builtin {
    fn mechanism(&self) -> Mechanism<'static> {
        MECHANISM
    }
}

impl StreamCipher for Builtin {
    fn encryptor(
        &self,
        key: &[u8; 32],
        context: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<(Box<dyn StreamWriter>, Vec<u8>)> {
        let (encryptor, prefix) = StreamEncryptor::new(key, context, rng)?;
        Ok((Box::new(encryptor), prefix))
    }

    fn decryptor(&self, key: &[u8; 32], context: &[u8]) -> Result<Box<dyn StreamReader>> {
        Ok(Box::new(StreamDecryptor::new(key, context)?))
    }

    fn max_buffered(&self) -> usize {
        4 + MAX_CIPHERTEXT
    }
}

impl StreamWriter for StreamEncryptor {
    fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        StreamEncryptor::update(self, input, out)
    }

    fn finish(self: Box<Self>, out: &mut Vec<u8>) -> Result<()> {
        StreamEncryptor::finish(*self, out)
    }
}

impl StreamReader for StreamDecryptor {
    fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        StreamDecryptor::update(self, input, out)
    }

    fn buffered(&self) -> usize {
        StreamDecryptor::buffered(self)
    }

    fn finish(self: Box<Self>) -> Result<()> {
        StreamDecryptor::finish(*self)
    }
}
