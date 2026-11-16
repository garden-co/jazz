//! The conformance kit every mechanism implementation runs, built-in or
//! custom. Enable the `conformance` feature (normally as a dev-dependency
//! feature) and call the `check_*` function for the role from a test:
//!
//! ```ignore
//! #[test]
//! fn conforms() {
//!     jazz_e2ee::conformance::check_key_sealer(&MyKem, &my_vectors()).unwrap();
//! }
//! ```
//!
//! Each check proves round trips, binding to key and context, rejection of
//! tampered, truncated and extended input (and, for streams, of reordered,
//! duplicated and dropped segments), routing by the mechanism's own
//! envelope header, and that the declared vectors reproduce exactly when
//! replayed with their recorded random bytes.
//!
//! Zeroization is enforced by the trait signatures rather than observed:
//! secrets cross the traits only as `Zeroizing` values or
//! [`SecretKey`](crate::agility::SecretKey)s, which clear on drop, and the
//! kit checks that key pairs do not print their secret half. Clearing memory
//! is not observable from safe Rust.

use core::fmt;
use std::sync::Arc;

use crate::agility::{
    CellCipher, EqualityTagger, KeyPair, KeySealer, KeyWrapper, MechanismId, MechanismImpl,
    PublicKey, Signer, StreamCipher,
};
use crate::envelope;
use crate::error::{Error, Result};
use crate::random::RandomSource;

/// A failed conformance check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub mechanism: String,
    pub check: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.mechanism, self.check)
    }
}

impl std::error::Error for Failure {}

pub type Outcome = core::result::Result<(), Failure>;

/// Replays recorded random bytes, then fails.
#[derive(Debug, Clone)]
pub struct FixedRandom(pub Vec<u8>);

impl RandomSource for FixedRandom {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        if self.0.len() < dest.len() {
            return Err(Error::RandomUnavailable);
        }
        dest.copy_from_slice(&self.0[..dest.len()]);
        self.0.drain(..dest.len());
        Ok(())
    }
}

/// Deterministic test inputs; not a secure random source.
struct Inputs(u64);

impl Inputs {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn bytes(&mut self, length: usize) -> Vec<u8> {
        (0..length).map(|_| self.next() as u8).collect()
    }

    fn key(&mut self) -> [u8; 32] {
        self.bytes(32).try_into().expect("32 bytes")
    }
}

impl RandomSource for Inputs {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        dest.iter_mut().for_each(|byte| *byte = self.next() as u8);
        Ok(())
    }
}

/// How an operation on suspect input ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    Accepted,
    /// Rejected as malformed, unauthentic or of another mechanism.
    Rejected,
    /// Failed with an error that is not a rejection of the input, such as a
    /// provider failure.
    Misreported(Error),
}

fn verdict<T>(result: &Result<T>) -> Verdict {
    match result {
        Ok(_) => Verdict::Accepted,
        Err(Error::InvalidCiphertext | Error::UnsupportedFormat) => Verdict::Rejected,
        Err(error) => Verdict::Misreported(*error),
    }
}

/// A verification result: `Ok(false)` is the rejection.
fn verified(result: Result<bool>) -> Verdict {
    match result {
        Ok(true) => Verdict::Accepted,
        Ok(false) => Verdict::Rejected,
        Err(error) => Verdict::Misreported(error),
    }
}

struct Checker {
    mechanism: String,
}

impl Checker {
    fn new(implementation: &dyn MechanismImpl) -> core::result::Result<Self, Failure> {
        let mechanism = implementation.mechanism();
        let checker = Self {
            mechanism: format!("{}/{}", mechanism.id, mechanism.version),
        };
        checker.ensure(
            mechanism.validate().is_ok(),
            "mechanism ID is 1-64 characters of [a-z0-9.-] and the version is non-zero",
        )?;
        Ok(checker)
    }

    fn ensure(&self, condition: bool, check: &str) -> Outcome {
        if condition {
            Ok(())
        } else {
            Err(Failure {
                mechanism: self.mechanism.clone(),
                check: check.to_owned(),
            })
        }
    }

    /// The input must be rejected as invalid, not accepted and not reported
    /// as a provider failure.
    fn reject(&self, verdict: Verdict, check: &str) -> Outcome {
        match verdict {
            Verdict::Rejected => Ok(()),
            Verdict::Accepted => self.ensure(false, check),
            Verdict::Misreported(error) => self.ensure(
                false,
                &format!("{check} (as invalid input, not as {error:?})"),
            ),
        }
    }

    fn ok<T>(&self, result: Result<T>, check: &str) -> core::result::Result<T, Failure> {
        result.map_err(|_| Failure {
            mechanism: self.mechanism.clone(),
            check: check.to_owned(),
        })
    }

    fn own_envelope(
        &self,
        implementation: &dyn MechanismImpl,
        bytes: &[u8],
        what: &str,
    ) -> Outcome {
        self.ensure(
            envelope::decode(implementation.mechanism(), bytes).is_ok(),
            &format!("{what} is an envelope of the mechanism's own ID and version"),
        )
    }

    /// Every flipped bit (sampled when long), every truncation (sampled when
    /// long) and one appended byte must be rejected.
    fn tamper(&self, original: &[u8], what: &str, accepts: &dyn Fn(&[u8]) -> Verdict) -> Outcome {
        self.ensure(
            accepts(original) == Verdict::Accepted,
            &format!("{what} is accepted unmodified"),
        )?;
        let step = (original.len() / 512).max(1);
        for index in (0..original.len()).step_by(step) {
            let bits = if step == 1 { 0..8 } else { 0..1 };
            for bit in bits {
                let mut changed = original.to_vec();
                changed[index] ^= 1 << bit;
                self.reject(
                    accepts(&changed),
                    &format!("{what} with bit {bit} of byte {index} flipped is rejected"),
                )?;
            }
        }
        for length in (0..original.len()).step_by(step) {
            self.reject(
                accepts(&original[..length]),
                &format!("{what} truncated to {length} bytes is rejected"),
            )?;
        }
        self.reject(
            accepts(&original[..original.len() - 1]),
            &format!("{what} without its last byte is rejected"),
        )?;
        self.reject(
            accepts(&[original, &[0]].concat()),
            &format!("{what} with a byte appended is rejected"),
        )
    }

    /// A failing random source must fail the operation as
    /// [`Error::RandomUnavailable`] or [`Error::CryptoFailure`].
    fn random_failure<T>(&self, result: Result<T>, what: &str) -> Outcome {
        self.ensure(
            matches!(result, Err(Error::RandomUnavailable | Error::CryptoFailure)),
            &format!("{what} with a failing random source fails as unavailable randomness"),
        )
    }

    /// A provider failure must surface as [`Error::CryptoFailure`].
    fn provider_failure<T>(&self, result: Result<T>, what: &str) -> Outcome {
        match result {
            Err(Error::CryptoFailure) => Ok(()),
            Ok(_) => self.ensure(false, &format!("{what} fails when the provider fails")),
            Err(error) => self.ensure(
                false,
                &format!("{what} reports a provider failure as CryptoFailure, not {error:?}"),
            ),
        }
    }

    fn secret_not_printed(&self, pair: &KeyPair) -> Outcome {
        let printed = format!("{pair:?}");
        let secret = pair.secret.as_bytes();
        let leaked = secret.len() >= 4 && printed.contains(&format!("{:?}", &secret[..4]));
        self.ensure(!leaked, "a key pair's debug output omits its secret key")
    }
}

const SIZES: [usize; 5] = [0, 1, 31, 1000, 70_000];

/// A cell cipher vector: encrypting `plaintext` with `random` as the random
/// bytes gives exactly `envelope`.
#[derive(Debug, Clone)]
pub struct CellVector {
    pub key: [u8; 32],
    pub context: Vec<u8>,
    pub random: Vec<u8>,
    pub plaintext: Vec<u8>,
    pub envelope: Vec<u8>,
}

pub fn check_cell_cipher(cipher: &dyn CellCipher, vectors: &[CellVector]) -> Outcome {
    let c = Checker::new(cipher)?;
    c.random_failure(
        cipher.encrypt(&[0; 32], b"", b"x", &mut FixedRandom(Vec::new())),
        "encrypt",
    )?;
    let mut inputs = Inputs(0x0c0f_fee0);
    for size in SIZES {
        let (key, context, plaintext) = (inputs.key(), inputs.bytes(40), inputs.bytes(size));
        let sealed = c.ok(
            cipher.encrypt(&key, &context, &plaintext, &mut inputs),
            "encrypt succeeds",
        )?;
        c.own_envelope(cipher, &sealed, "ciphertext")?;
        let opened = c.ok(
            cipher.decrypt(&key, &context, &sealed),
            "decrypt round-trips",
        )?;
        c.ensure(*opened == plaintext, "decrypt returns the plaintext")?;
        let again = c.ok(
            cipher.encrypt(&key, &context, &plaintext, &mut inputs),
            "encrypt succeeds",
        )?;
        c.ensure(again != sealed, "equal inputs encrypt differently")?;
        let mut other_key = key;
        other_key[0] ^= 1;
        c.reject(
            verdict(&cipher.decrypt(&other_key, &context, &sealed)),
            "another key is rejected",
        )?;
        let mut other_context = context.clone();
        other_context[0] ^= 1;
        c.reject(
            verdict(&cipher.decrypt(&key, &other_context, &sealed)),
            "another context is rejected",
        )?;
        if size <= 31 {
            c.tamper(&sealed, "ciphertext", &|value| {
                verdict(&cipher.decrypt(&key, &context, value))
            })?;
        }
    }
    for (index, vector) in vectors.iter().enumerate() {
        let mut random = FixedRandom(vector.random.clone());
        let sealed = c.ok(
            cipher.encrypt(&vector.key, &vector.context, &vector.plaintext, &mut random),
            &format!("vector {index} encrypts"),
        )?;
        c.ensure(
            sealed == vector.envelope,
            &format!("vector {index} reproduces exactly"),
        )?;
        let opened = c.ok(
            cipher.decrypt(&vector.key, &vector.context, &vector.envelope),
            &format!("vector {index} decrypts"),
        )?;
        c.ensure(
            *opened == vector.plaintext,
            &format!("vector {index} decrypts to its plaintext"),
        )?;
    }
    Ok(())
}

/// A key wrapping vector.
#[derive(Debug, Clone)]
pub struct WrapVector {
    pub wrapping_key: [u8; 32],
    pub context: Vec<u8>,
    pub key: [u8; 32],
    pub random: Vec<u8>,
    pub envelope: Vec<u8>,
}

pub fn check_key_wrapper(wrapper: &dyn KeyWrapper, vectors: &[WrapVector]) -> Outcome {
    let c = Checker::new(wrapper)?;
    c.random_failure(
        wrapper.wrap(&[0; 32], b"", &[0; 32], &mut FixedRandom(Vec::new())),
        "wrap",
    )?;
    let mut inputs = Inputs(0x0a11_ce00);
    let (wrapping_key, context, key) = (inputs.key(), inputs.bytes(40), inputs.key());
    let wrapped = c.ok(
        wrapper.wrap(&wrapping_key, &context, &key, &mut inputs),
        "wrap succeeds",
    )?;
    c.own_envelope(wrapper, &wrapped, "wrapped key")?;
    let unwrapped = c.ok(
        wrapper.unwrap(&wrapping_key, &context, &wrapped),
        "unwrap round-trips",
    )?;
    c.ensure(*unwrapped == key, "unwrap returns the key")?;
    let again = c.ok(
        wrapper.wrap(&wrapping_key, &context, &key, &mut inputs),
        "wrap succeeds",
    )?;
    c.ensure(again != wrapped, "equal inputs wrap differently")?;
    let mut other_key = wrapping_key;
    other_key[0] ^= 1;
    c.reject(
        verdict(&wrapper.unwrap(&other_key, &context, &wrapped)),
        "another wrapping key is rejected",
    )?;
    let mut other_context = context.clone();
    other_context[0] ^= 1;
    c.reject(
        verdict(&wrapper.unwrap(&wrapping_key, &other_context, &wrapped)),
        "another context is rejected",
    )?;
    c.tamper(&wrapped, "wrapped key", &|value| {
        verdict(&wrapper.unwrap(&wrapping_key, &context, value))
    })?;
    for (index, vector) in vectors.iter().enumerate() {
        let mut random = FixedRandom(vector.random.clone());
        let wrapped = c.ok(
            wrapper.wrap(
                &vector.wrapping_key,
                &vector.context,
                &vector.key,
                &mut random,
            ),
            &format!("vector {index} wraps"),
        )?;
        c.ensure(
            wrapped == vector.envelope,
            &format!("vector {index} reproduces exactly"),
        )?;
        let key = c.ok(
            wrapper.unwrap(&vector.wrapping_key, &vector.context, &vector.envelope),
            &format!("vector {index} unwraps"),
        )?;
        c.ensure(
            *key == vector.key,
            &format!("vector {index} unwraps to its key"),
        )?;
    }
    Ok(())
}

/// A key sealing vector: the recipient is derived from `recipient_ikm`, and
/// sealing `key` with `random` as the random bytes gives exactly `envelope`.
#[derive(Debug, Clone)]
pub struct SealVector {
    pub recipient_ikm: Vec<u8>,
    pub recipient_public: Vec<u8>,
    pub context: Vec<u8>,
    pub key: [u8; 32],
    pub random: Vec<u8>,
    pub envelope: Vec<u8>,
}

pub fn check_key_sealer(sealer: &dyn KeySealer, vectors: &[SealVector]) -> Outcome {
    let c = Checker::new(sealer)?;
    c.random_failure(
        sealer.generate_key_pair(&mut FixedRandom(Vec::new())),
        "key generation",
    )?;
    let mut inputs = Inputs(0x5ea1_ed00);
    let ikm = inputs.bytes(32);
    let pair = c.ok(
        sealer.derive_key_pair(&ikm),
        "key pairs derive from 32 bytes",
    )?;
    let again = c.ok(
        sealer.derive_key_pair(&ikm),
        "key pairs derive from 32 bytes",
    )?;
    c.ensure(
        pair.public == again.public && pair.secret == again.secret,
        "derivation is deterministic",
    )?;
    c.ensure(
        pair.public.mechanism == sealer.mechanism(),
        "public keys name the mechanism",
    )?;
    c.secret_not_printed(&pair)?;
    c.random_failure(
        sealer.seal(&pair.public, b"", &[0; 32], &mut FixedRandom(Vec::new())),
        "seal",
    )?;
    let other = c.ok(sealer.generate_key_pair(&mut inputs), "key pairs generate")?;
    c.ensure(
        other.public != pair.public,
        "different input gives a different key pair",
    )?;
    let (context, key) = (inputs.bytes(40), inputs.key());
    let sealed = c.ok(
        sealer.seal(&pair.public, &context, &key, &mut inputs),
        "seal succeeds",
    )?;
    c.own_envelope(sealer, &sealed, "sealed key")?;
    let opened = c.ok(sealer.open(&pair, &context, &sealed), "open round-trips")?;
    c.ensure(*opened == key, "open returns the key")?;
    let resealed = c.ok(
        sealer.seal(&pair.public, &context, &key, &mut inputs),
        "seal succeeds",
    )?;
    c.ensure(resealed != sealed, "equal inputs seal differently")?;
    c.reject(
        verdict(&sealer.open(&other, &context, &sealed)),
        "another recipient cannot open",
    )?;
    let mut other_context = context.clone();
    other_context[0] ^= 1;
    c.reject(
        verdict(&sealer.open(&pair, &other_context, &sealed)),
        "another context is rejected",
    )?;
    c.tamper(&sealed, "sealed key", &|value| {
        verdict(&sealer.open(&pair, &context, value))
    })?;
    let foreign = PublicKey {
        mechanism: MechanismId::new("org.invalid.other", 1),
        bytes: pair.public.bytes.clone(),
    };
    c.ensure(
        sealer.seal(&foreign, &context, &key, &mut inputs) == Err(Error::RecipientUnsupported),
        "a public key of another mechanism is refused as unsupported",
    )?;
    for (index, vector) in vectors.iter().enumerate() {
        let recipient = c.ok(
            sealer.derive_key_pair(&vector.recipient_ikm),
            &format!("vector {index} derives its recipient"),
        )?;
        c.ensure(
            recipient.public.bytes == vector.recipient_public,
            &format!("vector {index} derives its recipient public key"),
        )?;
        let mut random = FixedRandom(vector.random.clone());
        let sealed = c.ok(
            sealer.seal(&recipient.public, &vector.context, &vector.key, &mut random),
            &format!("vector {index} seals"),
        )?;
        c.ensure(
            sealed == vector.envelope,
            &format!("vector {index} reproduces exactly"),
        )?;
        let key = c.ok(
            sealer.open(&recipient, &vector.context, &vector.envelope),
            &format!("vector {index} opens"),
        )?;
        c.ensure(
            *key == vector.key,
            &format!("vector {index} opens to its key"),
        )?;
    }
    Ok(())
}

/// A signature vector. `deterministic` declares that signing reproduces
/// `signature` exactly; otherwise only verification is checked.
#[derive(Debug, Clone)]
pub struct SignatureVector {
    pub seed: Vec<u8>,
    pub public: Vec<u8>,
    pub record: Vec<u8>,
    pub signature: Vec<u8>,
    pub deterministic: bool,
}

pub fn check_signer(signer: &dyn Signer, vectors: &[SignatureVector]) -> Outcome {
    let c = Checker::new(signer)?;
    c.random_failure(
        signer.generate_key_pair(&mut FixedRandom(Vec::new())),
        "key generation",
    )?;
    let mut inputs = Inputs(0x5160_0000);
    let pair = c.ok(signer.generate_key_pair(&mut inputs), "key pairs generate")?;
    c.ensure(
        pair.public.mechanism == signer.mechanism(),
        "public keys name the mechanism",
    )?;
    c.secret_not_printed(&pair)?;
    let seed = inputs.bytes(32);
    let derived = c.ok(
        signer.derive_key_pair(&seed),
        "key pairs derive from a 32-byte seed",
    )?;
    let again = c.ok(
        signer.derive_key_pair(&seed),
        "key pairs derive from a 32-byte seed",
    )?;
    c.ensure(
        derived.public == again.public,
        "derivation is deterministic",
    )?;
    let record = inputs.bytes(100);
    let signature = c.ok(signer.sign(&pair, &record), "sign succeeds")?;
    c.own_envelope(signer, &signature, "signature")?;
    c.ensure(
        signer.verify(&pair.public, &record, &signature) == Ok(true),
        "the signature verifies",
    )?;
    c.reject(
        verified(signer.verify(&derived.public, &record, &signature)),
        "another key does not verify",
    )?;
    let mut other_record = record.clone();
    other_record[0] ^= 1;
    c.reject(
        verified(signer.verify(&pair.public, &other_record, &signature)),
        "another record does not verify",
    )?;
    c.reject(
        verified(signer.verify(&pair.public, b"", &signature)),
        "an empty record does not verify",
    )?;
    c.tamper(&signature, "signature", &|value| {
        verified(signer.verify(&pair.public, &record, value))
    })?;
    let foreign = PublicKey {
        mechanism: MechanismId::new("org.invalid.other", 1),
        bytes: pair.public.bytes.clone(),
    };
    c.reject(
        verified(signer.verify(&foreign, &record, &signature)),
        "a public key of another mechanism does not verify",
    )?;
    for (index, vector) in vectors.iter().enumerate() {
        let pair = c.ok(
            signer.derive_key_pair(&vector.seed),
            &format!("vector {index} derives"),
        )?;
        c.ensure(
            pair.public.bytes == vector.public,
            &format!("vector {index} derives its public key"),
        )?;
        c.ensure(
            signer.verify(&pair.public, &vector.record, &vector.signature) == Ok(true),
            &format!("vector {index} verifies"),
        )?;
        if vector.deterministic {
            let signature = c.ok(
                signer.sign(&pair, &vector.record),
                &format!("vector {index} signs"),
            )?;
            c.ensure(
                signature == vector.signature,
                &format!("vector {index} reproduces exactly"),
            )?;
        }
    }
    Ok(())
}

/// An equality token vector.
#[derive(Debug, Clone)]
pub struct EqualityVector {
    pub key: [u8; 32],
    pub context: Vec<u8>,
    pub value: Vec<u8>,
    pub token: Vec<u8>,
}

pub fn check_equality_tagger(tagger: &dyn EqualityTagger, vectors: &[EqualityVector]) -> Outcome {
    let c = Checker::new(tagger)?;
    let mut inputs = Inputs(0xe9a1_0000);
    let (key, context, value) = (inputs.key(), inputs.bytes(40), inputs.bytes(20));
    let token = c.ok(tagger.token(&key, &context, &value), "token succeeds")?;
    c.own_envelope(tagger, &token, "token")?;
    c.ensure(
        tagger.token(&key, &context, &value).ok() == Some(token.clone()),
        "tokens are deterministic",
    )?;
    let mut other_key = key;
    other_key[0] ^= 1;
    c.ensure(
        tagger.token(&other_key, &context, &value).ok() != Some(token.clone()),
        "the key separates tokens",
    )?;
    let mut other_context = context.clone();
    other_context[0] ^= 1;
    c.ensure(
        tagger.token(&key, &other_context, &value).ok() != Some(token.clone()),
        "the context separates tokens",
    )?;
    let mut other_value = value.clone();
    other_value[0] ^= 1;
    c.ensure(
        tagger.token(&key, &context, &other_value).ok() != Some(token.clone()),
        "the value separates tokens",
    )?;
    c.ensure(
        tagger.token(&key, &context, b"").is_ok(),
        "empty values have tokens",
    )?;
    for (index, vector) in vectors.iter().enumerate() {
        let token = c.ok(
            tagger.token(&vector.key, &vector.context, &vector.value),
            &format!("vector {index} tokenises"),
        )?;
        c.ensure(
            token == vector.token,
            &format!("vector {index} reproduces exactly"),
        )?;
    }
    Ok(())
}

/// A stream vector: encrypting `plaintext` in one update with `random` as
/// the random bytes gives exactly `encoding`.
#[derive(Debug, Clone)]
pub struct StreamVector {
    pub key: [u8; 32],
    pub context: Vec<u8>,
    pub random: Vec<u8>,
    pub plaintext: Vec<u8>,
    pub encoding: Vec<u8>,
}

fn stream_encrypt(
    cipher: &dyn StreamCipher,
    key: &[u8; 32],
    context: &[u8],
    plaintext: &[u8],
    rng: &mut dyn RandomSource,
) -> Result<(Vec<u8>, usize)> {
    let (mut writer, mut out) = cipher.encryptor(key, context, rng)?;
    let prefix = out.len();
    writer.update(plaintext, &mut out)?;
    writer.finish(&mut out)?;
    Ok((out, prefix))
}

/// Decrypts in `chunk`-sized updates.
fn stream_decrypt(
    cipher: &dyn StreamCipher,
    key: &[u8; 32],
    context: &[u8],
    encoding: &[u8],
    chunk: usize,
) -> Result<Vec<u8>> {
    stream_decrypt_bounded(cipher, key, context, encoding, chunk).0
}

/// [`stream_decrypt`], also reporting whether the reader ever held more than
/// its declared bound.
fn stream_decrypt_bounded(
    cipher: &dyn StreamCipher,
    key: &[u8; 32],
    context: &[u8],
    encoding: &[u8],
    chunk: usize,
) -> (Result<Vec<u8>>, bool) {
    let mut exceeded = false;
    let result = (|| {
        let mut reader = cipher.decryptor(key, context)?;
        let mut out = Vec::new();
        for part in encoding.chunks(chunk.max(1)) {
            reader.update(part, &mut out)?;
            exceeded |= reader.buffered() > cipher.max_buffered();
        }
        reader.finish()?;
        Ok(out)
    })();
    (result, exceeded)
}

pub fn check_stream_cipher(cipher: &dyn StreamCipher, vectors: &[StreamVector]) -> Outcome {
    let c = Checker::new(cipher)?;
    c.random_failure(
        cipher
            .encryptor(&[0; 32], b"", &mut FixedRandom(Vec::new()))
            .map(|_| ()),
        "starting a stream",
    )?;
    let mut inputs = Inputs(0x57ea_0000);
    let (key, context) = (inputs.key(), inputs.bytes(40));
    for size in [0, 1, 1000, 65_536, 65_537, 200_000, 1 << 20] {
        let plaintext = inputs.bytes(size);
        let (encoding, _) = c.ok(
            stream_encrypt(cipher, &key, &context, &plaintext, &mut inputs),
            "encrypt succeeds",
        )?;
        c.own_envelope(cipher, &encoding, "the encoding")?;
        for chunk in [1, 7, 4096, 100_000, usize::MAX] {
            if chunk < 4096 && size > 1000 {
                continue;
            }
            let (result, exceeded) =
                stream_decrypt_bounded(cipher, &key, &context, &encoding, chunk);
            c.ensure(!exceeded, "the reader stays within max_buffered")?;
            c.ensure(
                result.as_deref() == Ok(&plaintext[..]),
                &format!("{size} bytes round-trip in {chunk}-byte chunks"),
            )?;
        }
    }
    let plaintext = inputs.bytes(200_000);
    let (encoding, prefix) = c.ok(
        stream_encrypt(cipher, &key, &context, &plaintext, &mut inputs),
        "encrypt succeeds",
    )?;
    let (again, _) = c.ok(
        stream_encrypt(cipher, &key, &context, &plaintext, &mut inputs),
        "encrypt succeeds",
    )?;
    c.ensure(again != encoding, "equal inputs encrypt differently")?;
    let accepts =
        |value: &[u8]| verdict(&stream_decrypt(cipher, &key, &context, value, usize::MAX));
    let mut other_key = key;
    other_key[0] ^= 1;
    c.reject(
        verdict(&stream_decrypt(
            cipher,
            &other_key,
            &context,
            &encoding,
            usize::MAX,
        )),
        "another key is rejected",
    )?;
    let mut other_context = context.clone();
    other_context[0] ^= 1;
    c.reject(
        verdict(&stream_decrypt(
            cipher,
            &key,
            &other_context,
            &encoding,
            usize::MAX,
        )),
        "another context is rejected",
    )?;
    let small = inputs.bytes(1000);
    let (small, _) = c.ok(
        stream_encrypt(cipher, &key, &context, &small, &mut inputs),
        "encrypt succeeds",
    )?;
    c.tamper(&small, "the encoding", &accepts)?;
    // Rearranged, duplicated and dropped segments of the body.
    let body = encoding.len() - prefix;
    for length in [1, 17, 4096, 65_536] {
        if 2 * length > body {
            continue;
        }
        let (first, second) = (prefix, prefix + length);
        let mut swapped = encoding.clone();
        swapped[first..first + length].copy_from_slice(&encoding[second..second + length]);
        swapped[second..second + length].copy_from_slice(&encoding[first..first + length]);
        if swapped != encoding {
            c.reject(
                accepts(&swapped),
                &format!("swapped {length}-byte segments are rejected"),
            )?;
        }
        let duplicated = [
            &encoding[..second],
            &encoding[first..second],
            &encoding[second..],
        ]
        .concat();
        c.reject(
            accepts(&duplicated),
            &format!("a duplicated {length}-byte segment is rejected"),
        )?;
        let dropped = [&encoding[..first], &encoding[second..]].concat();
        c.reject(
            accepts(&dropped),
            &format!("a dropped {length}-byte segment is rejected"),
        )?;
        let tail = encoding.len() - length;
        c.reject(
            accepts(&encoding[..tail]),
            &format!("the last {length} bytes dropped are rejected"),
        )?;
    }
    for (index, vector) in vectors.iter().enumerate() {
        let mut random = FixedRandom(vector.random.clone());
        let (encoding, _) = c.ok(
            stream_encrypt(
                cipher,
                &vector.key,
                &vector.context,
                &vector.plaintext,
                &mut random,
            ),
            &format!("vector {index} encrypts"),
        )?;
        c.ensure(
            encoding == vector.encoding,
            &format!("vector {index} reproduces exactly"),
        )?;
        let decrypted = stream_decrypt(
            cipher,
            &vector.key,
            &vector.context,
            &vector.encoding,
            usize::MAX,
        );
        c.ensure(
            decrypted.as_deref() == Ok(&vector.plaintext[..]),
            &format!("vector {index} decrypts"),
        )?;
    }
    Ok(())
}

// Provider failures. Each `check_failing_*` takes an implementation whose
// provider fails on every operation (a test double of the real mechanism,
// such as one backed by an unavailable hardware key) and checks that every
// operation, and the registry and record helpers that route to it, report
// `Error::CryptoFailure`: never `InvalidCiphertext`, never a failed
// verification, never success.

const FAILING_EPOCH: &str = "00000000-0000-4000-8000-000000000000";
const FAILING_IDENTITY: crate::records::CellIdentity<'static> = crate::records::CellIdentity {
    application: "conformance",
    scope_id: "scope",
    identifier: "identifier",
    table_id: "table",
    row_id: "00000000-0000-4000-8000-000000000001",
    column_id: "column",
    epoch_id: FAILING_EPOCH,
};

/// An envelope of `implementation`'s mechanism with an arbitrary payload.
fn some_envelope(implementation: &dyn MechanismImpl) -> Vec<u8> {
    envelope::encode(implementation.mechanism(), &[0x5a; 96]).expect("validated mechanism")
}

fn failing_key_pair(implementation: &dyn MechanismImpl) -> KeyPair {
    KeyPair {
        public: PublicKey {
            mechanism: implementation.mechanism().into(),
            bytes: vec![9; 32],
        },
        secret: crate::agility::SecretKey::new(vec![9; 32]),
    }
}

pub fn check_failing_cell_cipher(failing: Arc<dyn CellCipher>) -> Outcome {
    let c = Checker::new(&*failing)?;
    let mut inputs = Inputs(1);
    let sealed = some_envelope(&*failing);
    c.provider_failure(
        failing.encrypt(&[1; 32], b"ctx", b"x", &mut inputs),
        "encrypt",
    )?;
    c.provider_failure(failing.decrypt(&[1; 32], b"ctx", &sealed), "decrypt")?;
    let mut registry = crate::registry::Registry::empty();
    c.ok(
        registry.insert_cell_cipher(failing.clone()),
        "the mechanism registers",
    )?;
    let stored = c.ok(
        crate::records::encode_cell_record(FAILING_EPOCH, &sealed),
        "the record frames",
    )?;
    c.provider_failure(
        crate::records::decrypt_cell_value(&registry, &[1; 32], &FAILING_IDENTITY, b"{}", &stored),
        "decrypting a stored cell",
    )
}

pub fn check_failing_key_wrapper(failing: Arc<dyn KeyWrapper>) -> Outcome {
    let c = Checker::new(&*failing)?;
    let mut inputs = Inputs(2);
    c.provider_failure(
        failing.wrap(&[1; 32], b"ctx", &[2; 32], &mut inputs),
        "wrap",
    )?;
    c.provider_failure(
        failing.unwrap(&[1; 32], b"ctx", &some_envelope(&*failing)),
        "unwrap",
    )
}

pub fn check_failing_key_sealer(failing: Arc<dyn KeySealer>) -> Outcome {
    let c = Checker::new(&*failing)?;
    let mut inputs = Inputs(3);
    let pair = failing_key_pair(&*failing);
    let sealed = some_envelope(&*failing);
    c.provider_failure(failing.derive_key_pair(&[1; 32]), "key derivation")?;
    c.provider_failure(
        failing.seal(&pair.public, b"ctx", &[2; 32], &mut inputs),
        "seal",
    )?;
    c.provider_failure(failing.open(&pair, b"ctx", &sealed), "open")?;
    let mut registry = crate::registry::Registry::empty();
    c.ok(
        registry.insert_key_sealer(failing.clone()),
        "the mechanism registers",
    )?;
    c.provider_failure(
        registry.open_sealed(std::slice::from_ref(&pair), b"ctx", &sealed),
        "opening through the registry",
    )
}

pub fn check_failing_signer(failing: Arc<dyn Signer>) -> Outcome {
    let c = Checker::new(&*failing)?;
    let pair = failing_key_pair(&*failing);
    let signature = some_envelope(&*failing);
    c.provider_failure(failing.derive_key_pair(&[1; 32]), "key derivation")?;
    c.provider_failure(failing.sign(&pair, b"record"), "sign")?;
    c.provider_failure(
        failing.verify(&pair.public, b"record", &signature),
        "verify",
    )?;
    let mut registry = crate::registry::Registry::empty();
    c.ok(
        registry.insert_signer(failing.clone()),
        "the mechanism registers",
    )?;
    c.provider_failure(
        registry.verify(&pair.public, b"record", &signature),
        "verifying through the registry",
    )
}

pub fn check_failing_equality_tagger(failing: Arc<dyn EqualityTagger>) -> Outcome {
    let c = Checker::new(&*failing)?;
    c.provider_failure(failing.token(&[1; 32], b"ctx", b"value"), "token")
}

pub fn check_failing_stream_cipher(failing: Arc<dyn StreamCipher>) -> Outcome {
    let c = Checker::new(&*failing)?;
    let mut inputs = Inputs(4);
    let encoding = some_envelope(&*failing);
    c.provider_failure(
        (|| {
            let (mut writer, mut out) = failing.encryptor(&[1; 32], b"ctx", &mut inputs)?;
            writer.update(b"value", &mut out)?;
            writer.finish(&mut out)
        })(),
        "encrypt",
    )?;
    c.provider_failure(
        stream_decrypt(&*failing, &[1; 32], b"ctx", &encoding, usize::MAX),
        "decrypt",
    )?;
    let mut registry = crate::registry::Registry::empty();
    c.ok(
        registry.insert_stream_cipher(failing.clone()),
        "the mechanism registers",
    )?;
    let stored = [
        c.ok(
            crate::records::stream_record_prefix(FAILING_EPOCH),
            "the record frames",
        )?,
        encoding,
    ]
    .concat();
    c.provider_failure(
        crate::records::decrypt_stream_value(
            &registry,
            &[1; 32],
            &FAILING_IDENTITY,
            b"{}",
            &stored,
        ),
        "decrypting a stored stream",
    )
}
