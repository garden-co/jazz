//! Mechanism agility: the built-ins pass the conformance kit with the spec
//! vectors, the kit catches broken mechanisms, and the registry routes,
//! selects and refuses as specified.

mod common;

use std::sync::Arc;

use common::{array, hex, vectors as v};
use jazz_e2ee::agility::{
    CellCipher, EqualityTagger, KeyPair, KeySealer, KeyWrapper, MechanismAllowList,
    MechanismConfig, MechanismId, MechanismImpl, PublicKey, Signer, StreamCipher, StreamReader,
    StreamWriter, is_custom_id,
};
use jazz_e2ee::conformance::{self, FixedRandom};
use jazz_e2ee::envelope::Mechanism;
use jazz_e2ee::mechanisms::{cell, equality, key_envelope, key_seal, signature, stream};
use jazz_e2ee::primitives::sign;
use jazz_e2ee::records::{self, CellIdentity};
use jazz_e2ee::registry::Registry;
use jazz_e2ee::{Error, RandomSource, Result, Zeroizing};

const STREAM_HELLO: &str = "4a453245011d6a617a7a2e73747265616d2e786368616368613230706f6c793133303500000001000102030405060708090a0b0c0d0e0f10111200000015f379dad1393c199f0448eb4c646e414886a6bfdae500000010a3a6e6facd9827eb4d173bb309517cb0";

#[test]
fn builtins_conform_with_the_spec_vectors() {
    let root: [u8; 32] = array(v::ROOT);
    let context = hex(v::CONTEXT);
    conformance::check_cell_cipher(
        &cell::Builtin,
        &[conformance::CellVector {
            key: root,
            context: context.clone(),
            random: hex(v::NONCE),
            plaintext: hex(v::PLAINTEXT),
            envelope: hex(v::CELL),
        }],
    )
    .unwrap();
    conformance::check_key_wrapper(
        &key_envelope::Builtin,
        &[conformance::WrapVector {
            wrapping_key: root,
            context: context.clone(),
            key: array(v::KEY),
            random: hex(v::NONCE),
            envelope: hex(v::WRAP),
        }],
    )
    .unwrap();
    conformance::check_key_sealer(
        &key_seal::Builtin,
        &[conformance::SealVector {
            recipient_ikm: hex(v::SEED),
            recipient_public: hex(v::PUBLIC_KEY),
            context: context.clone(),
            key: array(v::KEY),
            random: hex(v::EPHEMERAL),
            envelope: hex(v::SEALED),
        }],
    )
    .unwrap();
    // RFC 8032 test 1's key, signing this mechanism's framing.
    let seed = hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    let pair = sign::SigningKeyPair::from_seed(&seed.clone().try_into().unwrap());
    let record = b"account/epoch/membership revision".to_vec();
    let header = signature::MECHANISM.header().unwrap();
    let raw = sign::sign(&pair.secret_key, &[header.as_slice(), &record].concat()).unwrap();
    conformance::check_signer(
        &signature::Builtin,
        &[conformance::SignatureVector {
            seed,
            public: pair.public_key.to_vec(),
            record,
            signature: [header.as_slice(), &raw].concat(),
            deterministic: true,
        }],
    )
    .unwrap();
    conformance::check_equality_tagger(
        &equality::Builtin,
        &[conformance::EqualityVector {
            key: root,
            context: context.clone(),
            value: hex(v::PLAINTEXT),
            token: hex(v::EQUALITY),
        }],
    )
    .unwrap();
    conformance::check_stream_cipher(
        &stream::Builtin,
        &[conformance::StreamVector {
            key: root,
            context,
            random: hex(v::NONCE)[..19].to_vec(),
            plaintext: hex(v::PLAINTEXT),
            encoding: hex(STREAM_HELLO),
        }],
    )
    .unwrap();
}

/// A cell cipher that forgets to bind its context.
struct UnboundCell;

impl MechanismImpl for UnboundCell {
    fn mechanism(&self) -> Mechanism<'static> {
        Mechanism::new("org.example.unbound", 1)
    }
}

impl CellCipher for UnboundCell {
    fn encrypt(
        &self,
        key: &[u8; 32],
        _context: &[u8],
        plaintext: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        let inner = cell::encrypt(key, b"", plaintext, rng)?;
        jazz_e2ee::envelope::encode(self.mechanism(), &inner)
    }

    fn decrypt(
        &self,
        key: &[u8; 32],
        _context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        let inner = jazz_e2ee::envelope::decode(self.mechanism(), envelope)?;
        cell::decrypt(key, b"", inner)
    }
}

/// A stream cipher whose reader reports success without its end.
struct TruncatableStream;

struct Lenient(Box<dyn StreamReader>);

impl StreamReader for Lenient {
    fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        self.0.update(input, out)
    }

    fn buffered(&self) -> usize {
        self.0.buffered()
    }

    fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

impl MechanismImpl for TruncatableStream {
    fn mechanism(&self) -> Mechanism<'static> {
        stream::MECHANISM
    }
}

impl StreamCipher for TruncatableStream {
    fn encryptor(
        &self,
        key: &[u8; 32],
        context: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<(Box<dyn StreamWriter>, Vec<u8>)> {
        stream::Builtin.encryptor(key, context, rng)
    }

    fn decryptor(&self, key: &[u8; 32], context: &[u8]) -> Result<Box<dyn StreamReader>> {
        Ok(Box::new(Lenient(stream::Builtin.decryptor(key, context)?)))
    }

    fn max_buffered(&self) -> usize {
        stream::Builtin.max_buffered()
    }
}

/// A sealer that ignores which mechanism a recipient key belongs to.
struct Promiscuous;

impl MechanismImpl for Promiscuous {
    fn mechanism(&self) -> Mechanism<'static> {
        key_seal::MECHANISM
    }
}

impl KeySealer for Promiscuous {
    fn derive_key_pair(&self, ikm: &[u8]) -> Result<KeyPair> {
        key_seal::Builtin.derive_key_pair(ikm)
    }

    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        key_seal::seal(
            recipient.bytes[..]
                .try_into()
                .map_err(|_| Error::InvalidInput)?,
            context,
            key,
            rng,
        )
    }

    fn open(
        &self,
        recipient: &KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        key_seal::Builtin.open(recipient, context, envelope)
    }
}

#[test]
fn the_kit_catches_broken_mechanisms() {
    let failure = conformance::check_cell_cipher(&UnboundCell, &[]).unwrap_err();
    assert_eq!(failure.check, "another context is rejected");
    let failure = conformance::check_stream_cipher(&TruncatableStream, &[]).unwrap_err();
    assert!(failure.check.contains("rejected"), "{failure}");
    let failure = conformance::check_key_sealer(&Promiscuous, &[]).unwrap_err();
    assert_eq!(
        failure.check,
        "a public key of another mechanism is refused as unsupported"
    );
    // A vector that does not reproduce.
    let failure = conformance::check_equality_tagger(
        &equality::Builtin,
        &[conformance::EqualityVector {
            key: [0; 32],
            context: vec![],
            value: vec![],
            token: vec![1, 2, 3],
        }],
    )
    .unwrap_err();
    assert_eq!(failure.check, "vector 0 reproduces exactly");
}

#[test]
fn custom_ids_are_namespaced() {
    for id in ["com.example.kem", "org.example.pq-kem.v2", "io.x"] {
        assert!(is_custom_id(id), "{id}");
    }
    for id in [
        "kem",
        "jazz.custom",
        "Com.example",
        "com..example",
        "com.example.",
        ".com",
        "com.-x",
        &format!("com.{}", "a".repeat(61)),
    ] {
        assert!(!is_custom_id(id), "{id}");
    }
    let mut registry = Registry::builtin();
    assert_eq!(
        registry.register_cell_cipher(Arc::new(cell::Builtin)),
        Err(Error::InvalidInput)
    );
    registry
        .register_cell_cipher(Arc::new(UnboundCell))
        .unwrap();
    assert_eq!(
        registry.register_cell_cipher(Arc::new(UnboundCell)),
        Err(Error::InvalidInput)
    );
}

const IDENTITY: CellIdentity<'static> = CellIdentity {
    application: "app",
    scope_id: "scope",
    identifier: "space",
    table_id: "table",
    row_id: "00000000-0000-4000-8000-000000000001",
    column_id: "column",
    epoch_id: "00000000-0000-4000-8000-000000000002",
};

#[test]
fn readers_route_by_envelope_and_writers_by_config() {
    let mut registry = Registry::builtin();
    registry
        .register_cell_cipher(Arc::new(UnboundCell))
        .unwrap();
    let key = [3u8; 32];
    let mut rng = FixedRandom(vec![9; 1024]);
    let config = MechanismConfig {
        cell: MechanismId::new("org.example.unbound", 1),
        ..MechanismConfig::default()
    };
    let suite = registry.select(&config).unwrap();
    assert_eq!(suite.cell.mechanism().id, "org.example.unbound");
    let custom =
        records::encrypt_cell_value(suite.cell, &key, &IDENTITY, b"{}", b"value", &mut rng)
            .unwrap();
    let builtin =
        records::encrypt_cell_value(&cell::Builtin, &key, &IDENTITY, b"{}", b"value", &mut rng)
            .unwrap();
    for stored in [&custom, &builtin] {
        assert_eq!(
            *records::decrypt_cell_value(&registry, &key, &IDENTITY, b"{}", stored).unwrap(),
            b"value"
        );
    }
    // A reader without the mechanism reports an unsupported format.
    assert_eq!(
        records::decrypt_cell_value(&Registry::builtin(), &key, &IDENTITY, b"{}", &custom)
            .map(|_| ()),
        Err(Error::UnsupportedFormat)
    );
    // A configuration naming an unregistered mechanism is refused.
    let unknown = MechanismConfig {
        stream: MechanismId::new("org.example.missing", 1),
        ..MechanismConfig::default()
    };
    assert!(matches!(
        registry.select(&unknown),
        Err(Error::UnknownMechanism)
    ));
    assert!(
        Registry::empty()
            .select(&MechanismConfig::default())
            .is_err()
    );
    assert!(
        Registry::builtin()
            .select(&MechanismConfig::default())
            .is_ok()
    );
}

/// The application's allow-list restricts readers and writers: a
/// registered mechanism that is not listed is an unsupported format to
/// readers and cannot be selected, and listing an unregistered one is
/// refused.
#[test]
fn the_allow_list_retires_mechanisms() {
    let mut registry = Registry::builtin();
    registry
        .register_cell_cipher(Arc::new(UnboundCell))
        .unwrap();
    let key = [3u8; 32];
    let mut rng = FixedRandom(vec![9; 1024]);
    let custom =
        records::encrypt_cell_value(&UnboundCell, &key, &IDENTITY, b"{}", b"value", &mut rng)
            .unwrap();
    let builtins = registry.allowing(&MechanismAllowList::default()).unwrap();
    assert_eq!(
        records::decrypt_cell_value(&builtins, &key, &IDENTITY, b"{}", &custom).map(|_| ()),
        Err(Error::UnsupportedFormat)
    );
    let config = MechanismConfig {
        cell: MechanismId::new("org.example.unbound", 1),
        ..MechanismConfig::default()
    };
    assert!(matches!(
        builtins.select(&config),
        Err(Error::UnknownMechanism)
    ));
    let both = MechanismAllowList {
        cell: vec![
            cell::MECHANISM.into(),
            MechanismId::new("org.example.unbound", 1),
        ],
        ..MechanismAllowList::default()
    };
    let listed = registry.allowing(&both).unwrap();
    assert!(records::decrypt_cell_value(&listed, &key, &IDENTITY, b"{}", &custom).is_ok());
    let missing = MechanismAllowList {
        stream: vec![MechanismId::new("org.example.missing", 1)],
        ..MechanismAllowList::default()
    };
    assert!(matches!(
        registry.allowing(&missing),
        Err(Error::UnknownMechanism)
    ));
}

/// Opening a sealed key tries every key pair of the envelope's mechanism,
/// not only the first.
#[test]
fn open_sealed_tries_every_key_of_the_mechanism() {
    let registry = Registry::builtin();
    let mut rng = FixedRandom((0..=255).cycle().take(4096).collect());
    let old = key_seal::Builtin.generate_key_pair(&mut rng).unwrap();
    let new = key_seal::Builtin.generate_key_pair(&mut rng).unwrap();
    let sealed = key_seal::Builtin
        .seal(&new.public, b"context", &[5; 32], &mut rng)
        .unwrap();
    let opened = registry
        .open_sealed(&[old.clone(), new.clone()], b"context", &sealed)
        .unwrap();
    assert_eq!(*opened, [5; 32]);
    assert_eq!(
        registry
            .open_sealed(core::slice::from_ref(&old), b"context", &sealed)
            .map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
}

#[test]
fn public_keys_carry_their_mechanism() {
    let registry = Registry::builtin();
    let suite = registry.select(&MechanismConfig::default()).unwrap();
    let mut rng = FixedRandom(vec![5; 1024]);
    let device = suite.key_seal.generate_key_pair(&mut rng).unwrap();
    let encoded = device.public.encode().unwrap();
    assert_eq!(
        encoded[..key_seal::MECHANISM.header().unwrap().len()],
        key_seal::MECHANISM.header().unwrap()[..]
    );
    assert_eq!(PublicKey::decode(&encoded).unwrap(), device.public);
    assert_eq!(
        PublicKey::decode(&encoded[..5]),
        Err(Error::InvalidCiphertext)
    );
    // Sealing goes only to an advertised key of the selected mechanism.
    let other = PublicKey {
        mechanism: MechanismId::new("com.example.kem", 1),
        bytes: device.public.bytes.clone(),
    };
    let key = [8u8; 32];
    assert_eq!(
        suite.seal_to(std::slice::from_ref(&other), b"ctx", &key, &mut rng),
        Err(Error::RecipientUnsupported)
    );
    let sealed = suite
        .seal_to(&[other, device.public.clone()], b"ctx", &key, &mut rng)
        .unwrap();
    assert_eq!(
        *registry
            .open_sealed(std::slice::from_ref(&device), b"ctx", &sealed)
            .unwrap(),
        key
    );
    assert_eq!(
        registry.open_sealed(&[], b"ctx", &sealed).map(|_| ()),
        Err(Error::NoKeyForMechanism)
    );
    // Signatures verify through the registry by the key's mechanism.
    let signer = suite.signature.generate_key_pair(&mut rng).unwrap();
    let signed = suite.signature.sign(&signer, b"record").unwrap();
    assert_eq!(
        registry.verify(&signer.public, b"record", &signed),
        Ok(true)
    );
    assert_eq!(
        registry.verify(&signer.public, b"other", &signed),
        Ok(false)
    );
    let relabelled = PublicKey {
        mechanism: MechanismId::new("com.example.sig", 1),
        bytes: signer.public.bytes.clone(),
    };
    assert_eq!(
        registry.verify(&relabelled, b"record", &signed),
        Err(Error::UnsupportedFormat)
    );
    assert_eq!(
        signature::Builtin.sign(
            &KeyPair {
                public: relabelled,
                secret: signer.secret.clone()
            },
            b"record"
        ),
        Err(Error::RecipientUnsupported)
    );
}

/// Every operation fails in the provider, as a mechanism backed by an
/// unavailable hardware key would. `misreport` makes it disguise the failure
/// as invalid ciphertext and a failed verification instead.
struct Broken {
    misreport: bool,
    /// Only verification disguises the failure, as a failed verification.
    quiet_verify: bool,
}

impl Broken {
    fn fail<T>(&self) -> Result<T> {
        Err(if self.misreport {
            Error::InvalidCiphertext
        } else {
            Error::CryptoFailure
        })
    }
}

impl MechanismImpl for Broken {
    fn mechanism(&self) -> Mechanism<'static> {
        Mechanism::new("org.example.broken", 1)
    }
}

impl CellCipher for Broken {
    fn encrypt(
        &self,
        _: &[u8; 32],
        _: &[u8],
        _: &[u8],
        _: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        self.fail()
    }

    fn decrypt(&self, _: &[u8; 32], _: &[u8], _: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        self.fail()
    }
}

impl KeyWrapper for Broken {
    fn wrap(
        &self,
        _: &[u8; 32],
        _: &[u8],
        _: &[u8; 32],
        _: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        self.fail()
    }

    fn unwrap(&self, _: &[u8; 32], _: &[u8], _: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
        self.fail()
    }
}

impl KeySealer for Broken {
    fn derive_key_pair(&self, _: &[u8]) -> Result<KeyPair> {
        self.fail()
    }

    fn seal(
        &self,
        _: &PublicKey,
        _: &[u8],
        _: &[u8; 32],
        _: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        self.fail()
    }

    fn open(&self, _: &KeyPair, _: &[u8], _: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
        self.fail()
    }
}

impl Signer for Broken {
    fn derive_key_pair(&self, _: &[u8]) -> Result<KeyPair> {
        self.fail()
    }

    fn sign(&self, _: &KeyPair, _: &[u8]) -> Result<Vec<u8>> {
        self.fail()
    }

    fn verify(&self, _: &PublicKey, _: &[u8], _: &[u8]) -> Result<bool> {
        if self.misreport || self.quiet_verify {
            Ok(false)
        } else {
            Err(Error::CryptoFailure)
        }
    }
}

impl EqualityTagger for Broken {
    fn token(&self, _: &[u8; 32], _: &[u8], _: &[u8]) -> Result<Vec<u8>> {
        self.fail()
    }
}

struct BrokenWriter;

impl StreamWriter for BrokenWriter {
    fn update(&mut self, _: &[u8], _: &mut Vec<u8>) -> Result<()> {
        Err(Error::CryptoFailure)
    }

    fn finish(self: Box<Self>, _: &mut Vec<u8>) -> Result<()> {
        Err(Error::CryptoFailure)
    }
}

impl StreamCipher for Broken {
    fn encryptor(
        &self,
        _: &[u8; 32],
        _: &[u8],
        _: &mut dyn RandomSource,
    ) -> Result<(Box<dyn StreamWriter>, Vec<u8>)> {
        Ok((Box::new(BrokenWriter), self.mechanism().header()?))
    }

    fn decryptor(&self, _: &[u8; 32], _: &[u8]) -> Result<Box<dyn StreamReader>> {
        self.fail()
    }

    fn max_buffered(&self) -> usize {
        0
    }
}

#[test]
fn provider_failures_fail_loudly() {
    let broken = Arc::new(Broken {
        misreport: false,
        quiet_verify: false,
    });
    conformance::check_failing_cell_cipher(broken.clone()).unwrap();
    conformance::check_failing_key_wrapper(broken.clone()).unwrap();
    conformance::check_failing_key_sealer(broken.clone()).unwrap();
    conformance::check_failing_signer(broken.clone()).unwrap();
    conformance::check_failing_equality_tagger(broken.clone()).unwrap();
    conformance::check_failing_stream_cipher(broken).unwrap();

    // A mechanism that disguises its provider failure is caught.
    let disguised = Arc::new(Broken {
        misreport: true,
        quiet_verify: false,
    });
    let failure = conformance::check_failing_cell_cipher(disguised.clone()).unwrap_err();
    assert_eq!(
        failure.check,
        "encrypt reports a provider failure as CryptoFailure, not InvalidCiphertext"
    );
    let failure = conformance::check_failing_signer(Arc::new(Broken {
        misreport: false,
        quiet_verify: true,
    }))
    .unwrap_err();
    assert_eq!(failure.check, "verify fails when the provider fails");
    let failure = conformance::check_failing_key_sealer(disguised).unwrap_err();
    assert!(failure.check.contains("not InvalidCiphertext"), "{failure}");

    // And the built-in and correct mechanisms reject bad input as invalid,
    // not as a provider failure: covered by the conformance checks above.
}

#[test]
fn record_helpers_propagate_provider_failures() {
    let mut registry = Registry::builtin();
    registry
        .register_cell_cipher(Arc::new(Broken {
            misreport: false,
            quiet_verify: false,
        }))
        .unwrap();
    registry
        .register_signer(Arc::new(Broken {
            misreport: false,
            quiet_verify: false,
        }))
        .unwrap();
    let inner =
        jazz_e2ee::envelope::encode(Mechanism::new("org.example.broken", 1), &[0; 64]).unwrap();
    let stored = records::encode_cell_record(IDENTITY.epoch_id, &inner).unwrap();
    assert_eq!(
        records::decrypt_cell_value(&registry, &[1; 32], &IDENTITY, b"{}", &stored).map(|_| ()),
        Err(Error::CryptoFailure)
    );
    let public = PublicKey {
        mechanism: MechanismId::new("org.example.broken", 1),
        bytes: vec![0; 32],
    };
    assert_eq!(
        registry.verify(&public, b"record", &inner),
        Err(Error::CryptoFailure)
    );
    // A genuinely corrupt record is still invalid ciphertext.
    let mut rng = FixedRandom(vec![4; 64]);
    let mut good =
        records::encrypt_cell_value(&cell::Builtin, &[1; 32], &IDENTITY, b"{}", b"v", &mut rng)
            .unwrap();
    let last = good.len() - 1;
    good[last] ^= 1;
    assert_eq!(
        records::decrypt_cell_value(&registry, &[1; 32], &IDENTITY, b"{}", &good).map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
}
