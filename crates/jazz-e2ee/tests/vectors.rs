//! Every stored vector from the format specs reproduces byte for byte.

mod common;

use aead_stream::aead::Payload;
use common::{FixedRandom, array, hex, to_hex, vectors as v};
use jazz_e2ee::context::{CryptoContext, frame_record};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::{cell, equality, key_envelope, key_seal, signature, stream};
use jazz_e2ee::primitives::hpke::{self, HpkeKeyPair};
use jazz_e2ee::primitives::{hash, sign};
use jazz_e2ee::records::{self, CellIdentity};
use jazz_e2ee::{Error, Zeroizing};

fn root() -> [u8; 32] {
    array(v::ROOT)
}

fn context() -> Vec<u8> {
    hex(v::CONTEXT)
}

fn device() -> HpkeKeyPair {
    HpkeKeyPair {
        public_key: array(v::PUBLIC_KEY),
        secret_key: Zeroizing::new(array(v::PRIVATE_KEY)),
    }
}

#[test]
fn envelope_fixture() {
    let test = Mechanism::new("test", 1);
    let encoded = envelope::encode(test, &[0xaa]).unwrap();
    assert_eq!(to_hex(&encoded), "4a45324501047465737400000001aa");
    assert_eq!(envelope::decode(test, &encoded).unwrap(), [0xaa]);
    assert_eq!(
        envelope::decode(Mechanism::new("test", 2), &encoded),
        Err(Error::UnsupportedFormat)
    );
    assert_eq!(
        envelope::decode(Mechanism::new("tess", 1), &encoded),
        Err(Error::UnsupportedFormat)
    );
    for truncated in 0..encoded.len() - 1 {
        assert_eq!(
            envelope::decode(test, &encoded[..truncated]),
            Err(Error::InvalidCiphertext),
            "truncated to {truncated}"
        );
    }
    let mut format = encoded.clone();
    format[4] = 2;
    assert_eq!(
        envelope::decode(test, &format),
        Err(Error::InvalidCiphertext)
    );
    for invalid in ["", "Test", "a_b", &"a".repeat(65)] {
        assert_eq!(
            envelope::encode(Mechanism::new(invalid, 1), &[]),
            Err(Error::InvalidInput)
        );
    }
    assert_eq!(
        envelope::encode(Mechanism::new("test", 0), &[]),
        Err(Error::InvalidInput)
    );
    assert!(envelope::encode(Mechanism::new(&"a".repeat(64), u32::MAX), &[]).is_ok());
}

#[test]
fn context_fixture() {
    let encoded = CryptoContext {
        application: "a",
        policy: "p",
        scope: "s",
        identifier: "i",
        table: "t",
        row: "r",
        column: "c",
        epoch: "e",
        recipient: "d",
    }
    .encode()
    .unwrap();
    assert_eq!(encoded, context());
    let required = CryptoContext {
        application: "a",
        policy: "p",
        scope: "s",
        identifier: "i",
        epoch: "e",
        ..CryptoContext::default()
    };
    assert!(required.encode().is_ok());
    for missing in 0..5 {
        let mut context = required;
        match missing {
            0 => context.application = "",
            1 => context.policy = "",
            2 => context.scope = "",
            3 => context.identifier = "",
            _ => context.epoch = "",
        }
        assert_eq!(context.encode(), Err(Error::InvalidInput));
    }
    let long = "x".repeat(65_535);
    assert!(
        CryptoContext {
            table: &long,
            ..required
        }
        .encode()
        .is_ok()
    );
    let too_long = "x".repeat(65_536);
    assert_eq!(
        CryptoContext {
            table: &too_long,
            ..required
        }
        .encode(),
        Err(Error::InvalidInput)
    );
}

#[test]
fn cell_vector() {
    let header = cell::MECHANISM.header().unwrap();
    let aad = [header.as_slice(), &context()].concat();
    assert_eq!(to_hex(&aad), v::CELL_AAD);
    let derived = hash::keyed_hash_parts(&root(), &[b"jazz.e2ee.cell-key.v1\0", &aad]);
    assert_eq!(to_hex(&*derived), v::CELL_DERIVED);
    let mut rng = FixedRandom(hex(v::NONCE));
    let encrypted = cell::encrypt(&root(), &context(), &hex(v::PLAINTEXT), &mut rng).unwrap();
    assert_eq!(to_hex(&encrypted), v::CELL);
    let decrypted = cell::decrypt(&root(), &context(), &hex(v::CELL)).unwrap();
    assert_eq!(*decrypted, hex(v::PLAINTEXT));
}

#[test]
fn wrap_vector() {
    let header = key_envelope::MECHANISM.header().unwrap();
    let aad = [header.as_slice(), &[1], &context()].concat();
    assert_eq!(to_hex(&aad), v::WRAP_AAD);
    let derived = hash::keyed_hash_parts(&root(), &[b"jazz.e2ee.wrap-key.v1\0", &aad]);
    assert_eq!(to_hex(&*derived), v::WRAP_DERIVED);
    let mut rng = FixedRandom(hex(v::NONCE));
    let wrapped = key_envelope::wrap(&root(), &context(), &array(v::KEY), &mut rng).unwrap();
    assert_eq!(to_hex(&wrapped), v::WRAP);
    let unwrapped = key_envelope::unwrap(&root(), &context(), &hex(v::WRAP)).unwrap();
    assert_eq!(to_hex(&*unwrapped), v::KEY);
}

#[test]
fn sealed_vector() {
    let recipient = HpkeKeyPair::derive(&hex(v::SEED));
    assert_eq!(to_hex(&recipient.public_key), v::PUBLIC_KEY);
    // `hpke` stores X25519 secret keys clamped; the fixture's unclamped key
    // is the same scalar.
    assert_eq!(*recipient.secret_key, clamp(array(v::PRIVATE_KEY)));
    assert_eq!(
        to_hex(&hpke::public_key(&array(v::PRIVATE_KEY)).unwrap()),
        v::PUBLIC_KEY
    );
    let sealed = key_seal::seal(
        &array(v::PUBLIC_KEY),
        &context(),
        &array(v::KEY),
        &mut FixedRandom(hex(v::EPHEMERAL)),
    )
    .unwrap();
    assert_eq!(to_hex(&sealed), v::SEALED);
    let key = key_seal::open(&device(), &context(), &hex(v::SEALED)).unwrap();
    assert_eq!(to_hex(&*key), v::KEY);
    // Another recipient cannot open it.
    let other = HpkeKeyPair::derive(&[1; 32]);
    assert_eq!(
        key_seal::open(&other, &context(), &sealed).map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
}

fn clamp(mut scalar: [u8; 32]) -> [u8; 32] {
    scalar[0] &= 248;
    scalar[31] &= 127;
    scalar[31] |= 64;
    scalar
}

/// RFC 9180 appendix A.2.1: DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
/// ChaCha20Poly1305, base mode, sequence number 0 (the single-shot message).
#[test]
fn rfc9180_vector() {
    let ephemeral = HpkeKeyPair::derive(&hex(
        "909a9b35d3dc4713a5e72a4da274b55d3d3821a37e5d099e74a647db583a904b",
    ));
    assert_eq!(
        to_hex(&ephemeral.public_key),
        "1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a"
    );
    assert_eq!(
        *ephemeral.secret_key,
        clamp(array(
            "f4ec9b33b792c372c1d2c2063507b684ef925b8c75a42dbcbf57d63ccd381600"
        ))
    );
    let recipient = HpkeKeyPair::derive(&hex(
        "1ac01f181fdf9f352797655161c58b75c656a6cc2716dcb66372da835542e1df",
    ));
    assert_eq!(
        to_hex(&recipient.public_key),
        "4310ee97d88cc1f088a5576c77ab0cf5c3ac797f3d95139c6c84b5429c59662a"
    );
    let rfc_secret: [u8; 32] =
        array("8057991eef8f1f1af18f4a9491d16a1ce333f695d4db8e38da75975c4478e0fb");
    assert_eq!(*recipient.secret_key, clamp(rfc_secret));
    assert_eq!(hpke::public_key(&rfc_secret).unwrap(), recipient.public_key);
    let info = hex("4f6465206f6e2061204772656369616e2055726e");
    let aad = hex("436f756e742d30");
    let plaintext = hex("4265617574792069732074727574682c20747275746820626561757479");
    let (encapsulated, ciphertext) = hpke::seal(
        &recipient.public_key,
        &info,
        &aad,
        &plaintext,
        &mut FixedRandom(hex(
            "909a9b35d3dc4713a5e72a4da274b55d3d3821a37e5d099e74a647db583a904b",
        )),
    )
    .unwrap();
    assert_eq!(
        to_hex(&encapsulated),
        "1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a"
    );
    assert_eq!(
        to_hex(&ciphertext),
        "1c5250d8034ec2b784ba2cfd69dbdb8af406cfe3ff938e131f0def8c8b60b4db\
         21993c62ce81883d2dd1b51a28"
    );
    let opened = hpke::open(
        &recipient.secret_key,
        &encapsulated,
        &info,
        &aad,
        &ciphertext,
    )
    .unwrap();
    assert_eq!(*opened, plaintext);
    let mut wrong_aad = aad.clone();
    wrong_aad[0] ^= 1;
    assert_eq!(
        hpke::open(
            &recipient.secret_key,
            &encapsulated,
            &info,
            &wrong_aad,
            &ciphertext
        )
        .map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
    assert_eq!(
        hpke::open(
            &recipient.secret_key,
            &encapsulated,
            b"other info",
            &aad,
            &ciphertext
        )
        .map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
    // A small-order recipient key is refused as invalid input, and a failing random source
    // produces no output.
    assert_eq!(
        hpke::seal(
            &[0; 32],
            &info,
            &aad,
            &plaintext,
            &mut FixedRandom(vec![7; 32])
        )
        .map(|_| ()),
        Err(Error::InvalidInput)
    );
    assert_eq!(
        hpke::seal(
            &recipient.public_key,
            &info,
            &aad,
            &plaintext,
            &mut FixedRandom(vec![])
        )
        .map(|_| ()),
        Err(Error::RandomUnavailable)
    );
}

#[test]
fn equality_vector() {
    let header = equality::MECHANISM.header().unwrap();
    let input = frame_record(&[&header, &context()]).unwrap();
    assert_eq!(to_hex(&input), v::EQUALITY_CONTEXT);
    assert_eq!(
        to_hex(&*hash::keyed_hash_parts(
            &root(),
            &[b"jazz.e2ee.equality-key.v1\0", &input]
        )),
        v::EQUALITY_DERIVED
    );
    let token = equality::token(&root(), &context(), &hex(v::PLAINTEXT)).unwrap();
    assert_eq!(to_hex(&token), v::EQUALITY);
    // Deterministic, and separated by key, context and value.
    assert_eq!(
        equality::token(&root(), &context(), &hex(v::PLAINTEXT)).unwrap(),
        token
    );
    let mut key = root();
    key[0] ^= 1;
    assert_ne!(
        equality::token(&key, &context(), &hex(v::PLAINTEXT)).unwrap(),
        token
    );
    let mut changed = context();
    changed[0] ^= 1;
    assert_ne!(
        equality::token(&root(), &changed, &hex(v::PLAINTEXT)).unwrap(),
        token
    );
    assert_ne!(
        equality::token(&root(), &context(), b"hellp").unwrap(),
        token
    );
}

#[test]
fn stream_vector() {
    let header = stream::MECHANISM.header().unwrap();
    let aad = frame_record(&[&header, &context()]).unwrap();
    assert_eq!(
        to_hex(&*hash::keyed_hash_parts(
            &root(),
            &[b"jazz.e2ee.stream-key.v1\0", &aad]
        )),
        v::STREAM_DERIVED
    );
    let encoded = hex(v::STREAM);
    let decrypted = stream::decrypt(&root(), &context(), &encoded).unwrap();
    assert_eq!(*decrypted, b"hellohello");
    // The fixture's records, rebuilt with RustCrypto's STREAM directly.
    let prefix: [u8; 19] = encoded[header.len()..header.len() + 19].try_into().unwrap();
    assert_eq!(prefix, hex(v::NONCE)[..19]);
    let derived = hash::keyed_hash_parts(&root(), &[b"jazz.e2ee.stream-key.v1\0", &aad]);
    let mut rebuilt = encoded[..header.len() + 19].to_vec();
    let mut writer = aead_stream::EncryptorBE32::<chacha20poly1305::XChaCha20Poly1305>::new(
        (&*derived).into(),
        (&prefix).into(),
    );
    for message in [b"hello", b"hello"] {
        let record = writer
            .encrypt_next(Payload {
                msg: &message[..],
                aad: &aad,
            })
            .unwrap();
        rebuilt.extend_from_slice(&(record.len() as u32).to_be_bytes());
        rebuilt.extend_from_slice(&record);
    }
    let last = writer
        .encrypt_last(Payload {
            msg: &[],
            aad: &aad,
        })
        .unwrap();
    rebuilt.extend_from_slice(&(last.len() as u32).to_be_bytes());
    rebuilt.extend_from_slice(&last);
    assert_eq!(to_hex(&rebuilt), v::STREAM);
    // The crate's writer, given the fixture's nonce prefix, reproduces the
    // header and the first record, then coalesces the rest.
    let (mut encryptor, mut out) =
        stream::StreamEncryptor::new(&root(), &context(), &mut FixedRandom(prefix.to_vec()))
            .unwrap();
    encryptor.update(b"hello", &mut out).unwrap();
    encryptor.finish(&mut out).unwrap();
    let first_record = header.len() + 19 + 4 + 5 + 16;
    assert_eq!(out[..first_record], encoded[..first_record]);
    assert_eq!(
        *stream::decrypt(&root(), &context(), &out).unwrap(),
        b"hello"
    );
    assert_eq!(to_hex(&out), STREAM_HELLO);
}

/// The crate's writer output for `hello`, pinned in `E2EE_LARGE_VALUE_FORMAT.md`.
const STREAM_HELLO: &str = "4a453245011d6a617a7a2e73747265616d2e786368616368613230706f6c793133303500000001000102030405060708090a0b0c0d0e0f10111200000015af44ecab95efa19a05ffd9d05ad84a354cffbed12a00000010bb21e362d42391ffb6905ea1550acd09";

#[test]
fn every_byte_of_each_vector_is_authenticated() {
    let device = device();
    type Opens<'a> = &'a dyn Fn(&[u8]) -> bool;
    let cases: [(&str, Opens); 5] = [
        (v::CELL, &|value| {
            cell::decrypt(&root(), &context(), value).is_ok()
        }),
        (v::WRAP, &|value| {
            key_envelope::unwrap(&root(), &context(), value).is_ok()
        }),
        (v::SEALED, &|value| {
            key_seal::open(&device, &context(), value).is_ok()
        }),
        (v::STREAM, &|value| {
            stream::decrypt(&root(), &context(), value).is_ok()
        }),
        (v::EQUALITY, &|value| {
            value == equality::token(&root(), &context(), &hex(v::PLAINTEXT)).unwrap()
        }),
    ];
    for (vector, opens) in cases {
        let original = hex(vector);
        assert!(opens(&original));
        for index in 0..original.len() {
            for bit in 0..8 {
                let mut corrupted = original.clone();
                corrupted[index] ^= 1 << bit;
                assert!(
                    !opens(&corrupted),
                    "accepted a flipped bit at {index}:{bit}"
                );
            }
        }
        assert!(!opens(&original[..original.len() - 1]));
        assert!(!opens(&[original.as_slice(), &[0]].concat()));
    }
}

#[test]
fn wrong_key_or_context_fails() {
    let mut key = root();
    key[31] ^= 0x80;
    let mut changed = context();
    *changed.last_mut().unwrap() ^= 1;
    for (key, context) in [(key, context()), (root(), changed)] {
        assert_eq!(
            cell::decrypt(&key, &context, &hex(v::CELL)),
            Err(Error::InvalidCiphertext)
        );
        assert_eq!(
            key_envelope::unwrap(&key, &context, &hex(v::WRAP)).map(|_| ()),
            Err(Error::InvalidCiphertext)
        );
        assert_eq!(
            stream::decrypt(&key, &context, &hex(v::STREAM)).map(|_| ()),
            Err(Error::InvalidCiphertext)
        );
    }
}

/// RFC 8032 test 1, and the signature envelope framing the removed Node test
/// checked against OpenSSL.
#[test]
fn rfc8032_signature() {
    let seed = array::<32>("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    let pair = sign::SigningKeyPair::from_seed(&seed);
    assert_eq!(
        to_hex(&pair.public_key),
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    );
    assert_eq!(
        to_hex(&sign::sign(&pair.secret_key, b"").unwrap()),
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155\
         5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    );
    let header = hex("4a45324501106a617a7a2e736f6469756d2e7369676e00000001");
    assert_eq!(signature::MECHANISM.header().unwrap(), header);
    let record = b"account/epoch/membership revision";
    let signed = signature::sign(&pair.secret_key, record).unwrap();
    let expected = sign::sign(&pair.secret_key, &[header.as_slice(), record].concat()).unwrap();
    assert_eq!(signed, [header.as_slice(), &expected].concat());
    assert!(signature::verify(&pair.public_key, record, &signed));
    for index in 0..signed.len() {
        let mut changed = signed.clone();
        changed[index] ^= 1;
        assert!(!signature::verify(&pair.public_key, record, &changed));
    }
    assert!(!signature::verify(&pair.public_key, b"", &signed));
    assert!(!signature::verify(&[0; 32], record, &signed));
    assert!(!signature::verify(&pair.public_key, record, &signed[1..]));
    assert!(!signature::verify(&pair.public_key[1..], record, &signed));
    let mut inconsistent = pair.secret_key.clone();
    inconsistent[63] ^= 1;
    assert_eq!(
        signature::sign(&inconsistent, record),
        Err(Error::InvalidInput)
    );
}

const IDENTITY: CellIdentity<'static> = CellIdentity {
    application: "test-app",
    scope_id: "00000000-0000-0000-0000-000000000001",
    identifier: "00000000-0000-0000-0000-000000000002",
    table_id: "00000000-0000-0000-0000-000000000003",
    row_id: "00000000-0000-7000-8000-000000000004",
    column_id: "00000000-0000-0000-0000-000000000005",
    epoch_id: "00000000-0000-4000-8000-000000000006",
};
const TEXT_TYPE: &[u8] = br#"{"column_type":{"type":"Text"},"nullable":false}"#;
const BYTES_TYPE: &[u8] = br#"{"column_type":{"type":"Bytea"},"nullable":false}"#;

fn frames(mut value: &[u8]) -> Vec<Vec<u8>> {
    let mut parts = Vec::new();
    while !value.is_empty() {
        let length = u32::from_be_bytes(value[..4].try_into().unwrap()) as usize;
        parts.push(value[4..4 + length].to_vec());
        value = &value[4 + length..];
    }
    parts
}

fn context_fields(context: &[u8]) -> Vec<String> {
    assert_eq!(context[..5], hex("4a45324301"));
    frames(&context[5..])
        .into_iter()
        .map(|field| String::from_utf8(field).unwrap())
        .collect()
}

/// The cell- and stream-record corpus from the removed TypeScript tests.
#[test]
fn record_routing_corpus() {
    let corpus = Mechanism::new("test.corpus", 1);
    let epoch = to_hex(IDENTITY.epoch_id.as_bytes());
    let inner = envelope::encode(corpus, &[0]).unwrap();
    let record = records::encode_cell_record(IDENTITY.epoch_id, &inner).unwrap();
    assert_eq!(
        to_hex(&record),
        format!(
            "4a45324501156a617a7a2e653265652e63656c6c2d7265636f726400000001{epoch}\
             4a453245010b746573742e636f727075730000000100"
        )
    );
    // The stream encoding follows the epoch directly, its header carried once.
    let mut stream_record = records::stream_record_prefix(IDENTITY.epoch_id).unwrap();
    stream_record.extend_from_slice(&envelope::encode(corpus, &[0xde, 0xad]).unwrap());
    assert_eq!(
        to_hex(&stream_record),
        format!(
            "4a45324501176a617a7a2e653265652e73747265616d2d7265636f726400000001{epoch}\
             4a453245010b746573742e636f7270757300000001dead"
        )
    );
    let stored = records::read_stored_record(&record).unwrap();
    assert_eq!(stored.kind, records::RecordKind::Cell);
    assert_eq!(stored.epoch_id, IDENTITY.epoch_id);
    assert_eq!(stored.ciphertext, inner);
    let stored = records::read_stored_record(&stream_record).unwrap();
    assert_eq!(stored.kind, records::RecordKind::Stream);
    assert_eq!(
        envelope::decode(corpus, stored.ciphertext).unwrap(),
        [0xde, 0xad]
    );
}

#[test]
fn record_contexts() {
    let cell_context = records::cell_context(&IDENTITY, TEXT_TYPE).unwrap();
    let parts = frames(&cell_context);
    assert_eq!(parts.len(), 2);
    assert_eq!(
        context_fields(&parts[0]),
        [
            IDENTITY.application,
            "jazz.e2ee.cell-record.v1",
            IDENTITY.scope_id,
            IDENTITY.identifier,
            IDENTITY.table_id,
            IDENTITY.row_id,
            IDENTITY.column_id,
            IDENTITY.epoch_id,
            "",
        ]
    );
    assert_eq!(parts[1], TEXT_TYPE);
    let stream_context = records::stream_context(&IDENTITY, BYTES_TYPE).unwrap();
    let parts = frames(&stream_context);
    assert_eq!(parts.len(), 2);
    assert_eq!(context_fields(&parts[0])[1], "jazz.e2ee.stream-record.v1");
    assert_eq!(parts[1], BYTES_TYPE);
    let equality = records::EqualityIdentity {
        application: IDENTITY.application,
        scope_id: IDENTITY.scope_id,
        identifier: IDENTITY.identifier,
        table_id: IDENTITY.table_id,
        column_id: IDENTITY.column_id,
        epoch_id: IDENTITY.epoch_id,
    };
    let parts = frames(&records::equality_context(&equality, TEXT_TYPE).unwrap());
    assert_eq!(
        context_fields(&parts[0]),
        [
            IDENTITY.application,
            "jazz.e2ee.equality.v1",
            IDENTITY.scope_id,
            IDENTITY.identifier,
            IDENTITY.table_id,
            "",
            IDENTITY.column_id,
            IDENTITY.epoch_id,
            "",
        ]
    );
    for invalid in [
        CellIdentity {
            row_id: "row",
            ..IDENTITY
        },
        CellIdentity {
            epoch_id: "00000000-0000-0000-0000-00000000000A",
            ..IDENTITY
        },
        CellIdentity {
            table_id: "",
            ..IDENTITY
        },
        CellIdentity {
            column_id: "",
            ..IDENTITY
        },
    ] {
        assert_eq!(
            records::cell_context(&invalid, TEXT_TYPE),
            Err(Error::InvalidInput)
        );
    }
}

#[test]
fn stored_values_round_trip() {
    let registry = jazz_e2ee::registry::Registry::builtin();
    let key = [7u8; 32];
    let mut rng = common::TestRandom(1);
    // "synthetic" as Jazz's native row encoding of a non-null Text value.
    let value = hex("0073796e746865746963");
    let stored =
        records::encrypt_cell_value(&cell::Builtin, &key, &IDENTITY, TEXT_TYPE, &value, &mut rng)
            .unwrap();
    assert_eq!(
        *records::decrypt_cell_value(&registry, &key, &IDENTITY, TEXT_TYPE, &stored).unwrap(),
        value
    );
    assert_eq!(
        records::decrypt_cell_value(&registry, &key, &IDENTITY, BYTES_TYPE, &stored).map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
    let other_row = CellIdentity {
        row_id: "00000000-0000-7000-8000-000000000009",
        ..IDENTITY
    };
    assert_eq!(
        records::decrypt_cell_value(&registry, &key, &other_row, TEXT_TYPE, &stored).map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
    let other_epoch = CellIdentity {
        epoch_id: "00000000-0000-4000-8000-000000000009",
        ..IDENTITY
    };
    assert_eq!(
        records::decrypt_cell_value(&registry, &key, &other_epoch, TEXT_TYPE, &stored).map(|_| ()),
        Err(Error::InvalidCiphertext)
    );

    let large: Vec<u8> = (0..131_073u32).map(|index| index as u8).collect();
    let streamed = records::encrypt_stream_value(
        &stream::Builtin,
        &key,
        &IDENTITY,
        BYTES_TYPE,
        &large,
        &mut rng,
    )
    .unwrap();
    assert_eq!(
        records::read_stored_record(&streamed).unwrap().kind,
        records::RecordKind::Stream
    );
    // The stream mechanism header appears once, starting the stream encoding
    // straight after the epoch.
    let header = stream::MECHANISM.header().unwrap();
    let prefix = records::stream_record_prefix(IDENTITY.epoch_id).unwrap();
    assert_eq!(streamed[..prefix.len()], prefix[..]);
    let encoding = &streamed[prefix.len()..];
    assert_eq!(encoding[..header.len()], header[..]);
    assert_eq!(
        encoding
            .windows(header.len())
            .filter(|w| *w == header.as_slice())
            .count(),
        1
    );
    assert_eq!(
        *stream::decrypt(
            &key,
            &records::stream_context(&IDENTITY, BYTES_TYPE).unwrap(),
            encoding
        )
        .unwrap(),
        large
    );
    assert_eq!(
        *records::decrypt_stream_value(&registry, &key, &IDENTITY, BYTES_TYPE, &streamed).unwrap(),
        large
    );
    // A cell record is not a stream record, and the reverse.
    assert_eq!(
        records::decrypt_stream_value(&registry, &key, &IDENTITY, BYTES_TYPE, &stored).map(|_| ()),
        Err(Error::UnsupportedFormat)
    );
    assert_eq!(
        records::decrypt_cell_value(&registry, &key, &IDENTITY, BYTES_TYPE, &streamed).map(|_| ()),
        Err(Error::UnsupportedFormat)
    );
    let mut truncated = streamed.clone();
    truncated.truncate(streamed.len() - 1);
    assert_eq!(
        records::decrypt_stream_value(&registry, &key, &IDENTITY, BYTES_TYPE, &truncated)
            .map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
}

/// The framing examples pinned in `E2EE_CRYPTO_FORMAT.md` (cell record) and
/// `E2EE_LARGE_VALUE_FORMAT.md` (stream record).
#[test]
fn spec_wrapper_examples() {
    const EPOCH: &str = "00000000-0000-4000-8000-000000000000";
    let test_envelope = envelope::encode(Mechanism::new("test", 1), &[0xaa]).unwrap();
    let cell = records::encode_cell_record(EPOCH, &test_envelope).unwrap();
    assert_eq!(
        to_hex(&cell),
        "4a45324501156a617a7a2e653265652e63656c6c2d7265636f726400000001\
         30303030303030302d303030302d343030302d383030302d303030303030303030303030\
         4a45324501047465737400000001aa"
    );
    let routing = records::stream_record_prefix(EPOCH).unwrap();
    assert_eq!(
        to_hex(&routing),
        "4a45324501176a617a7a2e653265652e73747265616d2d7265636f726400000001\
         30303030303030302d303030302d343030302d383030302d303030303030303030303030"
    );
    // Followed by the stream encoding, here the fixture stream.
    assert_eq!(
        to_hex(&stream::MECHANISM.header().unwrap()),
        "4a453245011d6a617a7a2e73747265616d2e786368616368613230706f6c793133303500000001"
    );
    let stored = [routing.as_slice(), &hex(v::STREAM)].concat();
    let record = records::read_stored_record(&stored).unwrap();
    assert_eq!(record.kind, records::RecordKind::Stream);
    assert_eq!(record.epoch_id, EPOCH);
    assert_eq!(record.ciphertext, hex(v::STREAM));
    let record = records::read_stored_record(&cell).unwrap();
    assert_eq!(record.kind, records::RecordKind::Cell);
    assert_eq!(record.ciphertext, test_envelope);
}

/// The equality value encoding `V` and its tokens: tokens match Jazz's
/// plaintext equality (every NaN alike, `-0.0` equal to `0.0`, text by exact
/// bytes with no Unicode normalisation), and null differs from every value.
#[test]
fn equality_values() {
    use records::{EqualityValue as E, equality_value};
    let cases: [(E<'_>, &str); 12] = [
        (E::Null, "00"),
        (E::Bool(false), "0100"),
        (E::Bool(true), "0101"),
        (E::Int(-2), "01fffffffffffffffe"),
        (E::Timestamp(1_800_000_000_000), "01000001a3185c5000"),
        (E::Float(1.5), "013ff8000000000000"),
        (E::Float(0.0), "010000000000000000"),
        (E::Float(-0.0), "010000000000000000"),
        (E::Float(f64::NAN), "017ff8000000000000"),
        (
            E::Float(f64::from_bits(0xfff0_0000_0000_0001)),
            "017ff8000000000000",
        ),
        (E::Text(""), "01"),
        (E::Text("é"), "01c3a9"),
    ];
    for (value, encoded) in cases {
        assert_eq!(to_hex(&equality_value(&value)), encoded, "{value:?}");
    }
    assert_eq!(to_hex(&equality_value(&E::Text("e\u{301}"))), "0165cc81");
    assert_eq!(to_hex(&equality_value(&E::Bytes(&[0, 1]))), "010001");
    assert_eq!(
        to_hex(&equality_value(&E::Uuid([0xab; 16]))),
        "01abababababababababababababababab"
    );

    let identity = records::EqualityIdentity {
        application: IDENTITY.application,
        scope_id: IDENTITY.scope_id,
        identifier: IDENTITY.identifier,
        table_id: IDENTITY.table_id,
        column_id: IDENTITY.column_id,
        epoch_id: IDENTITY.epoch_id,
    };
    let float_type = br#"{"column_type":{"type":"Double"},"nullable":true}"#;
    let token = |value: E<'_>| {
        to_hex(
            &records::equality_token_of(&equality::Builtin, &root(), &identity, float_type, &value)
                .unwrap(),
        )
    };
    assert_eq!(token(E::Float(0.0)), token(E::Float(-0.0)));
    assert_eq!(token(E::Float(f64::NAN)), token(E::Float(-f64::NAN)));
    assert_ne!(token(E::Float(0.0)), token(E::Null));
    assert_ne!(token(E::Text("é")), token(E::Text("e\u{301}")));
    assert_eq!(
        token(E::Float(-0.0)),
        "4a45324501146a617a7a2e736f6469756d2e657175616c69747900000001cd771c3a1e2f0e55f0cfe1cfc6f1ff86eb4ae15f9b5c9270e769602f74f956f6"
    );
    // An equality identity is checked like a cell identity.
    assert_eq!(
        records::equality_context(
            &records::EqualityIdentity {
                epoch_id: "epoch",
                ..identity
            },
            TEXT_TYPE
        ),
        Err(Error::InvalidInput)
    );
}
