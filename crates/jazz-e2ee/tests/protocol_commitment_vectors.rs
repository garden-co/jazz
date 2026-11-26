//! Wire fixtures for the commitment record, recovery commitment seeds and
//! the commitment record a recovery root's registration writes.

use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::conformance::FixedRandom;
use jazz_e2ee::mechanisms::signature;
use jazz_e2ee::primitives::{sign::SigningKeyPair, slh_dsa::CommitmentKey};
use jazz_e2ee::protocol::account::build::{
    AccountWriter, RecoveryCommitmentKey, recovery_commitment,
};
use jazz_e2ee::protocol::{
    commitment::MECHANISM,
    derive::{RecoveryKeys, RecoverySecret, recovery_commitment_seed},
    domain::{AccountDomain, AppDomain},
    error::ProtocolError,
    records::roster::{Commitment, SubjectKind},
};
use jazz_e2ee::registry::Registry;

const APP: &str = "0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f";
const ALICE: &str = "a11ce000-0000-5000-8000-00000000a11c";
const ROW: &str = "11111111-1111-4111-8111-111111111111";
const DEVICE: &str = "22222222-2222-4222-8222-222222222222";
const OTHER: &str = "55555555-5555-4555-8555-555555555555";

fn domain(app: &str, env: &str, account: &str) -> AccountDomain {
    AppDomain::new(app, Some(env))
        .unwrap()
        .account(account)
        .unwrap()
}

fn commitment() -> Commitment {
    Commitment {
        id: ROW.into(),
        account_id: ALICE.into(),
        subject_kind: SubjectKind::Device,
        subject_id: DEVICE.into(),
        mechanism: MECHANISM.id.into(),
        version: 1,
        commitment: vec![0x11; 32],
        signature: vec![],
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Alice's recovery fixture keeps its root and classical keys across three
/// generations. The commitment seeds come from Python's keyed BLAKE2b and
/// the public keys from fips205 on independently derived seed triples.
#[test]
fn recovery_commitment_vectors() {
    let secret = RecoverySecret::from_bytes(std::array::from_fn(|i| i as u8));
    let domain = domain(APP, "prod", "alice");
    for (generation, (seed, public, root, signing, sealing)) in [
        (
            "a794677a0a370345c2f34858c9e9b7bc26bad46a416569cd696c193383ea354a",
            "e2e2ab218c20baf46e92464d6780932d4d00de5439afabe2ded0f5fcf43cbfda",
            "a4c5c670-5336-81dc-a21b-a455befbf096",
            "16e6857ea0aa9e666c087026136fa793458b465150e5ba75213017edbd07466e",
            "4cb2da93696a09b991912b3c49e0a1fdd7278948be5c6d6cbff34239236bde78",
        ),
        (
            "25dc30c8dff00beeab4b42c599d38ef3d296a3aae263f49ad86674b8a4eb642e",
            "74cb9a606a638cf72bdb4108322d03b380d956e5eb17e2f777633e4a51129381",
            "243d5113-0e7a-8965-b199-9c3e5db34384",
            "3f79c224afe647c4531ab80e64cd99b2eebf42a64c991c30364e0b6ff6ab3a09",
            "233555b1784c158b3a35cfc72ce5cfacdf1c5e7146756f7fbcf1e23cabaeea01",
        ),
        (
            "ff40f02617842a6e81c463e30317bb2b21223c4d99c70359852a22c50477bf5e",
            "e18b3f0f3826053c921b808d8b90c90cd55479b9a6090a1b17af394888d11970",
            "0a84cdb6-4329-8e28-a6a5-e21185b04763",
            "f811fbbcd58ffc5cc77dc08e11f50b3c970127cf5b586ebe6b6f67466102add1",
            "dae1c8d990e3671773f4ba2443709e0d4e26def02b0c32700b511f5b62406649",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let generation = generation as u64;
        let commitment_seed = recovery_commitment_seed(&secret, &domain, generation);
        assert_eq!(hex(&commitment_seed[..]), seed);
        assert_eq!(hex(&CommitmentKey::from_seed(&commitment_seed).1), public);
        let keys = RecoveryKeys::derive(&secret, &domain, generation).unwrap();
        assert_eq!(keys.root_id, root);
        assert_eq!(hex(&keys.signing.public.bytes), signing);
        assert_eq!(hex(&keys.sealing.public.bytes), sealing);
    }
}

/// Alice registers the recovery root of her fixture secret in account
/// `ALICE`: the registration's commitment record is the same whenever the
/// same secret registers at the same generation (only the row ID is drawn).
/// The signed bytes are assembled here, and the seed, root ID, signing key
/// and Ed25519 signature come from Python's keyed BLAKE2b and the
/// `cryptography` package; the SLH-DSA key derivation is pinned above.
#[test]
fn recovery_root_commitment_record_vector() {
    let secret = RecoverySecret::from_bytes(std::array::from_fn(|i| i as u8));
    let app = AppDomain::new(APP, Some("prod")).unwrap();
    let registry = Registry::builtin();
    let writer = AccountWriter {
        account_secret_root_id: None,
        suite: registry.select(&MechanismConfig::default()).unwrap(),
        registry: &registry,
        domain: &app,
        account_id: ALICE,
    };
    let application = domain(APP, "prod", ALICE);
    let record = |generation| {
        recovery_commitment(
            &writer,
            &secret,
            &RecoveryCommitmentKey::derive(&secret, &application, generation).unwrap(),
            &mut FixedRandom(vec![0x66; 16]),
        )
        .unwrap()
    };
    let d = format!(r#"["{APP}","prod","{ALICE}"]"#);
    for (generation, (seed, public, root, signing, signature)) in [
        (
            0,
            (
                "c92ea5c570d0ef791c80450b942f9bca1e49ade36de4eefd38d73167cdb5dd1c",
                "2ae40871d9032f2c93de201fb72c8be3b5af417742ede3ea45f5bdd4ccc553d5",
                "bdf7dfa1-160c-8e21-bee8-aef5fd8199ea",
                "12eff1cb01120109f0b52e5e84879b3f7591703d8755a63874b14fe98f021fd3",
                "4a45324501106a617a7a2e736f6469756d2e7369676e000000010b11e3f3411463dd689a7922b0263d9c1976b3eadffab447d431e3cd1cd51a3509b8aadb379b9c881dd545cfe5164a53f79fb831c16b5fb70abcba10efc66b04",
            ),
        ),
        (
            1,
            (
                "24e9d79801d9d24478c1884ac45222da7851a6be14eb7401bd75ea6456b8430c",
                "334b6b549eb6afc4e68f0d33e45989ef4be77c7bef100ae98595f5632485f10a",
                "526073c9-d6ca-80c1-8d14-582668dc4e22",
                "fbc1958517baeb8bcb7630baec6f5742c7e77e69d7a5ab14bdfc8802fbaed657",
                "4a45324501106a617a7a2e736f6469756d2e7369676e0000000115157fdc9e4887bb9626ba91c8e31fa8e9201833c55e3e24d11cfb941b4121b4998765c395fa94d178ac26f9f2a499214a9c82d7355386ed7bfe5189af2fba00",
            ),
        ),
    ] {
        let c = record(generation);
        assert_eq!(c, record(generation), "the same secret, the same record");
        c.validate().unwrap();
        let commitment_seed = recovery_commitment_seed(&secret, &application, generation);
        let keys = RecoveryKeys::derive(&secret, &application, generation).unwrap();
        assert_eq!(hex(&commitment_seed[..]), seed);
        assert_eq!(
            (
                c.id.as_str(),
                c.account_id.as_str(),
                c.subject_kind,
                c.subject_id.as_str(),
                c.mechanism.as_str(),
                c.version,
            ),
            (
                "66666666-6666-4666-a666-666666666666",
                ALICE,
                SubjectKind::RecoveryRoot,
                root,
                "jazz.commit.slh-dsa-sha2-128s",
                1,
            )
        );
        assert_eq!(hex(&c.commitment), public);
        assert_eq!(hex(&keys.signing.public.bytes), signing);
        let signed = fields(
            b"JE2C\x01",
            &[
                d.as_bytes(),
                b"jazz.e2ee.commitment.v1",
                b"account",
                ALICE.as_bytes(),
                b"__e2ee_commitments",
                c.id.as_bytes(),
                b"signature",
                generation.to_string().as_bytes(),
                root.as_bytes(),
                b"recovery-root",
                &[
                    b"JE2E\x01\x1djazz.commit.slh-dsa-sha2-128s\0\0\0\x01".as_slice(),
                    &c.commitment,
                ]
                .concat(),
            ],
        );
        assert_eq!(c.signed_bytes(&application, generation).unwrap(), signed);
        assert_eq!(hex(&c.signature), signature);
        assert_eq!(
            registry.verify(&keys.signing.public, &signed, &c.signature),
            Ok(true)
        );
    }
}

/// Alice's registration refuses a commitment key derived for another root:
/// from another secret, for another account or at another generation.
/// Nothing is signed, so no record with the wrong key can be staged.
#[test]
fn a_commitment_key_for_another_root_is_refused() {
    let secret = RecoverySecret::from_bytes(std::array::from_fn(|i| i as u8));
    let other_secret = RecoverySecret::from_bytes([0x42; 32]);
    let app = AppDomain::new(APP, Some("prod")).unwrap();
    let registry = Registry::builtin();
    let writer = AccountWriter {
        account_secret_root_id: None,
        suite: registry.select(&MechanismConfig::default()).unwrap(),
        registry: &registry,
        domain: &app,
        account_id: ALICE,
    };
    let commit = |key: &RecoveryCommitmentKey| {
        recovery_commitment(&writer, &secret, key, &mut FixedRandom(vec![0x66; 16]))
    };
    let key = |secret, account, generation| {
        RecoveryCommitmentKey::derive(secret, &domain(APP, "prod", account), generation).unwrap()
    };
    commit(&key(&secret, ALICE, 0)).unwrap();
    commit(&key(&secret, ALICE, 1)).unwrap();
    for wrong in [
        key(&other_secret, ALICE, 0),
        key(&secret, OTHER, 0),
        RecoveryCommitmentKey::derive(&secret, &domain(APP, "dev", ALICE), 0).unwrap(),
    ] {
        assert_eq!(
            commit(&wrong),
            Err(ProtocolError::Crypto(jazz_e2ee::Error::InvalidInput))
        );
    }
}

/// Bob follows the specification's recipe for the generation-0 fixture
/// commitment (E2EE_PROTOCOL.md §2.9, "Commitment"): the encoded context
/// `ctx{…}` (`JE2C 01` and its nine length-prefixed fields), then
/// `frame[subjectKind, env(mechanism, commitment)]`. The context itself is
/// not framed, unlike a recovery root's signed bytes. Those bytes are the
/// record's, and Alice's fixture signature verifies over them and not over
/// `frame[ctx, subjectKind, env]`.
#[test]
fn the_specified_commitment_signed_bytes_reproduce_the_vector() {
    let secret = RecoverySecret::from_bytes(std::array::from_fn(|i| i as u8));
    let application = domain(APP, "prod", ALICE);
    let (row, root) = (
        "66666666-6666-4666-a666-666666666666",
        "bdf7dfa1-160c-8e21-bee8-aef5fd8199ea",
    );
    let key = unhex("2ae40871d9032f2c93de201fb72c8be3b5af417742ede3ea45f5bdd4ccc553d5");
    let signature = unhex(
        "4a45324501106a617a7a2e736f6469756d2e7369676e000000010b11e3f3411463dd689a7922b0263d9c1976b3eadffab447d431e3cd1cd51a3509b8aadb379b9c881dd545cfe5164a53f79fb831c16b5fb70abcba10efc66b04",
    );
    let d = format!(r#"["{APP}","prod","{ALICE}"]"#);
    let ctx = fields(
        b"JE2C\x01",
        &[
            d.as_bytes(),
            b"jazz.e2ee.commitment.v1",
            b"account",
            ALICE.as_bytes(),
            b"__e2ee_commitments",
            row.as_bytes(),
            b"signature",
            b"0",
            root.as_bytes(),
        ],
    );
    let env = [
        b"JE2E\x01\x1djazz.commit.slh-dsa-sha2-128s\0\0\0\x01".as_slice(),
        &key,
    ]
    .concat();
    let specified = [ctx.clone(), fields(b"", &[b"recovery-root", &env])].concat();
    let record = Commitment {
        id: row.into(),
        account_id: ALICE.into(),
        subject_kind: SubjectKind::RecoveryRoot,
        subject_id: root.into(),
        mechanism: MECHANISM.id.into(),
        version: MECHANISM.version,
        commitment: key.clone(),
        signature: signature.clone(),
    };
    assert_eq!(record.signed_bytes(&application, 0).unwrap(), specified);
    let signing = RecoveryKeys::derive(&secret, &application, 0)
        .unwrap()
        .signing
        .public;
    let registry = Registry::builtin();
    assert_eq!(registry.verify(&signing, &specified, &signature), Ok(true));
    let framed_context = fields(b"", &[&ctx, b"recovery-root", &env]);
    assert_eq!(
        registry.verify(&signing, &framed_context, &signature),
        Ok(false)
    );
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

// Assemble the expected bytes directly, independent of the protocol encoders.
fn fields(prefix: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    for part in parts {
        out.extend_from_slice(&(part.len() as u32).to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// Bob reproduces the full signed byte string for Alice's commitment.
#[test]
fn record_vectors() {
    let application = domain(APP, "prod", ALICE);
    let d = format!(r#"["{APP}","prod","{ALICE}"]"#);
    let c = commitment();
    let cenv = [
        b"JE2E\x01\x1djazz.commit.slh-dsa-sha2-128s\0\0\0\x01".as_slice(),
        &[0x11; 32],
    ]
    .concat();
    let expected = fields(
        b"JE2C\x01",
        &[
            d.as_bytes(),
            b"jazz.e2ee.commitment.v1",
            b"account",
            ALICE.as_bytes(),
            b"__e2ee_commitments",
            ROW.as_bytes(),
            b"signature",
            b"12",
            DEVICE.as_bytes(),
            b"device",
            &cenv,
        ],
    );
    assert_eq!(c.signed_bytes(&application, 12).unwrap(), expected);
}

/// Alice's classical signature fails when Bob changes one bound field,
/// including the generation to which a device is requesting admission.
#[test]
fn commitment_signature_binds_each_field() {
    let application = domain(APP, "prod", ALICE);
    let c = commitment();
    let key = SigningKeyPair::from_seed(&[9; 32]);
    let signature =
        signature::sign(&key.secret_key, &c.signed_bytes(&application, 12).unwrap()).unwrap();
    let valid = |c: &Commitment, d: &AccountDomain, g| {
        signature::verify(&key.public_key, &c.signed_bytes(d, g).unwrap(), &signature)
    };
    assert!(valid(&c, &application, 12));
    let mutations: &[fn(&mut Commitment)] = &[
        |row| row.id = OTHER.into(),
        |row| row.account_id = OTHER.into(),
        |row| row.subject_kind = SubjectKind::RecoveryRoot,
        |row| row.subject_id = OTHER.into(),
        |row| row.mechanism = "future".into(),
        |row| row.version = 2,
        |row| row.commitment = vec![0x12; 32],
    ];
    for mutate in mutations {
        let mut altered = c.clone();
        mutate(&mut altered);
        assert!(!valid(&altered, &application, 12));
    }
    for d in [
        domain(OTHER, "prod", ALICE),
        domain(APP, "dev", ALICE),
        domain(APP, "prod", OTHER),
    ] {
        assert!(!valid(&c, &d, 12));
    }
    assert!(!valid(&c, &application, 13));
}

/// Bob refuses an SLH-DSA-SHA2-128s commitment whose key is not exactly 32
/// bytes, as a record and as signed bytes. A key of the same lengths under a
/// mechanism or version he doesn't know stays opaque: well formed, and
/// unsupported only when used.
#[test]
fn a_known_mechanism_needs_its_exact_key_length() {
    let application = domain(APP, "prod", ALICE);
    let c = Commitment {
        signature: vec![1],
        ..commitment()
    };
    c.validate().unwrap();
    let invalid = Err(ProtocolError::InvalidRecord("invalid commitment length"));
    for len in [31, 33] {
        let wrong = Commitment {
            commitment: vec![0x11; len],
            ..c.clone()
        };
        assert_eq!(wrong.validate(), invalid, "{len} bytes");
        assert_eq!(
            wrong.signed_bytes(&application, 0).map(drop),
            invalid,
            "{len} bytes"
        );
        for (mechanism, version) in [("future", 1), (MECHANISM.id, 2)] {
            let unknown = Commitment {
                mechanism: mechanism.into(),
                version,
                ..wrong.clone()
            };
            unknown.validate().unwrap();
            unknown.signed_bytes(&application, 0).unwrap();
        }
    }
}

/// Bob can authenticate the bytes of an unknown key.
#[test]
fn row_shapes_and_mechanisms() {
    let mut c = Commitment {
        mechanism: "future".into(),
        ..commitment()
    };
    let bytes = c.signed_bytes(&domain(APP, "prod", ALICE), 0).unwrap();
    let key = SigningKeyPair::from_seed(&[9; 32]);
    assert!(c.validate().is_err());
    c.signature = signature::sign(&key.secret_key, &bytes).unwrap();
    assert!(signature::verify(&key.public_key, &bytes, &c.signature));
    for len in [1, 65_536] {
        c.commitment = vec![0; len];
        c.validate().unwrap();
    }
    for len in [0, 65_537] {
        c.commitment = vec![0; len];
        assert!(c.validate().is_err());
    }
}

/// Alice's root has several commitment rows, as the table policy allows
/// (§1.4, R11a): malformed ones, forged ones and a second valid one. Bob
/// counts the earliest valid row by accepted position, ties by smallest ID,
/// no earlier than the root (§2.9, "Which commitment counts"). A later
/// valid row never replaces it, and an invalid earlier row never blocks it.
#[test]
fn the_earliest_valid_commitment_counts() {
    use jazz_e2ee::protocol::position::{Accepted, Position};
    use jazz_e2ee::protocol::records::roster::{CommittedRoot, Counted, counting_commitment};

    let secret = RecoverySecret::from_bytes(std::array::from_fn(|i| i as u8));
    let app = AppDomain::new(APP, Some("prod")).unwrap();
    let registry = Registry::builtin();
    let writer = AccountWriter {
        account_secret_root_id: None,
        suite: registry.select(&MechanismConfig::default()).unwrap(),
        registry: &registry,
        domain: &app,
        account_id: ALICE,
    };
    let application = domain(APP, "prod", ALICE);
    let keys = RecoveryKeys::derive(&secret, &application, 0).unwrap();
    let key = RecoveryCommitmentKey::derive(&secret, &application, 0).unwrap();
    let valid = |id_byte: u8| {
        recovery_commitment(&writer, &secret, &key, &mut FixedRandom(vec![id_byte; 16])).unwrap()
    };
    // Re-signs a row with the root's own key, so only its content decides.
    let signed = |mut row: Commitment| {
        let bytes = row.signed_bytes(&application, 0).unwrap();
        row.signature = registry
            .signer(keys.signing.public.mechanism.as_mechanism())
            .unwrap()
            .sign(&keys.signing, &bytes)
            .unwrap();
        row
    };
    let root = CommittedRoot {
        account_id: ALICE,
        root_id: &keys.root_id,
        generation: 0,
        position: Position(10),
        signing: &keys.signing.public,
    };
    let first = valid(0x66);
    let second = valid(0x22);
    let wrong_key = {
        let mut row = valid(0x33);
        row.commitment = vec![0x44; 32];
        row
    };
    // The record refuses to build signed bytes for it, so it can't be re-signed.
    let short_key = Commitment {
        commitment: vec![0x44; 31],
        ..valid(0x34)
    };
    let device_kind = signed(Commitment {
        subject_kind: SubjectKind::Device,
        subject_id: DEVICE.into(),
        ..valid(0x35)
    });
    let other_root = Commitment {
        subject_id: OTHER.into(),
        ..valid(0x36)
    };
    let not_a_signature = Commitment {
        signature: vec![0x01],
        ..valid(0x37)
    };
    let future = signed(Commitment {
        mechanism: "jazz.commit.future".into(),
        version: 1,
        commitment: vec![0x55; 64],
        ..valid(0x11)
    });
    let at = |position, row: &Commitment| Accepted::new(Position(position), row.clone());
    let count = |rows: &[Accepted<Commitment>]| {
        counting_commitment(&registry, &application, &root, rows)
            .unwrap()
            .map(|counted| match counted {
                Counted::Known(row) => ("known", row.id.clone()),
                Counted::Unsupported(row) => ("unsupported", row.id.clone()),
            })
    };
    let known = |row: &Commitment| Some(("known", row.id.clone()));

    // Invalid rows, before or beside the valid ones, never count.
    let invalid = [
        &wrong_key,
        &short_key,
        &device_kind,
        &other_root,
        &not_a_signature,
    ];
    for row in invalid {
        assert_eq!(count(&[at(11, row)]), None, "{row:?}");
    }
    let mut rows: Vec<_> = invalid.iter().map(|row| at(11, row)).collect();
    rows.extend([at(13, &second), at(12, &first)]);
    assert_eq!(count(&rows), known(&first));
    // A later valid row never replaces the first, in any read order.
    rows.reverse();
    assert_eq!(count(&rows), known(&first));
    // At one position, the smallest ID counts ("22…" before "66…").
    assert_eq!(count(&[at(12, &first), at(12, &second)]), known(&second));
    // The root's own transaction counts; a row before the root does not.
    assert_eq!(count(&[at(10, &first), at(9, &second)]), known(&first));
    assert_eq!(count(&[at(9, &second)]), None);
    // An earliest valid row of an unknown mechanism is unsupported: Bob
    // does not fall through to a later row of a mechanism he knows.
    assert_eq!(
        count(&[at(12, &first), at(11, &future)]),
        Some(("unsupported", future.id.clone()))
    );
    assert_eq!(count(&[at(11, &first), at(12, &future)]), known(&first));
}
