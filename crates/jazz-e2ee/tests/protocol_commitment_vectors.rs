//! Wire fixtures for the three admission records and recovery commitment seeds.

use jazz_e2ee::Zeroizing;
use jazz_e2ee::conformance::FixedRandom;
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::signature;
use jazz_e2ee::primitives::{sign::SigningKeyPair, slh_dsa::CommitmentKey};
use jazz_e2ee::protocol::{
    commitment::{ENDORSEMENT_CONTEXT, MECHANISM, endorsement_digest, verify_endorsement},
    derive::{RecoveryKeys, RecoverySecret, recovery_commitment_seed},
    domain::{AccountDomain, AppDomain},
    error::{ProtocolError, UnsupportedCapability},
    records::{
        KeyColumns,
        roster::{Commitment, Endorsement, EndorsementSignature, SubjectKind},
    },
};

const APP: &str = "0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f";
const ALICE: &str = "a11ce000-0000-5000-8000-00000000a11c";
const ROW: &str = "11111111-1111-4111-8111-111111111111";
const DEVICE: &str = "22222222-2222-4222-8222-222222222222";
const ENDORSER: &str = "33333333-3333-4333-8333-333333333333";
const EPOCH: &str = "44444444-4444-4444-8444-444444444444";
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

fn header() -> Endorsement {
    Endorsement {
        id: ROW.into(),
        account_id: ALICE.into(),
        endorser_kind: SubjectKind::RecoveryRoot,
        endorser_id: ENDORSER.into(),
        subject_kind: SubjectKind::Device,
        subject_id: DEVICE.into(),
        signature_digest: vec![],
    }
}

fn keys() -> KeyColumns {
    KeyColumns {
        signing_mechanism: "jazz.sodium.sign".into(),
        signing_version: 1,
        signing_public_key: vec![0x22; 32],
        mechanism: "jazz.wrap.hpke-x25519".into(),
        version: 1,
        public_key: vec![0x33; 32],
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

// Assemble the expected bytes directly, independent of the protocol encoders.
fn fields(prefix: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    for part in parts {
        out.extend_from_slice(&(part.len() as u32).to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// Bob reproduces the full signed byte strings for Alice's commitment and
/// endorsement, and the digest of the entire signature column.
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

    let sign = [
        b"JE2E\x01\x10jazz.sodium.sign\0\0\0\x01".as_slice(),
        &[0x22; 32],
    ]
    .concat();
    let seal = [
        b"JE2E\x01\x15jazz.wrap.hpke-x25519\0\0\0\x01".as_slice(),
        &[0x33; 32],
    ]
    .concat();
    let column = format!("endorsement:recovery-root:{ENDORSER}");
    let expected = fields(
        b"JE2C\x01",
        &[
            d.as_bytes(),
            b"jazz.e2ee.endorsement.v1",
            b"account",
            ALICE.as_bytes(),
            b"__e2ee_endorsements",
            ROW.as_bytes(),
            column.as_bytes(),
            EPOCH.as_bytes(),
            DEVICE.as_bytes(),
            b"12",
            b"device",
            &cenv,
            &sign,
            &seal,
        ],
    );
    assert_eq!(
        header()
            .message(&application, 12, EPOCH, &c, &keys())
            .unwrap(),
        expected
    );
    assert_eq!(ENDORSEMENT_CONTEXT.as_bytes(), b"jazz.e2ee.endorsement.v1");
    let signature = envelope::encode(MECHANISM, &[0x77; 7856]).unwrap();
    assert_eq!(
        hex(&endorsement_digest(&signature)),
        "fb03a2c89c6ab0c577c175996037172543aaae25ff3eb9710a43e104abb3d57e"
    );
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

/// Bob verifies Alice's endorsement only for the original admission and both
/// of the admitted subject's classical keys, each under its stated mechanism.
#[test]
fn endorsement_binds_each_field() {
    let d = domain(APP, "prod", ALICE);
    let h = header();
    let c = commitment();
    let keys = keys();
    let (signer, public) = CommitmentKey::from_seed(&Zeroizing::new([7; 32]));
    let m = h.message(&d, 12, EPOCH, &c, &keys).unwrap();
    let raw = signer
        .sign(
            &public,
            &m,
            ENDORSEMENT_CONTEXT.as_bytes(),
            &mut FixedRandom(vec![5; 16]),
        )
        .unwrap();
    let signature = envelope::encode(MECHANISM, &raw).unwrap();
    let valid = |h: &Endorsement, d: &AccountDomain, g, epoch, c: &Commitment, k: &KeyColumns| {
        verify_endorsement(
            MECHANISM,
            &public,
            &h.message(d, g, epoch, c, k).unwrap(),
            &signature,
        )
        .unwrap()
    };
    assert!(valid(&h, &d, 12, EPOCH, &c, &keys));
    let mutations: &[fn(&mut Endorsement)] = &[
        |row| row.id = OTHER.into(),
        |row| row.account_id = OTHER.into(),
        |row| row.endorser_kind = SubjectKind::Device,
        |row| row.endorser_id = OTHER.into(),
        |row| row.subject_kind = SubjectKind::RecoveryRoot,
        |row| row.subject_id = OTHER.into(),
    ];
    for mutate in mutations {
        let mut altered = h.clone();
        mutate(&mut altered);
        assert!(!valid(&altered, &d, 12, EPOCH, &c, &keys));
    }
    for alternate in [
        domain(OTHER, "prod", ALICE),
        domain(APP, "dev", ALICE),
        domain(APP, "prod", OTHER),
    ] {
        assert!(!valid(&h, &alternate, 12, EPOCH, &c, &keys));
    }
    assert!(!valid(&h, &d, 13, EPOCH, &c, &keys));
    assert!(!valid(&h, &d, 12, OTHER, &c, &keys));
    let mutations: &[fn(&mut Commitment)] = &[
        |row| row.mechanism = "future".into(),
        |row| row.version = 2,
        |row| row.commitment = vec![0x12; 32],
    ];
    for mutate in mutations {
        let mut altered = c.clone();
        mutate(&mut altered);
        assert!(!valid(&h, &d, 12, EPOCH, &altered, &keys));
    }
    let mutations: &[fn(&mut KeyColumns)] = &[
        |row| row.signing_mechanism = "future".into(),
        |row| row.signing_version = 2,
        |row| row.signing_public_key = vec![0x23; 32],
        |row| row.mechanism = "future".into(),
        |row| row.version = 2,
        |row| row.public_key = vec![0x34; 32],
    ];
    for mutate in mutations {
        let mut altered = keys.clone();
        mutate(&mut altered);
        assert!(!valid(&h, &d, 12, EPOCH, &c, &altered));
    }
    assert!(!jazz_e2ee::primitives::slh_dsa::verify(&public, &m, b"other context", &raw).unwrap());
}

/// Bob can authenticate the bytes of an unknown key. Its use, or an
/// unknown signature envelope, reports the missing capability.
#[test]
fn row_shapes_and_mechanisms() {
    let unknown = Mechanism::new("future", 2);
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
    let mut h = header();
    assert!(h.validate().is_err());
    h.signature_digest = vec![0; 32];
    h.validate().unwrap();
    let s = EndorsementSignature {
        id: ROW.into(),
        account_id: ALICE.into(),
        signature: envelope::encode(unknown, &[1]).unwrap(),
    };
    s.validate().unwrap();
    let unsupported = Err(ProtocolError::Unsupported(
        UnsupportedCapability::CommitmentMechanism,
    ));
    assert_eq!(
        verify_endorsement(MECHANISM, &[], b"", &s.signature),
        unsupported
    );
    let short = envelope::encode(MECHANISM, &[1]).unwrap();
    assert_eq!(verify_endorsement(unknown, &[], b"", &short), unsupported);
    assert_eq!(
        verify_endorsement(MECHANISM, &[0; 32], b"", &short),
        Ok(false)
    );
    for bad in [vec![1], envelope::encode(MECHANISM, &[]).unwrap()] {
        assert!(
            EndorsementSignature {
                signature: bad,
                ..s.clone()
            }
            .validate()
            .is_err()
        );
    }
}
