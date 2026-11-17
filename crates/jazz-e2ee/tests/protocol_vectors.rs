//! Protocol fixtures (`E2EE_PROTOCOL.md` §0, §1.2, §1.3, §2, §3): the
//! recovery and account sealing key derivations, the canonical encodings,
//! and the signed bytes and contexts of every record kind reproduce byte for
//! byte.
//!
//! Derivations are cross-checked against libsodium (keyed BLAKE2b, Ed25519
//! seed key pairs); signed bytes and contexts are rebuilt here from the
//! spec's formulas with the context and framing primitives, and their
//! SHA-256 digests are pinned.

mod common;

use common::{TestRandom, to_hex};
use jazz_e2ee::agility::{KeySealer, MechanismImpl};
use jazz_e2ee::context::{CryptoContext, frame_record};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::{key_envelope, key_seal, signature};
use jazz_e2ee::primitives::hash;
use jazz_e2ee::protocol::derive::{self, RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::domain::AppDomain;
use jazz_e2ee::protocol::error::ProtocolError;
use jazz_e2ee::protocol::ids::{self, IdForm};
use jazz_e2ee::protocol::json;
use jazz_e2ee::protocol::records::KeyColumns;
use jazz_e2ee::protocol::records::account::*;
use jazz_e2ee::protocol::records::space::*;
use libsodium_sys as ffi;

const APP: &str = "0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f";
/// Account IDs are UUIDs of any version; Jazz's registry mints version 5.
const ALICE: &str = "a11ce000-0000-5000-8000-00000000a11c";
const BOB: &str = "b0b00000-0000-5000-8000-00000000b0b0";
const SCOPE: &str = "5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6";
const ROW: &str = "018f2e3d-4c5b-7a69-8877-665544332211";
const ROOT: &str = "d4d4d4d4-d4d4-4d4d-8d4d-d4d4d4d4d4d4";
const D1: &str = "11111111-1111-4111-8111-111111111111";
const D2: &str = "22222222-2222-4222-8222-222222222222";
const E1: &str = "e1e1e1e1-e1e1-4e1e-8e1e-e1e1e1e1e1e1";
const E2: &str = "e2e2e2e2-e2e2-4e2e-ae2e-e2e2e2e2e2e2";
const E3: &str = "e3e3e3e3-e3e3-7e3e-be3e-e3e3e3e3e3e3";
/// Account epochs of the space records' author and recipient.
const AE: &str = "aeaeaeae-aeae-4aea-8aea-aeaeaeaeaeae";
const BE: &str = "bebebebe-bebe-4beb-9beb-bebebebebebe";
const R1: &str = "a1a1a1a1-a1a1-4a1a-9a1a-a1a1a1a1a1a1";
const R2: &str = "b2b2b2b2-b2b2-7b2b-bb2b-b2b2b2b2b2b2";

fn sodium_init() {
    // SAFETY: libsodium's documented initialisation, idempotent.
    assert!(unsafe { ffi::sodium_init() } >= 0);
}

fn sodium_sha256(input: &[u8]) -> [u8; 32] {
    sodium_init();
    let mut out = [0u8; 32];
    // SAFETY: `out` has the 32 bytes SHA-256 writes; the input pointer and
    // length describe `input`.
    unsafe { ffi::crypto_hash_sha256(out.as_mut_ptr(), input.as_ptr(), input.len() as u64) };
    out
}

fn sodium_keyed(key: &[u8; 32], input: &[u8]) -> [u8; 32] {
    sodium_init();
    let mut out = [0u8; 32];
    // SAFETY: 32-byte output and key buffers; the input pointer and length
    // describe `input`.
    let status = unsafe {
        ffi::crypto_generichash(
            out.as_mut_ptr(),
            32,
            input.as_ptr(),
            input.len() as u64,
            key.as_ptr(),
            32,
        )
    };
    assert_eq!(status, 0);
    out
}

fn sodium_sign_public(seed: &[u8; 32]) -> [u8; 32] {
    sodium_init();
    let mut public = [0u8; 32];
    let mut secret = [0u8; 64];
    // SAFETY: buffers of the sizes `crypto_sign_seed_keypair` writes and reads.
    unsafe {
        ffi::crypto_sign_seed_keypair(public.as_mut_ptr(), secret.as_mut_ptr(), seed.as_ptr())
    };
    public
}

/// The version 8 UUID of 16 bytes, formatted here independently.
fn v8(digest: &[u8]) -> String {
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x80;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex = to_hex(&b);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn domain() -> AppDomain {
    AppDomain::new(APP, Some("prod")).unwrap()
}

/// Application domains are canonical JSON of the configured app ID and
/// environment (and account); no URL takes part. The environment defaults
/// to `dev`.
#[test]
fn application_domains() {
    let domain = domain();
    assert_eq!(domain.shared().as_str(), format!(r#"["{APP}","prod"]"#));
    assert_eq!(
        domain.account("alice").unwrap().as_str(),
        format!(r#"["{APP}","prod","alice"]"#)
    );
    assert_eq!(
        AppDomain::new(APP, None).unwrap().shared().as_str(),
        format!(r#"["{APP}","dev"]"#)
    );
    assert_eq!(
        domain.account("al\"ice\n").unwrap().as_str(),
        format!(r#"["{APP}","prod","al\"ice\n"]"#)
    );
    assert!(AppDomain::new("https://example.com/apps/x", None).is_err());
    assert!(AppDomain::new(&APP.to_uppercase(), None).is_err());
    assert!(AppDomain::new(APP, Some("")).is_err());
    assert!(domain.account("").is_err());
}

/// UUID forms: random IDs are versions 4 and 7, derived recovery root IDs
/// version 8, account IDs and the space address any version; always
/// lowercase with the RFC variant.
#[test]
fn uuid_forms() {
    assert!(ids::is_id(D1, IdForm::Random));
    assert!(ids::is_id(R2, IdForm::Random));
    assert!(!ids::is_id(&E1.to_uppercase(), IdForm::Any));
    assert!(!ids::is_id(
        "11111111-1111-4111-7111-111111111111",
        IdForm::Any
    ));
    assert!(!ids::is_id(
        "11111111-1111-1111-8111-111111111111",
        IdForm::RandomOrDerived
    ));
    assert!(ids::is_id(
        "11111111-1111-1111-8111-111111111111",
        IdForm::Any
    ));
    assert!(!ids::is_id("alice", IdForm::Any));
    assert!(ids::is_id(ALICE, IdForm::Any));
    assert!(!ids::is_id(ALICE, IdForm::Random));
    let derived = ids::uuid_v8([0x5a; 16]);
    assert_eq!(ids::uuid_version(&derived), Some(8));
    assert!(ids::is_id(&derived, IdForm::RandomOrDerived));
    assert!(!ids::is_id(&derived, IdForm::Random));
    let mut rng = TestRandom(1);
    let random = ids::random_v4(&mut rng).unwrap();
    assert_eq!(ids::uuid_version(&random), Some(4));
}

/// The recovery derivation of §2.9 from secret bytes `00`–`1f` for account
/// `alice`, cross-checked step by step with libsodium; pinned.
#[test]
fn recovery_derivation() {
    let secret_bytes: [u8; 32] = core::array::from_fn(|i| i as u8);
    let secret = RecoverySecret::from_bytes(secret_bytes);
    let account_domain = domain().account("alice").unwrap();
    let keys = RecoveryKeys::derive(&secret, &account_domain, 0).unwrap();

    let seed = sodium_keyed(
        &secret_bytes,
        &[
            b"jazz.e2ee.recovery-seed.v1\0".as_slice(),
            account_domain.as_str().as_bytes(),
        ]
        .concat(),
    );
    let kem_ikm = sodium_keyed(&seed, b"jazz.e2ee.recovery-kem-key.v1\0");
    let sign_seed = sodium_keyed(&seed, b"jazz.e2ee.recovery-sign-key.v1\0");
    let root_id = sodium_keyed(&seed, b"jazz.e2ee.recovery-root-id.v1\0");
    assert_eq!(keys.root_id, v8(&root_id));
    assert_eq!(keys.signing.public.bytes, sodium_sign_public(&sign_seed));
    assert_eq!(
        keys.sealing.public,
        key_seal::Builtin.derive_key_pair(&kem_ikm).unwrap().public
    );
    assert_eq!(keys.sealing.public.mechanism, key_seal::MECHANISM);
    assert_eq!(keys.signing.public.mechanism, signature::MECHANISM);

    assert_eq!(keys.root_id, "a4c5c670-5336-81dc-a21b-a455befbf096");
    assert_eq!(
        to_hex(&keys.sealing.public.bytes),
        "4cb2da93696a09b991912b3c49e0a1fdd7278948be5c6d6cbff34239236bde78"
    );
    assert_eq!(
        to_hex(&keys.signing.public.bytes),
        "16e6857ea0aa9e666c087026136fa793458b465150e5ba75213017edbd07466e"
    );

    // Another account or environment gives another root from one secret.
    let other = RecoveryKeys::derive(&secret, &domain().account("bob").unwrap(), 0).unwrap();
    assert_ne!(other.root_id, keys.root_id);
}

/// The recovery secret's text form: `jazz-auth-v1:` and 43 unpadded
/// base64url characters, canonical only.
#[test]
fn recovery_secret_format() {
    let secret = RecoverySecret::from_bytes(core::array::from_fn(|i| i as u8));
    let text = secret.format();
    assert_eq!(
        text.as_str(),
        "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
    );
    assert_eq!(RecoverySecret::parse(&text).unwrap(), secret);
    let ones = RecoverySecret::from_bytes([0xff; 32]);
    assert_eq!(RecoverySecret::parse(&ones.format()).unwrap(), ones);
    for bad in [
        "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh9",
        "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
        "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh+",
        "jazz-auth-v2:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
        "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
    ] {
        assert!(RecoverySecret::parse(bad).is_err(), "{bad}");
    }
}

/// The account sealing key pair of §2.12, from epoch key `00`–`1f` and
/// `jazz.wrap.hpke-x25519` v1: the mechanism header, the input keying
/// material (cross-checked with libsodium) and the public key are the
/// spec's fixture.
#[test]
fn account_sealing_key_derivation() {
    let epoch_key: [u8; 32] = core::array::from_fn(|i| i as u8);
    let header = key_seal::Builtin.mechanism().header().unwrap();
    assert_eq!(
        to_hex(&header),
        "4a45324501156a617a7a2e777261702e68706b652d78323535313900000001"
    );
    let ikm = derive::account_sealing_ikm(&key_seal::Builtin, &epoch_key).unwrap();
    assert_eq!(
        *ikm,
        sodium_keyed(
            &epoch_key,
            &[b"jazz.e2ee.account-kem-key.v1\0".as_slice(), &header].concat()
        )
    );
    assert_eq!(
        to_hex(&ikm[..]),
        "ac4df8236cf35730587626b161094aafe5fc81447b7b051142e49322fbdb0aea"
    );
    let pair = derive::account_sealing_key_pair(&key_seal::Builtin, &epoch_key).unwrap();
    assert_eq!(pair.public.mechanism, key_seal::MECHANISM);
    assert_eq!(
        pair.public,
        key_seal::Builtin.derive_key_pair(&ikm[..]).unwrap().public
    );
    assert_eq!(
        to_hex(&pair.public.bytes),
        "49dcff86a89b83bdde5eda452d647296878278a9fe29f2df2e93b1ad3b014773"
    );
    // Another epoch key gives another key pair.
    let other = derive::account_sealing_key_pair(&key_seal::Builtin, &[7; 32]).unwrap();
    assert_ne!(other.public, pair.public);
}

/// The local device check and recovery material check bytes.
#[test]
fn check_bytes() {
    assert_eq!(
        derive::local_device_check("scope"),
        b"jazz.e2ee.local-device-check.v1\0scope"
    );
    assert_eq!(
        derive::recovery_material_check("scope", R1),
        format!("jazz.e2ee.recovery-material-check.v1\0scope\0{R1}").into_bytes()
    );
}

/// Canonical JSON: `JSON.stringify` escaping, UTF-16 ordering, and strict
/// decoding that refuses every non-canonical form.
#[test]
fn canonical_json() {
    assert_eq!(
        json::strings(&["a\"b\\c", "\u{8}\u{c}\n\r\t\u{1}\u{1f}", "é/\u{7f}"]),
        "[\"a\\\"b\\\\c\",\"\\b\\f\\n\\r\\t\\u0001\\u001f\",\"é/\u{7f}\"]"
    );
    // U+FF5E sorts after U+1F600 by byte but before it by UTF-16 code unit.
    let revision = json::encode_revision(["\u{1F600}", "\u{FF5E}"]).unwrap();
    assert_eq!(
        String::from_utf8(revision).unwrap(),
        "[\"\u{1F600}\",\"\u{FF5E}\"]"
    );
    assert_eq!(
        json::encode_id_list([E2, D1, E1]).unwrap(),
        format!(r#"["{D1}","{E1}","{E2}"]"#).into_bytes()
    );
    assert_eq!(json::decode_id_list(b"[]").unwrap(), Vec::<String>::new());
    assert!(json::encode_id_list([D1, D1]).is_err());
    assert!(json::encode_id_list(["alice"]).is_err());
    assert!(json::encode_revision([""]).is_err());
    let deliveries = json::encode_delivery_map([(D2, vec![255u8, 0]), (D1, vec![1u8])]).unwrap();
    assert_eq!(
        String::from_utf8(deliveries.clone()).unwrap(),
        format!(r#"[["{D1}",[1]],["{D2}",[255,0]]]"#)
    );
    assert_eq!(json::decode_delivery_map(&deliveries).unwrap().len(), 2);
    assert!(json::encode_delivery_map([(D1, Vec::<u8>::new())]).is_err());
    let non_canonical = [
        format!(r#"[ "{D1}"]"#),
        format!(r#"["{E1}","{D1}"]"#),
        format!(r#"["{D1}","{D1}"]"#),
        format!(r#"["{D1}"],"#),
        "[\"\\u0061\"]".to_owned(),
        "[\"a\\/b\"]".to_owned(),
        "[1]".to_owned(),
        "\"x\"".to_owned(),
        "[[[[\"x\"]]]]".to_owned(),
        "[\"\\ud83d\\ude00\"]".to_owned(),
    ];
    for text in &non_canonical {
        assert!(json::decode_revision(text.as_bytes()).is_err(), "{text}");
    }
    for text in [
        format!(r#"[["{D1}",[01]]]"#),
        format!(r#"[["{D1}",[256]]]"#),
        format!(r#"[["{D1}",[]]]"#),
        format!(r#"[["{D1}",[1],2]]"#),
    ] {
        assert!(
            json::decode_delivery_map(text.as_bytes()).is_err(),
            "{text}"
        );
    }
}

fn keys() -> KeyColumns {
    KeyColumns {
        public_key: vec![0x11; 32],
        mechanism: "jazz.wrap.hpke-x25519".to_owned(),
        version: 1,
        signing_public_key: vec![0x22; 32],
        signing_mechanism: "jazz.sodium.sign".to_owned(),
        signing_version: 1,
    }
}

/// A canonical context with every field given, in the spec's order.
#[allow(clippy::too_many_arguments)]
fn ctx(
    application: &str,
    policy: &str,
    scope: &str,
    identifier: &str,
    table: &str,
    row: &str,
    column: &str,
    epoch: &str,
    recipient: &str,
) -> Vec<u8> {
    CryptoContext {
        application,
        policy,
        scope,
        identifier,
        table,
        row,
        column,
        epoch,
        recipient,
    }
    .encode()
    .unwrap()
}

fn append(mut prefix: Vec<u8>, parts: &[&[u8]]) -> Vec<u8> {
    prefix.extend_from_slice(&frame_record(parts).unwrap());
    prefix
}

fn env(id: &str, payload: &[u8]) -> Vec<u8> {
    envelope::encode(Mechanism::new(id, 1), payload).unwrap()
}

/// Pinned SHA-256 digests of each record kind's signed bytes and contexts.
const SIGNED_DIGESTS: &[(&str, &str)] = &[
    (
        "public approval",
        "3933b20201f63442a778751094ac61a868639c1f92c3db9078d9c9cd29f8604c",
    ),
    (
        "recovery approval",
        "0b235d59c0f4356913300e8c03380d6b78e9ee73776b95b67db879d55dcbac73",
    ),
    (
        "proof",
        "37be290f12cb75e3bb785edc30be786055f207889aa7362b9997104ff23afea3",
    ),
    (
        "private approval",
        "c40fce76268353969df7bebd823203a79c3e1bbe58ce575a5f1c7da487381a39",
    ),
    (
        "delivery marker context",
        "4c359a10bf33ce207965dd1b2b840a741daf0977aae4a294a77cd91f900aa639",
    ),
    (
        "identity context",
        "c01ed5f6fff94f24cb96f1086ae0ae6baf1ec5d091dec258b037754d30342e45",
    ),
    (
        "public successor",
        "f0645b2c559531b74252c093ebc2e486e1fe73ceb3b95d69148d903efbdcc03a",
    ),
    (
        "private successor",
        "64fe1afb62c6bb148b64a906545b582ff9680cfdca38dab55dcc2a2655032e10",
    ),
    (
        "recovery root",
        "3c14d7506f3fbe7c8f98ffcb62e3b5865bef12d4becb41773320e047ad1d41d1",
    ),
    (
        "local-first account root",
        "a40be2825d055a2c33e2289958ae563497a0bd6291c8d3a18aeef0dd6f733942",
    ),
    (
        "recovery delivery context",
        "ad79282df32943d66e55c317dfe867444a290e86450e6adc9ccc34db5d73b017",
    ),
    (
        "account sealing key",
        "811b76df3c20c55eb998dccd393fa952b1309c4d6b1f108db6bc8b19575a7d2d",
    ),
    (
        "device removal request",
        "ba7a8549121b1bb6f4d937db0298deace3c411856e1f3a00d359d8dd3dd90a4b",
    ),
    (
        "space root",
        "f347181edf79d1fb84704603d172525025a6ab76c9f31bde9661d64cdd28c140",
    ),
    (
        "space root marker context",
        "ba230b93c7d6574cf98e2f3a0d37598ac66381134b748736aeef24f2663098c1",
    ),
    (
        "space root account envelope context",
        "d062a07f180753c2af6bb6b23e9a43bb3e813496c44b6e3bcf7f98d03f7b6073",
    ),
    (
        "space successor",
        "121fcd67394e7b3fe38662b41bc4baf806030fbd3e1f17869559d59d10e0a327",
    ),
    (
        "space successor marker context",
        "4ccfd24f6231c231e4e203a39d17babe301379569ac5259cce7382529311dde1",
    ),
    (
        "space successor history context",
        "7a87b5a2663ed2b6ccb0f1ab37bc5f23f40e035551fd8f5eaec4b6876ddba602",
    ),
    (
        "space successor account envelope context",
        "0448a531c4763e66d89d9b3b499ee4e88a202c351e62c566e72111731ad400a2",
    ),
    (
        "space delivery context",
        "c4fa4d613e46faeb54457df953e00c23f43aa49abd33497b694ebc8778f19c93",
    ),
];

/// Every record kind's signed bytes and contexts equal the spec's formula,
/// rebuilt here, and their digests are pinned.
#[test]
fn signed_bytes() {
    let d = domain();
    let da = d.account(ALICE).unwrap();
    let wrap = key_envelope::MECHANISM.id;
    let scope = SpaceScope::new(SCOPE, ROW, ROOT).unwrap();
    let x = |column: &str, recipient: &str, epoch: &str| {
        ctx(
            da.as_str(),
            "jazz.e2ee.space.v1",
            SCOPE,
            ROW,
            "__e2ee_spaces",
            ROOT,
            column,
            epoch,
            recipient,
        )
    };
    let mut cases: Vec<(&str, Vec<u8>, Vec<u8>)> = Vec::new();

    // Account records (§2.5, §2.8, §2.9).
    let approval = PublicDeviceApproval {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        device_id: D2.into(),
        signer_id: D1.into(),
        signature: vec![],
        recovery_root_id: None,
        recovery_signature: None,
    };
    cases.push((
        "public approval",
        approval.signed_bytes(&da, &keys()).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.public-device-approval.v1",
                "account",
                ALICE,
                "__e2ee_public_device_approvals",
                R1,
                &format!("approval-signature:{D1}"),
                E1,
                D2,
            ),
            &[
                &env("jazz.sodium.sign", &[0x22; 32]),
                &env("jazz.wrap.hpke-x25519", &[0x11; 32]),
            ],
        ),
    ));
    let recovery_approval = PublicDeviceApproval {
        signer_id: D2.into(),
        recovery_root_id: Some(R2.into()),
        recovery_signature: Some(vec![]),
        ..approval.clone()
    };
    cases.push((
        "recovery approval",
        recovery_approval.signed_bytes(&da, &keys()).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.recovery-device-approval.v1",
                "account",
                ALICE,
                "__e2ee_public_device_approvals",
                R1,
                &format!("recovery-signature:{R2}"),
                E1,
                D2,
            ),
            &[
                &env("jazz.sodium.sign", &[0x22; 32]),
                &env("jazz.wrap.hpke-x25519", &[0x11; 32]),
            ],
        ),
    ));
    let challenge = ChallengeCoordinates {
        id: R1,
        device_id: D2,
        epoch_id: E1,
    };
    let p = |column: &str| {
        ctx(
            da.as_str(),
            "jazz.e2ee.device-approval.v1",
            "account",
            ALICE,
            "__e2ee_device_challenges",
            R1,
            column,
            E1,
            D2,
        )
    };
    cases.push((
        "proof",
        proof_signed_bytes(&da, ALICE, challenge, b"proof").unwrap(),
        append(p("proof-signature"), &[b"proof"]),
    ));
    cases.push((
        "private approval",
        approval_signed_bytes(&da, ALICE, challenge, D1, b"marker").unwrap(),
        append(p(&format!("approval-signature:{D1}")), &[b"marker"]),
    ));
    cases.push((
        "delivery marker context",
        delivery_marker_context(&da, ALICE, challenge, b"sealed").unwrap(),
        append(p("delivery-verification"), &[b"sealed"]),
    ));
    cases.push((
        "identity context",
        identity_context(&da, ALICE, ALICE, E1, "envelope", D1).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.account-identity.v1",
            "account",
            ALICE,
            "__e2ee_account_identities",
            ALICE,
            "envelope",
            E1,
            D1,
        ),
    ));
    let membership = json::encode_id_list([D1]).unwrap();
    let public_successor = PublicAccountSuccessor {
        id: R1.into(),
        account_id: ALICE.into(),
        predecessor: E1.into(),
        epoch_id: E2.into(),
        signer_id: D1.into(),
        action: AccountAction::RemoveDevice,
        removed_device_id: Some(D2.into()),
        retired_recovery_root_ids: b"[]".to_vec(),
        membership: membership.clone(),
        revision: json::encode_revision([R2]).unwrap(),
        signature: vec![],
    };
    cases.push((
        "public successor",
        public_successor.signed_bytes(&da).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.public-account-successor.v1",
                "account",
                ALICE,
                "__e2ee_public_account_successors",
                R1,
                "signature",
                E2,
                D1,
            ),
            &[
                E1.as_bytes(),
                b"remove-device",
                D2.as_bytes(),
                b"[]",
                &membership,
                &public_successor.revision,
            ],
        ),
    ));
    let deliveries = json::encode_delivery_map([(D1, vec![7u8; 3])]).unwrap();
    let private_successor = AccountSuccessor {
        id: R1.into(),
        account_id: ALICE.into(),
        predecessor: E1.into(),
        epoch_id: E2.into(),
        signer_id: D1.into(),
        action: AccountAction::RetireRecoveryRoot,
        removed_device_id: None,
        retired_recovery_root_ids: json::encode_root_id_list([R2]).unwrap(),
        membership: membership.clone(),
        revision: json::encode_id_list([R2]).unwrap(),
        verification: b"marker".to_vec(),
        history: b"history".to_vec(),
        deliveries: deliveries.clone(),
        signature: vec![],
    };
    cases.push((
        "private successor",
        private_successor.signed_bytes(&da).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.account-successor.v1",
                "account",
                ALICE,
                "__e2ee_account_successors",
                R1,
                "signature",
                E2,
                D1,
            ),
            &[
                E1.as_bytes(),
                b"retire-recovery-root",
                R2.as_bytes(),
                format!("[\"{R2}\"]").as_bytes(),
                &membership,
                &private_successor.revision,
                b"marker",
                b"history",
                &deliveries,
            ],
        ),
    ));
    let root = RecoveryRoot {
        id: R2.into(),
        account_id: ALICE.into(),
        signer_id: D1.into(),
        epoch_id: E1.into(),
        keys: keys(),
        signature: vec![],
    };
    cases.push((
        "recovery root",
        root.signed_bytes(&da).unwrap(),
        frame_record(&[
            &ctx(
                da.as_str(),
                "jazz.e2ee.recovery.v1",
                "account",
                ALICE,
                "__e2ee_recovery_roots",
                R2,
                &json::strings(&["root", D1]),
                E1,
                "",
            ),
            &env("jazz.sodium.sign", &[0x22; 32]),
            &env("jazz.wrap.hpke-x25519", &[0x11; 32]),
        ])
        .unwrap(),
    ));
    let account_root = AccountRoot {
        id: R1.into(),
        ledger_version: 1,
        generation: 0,
        account_id: ALICE.into(),
        device_id: D1.into(),
        epoch_id: E1.into(),
        kind: AccountKind::LocalFirst,
        local_first_public_key: Some(vec![0x33; 32]),
        local_first_signature: Some(vec![]),
    };
    cases.push((
        "local-first account root",
        account_root.signed_bytes(&da, &keys()).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.local-first-account-root.v1",
                "account",
                ALICE,
                "__e2ee_account_roots",
                R1,
                "local-first-signature",
                E1,
                D1,
            ),
            &[
                b"0",
                b"1",
                &[0x33; 32],
                &env("jazz.sodium.sign", &[0x22; 32]),
                &env("jazz.wrap.hpke-x25519", &[0x11; 32]),
            ],
        ),
    ));
    let app_root = AccountRoot {
        kind: AccountKind::App,
        local_first_public_key: None,
        local_first_signature: None,
        ..account_root.clone()
    };
    assert!(app_root.validate().is_ok());
    assert!(app_root.signed_bytes(&da, &keys()).is_err());
    for broken in [
        AccountRoot {
            local_first_signature: None,
            ..account_root.clone()
        },
        AccountRoot {
            local_first_public_key: Some(vec![0x33; 31]),
            ..account_root.clone()
        },
        AccountRoot {
            kind: AccountKind::App,
            ..account_root.clone()
        },
        AccountRoot {
            local_first_signature: Some(vec![]),
            ..account_root.clone()
        },
    ] {
        assert!(matches!(
            broken.validate(),
            Err(ProtocolError::InvalidRecord(_))
        ));
    }
    let recovery_delivery = RecoveryDelivery {
        id: R1.into(),
        root_id: R2.into(),
        epoch_id: E1.into(),
        envelope: vec![1],
    };
    cases.push((
        "recovery delivery context",
        recovery_delivery_context(&da, ALICE, &recovery_delivery).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.recovery.v1",
            "account",
            ALICE,
            "__e2ee_recovery_deliveries",
            R1,
            "envelope",
            E1,
            R2,
        ),
    ));

    // The account sealing key (§2.12).
    let account_key = AccountSealingKey {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        signer_id: D1.into(),
        mechanism: "jazz.wrap.hpke-x25519".into(),
        version: 1,
        public_key: vec![0x11; 32],
        signature: vec![],
    };
    cases.push((
        "account sealing key",
        account_key.signed_bytes(&da).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.account-key.v1",
                "account",
                ALICE,
                "__e2ee_account_keys",
                R1,
                D1,
                E1,
                "",
            ),
            &[&env("jazz.wrap.hpke-x25519", &[0x11; 32])],
        ),
    ));
    let removal_request = DeviceRemovalRequest {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        device_id: D2.into(),
        signature: vec![],
    };
    cases.push((
        "device removal request",
        removal_request.signed_bytes(&da).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.device-removal-request.v1",
            "account",
            ALICE,
            "__e2ee_device_removal_requests",
            R1,
            "signature",
            E1,
            D2,
        ),
    ));

    // Space records (§3.1): the root at epoch E2, then a recovery rotation
    // to E3 whose history opens E1, so every coordinate is distinct.
    let space_root = SpaceRoot {
        id: ROOT.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        creator_account_id: ALICE.into(),
        device_id: D1.into(),
        account_epoch_id: AE.into(),
        epoch_id: E2.into(),
        mechanism: wrap.into(),
        version: 1,
        verification: env(wrap, b"marker"),
        account_envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    assert_eq!(space_root.scope().unwrap(), scope);
    cases.push((
        "space root",
        space_root.signed_bytes(&da).unwrap(),
        frame_record(&[
            &x(&json::strings(&["root", ALICE, D1, AE]), "", E2),
            &env(wrap, &env(wrap, b"marker")),
            b"sealed",
        ])
        .unwrap(),
    ));
    cases.push((
        "space root marker context",
        space_root.marker_context(&da).unwrap(),
        x("verification", "", E2),
    ));
    cases.push((
        "space root account envelope context",
        space_root.account_envelope_context(&da).unwrap(),
        x("account-envelope", &json::strings(&[ALICE, AE]), E2),
    ));
    let space_successor = SpaceSuccessor {
        id: R1.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        predecessor: E2.into(),
        history_epoch_id: E1.into(),
        epoch_id: E3.into(),
        author_account_id: ALICE.into(),
        author_device_id: D1.into(),
        author_epoch_id: AE.into(),
        verification: b"marker".to_vec(),
        history: b"history".to_vec(),
        account_envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "space successor",
        space_successor.signed_bytes(&da, &scope).unwrap(),
        append(
            x(
                &json::strings(&["successor", R1, "signature"]),
                &json::strings(&[ALICE, D1, AE]),
                E3,
            ),
            &[
                E2.as_bytes(),
                E1.as_bytes(),
                b"marker",
                b"history",
                b"sealed",
            ],
        ),
    ));
    cases.push((
        "space successor marker context",
        space_successor.marker_context(&da, &scope).unwrap(),
        x("verification", "", E3),
    ));
    cases.push((
        "space successor history context",
        space_successor.history_context(&da, &scope).unwrap(),
        x(&json::strings(&["successor", R1, "history"]), E1, E3),
    ));
    cases.push((
        "space successor account envelope context",
        space_successor
            .account_envelope_context(&da, &scope)
            .unwrap(),
        x(
            &json::strings(&["successor", R1, "account-envelope"]),
            &json::strings(&[ALICE, AE]),
            E3,
        ),
    ));
    let delivery = SpaceDelivery {
        id: R2.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        key_epoch_id: E3.into(),
        sender_account_id: ALICE.into(),
        recipient_account_id: BOB.into(),
        recipient_account_epoch_id: BE.into(),
        envelope: b"sealed".to_vec(),
    };
    cases.push((
        "space delivery context",
        delivery.seal_context(&da, &scope).unwrap(),
        x(
            &json::strings(&["delivery", R2, ALICE]),
            &json::strings(&[BOB, BE]),
            E3,
        ),
    ));

    for (name, actual, expected) in &cases {
        assert_eq!(to_hex(actual), to_hex(expected), "{name}");
    }
    assert_eq!(cases.len(), SIGNED_DIGESTS.len());
    for ((name, actual, _), (pinned_name, digest)) in cases.iter().zip(SIGNED_DIGESTS) {
        assert_eq!(name, pinned_name);
        assert_eq!(hash::sha256(actual), sodium_sha256(actual), "{name}");
        assert_eq!(to_hex(&hash::sha256(actual)), *digest, "{name}");
    }
}

/// Each row's local invariants: ID forms (account IDs are UUIDs), enum
/// values, target columns and shapes.
#[test]
fn row_validation() {
    let invalid =
        |result: Result<(), ProtocolError>| matches!(result, Err(ProtocolError::InvalidRecord(_)));
    let approval = PublicDeviceApproval {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        device_id: D2.into(),
        signer_id: D1.into(),
        signature: vec![1],
        recovery_root_id: None,
        recovery_signature: None,
    };
    assert!(approval.validate().is_ok());
    assert!(invalid(
        PublicDeviceApproval {
            recovery_signature: Some(vec![1]),
            ..approval.clone()
        }
        .validate()
    ));
    assert!(invalid(
        PublicDeviceApproval {
            recovery_root_id: Some(R2.into()),
            recovery_signature: Some(vec![1]),
            ..approval.clone()
        }
        .validate()
    ));
    assert!(invalid(
        PublicDeviceApproval {
            id: ids::uuid_v8([0x5a; 16]),
            ..approval.clone()
        }
        .validate()
    ));
    for account_id in ["", "alice"] {
        assert!(invalid(
            PublicDeviceApproval {
                account_id: account_id.into(),
                ..approval.clone()
            }
            .validate()
        ));
    }

    let successor = PublicAccountSuccessor {
        id: R1.into(),
        account_id: ALICE.into(),
        predecessor: E1.into(),
        epoch_id: E2.into(),
        signer_id: D1.into(),
        action: AccountAction::RemoveDevice,
        removed_device_id: Some(D2.into()),
        retired_recovery_root_ids: b"[]".to_vec(),
        membership: json::encode_id_list([D1]).unwrap(),
        revision: b"[]".to_vec(),
        signature: vec![1],
    };
    assert!(successor.validate().is_ok());
    // A removal may also retire the recovery roots the device held (derived
    // ones included); a retirement names no device and exactly one root.
    let derived_root = ids::uuid_v8([0x5a; 16]);
    assert!(
        PublicAccountSuccessor {
            retired_recovery_root_ids: json::encode_root_id_list([R2, &derived_root]).unwrap(),
            ..successor.clone()
        }
        .validate()
        .is_ok()
    );
    let retirement = PublicAccountSuccessor {
        action: AccountAction::RetireRecoveryRoot,
        removed_device_id: None,
        retired_recovery_root_ids: json::encode_root_id_list([R2]).unwrap(),
        ..successor.clone()
    };
    assert_eq!(retirement.targets().unwrap(), (R2.to_owned(), Vec::new()));
    for ids in [
        b"[]".to_vec(),
        json::encode_root_id_list([R1, R2]).unwrap(),
        format!("[\"{R2}\",\"{R1}\"]").into_bytes(),
        format!("[\"{R2}\", \"{R1}\"]").into_bytes(),
    ] {
        assert!(invalid(
            PublicAccountSuccessor {
                retired_recovery_root_ids: ids,
                ..retirement.clone()
            }
            .validate()
            .map(|_| ())
        ));
    }
    assert!(invalid(
        PublicAccountSuccessor {
            action: AccountAction::RetireRecoveryRoot,
            retired_recovery_root_ids: json::encode_root_id_list([R2]).unwrap(),
            ..successor.clone()
        }
        .validate()
        .map(|_| ())
    ));
    assert!(invalid(
        PublicAccountSuccessor {
            action: AccountAction::RetireRecoveryRoot,
            ..successor.clone()
        }
        .validate()
        .map(|_| ())
    ));
    assert!(invalid(
        PublicAccountSuccessor {
            membership: json::encode_id_list([D1, D2]).unwrap(),
            ..successor.clone()
        }
        .validate()
        .map(|_| ())
    ));
    assert!(invalid(
        PublicAccountSuccessor {
            epoch_id: E1.into(),
            ..successor.clone()
        }
        .validate()
        .map(|_| ())
    ));
    assert!(invalid(
        PublicAccountSuccessor {
            revision: b"[ ]".to_vec(),
            ..successor.clone()
        }
        .validate()
        .map(|_| ())
    ));
    assert_eq!(
        AccountAction::parse("remove-device").unwrap(),
        AccountAction::RemoveDevice
    );
    assert!(AccountAction::parse("remove").is_err());

    // The identity's ID is the account ID at generation 0 and random after
    // a reset; nothing derives it.
    let identity = AccountIdentity {
        id: ALICE.into(),
        ledger_version: 1,
        generation: 0,
        device_id: D1.into(),
        epoch_id: E1.into(),
        envelope: vec![1],
        verification: vec![1],
    };
    assert!(identity.validate(ALICE).is_ok());
    assert!(invalid(identity.validate("alice")));
    assert!(invalid(
        AccountIdentity {
            id: R1.into(),
            ..identity.clone()
        }
        .validate(ALICE)
    ));
    assert!(invalid(
        AccountIdentity {
            generation: 1,
            ..identity.clone()
        }
        .validate(ALICE)
    ));
    let reset = AccountIdentity {
        generation: 1,
        id: R1.into(),
        ..identity
    };
    assert!(reset.validate(ALICE).is_ok());
    assert!(invalid(
        AccountIdentity {
            id: ids::uuid_v8([0x5a; 16]),
            ..reset
        }
        .validate(ALICE)
    ));

    let account_key = AccountSealingKey {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        signer_id: D1.into(),
        mechanism: "jazz.wrap.hpke-x25519".into(),
        version: 1,
        public_key: vec![0x11; 32],
        signature: vec![1],
    };
    assert!(account_key.validate().is_ok());
    for broken in [
        AccountSealingKey {
            id: ids::uuid_v8([0x5a; 16]),
            ..account_key.clone()
        },
        AccountSealingKey {
            account_id: "alice".into(),
            ..account_key.clone()
        },
        AccountSealingKey {
            signer_id: ALICE.into(),
            ..account_key.clone()
        },
        AccountSealingKey {
            mechanism: "Bad".into(),
            ..account_key.clone()
        },
        AccountSealingKey {
            public_key: vec![],
            ..account_key.clone()
        },
        AccountSealingKey {
            public_key: vec![0x11; 65_537],
            ..account_key.clone()
        },
        AccountSealingKey {
            signature: vec![],
            ..account_key.clone()
        },
    ] {
        assert!(invalid(broken.validate()));
    }

    // Space rows. The address is any UUID, every generated ID random.
    let member = SpaceMember {
        id: R1.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        account_id: BOB.into(),
    };
    assert!(member.validate().is_ok());
    for broken in [
        SpaceMember {
            id: ids::uuid_v8([0x5a; 16]),
            ..member.clone()
        },
        SpaceMember {
            identifier: "row-1".into(),
            ..member.clone()
        },
        SpaceMember {
            account_id: "bob".into(),
            ..member.clone()
        },
    ] {
        assert!(invalid(broken.validate()));
    }

    let wrap = key_envelope::MECHANISM.id;
    let root = SpaceRoot {
        id: ROOT.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        creator_account_id: ALICE.into(),
        device_id: D1.into(),
        account_epoch_id: AE.into(),
        epoch_id: E2.into(),
        mechanism: wrap.into(),
        version: 1,
        verification: env(wrap, b"marker"),
        account_envelope: vec![1],
        signature: vec![1],
    };
    assert!(root.validate().is_ok());
    for broken in [
        SpaceRoot {
            id: ids::uuid_v8([0x5a; 16]),
            ..root.clone()
        },
        SpaceRoot {
            creator_account_id: "alice".into(),
            ..root.clone()
        },
        SpaceRoot {
            verification: env("jazz.sodium.cell", b"marker"),
            ..root.clone()
        },
        SpaceRoot {
            account_envelope: vec![],
            ..root.clone()
        },
        SpaceRoot {
            signature: vec![],
            ..root.clone()
        },
    ] {
        assert!(invalid(broken.validate()));
    }

    let scope = root.scope().unwrap();
    let other_space = SpaceScope::new(SCOPE, D2, ROOT).unwrap();
    let successor = SpaceSuccessor {
        id: R1.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        predecessor: E2.into(),
        history_epoch_id: E2.into(),
        epoch_id: E3.into(),
        author_account_id: ALICE.into(),
        author_device_id: D1.into(),
        author_epoch_id: AE.into(),
        verification: vec![1],
        history: vec![1],
        account_envelope: vec![1],
        signature: vec![1],
    };
    assert!(successor.validate(&scope).is_ok());
    assert!(invalid(successor.validate(&other_space)));
    for broken in [
        SpaceSuccessor {
            epoch_id: E2.into(),
            ..successor.clone()
        },
        SpaceSuccessor {
            history_epoch_id: E3.into(),
            ..successor.clone()
        },
        SpaceSuccessor {
            author_account_id: "alice".into(),
            ..successor.clone()
        },
        SpaceSuccessor {
            history: vec![],
            ..successor.clone()
        },
        SpaceSuccessor {
            signature: vec![],
            ..successor.clone()
        },
    ] {
        assert!(invalid(broken.validate(&scope)));
    }

    let delivery = SpaceDelivery {
        id: R2.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        key_epoch_id: E3.into(),
        sender_account_id: ALICE.into(),
        recipient_account_id: BOB.into(),
        recipient_account_epoch_id: BE.into(),
        envelope: vec![1],
    };
    assert!(delivery.validate(&scope).is_ok());
    assert!(invalid(delivery.validate(&other_space)));
    for broken in [
        SpaceDelivery {
            id: ids::uuid_v8([0x5a; 16]),
            ..delivery.clone()
        },
        SpaceDelivery {
            recipient_account_id: "bob".into(),
            ..delivery.clone()
        },
        SpaceDelivery {
            recipient_account_epoch_id: BOB.into(),
            ..delivery.clone()
        },
        SpaceDelivery {
            envelope: vec![],
            ..delivery.clone()
        },
    ] {
        assert!(invalid(broken.validate(&scope)));
    }
    assert!(SpaceScope::new(SCOPE, ROW, ALICE).is_err());

    let mut keys = keys();
    keys.mechanism = "Bad".into();
    assert!(invalid(keys.validate()));
}

/// Above generation 0 the recovery seed also binds the generation
/// (`jazz.e2ee.recovery-generation-seed.v1`), so a reset account registers
/// a fresh root from the same secret; cross-checked with libsodium and
/// pinned. Generation 0 is the fixture above, unchanged.
#[test]
fn recovery_derivation_after_reset() {
    let secret_bytes: [u8; 32] = core::array::from_fn(|i| i as u8);
    let secret = RecoverySecret::from_bytes(secret_bytes);
    let account_domain = domain().account("alice").unwrap();
    let gen0 = RecoveryKeys::derive(&secret, &account_domain, 0).unwrap();
    let mut seen = vec![gen0.root_id.clone()];
    for (generation, pinned) in [
        (1u64, "243d5113-0e7a-8965-b199-9c3e5db34384"),
        (2u64, "0a84cdb6-4329-8e28-a6a5-e21185b04763"),
    ] {
        let keys = RecoveryKeys::derive(&secret, &account_domain, generation).unwrap();
        let seed = sodium_keyed(
            &secret_bytes,
            &[
                b"jazz.e2ee.recovery-generation-seed.v1\0".as_slice(),
                account_domain.as_str().as_bytes(),
                &[0],
                generation.to_string().as_bytes(),
            ]
            .concat(),
        );
        assert_eq!(
            keys.root_id,
            v8(&sodium_keyed(&seed, b"jazz.e2ee.recovery-root-id.v1\0"))
        );
        assert_eq!(
            keys.signing.public.bytes,
            sodium_sign_public(&sodium_keyed(&seed, b"jazz.e2ee.recovery-sign-key.v1\0"))
        );
        assert_eq!(
            keys.sealing.public,
            key_seal::Builtin
                .derive_key_pair(&sodium_keyed(&seed, b"jazz.e2ee.recovery-kem-key.v1\0"))
                .unwrap()
                .public
        );
        assert_eq!(keys.root_id, pinned, "generation {generation}");
        assert!(!seen.contains(&keys.root_id));
        seen.push(keys.root_id);
    }
}

/// Independent, byte-by-byte constructions, with no crate encoder, of the
/// public approval's, the recovery root's and the account sealing key's
/// signed bytes and of a space delivery's sealing context: `JE2C 01`, nine
/// u32be-length-prefixed fields, then u32be-length-prefixed framed parts.
/// The public approval is pinned in full.
#[test]
fn hand_computed_signed_bytes() {
    fn field(value: &[u8]) -> Vec<u8> {
        let mut out = (value.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(value);
        out
    }
    fn context(fields: [&str; 9]) -> Vec<u8> {
        let mut out = b"JE2C\x01".to_vec();
        for value in fields {
            out.extend(field(value.as_bytes()));
        }
        out
    }
    let da = domain().account(ALICE).unwrap();
    let app = format!("[\"{APP}\",\"prod\",\"{ALICE}\"]");
    assert_eq!(da.as_str(), app);

    let approval = PublicDeviceApproval {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        device_id: D2.into(),
        signer_id: D1.into(),
        signature: vec![],
        recovery_root_id: None,
        recovery_signature: None,
    };
    let column = format!("approval-signature:{D1}");
    let expected = context([
        &app,
        "jazz.e2ee.public-device-approval.v1",
        "account",
        ALICE,
        "__e2ee_public_device_approvals",
        R1,
        &column,
        E1,
        D2,
    ]);
    assert_eq!(to_hex(&expected), PUBLIC_APPROVAL_HEX);
    // The approval signs its device's keys after the context (§2.5).
    assert_eq!(
        approval.signed_bytes(&da, &keys()).unwrap(),
        append(
            expected,
            &[
                &env("jazz.sodium.sign", &[0x22; 32]),
                &env("jazz.wrap.hpke-x25519", &[0x11; 32]),
            ],
        )
    );

    let root = RecoveryRoot {
        id: R2.into(),
        account_id: ALICE.into(),
        signer_id: D1.into(),
        epoch_id: E1.into(),
        keys: keys(),
        signature: vec![],
    };
    let column = format!("[\"root\",\"{D1}\"]");
    let mut signing_env = b"JE2E\x01\x10jazz.sodium.sign\x00\x00\x00\x01".to_vec();
    signing_env.extend_from_slice(&[0x22; 32]);
    let mut sealing_env = b"JE2E\x01\x15jazz.wrap.hpke-x25519\x00\x00\x00\x01".to_vec();
    sealing_env.extend_from_slice(&[0x11; 32]);
    let mut expected = field(&context([
        &app,
        "jazz.e2ee.recovery.v1",
        "account",
        ALICE,
        "__e2ee_recovery_roots",
        R2,
        &column,
        E1,
        "",
    ]));
    expected.extend(field(&signing_env));
    expected.extend(field(&sealing_env));
    assert_eq!(root.signed_bytes(&da).unwrap(), expected);

    let account_key = AccountSealingKey {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        signer_id: D1.into(),
        mechanism: "jazz.wrap.hpke-x25519".into(),
        version: 1,
        public_key: vec![0x11; 32],
        signature: vec![],
    };
    let mut expected = context([
        &app,
        "jazz.e2ee.account-key.v1",
        "account",
        ALICE,
        "__e2ee_account_keys",
        R1,
        D1,
        E1,
        "",
    ]);
    expected.extend(field(&sealing_env));
    assert_eq!(account_key.signed_bytes(&da).unwrap(), expected);

    let delivery = SpaceDelivery {
        id: R2.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        key_epoch_id: E3.into(),
        sender_account_id: ALICE.into(),
        recipient_account_id: BOB.into(),
        recipient_account_epoch_id: BE.into(),
        envelope: vec![],
    };
    let column = format!("[\"delivery\",\"{R2}\",\"{ALICE}\"]");
    let recipient = format!("[\"{BOB}\",\"{BE}\"]");
    let expected = context([
        &app,
        "jazz.e2ee.space.v1",
        SCOPE,
        ROW,
        "__e2ee_spaces",
        ROOT,
        &column,
        E3,
        &recipient,
    ]);
    let scope = SpaceScope::new(SCOPE, ROW, ROOT).unwrap();
    assert_eq!(delivery.seal_context(&da, &scope).unwrap(), expected);
}

const PUBLIC_APPROVAL_HEX: &str = "4a45324301000000565b2230663865336134632d316432622d346335642d386536662d376138623963306431653266222c2270726f64222c2261313163653030302d303030302d353030302d383030302d303030303030303061313163225d000000236a617a7a2e653265652e7075626c69632d6465766963652d617070726f76616c2e7631000000076163636f756e740000002461313163653030302d303030302d353030302d383030302d3030303030303030613131630000001e5f5f653265655f7075626c69635f6465766963655f617070726f76616c730000002461316131613161312d613161312d346131612d396131612d61316131613161316131613100000037617070726f76616c2d7369676e61747572653a31313131313131312d313131312d343131312d383131312d3131313131313131313131310000002465316531653165312d653165312d346531652d386531652d6531653165316531653165310000002432323232323232322d323232322d343232322d383232322d323232323232323232323232";

/// Contexts and signed bytes come from a record's coordinates before its
/// outputs exist: a delivery's sealing context needs no envelope, a
/// successor's contexts need none of its outputs, a sealing key record's
/// signed bytes need no signature. Only a complete record validates.
#[test]
fn contexts_before_outputs() {
    let da = domain().account(ALICE).unwrap();
    let scope = SpaceScope::new(SCOPE, ROW, ROOT).unwrap();
    let mut delivery = SpaceDelivery {
        id: R1.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        key_epoch_id: E2.into(),
        sender_account_id: ALICE.into(),
        recipient_account_id: BOB.into(),
        recipient_account_epoch_id: BE.into(),
        envelope: Vec::new(),
    };
    let context = delivery.seal_context(&da, &scope).unwrap();
    assert!(delivery.validate(&scope).is_err());
    delivery.envelope = b"sealed".to_vec();
    assert_eq!(delivery.seal_context(&da, &scope).unwrap(), context);
    delivery.validate(&scope).unwrap();

    let mut successor = SpaceSuccessor {
        id: R1.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        predecessor: E1.into(),
        history_epoch_id: E1.into(),
        epoch_id: E2.into(),
        author_account_id: ALICE.into(),
        author_device_id: D1.into(),
        author_epoch_id: AE.into(),
        verification: Vec::new(),
        history: Vec::new(),
        account_envelope: Vec::new(),
        signature: Vec::new(),
    };
    successor.account_envelope_context(&da, &scope).unwrap();
    successor.history_context(&da, &scope).unwrap();
    successor.marker_context(&da, &scope).unwrap();
    assert!(successor.signed_bytes(&da, &scope).is_err());
    successor.verification = b"marker".to_vec();
    successor.history = b"history".to_vec();
    successor.account_envelope = b"sealed".to_vec();
    let bytes = successor.signed_bytes(&da, &scope).unwrap();
    assert!(successor.validate(&scope).is_err());
    successor.signature = vec![1];
    successor.validate(&scope).unwrap();
    assert_eq!(successor.signed_bytes(&da, &scope).unwrap(), bytes);

    let mut account_key = AccountSealingKey {
        id: R1.into(),
        account_id: ALICE.into(),
        epoch_id: E1.into(),
        signer_id: D1.into(),
        mechanism: "jazz.wrap.hpke-x25519".into(),
        version: 1,
        public_key: vec![0x11; 32],
        signature: Vec::new(),
    };
    let bytes = account_key.signed_bytes(&da).unwrap();
    assert!(account_key.validate().is_err());
    account_key.signature = vec![1];
    account_key.validate().unwrap();
    assert_eq!(account_key.signed_bytes(&da).unwrap(), bytes);

    let root = SpaceRoot {
        id: ROOT.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        creator_account_id: ALICE.into(),
        device_id: D1.into(),
        account_epoch_id: AE.into(),
        epoch_id: E2.into(),
        mechanism: key_envelope::MECHANISM.id.into(),
        version: 1,
        verification: Vec::new(),
        account_envelope: Vec::new(),
        signature: Vec::new(),
    };
    root.marker_context(&da).unwrap();
    root.account_envelope_context(&da).unwrap();
    // The marker must be an envelope of the root's own mechanism.
    let other_mechanism = SpaceRoot {
        verification: env("jazz.sodium.cell", b"marker"),
        account_envelope: b"sealed".to_vec(),
        ..root.clone()
    };
    assert!(other_mechanism.signed_bytes(&da).is_err());
    // Contexts of another space's records are refused.
    let other_space = SpaceScope::new(SCOPE, D2, ROOT).unwrap();
    assert!(successor.history_context(&da, &other_space).is_err());
    assert!(delivery.seal_context(&da, &other_space).is_err());
}
