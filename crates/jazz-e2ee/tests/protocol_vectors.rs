//! Protocol fixtures (`E2EE_PROTOCOL.md` §0, §1.2, §2, §3): every derived ID
//! label, the recovery and group key derivations, the recipient commitment
//! and the signed bytes of every record kind reproduce byte for byte.
//!
//! Derivations are cross-checked against libsodium (SHA-256, keyed
//! BLAKE2b, Ed25519 seed key pairs); signed bytes are rebuilt here from the
//! spec's formulas with the context and framing primitives, and their
//! SHA-256 digests are pinned.

mod common;

use common::{TestRandom, to_hex};
use jazz_e2ee::agility::KeySealer;
use jazz_e2ee::context::{CryptoContext, frame_record};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::{key_envelope, key_seal, signature};
use jazz_e2ee::primitives::hash;
use jazz_e2ee::protocol::derive::{self, RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::domain::AppDomain;
use jazz_e2ee::protocol::error::ProtocolError;
use jazz_e2ee::protocol::ids::{self, IdForm, StagedChange};
use jazz_e2ee::protocol::json;
use jazz_e2ee::protocol::records::account::*;
use jazz_e2ee::protocol::records::group::*;
use jazz_e2ee::protocol::records::scoped::{KeyRequest, Notice, NoticeScope};
use jazz_e2ee::protocol::records::space::*;
use jazz_e2ee::protocol::records::{
    KeyColumns, MemberKind, NoticeKind, Operation, Role, ScopeKind,
};
use libsodium_sys as ffi;

const APP: &str = "0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f";
const SCOPE: &str = "5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6";
const ROW: &str = "018f2e3d-4c5b-7a69-8877-665544332211";
const D1: &str = "11111111-1111-4111-8111-111111111111";
const D2: &str = "22222222-2222-4222-8222-222222222222";
const E1: &str = "e1e1e1e1-e1e1-4e1e-8e1e-e1e1e1e1e1e1";
const E2: &str = "e2e2e2e2-e2e2-4e2e-ae2e-e2e2e2e2e2e2";
const R1: &str = "a1a1a1a1-a1a1-4a1a-9a1a-a1a1a1a1a1a1";
const R2: &str = "b2b2b2b2-b2b2-7b2b-bb2b-b2b2b2b2b2b2";
const G1: &str = "c3c3c3c3-c3c3-4c3c-8c3c-c3c3c3c3c3c3";

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

/// Every public derived ID label: SHA-256 of the canonical JSON, cross-checked
/// with libsodium, with the version 8 bits; pinned.
#[test]
fn public_derived_ids() {
    let staged = StagedChange {
        role: "manager",
        operation: "add",
        kind: "account",
        recipient_id: "alice",
        n: 0,
    };
    let space = ids::space_id(SCOPE, ROW);
    let cases: [(&str, String, Vec<&str>, &str); 7] = [
        (
            "space",
            space.clone(),
            vec!["jazz.e2ee.space-id.v1", SCOPE, ROW],
            "619ee262-ebb9-865c-a8db-26ede397c780",
        ),
        (
            "account reset",
            ids::account_identity_id("alice", 1),
            vec!["jazz.e2ee.account-reset.v1", "alice", "1"],
            "0499408f-e71b-82e6-b00e-5cd5a54871e4",
        ),
        (
            "reset notice",
            ids::reset_notice_id(&space, "bob", 2),
            vec!["jazz.e2ee.reset-notice.v1", &space, "bob", "2"],
            "3341a6cb-739f-86d7-b467-b7a6697ea872",
        ),
        (
            "admission notice",
            ids::admission_notice_id(G1, "bob", 3),
            vec!["jazz.e2ee.admission-notice.v1", G1, "bob", "3"],
            "a3f2b36b-d96a-8cfd-8f7f-f76b551a701d",
        ),
        (
            "key request answer",
            ids::key_request_answer_id(R1),
            vec!["jazz.e2ee.key-request-answer.v1", R1],
            "7b2495e3-7747-819f-a09c-65567c132b7e",
        ),
        (
            "space grant",
            ids::space_grant_id(&space, staged),
            vec![
                "jazz.e2ee.space-grant-id.v1",
                &space,
                "manager",
                "add",
                "account",
                "alice",
                "0",
            ],
            "d0f306db-f7f6-81bc-bad7-c99ce5d5e1a2",
        ),
        (
            "group membership",
            ids::group_membership_id(G1, StagedChange { n: 12, ..staged }),
            vec![
                "jazz.e2ee.group-membership-id.v1",
                G1,
                "manager",
                "add",
                "account",
                "alice",
                "12",
            ],
            "f2836b75-48c9-80bd-8306-e571d645fd42",
        ),
    ];
    for (label, derived, parts, pinned) in cases {
        let independent = v8(&sodium_sha256(json::strings(&parts).as_bytes()));
        assert_eq!(derived, independent, "{label}");
        assert_eq!(derived, pinned, "{label}");
        assert!(ids::is_id(&derived, IdForm::Derived));
        assert!(!ids::is_id(&derived, IdForm::Random));
    }
    // Generation 0's identity is the account itself.
    assert_eq!(ids::account_identity_id("alice", 0), "alice");
}

/// UUID forms: random IDs are versions 4 and 7, derived ones version 8,
/// always lowercase with the RFC variant.
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

/// Group epoch key pairs (§3.1) from epoch key `20`–`3f`, cross-checked with
/// libsodium; and the recipient commitment with salt `00`–`1f`.
#[test]
fn group_keys_and_commitment() {
    let epoch_key: [u8; 32] = core::array::from_fn(|i| 0x20 + i as u8);
    let (sealing, signing) =
        derive::group_epoch_key_pairs(&key_seal::Builtin, &signature::Builtin, &epoch_key).unwrap();
    let kem_ikm = sodium_keyed(
        &epoch_key,
        &[
            b"jazz.e2ee.group-kem-key.v1\0".as_slice(),
            &env("jazz.wrap.hpke-x25519", &[]),
        ]
        .concat(),
    );
    let sign_seed = sodium_keyed(
        &epoch_key,
        &[
            b"jazz.e2ee.group-sign-key.v1\0".as_slice(),
            &env("jazz.sodium.sign", &[]),
        ]
        .concat(),
    );
    assert_eq!(
        sealing.public,
        key_seal::Builtin.derive_key_pair(&kem_ikm).unwrap().public
    );
    assert_eq!(signing.public.bytes, sodium_sign_public(&sign_seed));
    assert_eq!(
        to_hex(&sealing.public.bytes),
        "9101836ab4383917d38ffc10bbdd92fdce579c2aa058e1027b7b5a093421bd57"
    );
    assert_eq!(
        to_hex(&signing.public.bytes),
        "9d89b966d907e4bb3c54c2baa2b8aa9c4979a5d056440e3aad2cdc48bf62310c"
    );

    let salt: [u8; 32] = core::array::from_fn(|i| i as u8);
    let membership = json::encode_member_map([("alice", E1), ("bob", E2)]).unwrap();
    let revision = json::encode_revision(["space-grant:".to_owned() + R1]).unwrap();
    let commitment = derive::recipient_commitment(&salt, &membership, &revision).unwrap();
    let framed = frame_record(&[&membership, &revision]).unwrap();
    assert_eq!(
        commitment,
        sodium_keyed(
            &salt,
            &[b"jazz.e2ee.recipient-commitment.v1\0".as_slice(), &framed].concat()
        )
    );
    assert_eq!(
        to_hex(&commitment),
        "e7a1e4c01b42b4be0d6ab89df09316eebef4b81be89c34cc5ac63ff1941677b5"
    );
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
    let members = json::encode_member_map([("bob", E2), ("alice", E1)]).unwrap();
    assert_eq!(
        json::decode_member_map(&members).unwrap(),
        vec![
            ("alice".to_owned(), E1.to_owned()),
            ("bob".to_owned(), E2.to_owned())
        ]
    );
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

/// Pinned SHA-256 digests of each record kind's signed bytes.
const SIGNED_DIGESTS: &[(&str, &str)] = &[
    (
        "public approval",
        "48e3021a9cb5a1ae45883deb517de3682c8ba654471438e101d145ea5b8cefd2",
    ),
    (
        "recovery approval",
        "64b56b3ecc09dda2a1a1668cd767f66815b42967b263ef394fbf4eee55030ba0",
    ),
    (
        "proof",
        "d89278f913e7164ad71f8256f2ae4be8865b68c934e53725de58404db145acb5",
    ),
    (
        "private approval",
        "052745e830da1bbc4873b31f2816d205db09f7e22b9f13443d86fd00e9181536",
    ),
    (
        "delivery marker context",
        "82e0d4d5e4518187ea62e070a6dc8d0b2d70144b169516360673726a210f81bb",
    ),
    (
        "identity context",
        "4a7d74bd7bb8239ecff5cb8ebee1ca38a974a39008bc9d61661e9dedddaf1bb4",
    ),
    (
        "public successor",
        "cf9e203e93b191c5a3bf56cd52ff9bd2b4f3ff331ecbb518f1ca5dbe47d829f2",
    ),
    (
        "private successor",
        "070c4fad3a2c47261e2974d1540caa4a0a9aa4b1615c3ad34fc4eb2ff6c80a43",
    ),
    (
        "recovery root",
        "181861d75da53f4d7ae9675db94876fb9c2b3cfbfce1a1f65ce8235788802807",
    ),
    (
        "local-first account root",
        "e64e3656b255c4e638d80254fe13981bf7b95eaeb4c51a3c3fdd7cc6232c8209",
    ),
    (
        "recovery delivery context",
        "46461c62e2128155884595d677787ad0db2aadd9c2a8943aaf4f6890c8e97cc5",
    ),
    (
        "space root",
        "d4ae8cea889f24f95c1de39cb72b607cbc0916ec90c23416df4c21739f750319",
    ),
    (
        "space grant",
        "260b598fc3d3fe4bce067a1c6f257139138a4a222c9ca3bfc4e7f7dfa23eaa0a",
    ),
    (
        "space successor",
        "f8ac443fe950ba42243ffdcb88397de12956486913e84ed9b449d11a5bc4a796",
    ),
    (
        "space delivery",
        "3b3e8eb5dc3edfbd1fb60681238e13ab8c7ec9000dd7696157956ff90e05a639",
    ),
    (
        "space group delivery",
        "4b03bb289e337fd8bfa849c658fbb85aa1c4fc81dcfa32e6db283d2c8995f98a",
    ),
    (
        "space recovery delivery",
        "47ba294c8d9b194794c7b6235eda541581a46535bea9d3b8a592339730e26c89",
    ),
    (
        "space repair",
        "cf0dc7aebda173bd2bd9c3bfcce756f6bfacd33990c4f3fe45b66e58f86fdb82",
    ),
    (
        "space notice",
        "d6e88ad84e8977cf26348300f772a2eff5b7f89b9a19cbeff7c05cbe3aa61443",
    ),
    (
        "group notice",
        "80e208d7644120b725d423c529ea262146119021e2253c7eb54c64d845243cdb",
    ),
    (
        "key request",
        "1306b82a95b02755b08d9575aec28bdcf2258a9a8f19f1908fea18318e4f4f85",
    ),
    (
        "group root",
        "4ea6ec90054688bbf51ee94f952a06d8ee834db2cc329a2368ec0c64be08d9a6",
    ),
    (
        "group binding",
        "23574fc129e05a138297b5d2046bfa68597c07efd76090702986445f3ba7157a",
    ),
    (
        "group public key",
        "6bf416898b9fb758ae24204dffe1bd8ea448c8256057edb7dcf0307fe5e1c66b",
    ),
    (
        "group membership",
        "4d36206a72261b2479e3b186a8609daad68dab51c1b99d1eb7e03fdd310a08c4",
    ),
    (
        "group successor",
        "958bee2af71a8caafc8b8038cdf135d474836c03f18509bd0f85d8a866ea8307",
    ),
    (
        "group delivery",
        "58612a73c8d2ebb1f5daab4fc6bbde389849a5bc6266ee36015e128f796b81c7",
    ),
    (
        "group child delivery",
        "49f4e328d96a3395325c98994ae1231e6db0284f309cc017486c624454e76843",
    ),
    (
        "group recovery delivery",
        "5eef6289f089354083ec6c892842a3e2376d7f3bdcb1996a47cff0d341341db8",
    ),
    (
        "group repair",
        "9417094e6c2606e7990a99c9cd9090cbe4a478bf65a2bc8e201e540663913a2e",
    ),
    (
        "rotation request",
        "2906560cd53e0a2e2feae6bbf490cf171c013d5dd892b8aebd1ebf25a57b3657",
    ),
];

/// Every record kind's signed bytes equal the spec's formula, rebuilt here,
/// and their digests are pinned.
#[test]
fn signed_bytes() {
    let d = domain();
    let da = d.account("alice").unwrap();
    let ds = d.shared();
    let wrap = key_envelope::MECHANISM.id;
    let address = SpaceAddress::new(SCOPE, ROW).unwrap();
    let space = address.space_id.clone();
    let author = json::strings(&["alice", D1, E1]);
    let x = |column: &str, recipient: &str, epoch: &str| {
        ctx(
            da.as_str(),
            "jazz.e2ee.space.v1",
            SCOPE,
            ROW,
            "__e2ee_spaces",
            &space,
            column,
            epoch,
            recipient,
        )
    };
    let g = |column: &str, recipient: &str, epoch: &str| {
        ctx(
            da.as_str(),
            "jazz.e2ee.group.v1",
            "group",
            G1,
            "__e2ee_groups",
            G1,
            column,
            epoch,
            recipient,
        )
    };
    let mut cases: Vec<(&str, Vec<u8>, Vec<u8>)> = Vec::new();

    // Account records (§2.5, §2.8, §2.9).
    let approval = PublicDeviceApproval {
        id: R1.into(),
        account_id: "alice".into(),
        epoch_id: E1.into(),
        device_id: D2.into(),
        signer_id: D1.into(),
        signature: vec![],
        recovery_root_id: None,
        recovery_signature: None,
    };
    cases.push((
        "public approval",
        approval.signed_bytes(&da).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.public-device-approval.v1",
            "account",
            "alice",
            "__e2ee_public_device_approvals",
            R1,
            &format!("approval-signature:{D1}"),
            E1,
            D2,
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
        recovery_approval.signed_bytes(&da).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.recovery-device-approval.v1",
            "account",
            "alice",
            "__e2ee_public_device_approvals",
            R1,
            &format!("recovery-signature:{R2}"),
            E1,
            D2,
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
            "alice",
            "__e2ee_device_challenges",
            R1,
            column,
            E1,
            D2,
        )
    };
    cases.push((
        "proof",
        proof_signed_bytes(&da, "alice", challenge, b"proof").unwrap(),
        append(p("proof-signature"), &[b"proof"]),
    ));
    cases.push((
        "private approval",
        approval_signed_bytes(&da, "alice", challenge, D1, b"marker").unwrap(),
        append(p(&format!("approval-signature:{D1}")), &[b"marker"]),
    ));
    cases.push((
        "delivery marker context",
        delivery_marker_context(&da, "alice", challenge, b"sealed").unwrap(),
        append(p("delivery-verification"), &[b"sealed"]),
    ));
    cases.push((
        "identity context",
        identity_context(&da, "alice", "alice", E1, "envelope", D1).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.account-identity.v1",
            "account",
            "alice",
            "__e2ee_account_identities",
            "alice",
            "envelope",
            E1,
            D1,
        ),
    ));
    let membership = json::encode_id_list([D1]).unwrap();
    let public_successor = PublicAccountSuccessor {
        id: R1.into(),
        account_id: "alice".into(),
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
                "alice",
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
        account_id: "alice".into(),
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
                "alice",
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
        account_id: "alice".into(),
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
                "alice",
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
        account_id: "alice".into(),
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
                "alice",
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
        recovery_delivery_context(&da, "alice", &recovery_delivery).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.recovery.v1",
            "account",
            "alice",
            "__e2ee_recovery_deliveries",
            R1,
            "envelope",
            E1,
            R2,
        ),
    ));

    // Space records (§3.2).
    let space_root = SpaceRoot {
        id: space.clone(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        creator_account_id: "alice".into(),
        device_id: D1.into(),
        account_epoch_id: E1.into(),
        epoch_id: E2.into(),
        initial_grant_id: SpaceRoot::expected_initial_grant_id(&space, "alice"),
        mechanism: wrap.into(),
        version: 1,
        verification: env(wrap, b"marker"),
        author_envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    let initial_grant = space_root.initial_grant_id.clone();
    assert_eq!(
        initial_grant,
        ids::space_grant_id(
            &space,
            StagedChange {
                role: "manager",
                operation: "add",
                kind: "account",
                recipient_id: "alice",
                n: 0
            }
        )
    );
    cases.push((
        "space root",
        space_root.signed_bytes(&da).unwrap(),
        frame_record(&[
            &x(
                &json::strings(&["root", "alice", D1, E1, &initial_grant]),
                "",
                E2,
            ),
            &env(wrap, &env(wrap, b"marker")),
            b"sealed",
        ])
        .unwrap(),
    ));
    let grant = SpaceGrant {
        id: R1.into(),
        space_id: space.clone(),
        epoch_id: E2.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        operation: Operation::Add,
        role: Role::Member,
        recipient_kind: MemberKind::Account,
        recipient_id: "bob".into(),
        recipient_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "space grant",
        grant.signed_bytes(&da, &address).unwrap(),
        x(
            &json::strings(&["grant", R1, "alice", D1, E1, "add", "member"]),
            &json::strings(&["account", "bob", E1]),
            E2,
        ),
    ));
    let space_successor = SpaceSuccessor {
        id: R1.into(),
        space_id: space.clone(),
        predecessor: E1.into(),
        epoch_id: E2.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        commitment: vec![9; 32],
        verification: b"marker".to_vec(),
        history: b"history".to_vec(),
        author_envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "space successor",
        space_successor.signed_bytes(&da, &address).unwrap(),
        append(
            x(&json::strings(&["successor", R1, "signature"]), &author, E2),
            &[E1.as_bytes(), &[9; 32], b"marker", b"history", b"sealed"],
        ),
    ));
    let delivery = SpaceDelivery {
        id: R1.into(),
        space_id: space.clone(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_account_id: "bob".into(),
        recipient_device_id: D2.into(),
        recipient_epoch_id: E1.into(),
        envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "space delivery",
        delivery.signed_bytes(&da, &address).unwrap(),
        frame_record(&[
            &x(
                &json::strings(&["delivery", R1, "alice", D1, E1]),
                &json::strings(&["bob", D2, E1]),
                E2,
            ),
            b"sealed",
        ])
        .unwrap(),
    ));
    let group_delivery = SpaceGroupDelivery {
        id: R1.into(),
        space_id: space.clone(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_group_id: G1.into(),
        recipient_group_epoch_id: E1.into(),
        envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "space group delivery",
        group_delivery.signed_bytes(&da, &address).unwrap(),
        frame_record(&[
            &x(
                &json::strings(&["group-delivery", R1, "alice", D1, E1]),
                &json::strings(&[G1, E1]),
                E2,
            ),
            b"sealed",
        ])
        .unwrap(),
    ));
    let space_recovery = SpaceRecoveryDelivery {
        id: R1.into(),
        space_id: space.clone(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_account_id: "bob".into(),
        recovery_root_id: R2.into(),
        recipient_epoch_id: E1.into(),
        envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "space recovery delivery",
        space_recovery.signed_bytes(&da, &address).unwrap(),
        frame_record(&[
            &x(
                &json::strings(&["recovery-delivery", R1, "alice", D1, E1]),
                &json::strings(&["bob", R2, E1]),
                E2,
            ),
            b"sealed",
        ])
        .unwrap(),
    ));
    let repair = SpaceRepair {
        id: R1.into(),
        space_id: space.clone(),
        epoch_id: E2.into(),
        delivery_kind: DeliveryKind::Recovery,
        delivery_id: R2.into(),
        account_id: "bob".into(),
        device_id: D2.into(),
        account_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "space repair",
        repair.signed_bytes(&da, &address).unwrap(),
        x(
            &json::strings(&["repair", R1, "recovery", R2, "bob", D2]),
            E1,
            E2,
        ),
    ));
    let notice_id = ids::reset_notice_id(&space, "bob", 1);
    let notice = Notice {
        id: notice_id.clone(),
        scope_id: space.clone(),
        epoch_id: E2.into(),
        kind: NoticeKind::ResetApproved,
        account_id: "bob".into(),
        generation: 1,
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "space notice",
        notice
            .signed_bytes(&da, NoticeScope::Space(&address))
            .unwrap(),
        x(
            &json::strings(&["notice", &notice_id, "reset-approved", "alice", D1, E1]),
            &json::strings(&["bob", "1"]),
            E2,
        ),
    ));
    let admitted_id = ids::admission_notice_id(G1, "bob", 2);
    let group_notice = Notice {
        id: admitted_id.clone(),
        scope_id: G1.into(),
        kind: NoticeKind::Admitted,
        generation: 2,
        ..notice.clone()
    };
    cases.push((
        "group notice",
        group_notice
            .signed_bytes(&da, NoticeScope::Group(G1))
            .unwrap(),
        g(
            &json::strings(&["notice", &admitted_id, "admitted", "alice", D1, E1]),
            &json::strings(&["bob", "2"]),
            E2,
        ),
    ));
    let request = KeyRequest {
        id: R1.into(),
        scope_kind: ScopeKind::Space,
        scope_id: space.clone(),
        epoch_id: E2.into(),
        account_id: "bob".into(),
        device_id: D2.into(),
        account_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "key request",
        request.signed_bytes(&da).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.key-request.v1",
            "space",
            &space,
            "__e2ee_key_requests",
            R1,
            &json::strings(&["bob", D2]),
            E2,
            E1,
        ),
    ));

    // Group records (§3.1).
    let group_root = GroupRoot {
        id: G1.into(),
        account_id: "alice".into(),
        device_id: D1.into(),
        account_epoch_id: E1.into(),
        epoch_id: E2.into(),
        mechanism: wrap.into(),
        version: 1,
        verification: env(wrap, b"marker"),
        signature: vec![],
    };
    cases.push((
        "group root",
        group_root.signed_bytes(&da).unwrap(),
        frame_record(&[
            &g(&json::strings(&["root", "alice", D1, E1]), "", E2),
            &env(wrap, &env(wrap, b"marker")),
        ])
        .unwrap(),
    ));
    let binding = GroupBinding {
        id: G1.into(),
        group_id: G1.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "group binding",
        binding.signed_bytes(&da, E2).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.group-binding.v1",
            "group",
            G1,
            "__e2ee_group_bindings",
            G1,
            &json::strings(&[SCOPE, ROW]),
            E2,
            &author,
        ),
    ));
    let public_key = GroupPublicKey {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        predecessor_epoch_id: E1.into(),
        proof_epoch_id: E1.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        keys: keys(),
        key_signature: vec![1],
        predecessor_signature: vec![1],
        author_signature: vec![1],
    };
    cases.push((
        "group public key",
        public_key.signed_bytes(&ds).unwrap(),
        append(
            ctx(
                ds.as_str(),
                "jazz.e2ee.group-public-key.v1",
                "group",
                G1,
                "__e2ee_group_public_keys",
                R1,
                &json::strings(&[E1, E1, "alice", D1, E1]),
                E2,
                "",
            ),
            &[
                &env("jazz.wrap.hpke-x25519", &[0x11; 32]),
                &env("jazz.sodium.sign", &[0x22; 32]),
            ],
        ),
    ));
    let membership_record = GroupMembership {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        operation: Operation::Remove,
        role: Role::Manager,
        member_kind: MemberKind::Group,
        member_id: R2.into(),
        signature: vec![],
    };
    cases.push((
        "group membership",
        membership_record.signed_bytes(&da).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.group-membership.v1",
            "group",
            G1,
            "__e2ee_group_membership",
            R1,
            &json::strings(&["remove", "manager", "alice", D1, E1]),
            E2,
            &json::strings(&["group", R2]),
        ),
    ));
    let group_successor = GroupSuccessor {
        id: R1.into(),
        group_id: G1.into(),
        predecessor: E1.into(),
        proof_epoch_id: E1.into(),
        epoch_id: E2.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        commitment: vec![9; 32],
        verification: b"marker".to_vec(),
        history: b"history".to_vec(),
        author_envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "group successor",
        group_successor.signed_bytes(&da).unwrap(),
        append(
            ctx(
                da.as_str(),
                "jazz.e2ee.group-successor.v1",
                "group",
                G1,
                "__e2ee_group_successors",
                R1,
                "signature",
                E2,
                &author,
            ),
            &[
                E1.as_bytes(),
                E1.as_bytes(),
                &[9; 32],
                b"marker",
                b"history",
                b"sealed",
            ],
        ),
    ));
    let gd = GroupDelivery {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_account_id: "bob".into(),
        recipient_device_id: D2.into(),
        recipient_epoch_id: E1.into(),
        envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "group delivery",
        gd.signed_bytes(&da).unwrap(),
        frame_record(&[
            &g(
                &json::strings(&["delivery", R1, "alice", D1, E1]),
                &json::strings(&["bob", D2, E1]),
                E2,
            ),
            b"sealed",
        ])
        .unwrap(),
    ));
    let child = GroupChildDelivery {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_group_id: R2.into(),
        recipient_group_epoch_id: E1.into(),
        envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "group child delivery",
        child.signed_bytes(&da).unwrap(),
        frame_record(&[
            &g(
                &json::strings(&["child-delivery", R1, "alice", D1, E1]),
                &json::strings(&[R2, E1]),
                E2,
            ),
            b"sealed",
        ])
        .unwrap(),
    ));
    let group_recovery = GroupRecoveryDelivery {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_account_id: "bob".into(),
        recovery_root_id: R2.into(),
        recipient_epoch_id: E1.into(),
        envelope: b"sealed".to_vec(),
        signature: vec![],
    };
    cases.push((
        "group recovery delivery",
        group_recovery.signed_bytes(&da).unwrap(),
        frame_record(&[
            &ctx(
                da.as_str(),
                "jazz.e2ee.group-recovery.v1",
                "group",
                G1,
                "__e2ee_group_recovery_deliveries",
                R1,
                &author,
                E2,
                &json::strings(&["bob", R2, E1]),
            ),
            b"sealed",
        ])
        .unwrap(),
    ));
    let group_repair = GroupRepair {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        delivery_id: R2.into(),
        account_id: "bob".into(),
        device_id: D2.into(),
        account_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "group repair",
        group_repair.signed_bytes(&da).unwrap(),
        ctx(
            da.as_str(),
            "jazz.e2ee.group-repair.v1",
            "group",
            G1,
            "__e2ee_group_repairs",
            R1,
            &json::strings(&[R2, "bob", D2]),
            E2,
            E1,
        ),
    ));
    let covers = String::from_utf8(json::encode_id_list([R2]).unwrap()).unwrap();
    let rotation = RotationRequest {
        id: R1.into(),
        source_group_id: G1.into(),
        source_epoch_id: E2.into(),
        cause_kind: CauseKind::Rotation,
        cause_id: R2.into(),
        covers: covers.clone(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        signature: vec![],
    };
    cases.push((
        "rotation request",
        rotation.signed_bytes(&ds).unwrap(),
        append(
            ctx(
                ds.as_str(),
                "jazz.e2ee.rotation-request.v1",
                "group",
                G1,
                "__e2ee_rotation_requests",
                R1,
                &json::strings(&["rotation", R2, E2, "alice", D1, E1]),
                E1,
                "",
            ),
            &[covers.as_bytes()],
        ),
    ));

    for (name, actual, expected) in &cases {
        assert_eq!(to_hex(actual), to_hex(expected), "{name}");
    }
    assert_eq!(cases.len(), SIGNED_DIGESTS.len());
    for ((name, actual, _), (pinned_name, digest)) in cases.iter().zip(SIGNED_DIGESTS) {
        assert_eq!(name, pinned_name);
        assert_eq!(to_hex(&hash::sha256(actual)), *digest, "{name}");
    }
}

/// Each row's local invariants: ID forms, enum values, target columns,
/// derived IDs and shapes.
#[test]
fn row_validation() {
    let invalid =
        |result: Result<(), ProtocolError>| matches!(result, Err(ProtocolError::InvalidRecord(_)));
    let approval = PublicDeviceApproval {
        id: R1.into(),
        account_id: "alice".into(),
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
            id: ids::key_request_answer_id(R1),
            ..approval.clone()
        }
        .validate()
    ));
    assert!(invalid(
        PublicDeviceApproval {
            account_id: String::new(),
            ..approval.clone()
        }
        .validate()
    ));

    let successor = PublicAccountSuccessor {
        id: R1.into(),
        account_id: "alice".into(),
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
    assert!(Operation::parse("Add").is_err());
    assert_eq!(
        NoticeKind::parse("reset-approved").unwrap(),
        NoticeKind::ResetApproved
    );

    let identity = AccountIdentity {
        id: "alice".into(),
        ledger_version: 1,
        generation: 0,
        device_id: D1.into(),
        epoch_id: E1.into(),
        envelope: vec![1],
        verification: vec![1],
    };
    assert!(identity.validate("alice").is_ok());
    assert!(invalid(
        AccountIdentity {
            generation: 1,
            ..identity.clone()
        }
        .validate("alice")
    ));
    assert!(
        AccountIdentity {
            generation: 1,
            id: ids::account_identity_id("alice", 1),
            ..identity
        }
        .validate("alice")
        .is_ok()
    );

    let address = SpaceAddress::new(SCOPE, ROW).unwrap();
    let notice = Notice {
        id: ids::admission_notice_id(&address.space_id, "bob", 1),
        scope_id: address.space_id.clone(),
        epoch_id: E1.into(),
        kind: NoticeKind::Admitted,
        account_id: "bob".into(),
        generation: 1,
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        signature: vec![1],
    };
    assert!(notice.validate(NoticeScope::Space(&address)).is_ok());
    assert!(invalid(
        Notice {
            kind: NoticeKind::ResetApproved,
            ..notice.clone()
        }
        .validate(NoticeScope::Space(&address))
    ));
    assert!(invalid(notice.validate(NoticeScope::Group(G1))));
    // Neither kind names generation 0.
    assert!(invalid(
        Notice {
            id: ids::admission_notice_id(&address.space_id, "bob", 0),
            generation: 0,
            ..notice.clone()
        }
        .validate(NoticeScope::Space(&address))
    ));
    // A reset approval is by another manager, never by the approved
    // account; an admission notice may be the account's own.
    let approval = Notice {
        id: ids::reset_notice_id(&address.space_id, "bob", 1),
        kind: NoticeKind::ResetApproved,
        ..notice.clone()
    };
    assert!(approval.validate(NoticeScope::Space(&address)).is_ok());
    let self_approval = Notice {
        author_account_id: "bob".into(),
        ..approval
    };
    assert_eq!(
        self_approval.validate(NoticeScope::Space(&address)),
        Err(ProtocolError::InvalidRecord(
            "reset approval by the approved account"
        ))
    );
    assert!(
        Notice {
            author_account_id: "bob".into(),
            ..notice.clone()
        }
        .validate(NoticeScope::Space(&address))
        .is_ok()
    );

    let request = RotationRequest {
        id: R1.into(),
        source_group_id: G1.into(),
        source_epoch_id: String::new(),
        cause_kind: CauseKind::Remove,
        cause_id: R2.into(),
        covers: String::new(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        signature: vec![1],
    };
    assert!(request.validate().is_ok());
    assert!(invalid(
        RotationRequest {
            covers: "[]".into(),
            ..request.clone()
        }
        .validate()
    ));
    assert!(invalid(
        RotationRequest {
            cause_kind: CauseKind::Coverage,
            ..request.clone()
        }
        .validate()
    ));
    assert!(
        RotationRequest {
            cause_kind: CauseKind::Coverage,
            source_epoch_id: E1.into(),
            covers: "[]".into(),
            ..request
        }
        .validate()
        .is_ok()
    );

    let root_key = GroupPublicKey {
        id: G1.into(),
        group_id: G1.into(),
        epoch_id: E1.into(),
        predecessor_epoch_id: String::new(),
        proof_epoch_id: String::new(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        keys: keys(),
        key_signature: vec![1],
        predecessor_signature: vec![],
        author_signature: vec![1],
    };
    assert!(root_key.validate().is_ok());
    assert!(invalid(
        GroupPublicKey {
            id: R1.into(),
            ..root_key.clone()
        }
        .validate()
    ));
    assert!(invalid(
        GroupPublicKey {
            predecessor_signature: vec![1],
            ..root_key.clone()
        }
        .validate()
    ));

    let space_root_id = ids::space_id(SCOPE, ROW);
    assert_eq!(address.space_id, space_root_id);
    let grant = SpaceGrant {
        id: R1.into(),
        space_id: ids::space_id(SCOPE, D1),
        epoch_id: E1.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        operation: Operation::Add,
        role: Role::Member,
        recipient_kind: MemberKind::Group,
        recipient_id: "bob".into(),
        recipient_epoch_id: E1.into(),
        signature: vec![1],
    };
    assert!(invalid(grant.validate(&address)));
    assert!(invalid(
        SpaceGrant {
            space_id: space_root_id.clone(),
            ..grant.clone()
        }
        .validate(&address)
    ));
    assert!(
        SpaceGrant {
            space_id: space_root_id,
            recipient_kind: MemberKind::Account,
            ..grant
        }
        .validate(&address)
        .is_ok()
    );

    let recipients = SuccessorRecipients {
        id: R1.into(),
        scope_id: G1.into(),
        salt: vec![0; 31],
        membership: json::encode_member_map([("alice", E1)]).unwrap(),
        revision: b"[]".to_vec(),
    };
    assert!(invalid(recipients.validate()));
    let recipients = SuccessorRecipients {
        salt: vec![0; 32],
        ..recipients
    };
    assert_eq!(
        recipients.commitment().unwrap(),
        derive::recipient_commitment(&[0; 32], &recipients.membership, &recipients.revision)
            .unwrap()
    );

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

/// Independent, byte-by-byte constructions of three signed byte strings,
/// with no crate encoder: `JE2C 01`, nine u32be-length-prefixed fields, then
/// u32be-length-prefixed framed parts. The public approval is pinned in
/// full.
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
    let da = domain().account("alice").unwrap();
    let app = format!("[\"{APP}\",\"prod\",\"alice\"]");
    assert_eq!(da.as_str(), app);

    let approval = PublicDeviceApproval {
        id: R1.into(),
        account_id: "alice".into(),
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
        "alice",
        "__e2ee_public_device_approvals",
        R1,
        &column,
        E1,
        D2,
    ]);
    assert_eq!(approval.signed_bytes(&da).unwrap(), expected);
    assert_eq!(to_hex(&expected), PUBLIC_APPROVAL_HEX);

    let root = RecoveryRoot {
        id: R2.into(),
        account_id: "alice".into(),
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
        "alice",
        "__e2ee_recovery_roots",
        R2,
        &column,
        E1,
        "",
    ]));
    expected.extend(field(&signing_env));
    expected.extend(field(&sealing_env));
    assert_eq!(root.signed_bytes(&da).unwrap(), expected);

    let shared = domain().shared();
    let covers = format!("[\"{R2}\"]");
    let rotation = RotationRequest {
        id: R1.into(),
        source_group_id: G1.into(),
        source_epoch_id: E2.into(),
        cause_kind: CauseKind::Rotation,
        cause_id: R2.into(),
        covers: covers.clone(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        signature: vec![],
    };
    let column = format!("[\"rotation\",\"{R2}\",\"{E2}\",\"alice\",\"{D1}\",\"{E1}\"]");
    let mut expected = context([
        &format!("[\"{APP}\",\"prod\"]"),
        "jazz.e2ee.rotation-request.v1",
        "group",
        G1,
        "__e2ee_rotation_requests",
        R1,
        &column,
        E1,
        "",
    ]);
    expected.extend(field(covers.as_bytes()));
    assert_eq!(rotation.signed_bytes(&shared).unwrap(), expected);
}

const PUBLIC_APPROVAL_HEX: &str = "4a45324301000000375b2230663865336134632d316432622d346335642d386536662d376138623963306431653266222c2270726f64222c22616c696365225d000000236a617a7a2e653265652e7075626c69632d6465766963652d617070726f76616c2e7631000000076163636f756e7400000005616c6963650000001e5f5f653265655f7075626c69635f6465766963655f617070726f76616c730000002461316131613161312d613161312d346131612d396131612d61316131613161316131613100000037617070726f76616c2d7369676e61747572653a31313131313131312d313131312d343131312d383131312d3131313131313131313131310000002465316531653165312d653165312d346531652d386531652d6531653165316531653165310000002432323232323232322d323232322d343232322d383232322d323232323232323232323232";

/// Contexts and signed bytes come from a record's coordinates before its
/// outputs exist: a delivery's sealing context needs no envelope, a public
/// key record's signed bytes need none of the signatures over them. Only a
/// complete record validates.
#[test]
fn contexts_before_outputs() {
    let da = domain().account("alice").unwrap();
    let ds = domain().shared();
    let address = SpaceAddress::new(SCOPE, ROW).unwrap();
    let mut delivery = SpaceDelivery {
        id: R1.into(),
        space_id: address.space_id.clone(),
        epoch_id: E2.into(),
        sender_account_id: "alice".into(),
        sender_device_id: D1.into(),
        sender_epoch_id: E1.into(),
        recipient_account_id: "bob".into(),
        recipient_device_id: D2.into(),
        recipient_epoch_id: E1.into(),
        envelope: Vec::new(),
        signature: Vec::new(),
    };
    let context = delivery.seal_context(&da, &address).unwrap();
    assert!(delivery.signed_bytes(&da, &address).is_err());
    assert!(delivery.validate(&address).is_err());
    delivery.envelope = b"sealed".to_vec();
    assert_eq!(delivery.seal_context(&da, &address).unwrap(), context);
    let bytes = delivery.signed_bytes(&da, &address).unwrap();
    assert!(delivery.validate(&address).is_err());
    delivery.signature = vec![1];
    delivery.validate(&address).unwrap();
    assert_eq!(delivery.signed_bytes(&da, &address).unwrap(), bytes);

    let mut successor = SpaceSuccessor {
        id: R1.into(),
        space_id: address.space_id.clone(),
        predecessor: E1.into(),
        epoch_id: E2.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        commitment: Vec::new(),
        verification: Vec::new(),
        history: Vec::new(),
        author_envelope: Vec::new(),
        signature: Vec::new(),
    };
    successor.author_envelope_context(&da, &address).unwrap();
    successor.history_context(&da, &address).unwrap();
    successor.marker_context(&da, &address).unwrap();
    assert!(successor.signed_bytes(&da, &address).is_err());
    successor.commitment = vec![9; 32];
    successor.verification = b"marker".to_vec();
    successor.history = b"history".to_vec();
    successor.author_envelope = b"sealed".to_vec();
    successor.signed_bytes(&da, &address).unwrap();
    assert!(successor.validate(&address).is_err());

    let mut public_key = GroupPublicKey {
        id: R1.into(),
        group_id: G1.into(),
        epoch_id: E2.into(),
        predecessor_epoch_id: E1.into(),
        proof_epoch_id: E1.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        keys: keys(),
        key_signature: Vec::new(),
        predecessor_signature: Vec::new(),
        author_signature: Vec::new(),
    };
    let bytes = public_key.signed_bytes(&ds).unwrap();
    assert!(public_key.validate().is_err());
    public_key.key_signature = vec![1];
    public_key.author_signature = vec![1];
    assert!(public_key.validate().is_err());
    public_key.predecessor_signature = vec![1];
    public_key.validate().unwrap();
    assert_eq!(public_key.signed_bytes(&ds).unwrap(), bytes);

    let group_successor = GroupSuccessor {
        id: R1.into(),
        group_id: G1.into(),
        predecessor: E1.into(),
        proof_epoch_id: E1.into(),
        epoch_id: E2.into(),
        author_account_id: "alice".into(),
        author_device_id: D1.into(),
        author_epoch_id: E1.into(),
        commitment: Vec::new(),
        verification: Vec::new(),
        history: Vec::new(),
        author_envelope: Vec::new(),
        signature: Vec::new(),
    };
    group_successor.context(&da, "history").unwrap();
    assert!(
        GroupSuccessor {
            id: G1.into(),
            ..group_successor.clone()
        }
        .context(&da, "history")
        .is_err()
    );

    let space = address.space_id.clone();
    let root = SpaceRoot {
        id: space.clone(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        creator_account_id: "alice".into(),
        device_id: D1.into(),
        account_epoch_id: E1.into(),
        epoch_id: E2.into(),
        initial_grant_id: SpaceRoot::expected_initial_grant_id(&space, "alice"),
        mechanism: key_envelope::MECHANISM.id.into(),
        version: 1,
        verification: Vec::new(),
        author_envelope: Vec::new(),
        signature: Vec::new(),
    };
    root.marker_context(&da).unwrap();
    root.author_envelope_context(&da).unwrap();
    // The marker must be an envelope of the root's own mechanism.
    let other_mechanism = SpaceRoot {
        verification: env("jazz.sodium.cell", b"marker"),
        author_envelope: b"sealed".to_vec(),
        ..root.clone()
    };
    assert!(other_mechanism.signed_bytes(&da).is_err());
    let random_grant = SpaceRoot {
        initial_grant_id: R1.into(),
        ..root
    };
    assert!(random_grant.marker_context(&da).is_err());
}
