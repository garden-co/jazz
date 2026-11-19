//! The device document and floor records of the private store
//! (`E2EE_PROTOCOL.md` §6.1, §6.3a): the canonical encodings, pinned by
//! fixtures written by hand from the specification; the approval nonce's
//! two states; and the checks a floor record passes on load.

mod common;

use common::TestRandom;
use common::protocol::{ALICE, Env};
use jazz_e2ee::Zeroizing;
use jazz_e2ee::agility::{MechanismId, PublicKey};
use jazz_e2ee::protocol::Position;
use jazz_e2ee::protocol::account::AccountFloor;
use jazz_e2ee::protocol::content::local::{
    DeviceSecrets, FloorRecord, StoredApproval, StoredDevice, decode_device_document,
};
use jazz_e2ee::protocol::content::store::{Rejection, StoreOwner, UnrecognisedDocument};
use jazz_e2ee::protocol::records::KeyColumns;
use jazz_e2ee::protocol::records::account::nonce_commitment;

const DEVICE: &str = "d0000000-0000-4000-8000-000000000000";

fn fixture_device(approval: StoredApproval) -> StoredDevice {
    StoredDevice {
        scope: "s".into(),
        id: DEVICE.into(),
        sealing: PublicKey {
            mechanism: MechanismId::new("jazz.seal.hpke-x25519", 1),
            bytes: vec![1, 2],
        },
        protected_sealing_key: vec![3],
        signing: PublicKey {
            mechanism: MechanismId::new("jazz.sodium.sign", 1),
            bytes: vec![4],
        },
        protected_signing_key: vec![5, 6],
        approval_commitment: [7; 32],
        approval,
    }
}

fn sevens() -> String {
    vec!["7"; 32].join(",")
}

/// Contract: the device document is the canonical JSON the specification
/// gives, with the approval nonce as a protected blob until it is spent and
/// only `spent` after; it decodes back to the same device, and any other
/// format or shape is refused rather than read.
#[test]
fn device_documents_are_pinned_and_strict() {
    let unspent = fixture_device(StoredApproval::Unspent {
        protected_nonce: vec![9, 8],
    });
    let expected = format!(
        concat!(
            r#"{{"devices":[{{"approval":{{"commitment":[{sevens}],"#,
            r#""format":"jazz-e2ee-approval-nonce-v1","nonce":[9,8]}},"#,
            r#""id":"d0000000-0000-4000-8000-000000000000","#,
            r#""mechanism":"jazz.seal.hpke-x25519","privateKey":[3],"publicKey":[1,2],"#,
            r#""scope":"s","signingMechanism":"jazz.sodium.sign","#,
            r#""signingPrivateKey":[5,6],"signingPublicKey":[4],"signingVersion":1,"#,
            r#""version":1}}],"format":"jazz-e2ee-local-devices-v3"}}"#
        ),
        sevens = sevens()
    );
    let encoded = unspent.encode_document().unwrap();
    assert_eq!(String::from_utf8(encoded.clone()).unwrap(), expected);
    assert_eq!(decode_device_document(&encoded).unwrap(), Some(unspent));

    let spent = fixture_device(StoredApproval::Spent);
    let encoded = String::from_utf8(spent.encode_document().unwrap()).unwrap();
    assert!(encoded.contains(r#""format":"jazz-e2ee-approval-nonce-v1","spent":true}"#));
    assert!(!encoded.contains("nonce\":"));
    assert_eq!(
        decode_device_document(encoded.as_bytes()).unwrap(),
        Some(spent)
    );

    let empty = br#"{"devices":[],"format":"jazz-e2ee-local-devices-v3"}"#;
    assert_eq!(decode_device_document(empty).unwrap(), None);

    // The earlier format, without approval state, is another format.
    let v2 = expected
        .replace("local-devices-v3", "local-devices-v2")
        .replace(
            &format!(
                r#""approval":{{"commitment":[{}],"format":"jazz-e2ee-approval-nonce-v1","nonce":[9,8]}},"#,
                sevens()
            ),
            "",
        );
    for refused in [
        v2,
        expected.replace("\"nonce\":[9,8]", "\"nonce\":[9,8],\"spent\":true"),
        expected.replace(",\"nonce\":[9,8]", ""),
        expected.replace("{\"devices\"", " {\"devices\""),
        expected.replace("1}],\"format\"", "1},{}],\"format\""),
        "not json".to_owned(),
        // Not canonical: an escape where the character itself is written,
        // and a number with a leading zero.
        expected.replace("\"scope\":\"s\"", "\"scope\":\"\\u0073\""),
        expected.replace("\"signingVersion\":1", "\"signingVersion\":01"),
    ] {
        assert_ne!(refused, expected, "each case changes the document");
        assert_eq!(
            decode_device_document(refused.as_bytes()),
            Err(UnrecognisedDocument),
            "{refused}"
        );
    }
}

/// Contract: a stored device becomes the local device only with an
/// approval nonce that opens its commitment while unspent, and with no
/// nonce once spent; a spent device can answer no challenge.
#[test]
fn a_device_s_approval_nonce_must_match_its_state() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(7);
    let device = env.device(&mut rng);
    let nonce = device.approval_nonce.as_ref().unwrap().0.to_vec();
    let stored = |approval| StoredDevice {
        scope: env.application().as_str().to_owned(),
        id: device.id.clone(),
        sealing: device.sealing.public.clone(),
        protected_sealing_key: vec![],
        signing: device.signing.public.clone(),
        protected_signing_key: vec![],
        approval_commitment: device.approval_commitment,
        approval,
    };
    let secrets = |approval_nonce: Option<Vec<u8>>| DeviceSecrets {
        sealing: device.sealing.secret.clone(),
        signing: device.signing.secret.clone(),
        approval_nonce: approval_nonce.map(Zeroizing::new),
    };
    let unspent = || StoredApproval::Unspent {
        protected_nonce: vec![0],
    };

    let local = stored(unspent())
        .into_local(secrets(Some(nonce.clone())))
        .unwrap();
    assert_eq!(local.id, device.id);
    assert_eq!(local.approval_commitment, device.approval_commitment);
    assert!(local.approval_nonce.is_some());

    let spent = stored(StoredApproval::Spent)
        .into_local(secrets(None))
        .unwrap();
    assert!(spent.approval_nonce.is_none());

    let mut other = nonce.clone();
    other[0] ^= 1;
    assert_ne!(
        nonce_commitment(&device.id, &other).unwrap(),
        device.approval_commitment
    );
    for (approval, given) in [
        (unspent(), Some(other)),
        (unspent(), Some(vec![1; 31])),
        (unspent(), None),
        (StoredApproval::Spent, Some(nonce)),
    ] {
        assert!(stored(approval).into_local(secrets(given)).is_err());
    }
}

fn fixture_floor() -> AccountFloor {
    AccountFloor {
        through: Position(9_007_199_254_740_993),
        generation: 1,
        root_id: "10000000-0000-4000-8000-000000000001".into(),
        root_keys: KeyColumns {
            public_key: vec![1],
            mechanism: "jazz.seal.hpke-x25519".into(),
            version: 1,
            signing_public_key: vec![2],
            signing_mechanism: "jazz.sodium.sign".into(),
            signing_version: 1,
        },
        epoch_id: "e0000000-0000-4000-8000-000000000000".into(),
        epoch_position: Position(4),
        active_device: Some(DEVICE.into()),
    }
}

/// Contract: a floor record is the canonical JSON of §6.3a, positions as
/// decimal strings; it is signed by the device that keeps it and loads
/// only for that device and the account it is stored for, unaltered.
///
/// ```text
/// alice's device ──seal_floor──► record ──load_floor──► floor
///                                  │
///                                  ├── another device ──✗ binding
///                                  ├── another account ──✗ binding
///                                  └── altered ──✗ signature
/// ```
#[test]
fn floor_records_are_pinned_signed_and_bound() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(11);
    let device = env.device(&mut rng);
    let owner = StoreOwner {
        registry: &env.registry,
        domain: &env.domain,
        account_id: ALICE,
        device: &device,
    };
    let record = owner.seal_floor(ALICE, &fixture_floor()).unwrap();
    let encoded = String::from_utf8(record.encode().unwrap()).unwrap();
    let binding = owner.binding().unwrap();
    let application = binding.application.replace('"', "\\\"");
    let signature = record
        .signature
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let expected = format!(
        concat!(
            r#"{{"account":"a11ce000-0000-5000-8000-00000000a11c","#,
            r#""activeDevice":"d0000000-0000-4000-8000-000000000000","#,
            r#""binding":{{"application":"{application}","device":"{device}","keys":"{keys}"}},"#,
            r#""epochId":"e0000000-0000-4000-8000-000000000000","epochPosition":"4","#,
            r#""format":"jazz-e2ee-floor-v1","generation":"1","#,
            r#""rootId":"10000000-0000-4000-8000-000000000001","#,
            r#""rootKeys":{{"mechanism":"jazz.seal.hpke-x25519","publicKey":[1],"#,
            r#""signingMechanism":"jazz.sodium.sign","signingPublicKey":[2],"#,
            r#""signingVersion":1,"version":1}},"#,
            r#""signature":[{signature}],"through":"9007199254740993"}}"#
        ),
        application = application,
        device = device.id,
        keys = binding.keys,
        signature = signature,
    );
    assert_eq!(encoded, expected);
    let decoded = FloorRecord::decode(encoded.as_bytes()).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(
        owner.load_floor(ALICE, &decoded).unwrap(),
        Ok(fixture_floor())
    );

    // No active device is a floor of another account.
    let mut other_account = fixture_floor();
    other_account.active_device = None;
    let sealed = owner.seal_floor(ALICE, &other_account).unwrap();
    assert!(
        !String::from_utf8(sealed.encode().unwrap())
            .unwrap()
            .contains("activeDevice")
    );
    assert_eq!(owner.load_floor(ALICE, &sealed).unwrap(), Ok(other_account));

    let other = env.device(&mut rng);
    let other_owner = StoreOwner {
        device: &other,
        ..owner
    };
    assert_eq!(
        other_owner.load_floor(ALICE, &record).unwrap(),
        Err(Rejection::Binding)
    );
    let mut renamed = record.clone();
    renamed.account_id = "b0b00000-0000-5000-8000-00000000b0b0".into();
    assert_eq!(
        owner
            .load_floor("b0b00000-0000-5000-8000-00000000b0b0", &renamed)
            .unwrap(),
        Err(Rejection::Signature)
    );
    assert_eq!(
        owner
            .load_floor("b0b00000-0000-5000-8000-00000000b0b0", &record)
            .unwrap(),
        Err(Rejection::Binding)
    );
    let mut rolled_back = record.clone();
    rolled_back.floor.through = Position(1);
    assert_eq!(
        owner.load_floor(ALICE, &rolled_back).unwrap(),
        Err(Rejection::Signature)
    );

    for refused in [
        expected.replace("\"generation\":\"1\"", "\"generation\":1"),
        expected.replace("\"generation\":\"1\"", "\"generation\":\"01\""),
        expected.replace("floor-v1", "floor-v2"),
        expected.replace("{\"account\"", "{ \"account\""),
        // Not canonical: an escape where the character itself is written,
        // and a number with a leading zero.
        expected.replace("\"rootId\":\"1", "\"rootId\":\"\\u0031"),
        expected.replace("\"signingVersion\":1", "\"signingVersion\":01"),
    ] {
        assert_ne!(refused, expected, "each case changes the record");
        assert!(
            FloorRecord::decode(refused.as_bytes()).is_err(),
            "{refused}"
        );
    }
}
