//! Held-key documents of the private store (`E2EE_PROTOCOL.md` §6.1,
//! §6.2): the canonical encoding, pinned by fixtures written by hand from
//! the specification; sealing and signing; and every load-time check.

mod common;
mod content_support;

use std::sync::atomic::Ordering;

use common::TestRandom;
use common::mechanisms::{FLAKY_WRAP, flaky_registry};
use common::space::*;
use content_support::*;
use jazz_e2ee::agility::{MechanismConfig, PublicKey};
use jazz_e2ee::mechanisms::{key_seal, signature};
use jazz_e2ee::primitives::hash::sha256;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::cells::{CellInput, Opened, open_cell, seal_cells};
use jazz_e2ee::protocol::content::store::{
    Binding, DecodedDocument, HeldDocument, HeldKeyRecord, LineageRecord, Rejected, RejectedRecord,
    Rejection, StoreOwner, UnrecognisedDocument, decode_document, device_hash, prefix_hash,
};
use jazz_e2ee::protocol::content::{
    CreationFate, HeldKeys, HeldSpace, KeyMark, RowTarget, WriteChoice,
};
use jazz_e2ee::protocol::records::space::{SpaceRoot, SpaceSuccessor};
use jazz_e2ee::protocol::space::build::{self, Author};
use jazz_e2ee::registry::Registry;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// Fixture rows: not valid records, only values to encode.
const ROOT_ID: &str = "10000000-0000-4000-8000-000000000001";
const EPOCH_0: &str = "e0000000-0000-4000-8000-000000000000";
const EPOCH_1: &str = "e1000000-0000-4000-8000-000000000000";
const EPOCH_2: &str = "e2000000-0000-4000-8000-000000000000";

fn fixture_root() -> SpaceRoot {
    SpaceRoot {
        id: ROOT_ID.into(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        creator_account_id: ALICE.into(),
        device_id: "d0000000-0000-4000-8000-000000000000".into(),
        account_epoch_id: "a0000000-0000-4000-8000-000000000000".into(),
        epoch_id: EPOCH_0.into(),
        mechanism: "jazz.wrap.xchacha20poly1305".into(),
        version: 1,
        verification: vec![1, 2],
        account_envelope: vec![3],
        signature: vec![4, 255],
    }
}

fn fixture_successor(index: u8, predecessor: &str, epoch: &str) -> SpaceSuccessor {
    SpaceSuccessor {
        id: format!("2000000{index}-0000-4000-8000-000000000000"),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        predecessor: predecessor.into(),
        history_epoch_id: predecessor.into(),
        epoch_id: epoch.into(),
        author_account_id: ALICE.into(),
        author_device_id: "d0000000-0000-4000-8000-000000000000".into(),
        author_epoch_id: "a0000000-0000-4000-8000-000000000000".into(),
        verification: vec![index],
        history: vec![],
        account_envelope: vec![0],
        signature: vec![7],
    }
}

/// The stored root row, written by hand: §1.3 columns plus `id`, keys in
/// code unit order, bytes as arrays, `version` a number.
const ROOT_JSON: &str = concat!(
    r#"{"accountEnvelope":[3],"accountEpochId":"a0000000-0000-4000-8000-000000000000","#,
    r#""creatorAccountId":"a11ce000-0000-5000-8000-00000000a11c","#,
    r#""deviceId":"d0000000-0000-4000-8000-000000000000","#,
    r#""epochId":"e0000000-0000-4000-8000-000000000000","#,
    r#""id":"10000000-0000-4000-8000-000000000001","#,
    r#""identifier":"018f2e3d-4c5b-7a69-8877-665544332211","#,
    r#""mechanism":"jazz.wrap.xchacha20poly1305","#,
    r#""scopeId":"5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6","#,
    r#""signature":[4,255],"verification":[1,2],"version":1}"#
);

fn successor_json(index: u8, predecessor: &str, epoch: &str) -> String {
    format!(
        concat!(
            r#"{{"accountEnvelope":[0],"#,
            r#""authorAccountId":"a11ce000-0000-5000-8000-00000000a11c","#,
            r#""authorDeviceId":"d0000000-0000-4000-8000-000000000000","#,
            r#""authorEpochId":"a0000000-0000-4000-8000-000000000000","#,
            r#""epochId":"{epoch}","history":[],"historyEpochId":"{predecessor}","#,
            r#""id":"2000000{index}-0000-4000-8000-000000000000","#,
            r#""identifier":"018f2e3d-4c5b-7a69-8877-665544332211","#,
            r#""predecessor":"{predecessor}","#,
            r#""scopeId":"5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6","#,
            r#""signature":[7],"verification":[{index}]}}"#
        ),
        index = index,
        predecessor = predecessor,
        epoch = epoch
    )
}

/// The account domain as a JSON string inside JSON: its quotes escaped.
const APPLICATION: &str =
    r#"["0a0a0a0a-0000-4000-8000-000000000000","dev","a11ce000-0000-5000-8000-00000000a11c"]"#;
const APPLICATION_JSON: &str = r#""[\"0a0a0a0a-0000-4000-8000-000000000000\",\"dev\",\"a11ce000-0000-5000-8000-00000000a11c\"]""#;

fn fixture_binding() -> Binding {
    Binding {
        application: APPLICATION.into(),
        device: "d0000000-0000-4000-8000-000000000000".into(),
        keys: "ab".repeat(32),
    }
}

fn binding_json() -> String {
    format!(
        r#"{{"application":{APPLICATION_JSON},"device":"d0000000-0000-4000-8000-000000000000","keys":"{}"}}"#,
        "ab".repeat(32)
    )
}

// The pinned digests below were computed independently (Python's hashlib)
// over the hand-written texts, not by this crate.

#[test]
fn the_device_hash_is_pinned() {
    let sealing = PublicKey {
        mechanism: key_seal::MECHANISM.into(),
        bytes: vec![1, 2, 3],
    };
    let signing = PublicKey {
        mechanism: signature::MECHANISM.into(),
        bytes: vec![4, 5],
    };
    let framed = concat!(
        r#"["jazz-e2ee-held-device-v1",[[1,2,3],[4,5],"#,
        r#"{"id":"jazz.wrap.hpke-x25519","version":1},"#,
        r#"{"id":"jazz.sodium.sign","version":1}]]"#
    );
    let expected = hex(&sha256(framed.as_bytes()));
    assert_eq!(device_hash(&sealing, &signing).unwrap(), expected);
    assert_eq!(
        expected,
        "9d60cb262c856648a7725437da8016d1a10c1b17966453369daf7493a928bbed"
    );
    // Every input takes part.
    let other_version = PublicKey {
        mechanism: jazz_e2ee::agility::MechanismId::new("jazz.sodium.sign", 2),
        bytes: vec![4, 5],
    };
    assert_ne!(device_hash(&sealing, &other_version).unwrap(), expected);
    assert_ne!(device_hash(&signing, &sealing).unwrap(), expected);
}

#[test]
fn the_lineage_prefix_hash_is_pinned_at_depths_0_and_2() {
    let root = fixture_root();
    let successors = [
        fixture_successor(1, EPOCH_0, EPOCH_1),
        fixture_successor(2, EPOCH_1, EPOCH_2),
    ];
    let depth_0 = format!(r#"["jazz-e2ee-held-prefix-v1",[{ROOT_JSON},[]]]"#);
    let depth_2 = format!(
        r#"["jazz-e2ee-held-prefix-v1",[{ROOT_JSON},[{},{}]]]"#,
        successor_json(1, EPOCH_0, EPOCH_1),
        successor_json(2, EPOCH_1, EPOCH_2)
    );
    assert_eq!(
        prefix_hash(&root, &[]).unwrap(),
        hex(&sha256(depth_0.as_bytes()))
    );
    assert_eq!(
        prefix_hash(&root, &successors).unwrap(),
        hex(&sha256(depth_2.as_bytes()))
    );
    assert_eq!(
        prefix_hash(&root, &[]).unwrap(),
        "7bf68f4a08bc1f8864787068168dfa5fb1bc0a26b71deae6c3d24ad7d1c782db"
    );
    assert_eq!(
        prefix_hash(&root, &successors).unwrap(),
        "49cd636af60ee8836e53fe77cb99cd9e0ca594ae6be89f87e4b27eecf820fda4"
    );
}

#[test]
fn a_lineage_record_and_a_held_key_record_are_pinned() {
    let lineage = LineageRecord {
        binding: fixture_binding(),
        root: fixture_root(),
        successors: vec![fixture_successor(1, EPOCH_0, EPOCH_1)],
        signature: vec![9, 8],
    };
    let head = format!(
        r#"{{"binding":{},"format":"jazz-e2ee-held-lineage-v1","root":{ROOT_JSON}"#,
        binding_json()
    );
    let successors = format!(r#""successors":[{}]"#, successor_json(1, EPOCH_0, EPOCH_1));
    assert_eq!(
        String::from_utf8(lineage.encode().unwrap()).unwrap(),
        format!(r#"{head},"signature":[9,8],{successors}}}"#)
    );
    assert_eq!(
        String::from_utf8(lineage.signed_bytes().unwrap()).unwrap(),
        format!(r#"["jazz-e2ee-held-lineage-v1",{head},{successors}}}]"#)
    );
    assert_eq!(
        LineageRecord::decode(&lineage.encode().unwrap()),
        Ok(lineage)
    );

    let record = HeldKeyRecord {
        binding: fixture_binding(),
        root_id: ROOT_ID.into(),
        epoch_id: EPOCH_1.into(),
        depth: 1,
        prefix: "cd".repeat(32),
        unpublished: Some("tx-1".into()),
        refused: false,
        envelope: vec![5],
        signature: vec![6],
    };
    let head = format!(r#"{{"binding":{},"depth":"1""#, binding_json());
    let middle = format!(
        r#""epochId":"{EPOCH_1}","format":"jazz-e2ee-held-key-v3","prefix":"{}","rootId":"{ROOT_ID}""#,
        "cd".repeat(32)
    );
    let tail = r#""unpublished":{"transaction":"tx-1"}}"#;
    assert_eq!(
        String::from_utf8(record.encode().unwrap()).unwrap(),
        format!(r#"{head},"envelope":[5],{middle},"signature":[6],{tail}"#)
    );
    assert_eq!(
        String::from_utf8(record.body_bytes().unwrap()).unwrap(),
        format!(r#"["jazz-e2ee-held-key-v3",{head},{middle},{tail}]"#)
    );
    assert_eq!(
        String::from_utf8(record.signed_bytes().unwrap()).unwrap(),
        format!(r#"["jazz-e2ee-held-key-v3",{head},"envelope":[5],{middle},{tail}]"#)
    );
    assert_eq!(
        HeldKeyRecord::decode(&record.encode().unwrap()),
        Ok(record.clone())
    );
    // A refused record carries `refused: true` and no `unpublished`.
    let refused = HeldKeyRecord {
        unpublished: None,
        refused: true,
        ..record.clone()
    };
    let text = String::from_utf8(refused.encode().unwrap()).unwrap();
    assert!(text.contains(r#""refused":true,"rootId""#), "{text}");
    assert!(!text.contains("unpublished"));
    let published = HeldKeyRecord {
        unpublished: None,
        ..record
    };
    let text = String::from_utf8(published.encode().unwrap()).unwrap();
    assert!(!text.contains("refused") && !text.contains("unpublished"));
}

#[test]
fn decoding_refuses_anything_but_the_canonical_encoding() {
    let record = HeldKeyRecord {
        binding: fixture_binding(),
        root_id: ROOT_ID.into(),
        epoch_id: EPOCH_1.into(),
        depth: 1,
        prefix: "cd".repeat(32),
        unpublished: None,
        refused: false,
        envelope: vec![5],
        signature: vec![6],
    };
    let text = String::from_utf8(record.encode().unwrap()).unwrap();
    let variants = [
        text.replace(r#""depth":"1""#, r#""depth":"01""#),
        text.replace(r#""depth":"1""#, r#""depth":1"#),
        text.replace(r#""envelope":[5]"#, r#""envelope":[5],"extra":1"#),
        text.replace(r#""envelope":[5]"#, r#""envelope":[256]"#),
        text.replace(r#"{"binding""#, r#"{ "binding""#),
        text.replace(
            r#""format":"jazz-e2ee-held-key-v3""#,
            r#""format":"jazz-e2ee-held-key-v2""#,
        ),
        text.replace(r#""signature":[6]"#, r#""signature":[6],"refused":false"#),
        // Keys out of order.
        text.replace(r#""envelope":[5],"#, "")
            .replace(r#""signature":[6]"#, r#""signature":[6],"envelope":[5]"#),
        format!("{text} "),
    ];
    for variant in variants {
        assert!(
            HeldKeyRecord::decode(variant.as_bytes()).is_err(),
            "{variant}"
        );
    }
}

fn owner<'a>(world: &'a World, account: &'a str) -> StoreOwner<'a> {
    StoreOwner {
        registry: &world.shared.registry,
        domain: &world.shared.domain,
        account_id: account,
        device: &world.accounts[account].device,
    }
}

/// Alice's held keys of her space after one rotation, and her own lost
/// creation of the same address, refused.
fn alice_holding(seed: u64) -> (World, HeldSpace, HeldSpace) {
    let mut world = World::new(seed);
    world.account(ALICE);
    world.add_member(ALICE);
    world.create(ALICE);
    world.rotate(ALICE);
    let space = acquire_held(&world, ALICE, ROW);
    let lost = lost_creation(&world);
    (world, space, lost)
}

fn lost_creation(world: &World) -> HeldSpace {
    let own = world.own(ALICE);
    let read = world.read();
    let directory = read.directory(world);
    let device = world.accounts[ALICE].device.clone();
    let author = Author::new(&directory, ALICE, &own.key, &device).unwrap();
    let records = build::space_root(
        &world.writer(ALICE),
        &space(ROW),
        &author,
        &mut TestRandom(99),
    )
    .unwrap();
    let mut lost = HeldSpace::from_creation(
        &world.shared.registry,
        &world.shared.domain,
        &records,
        "tx-9",
    )
    .unwrap();
    lost.settle(CreationFate::NotAccepted, None);
    lost
}

fn stored(world: &World, space: &HeldSpace) -> Vec<u8> {
    owner(world, ALICE)
        .seal_document(space, 3, &mut TestRandom(5))
        .unwrap()
        .encode()
        .unwrap()
}

fn marks(space: &HeldSpace) -> Vec<(usize, KeyMark)> {
    space
        .held()
        .map(|(depth, mark)| (depth, mark.clone()))
        .collect()
}

#[test]
fn a_held_space_survives_the_store() {
    let (world, space, lost) = alice_holding(1);
    let domain = shared(&world);
    let row_space = content_support::space(ROW);
    let target = RowTarget {
        domain: &domain,
        space: &row_space,
        table_id: TABLE,
        row_id: ROW_1,
    };
    let mut before = HeldKeys::new();
    before.hold(space).unwrap();
    before.hold(lost).unwrap();
    let suite = world
        .shared
        .registry
        .select(&MechanismConfig::default())
        .unwrap();
    let WriteChoice::Key(key) = before.write_key(&row_space, false) else {
        panic!("a key");
    };
    let cell = seal_cells(
        &suite,
        &key,
        &target,
        &[CellInput {
            column: notes(),
            value: b"kept",
            equality: None,
        }],
        &mut TestRandom(1),
    )
    .unwrap()
    .cells
    .remove(0)
    .stored;

    let mut after = HeldKeys::new();
    for space in before.spaces() {
        let bytes = stored(&world, space);
        let document = decode_document(&bytes).unwrap();
        assert_eq!(document.revision, 3);
        let loaded = owner(&world, ALICE).load(&document).unwrap();
        assert_eq!(loaded.rejected, Vec::<Rejected>::new());
        let held = loaded.space.unwrap();
        assert_eq!(marks(&held), marks(space));
        assert_eq!(held.successors(), space.successors());
        after.hold(held).unwrap();
    }
    let WriteChoice::Key(key_after) = after.write_key(&row_space, false) else {
        panic!("a key");
    };
    assert_eq!(key_after.epoch_id(), key.epoch_id());
    assert_eq!(
        open_cell(
            &world.shared.registry,
            &after,
            &target,
            &notes(),
            &cell,
            false
        )
        .unwrap(),
        Opened::Value(jazz_e2ee::Zeroizing::new(b"kept".to_vec()))
    );
}

#[test]
fn a_refused_key_is_kept_for_good_through_the_store() {
    let (world, _, lost) = alice_holding(2);
    let owner = owner(&world, ALICE);
    let loaded = owner
        .load(&decode_document(&stored(&world, &lost)).unwrap())
        .unwrap()
        .space
        .unwrap();
    assert_eq!(loaded.mark(0), Some(&KeyMark::Refused));
    let mut held = HeldKeys::new();
    held.hold(loaded).unwrap();
    assert!(matches!(
        held.write_key(&content_support::space(ROW), false),
        WriteChoice::NeedsKey
    ));
    // A later record of the same epoch, unpublished, does not lift it, in
    // either order.
    let records = {
        let own = world.own(ALICE);
        let read = world.read();
        let directory = read.directory(&world);
        let device = world.accounts[ALICE].device.clone();
        let author = Author::new(&directory, ALICE, &own.key, &device).unwrap();
        build::space_root(
            &world.writer(ALICE),
            &content_support::space(ROW),
            &author,
            &mut TestRandom(99),
        )
        .unwrap()
    };
    let pending = HeldSpace::from_creation(
        &world.shared.registry,
        &world.shared.domain,
        &records,
        "tx-9",
    )
    .unwrap();
    let mut rng = TestRandom(3);
    let refused_record = owner.seal_key(&lost, 0, &mut rng).unwrap();
    let pending_record = owner.seal_key(&pending, 0, &mut rng).unwrap();
    for keys in [
        vec![refused_record.clone(), pending_record.clone()],
        vec![pending_record, refused_record],
    ] {
        let document = HeldDocument {
            revision: 0,
            lineage: owner.seal_lineage(&lost).unwrap(),
            keys,
        };
        let space = owner.load(&document.into()).unwrap().space.unwrap();
        assert_eq!(space.mark(0), Some(&KeyMark::Refused));
    }
}

/// Signs `record` again with Alice's device, as a device could after
/// changing it.
fn resign_key(world: &World, mut record: HeldKeyRecord) -> HeldKeyRecord {
    let device = &world.accounts[ALICE].device;
    let signer = world
        .shared
        .registry
        .signer(device.signing.public.mechanism.as_mechanism())
        .unwrap();
    record.signature = signer
        .sign(&device.signing, &record.signed_bytes().unwrap())
        .unwrap();
    record
}

fn resign_lineage(world: &World, mut record: LineageRecord) -> LineageRecord {
    let device = &world.accounts[ALICE].device;
    let signer = world
        .shared
        .registry
        .signer(device.signing.public.mechanism.as_mechanism())
        .unwrap();
    record.signature = signer
        .sign(&device.signing, &record.signed_bytes().unwrap())
        .unwrap();
    record
}

/// The reasons the key records of `keys` are skipped, with `lineage`.
fn key_rejections(
    world: &World,
    lineage: &LineageRecord,
    keys: Vec<HeldKeyRecord>,
) -> Vec<Option<Rejection>> {
    let count = keys.len();
    let loaded = owner(world, ALICE)
        .load(&DecodedDocument {
            revision: 0,
            lineage: Some(lineage.clone()),
            keys: keys.into_iter().map(Some).collect(),
        })
        .unwrap();
    (0..count)
        .map(|index| {
            loaded
                .rejected
                .iter()
                .find(|rejected| {
                    matches!(&rejected.record, RejectedRecord::Key { index: at, .. } if *at == index)
                })
                .map(|rejected| rejected.reason)
        })
        .collect()
}

#[test]
fn every_held_key_check_skips_only_its_record() {
    let (world, space, _) = alice_holding(3);
    let owner = owner(&world, ALICE);
    let document = owner.seal_document(&space, 0, &mut TestRandom(4)).unwrap();
    let lineage = document.lineage.clone();
    let good = document.keys[1].clone();
    assert_eq!(good.depth, 1);
    let device = &world.accounts[ALICE].device;
    let sealer = world
        .shared
        .registry
        .key_sealer(device.sealing.public.mechanism.as_mechanism())
        .unwrap();
    let with = |change: &dyn Fn(&mut HeldKeyRecord)| {
        let mut record = good.clone();
        change(&mut record);
        resign_key(&world, record)
    };
    let cases: Vec<(HeldKeyRecord, Rejection)> = vec![
        (
            {
                let mut record = good.clone();
                record.signature[70] ^= 1;
                record
            },
            Rejection::Signature,
        ),
        (
            with(&|record| record.binding.device = "d9000000-0000-4000-8000-000000000000".into()),
            Rejection::Binding,
        ),
        (
            with(&|record| record.binding.keys = "00".repeat(32)),
            Rejection::Binding,
        ),
        (
            with(&|record| record.root_id = "10000000-0000-4000-8000-000000000001".into()),
            Rejection::RootMismatch,
        ),
        (with(&|record| record.depth = 2), Rejection::Depth),
        (
            with(&|record| record.prefix = "00".repeat(32)),
            Rejection::Prefix,
        ),
        (
            with(&|record| {
                record.depth = 0;
                record.prefix = prefix_hash(&lineage.root, &[]).unwrap();
            }),
            Rejection::Epoch,
        ),
        (
            with(&|record| {
                record.unpublished = Some("tx".into());
                record.refused = true;
            }),
            Rejection::Malformed,
        ),
        (
            with(&|record| record.unpublished = Some(String::new())),
            Rejection::Malformed,
        ),
        // Changing the body changes the envelope's context.
        (
            with(&|record| record.unpublished = Some("tx".into())),
            Rejection::Envelope,
        ),
        (
            with(&|record| record.envelope[40] ^= 1),
            Rejection::Envelope,
        ),
        (
            with(&|record| {
                record.envelope = sealer
                    .seal(
                        &device.sealing.public,
                        &record.body_bytes().unwrap(),
                        &[7; 32],
                        &mut TestRandom(8),
                    )
                    .unwrap();
            }),
            Rejection::Marker,
        ),
    ];
    for (record, reason) in cases {
        // The bad record first, then the good one: only the bad one is
        // skipped.
        assert_eq!(
            key_rejections(&world, &lineage, vec![record, good.clone()]),
            vec![Some(reason), None],
            "{reason:?}"
        );
    }
    // An undecodable record is skipped too.
    let loaded = owner
        .load(&DecodedDocument {
            revision: 0,
            lineage: Some(lineage.clone()),
            keys: vec![None, Some(good.clone())],
        })
        .unwrap();
    assert_eq!(
        loaded.rejected,
        vec![Rejected {
            record: RejectedRecord::Key {
                index: 0,
                epoch_id: None
            },
            reason: Rejection::Malformed
        }]
    );
    assert_eq!(loaded.space.unwrap().held().count(), 1);
}

#[test]
fn a_lineage_that_fails_skips_the_document() {
    let (mut world, space, _) = alice_holding(4);
    let other = world.approve_device(ALICE);
    let owner_alice = owner(&world, ALICE);
    let document = owner_alice
        .seal_document(&space, 0, &mut TestRandom(4))
        .unwrap();
    let lineage_reason = |world: &World, owner: StoreOwner<'_>, lineage: LineageRecord| {
        let _ = world;
        let loaded = owner
            .load(&DecodedDocument {
                revision: 0,
                lineage: Some(lineage),
                keys: document.keys.iter().cloned().map(Some).collect(),
            })
            .unwrap();
        assert!(loaded.space.is_none());
        match loaded.rejected.as_slice() {
            [
                Rejected {
                    record: RejectedRecord::Lineage,
                    reason,
                },
            ] => *reason,
            other => panic!("{other:?}"),
        }
    };
    let mut tampered = document.lineage.clone();
    tampered.signature[70] ^= 1;
    assert_eq!(
        lineage_reason(&world, owner_alice, tampered),
        Rejection::Signature
    );
    let mut swapped = document.lineage.clone();
    swapped.successors[0].predecessor = swapped.successors[0].epoch_id.clone();
    assert_eq!(
        lineage_reason(&world, owner_alice, resign_lineage(&world, swapped)),
        Rejection::Malformed
    );
    let mut detached = document.lineage.clone();
    detached.successors[0].predecessor = "e9000000-0000-4000-8000-000000000000".into();
    detached.successors[0].history_epoch_id = detached.successors[0].predecessor.clone();
    assert_eq!(
        lineage_reason(&world, owner_alice, resign_lineage(&world, detached)),
        Rejection::NotAChain
    );
    let mut forked = document.lineage.clone();
    let mut second = forked.successors[0].clone();
    second.id = "30000000-0000-4000-8000-000000000000".into();
    forked.successors.push(second);
    assert_eq!(
        lineage_reason(&world, owner_alice, resign_lineage(&world, forked)),
        Rejection::NotAChain
    );
    let mut broken_root = document.lineage.clone();
    broken_root.root.verification.clear();
    assert_eq!(
        lineage_reason(&world, owner_alice, resign_lineage(&world, broken_root)),
        Rejection::Malformed
    );
    // A missing lineage is malformed.
    let loaded = owner_alice
        .load(&DecodedDocument {
            revision: 0,
            lineage: None,
            keys: Vec::new(),
        })
        .unwrap();
    assert_eq!(loaded.rejected[0].reason, Rejection::Malformed);
    // Another device of the account, or another account, is not bound.
    let other_owner = StoreOwner {
        device: &other,
        ..owner(&world, ALICE)
    };
    assert_eq!(
        lineage_reason(&world, other_owner, document.lineage.clone()),
        Rejection::Binding
    );
    let bob_owner = StoreOwner {
        account_id: BOB,
        ..owner(&world, ALICE)
    };
    assert_eq!(
        lineage_reason(&world, bob_owner, document.lineage.clone()),
        Rejection::Binding
    );
    // A signing mechanism this reader does not route is unsupported.
    let empty = Registry::empty();
    let unsupported = StoreOwner {
        registry: &empty,
        ..owner(&world, ALICE)
    };
    assert_eq!(
        lineage_reason(&world, unsupported, document.lineage.clone()),
        Rejection::Unsupported
    );
}

#[test]
fn a_provider_failure_fails_the_load() {
    let (registry, wrap_fails, _) = flaky_registry();
    let mut world = World::with_registry(5, registry);
    world.account_with(
        ALICE,
        MechanismConfig {
            key_wrap: FLAKY_WRAP.into(),
            ..MechanismConfig::default()
        },
    );
    world.add_member(ALICE);
    world.create(ALICE);
    let space = acquire_held(&world, ALICE, ROW);
    let document = decode_document(&stored(&world, &space)).unwrap();
    assert!(
        owner(&world, ALICE)
            .load(&document)
            .unwrap()
            .space
            .is_some()
    );
    wrap_fails.store(true, Ordering::SeqCst);
    assert_eq!(
        owner(&world, ALICE).load(&document).unwrap_err(),
        ProtocolError::ProviderFailure
    );
}

#[test]
fn an_unrecognised_document_is_refused_whole() {
    let (world, space, _) = alice_holding(6);
    let bytes = stored(&world, &space);
    let text = String::from_utf8(bytes.clone()).unwrap();
    for variant in [
        text.replace("jazz-e2ee-held-space-v1", "jazz-e2ee-held-space-v2"),
        text.replace(r#""format":"#, r#" "format":"#),
        text.replace(r#""revision":3"#, r#""revision":"3""#),
        text.replace(r#","revision":3"#, ""),
        format!("{text}\n"),
        "[]".into(),
        String::new(),
    ] {
        assert_eq!(
            decode_document(variant.as_bytes()),
            Err(UnrecognisedDocument),
            "{variant}"
        );
    }
    // A record inside it that does not decode is skipped, not the document.
    let damaged = text.replacen(r#""depth":"0""#, r#""depth":"x""#, 1);
    assert_ne!(damaged, text);
    let document = decode_document(damaged.as_bytes()).unwrap();
    assert!(document.lineage.is_some());
    assert_eq!(document.keys[0], None);
    assert!(document.keys[1].is_some());
}
