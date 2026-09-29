//! Internal receipts for the private provider-to-decoder proof boundary.
//! Public results cannot distinguish a verified lease from a raw installation,
//! or observe redundant hashing; all counters disappear from production builds.
use super::*;
use crate::chunks::{OwnedChunkProvider, TestChunkProvider};
use bytes::Bytes;
use futures::executor::block_on;
use std::{
    cell::Cell,
    rc::Rc,
    task::{Context, Poll, Waker},
};

thread_local! {
    static OBJECT_HASH_CALLS: Cell<usize> = const { Cell::new(0) };
}
pub(super) fn record_object_hash() {
    OBJECT_HASH_CALLS.with(|calls| calls.set(calls.get() + 1));
}
fn hashes() -> usize {
    OBJECT_HASH_CALLS.with(|calls| calls.replace(0))
}
fn leaf(kind: LargeValueKind, data: &[u8]) -> (ChunkNode, Bytes, ChunkRequest, NodeRef) {
    let node = ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind,
        bytes: data.to_vec(),
    };
    let encoded = Bytes::from(encode_node(&node).unwrap());
    let reference = NodeRef {
        object_hash: object_hash(&encoded),
        locator: Locator::from_seed(b"lease-proof"),
    };
    let request = ChunkRequest {
        object_hash: reference.object_hash.0,
        locator: reference.locator,
    };
    (node, encoded, request, reference)
}
fn load(
    node: &ChunkNode,
    reference: &NodeRef,
    inputs: &mut EvaluationInputs,
) -> Result<ChunkNode, IvmRuntimeError> {
    let ChunkNode::Leaf { kind, .. } = node else {
        unreachable!()
    };
    load_authenticated_node_attempt(
        FORMAT_VERSION,
        *kind,
        reference,
        node_logical_hash(node),
        inputs,
    )
}

#[test]
fn verified_lease_and_cache_hit_reuse_only_the_object_hash_proof() {
    for kind in [
        LargeValueKind::Bytes,
        LargeValueKind::String,
        LargeValueKind::Json,
    ] {
        for budget in [0, 1024] {
            let (node, bytes, request, reference) = leaf(kind, b"\"sample\"");
            let (provider, control) = TestChunkProvider::controlled([(request.clone(), bytes)]);
            let provider = OwnedChunkProvider::new_with_budget(Rc::new(provider), budget);
            hashes();
            for visit in 0..2 {
                let lease = block_on(provider.get(request.clone())).unwrap();
                assert_eq!(hashes(), usize::from(visit == 0 || budget == 0));
                let mut inputs = EvaluationInputs::default();
                inputs.install_chunk_from_provider(request.clone(), lease);
                for _ in 0..2 {
                    assert_eq!(load(&node, &reference, &mut inputs).unwrap(), node);
                    assert_eq!(
                        hashes(),
                        0,
                        "only the provider authenticates the object bytes"
                    );
                }
                drop(inputs);
                assert_eq!(provider.cache_stats().active_leases, 0);
                assert_eq!(provider.cache_stats().leased_bytes, 0);
            }
            assert_eq!(control.observed().len(), if budget == 0 { 2 } else { 1 });
        }
    }
}

#[test]
fn proof_for_another_object_does_not_authenticate_a_misinstalled_lease() {
    let (_, first, first_request, _) = leaf(LargeValueKind::Bytes, b"first");
    let (node, _, request, reference) = leaf(LargeValueKind::Bytes, b"other");
    let (provider, _) = TestChunkProvider::controlled([(first_request.clone(), first)]);
    let provider = OwnedChunkProvider::new(Rc::new(provider));
    let lease = block_on(provider.get(first_request)).unwrap();
    let mut inputs = EvaluationInputs::default();
    inputs.install_chunk_from_provider(request, lease);
    hashes();
    assert!(matches!(
        load(&node, &reference, &mut inputs),
        Err(IvmRuntimeError::LargeValue(Error::ObjectHashMismatch))
    ));
    assert_eq!(hashes(), 1);
}

#[test]
fn raw_replacement_drops_the_previous_proof_and_is_authenticated_again() {
    let (node, bytes, request, reference) = leaf(LargeValueKind::Bytes, b"original");
    let (provider, _) = TestChunkProvider::controlled([(request.clone(), bytes.clone())]);
    let provider = OwnedChunkProvider::new(Rc::new(provider));
    let mut inputs = EvaluationInputs::default();
    inputs.install_chunk_from_provider(
        request.clone(),
        block_on(provider.get(request.clone())).unwrap(),
    );
    inputs.install_chunk(request.clone(), Bytes::from_static(b"corrupt replacement"));
    assert_eq!(provider.cache_stats().active_leases, 0);
    hashes();
    assert!(matches!(
        load(&node, &reference, &mut inputs),
        Err(IvmRuntimeError::LargeValue(Error::ObjectHashMismatch))
    ));
    assert_eq!(hashes(), 1);
    inputs.install_chunk(request, bytes);
    assert_eq!(load(&node, &reference, &mut inputs).unwrap(), node);
    assert_eq!(
        hashes(),
        1,
        "unverified direct bytes always require authentication"
    );
}

#[test]
fn verified_object_still_requires_format_kind_logical_hash_and_canonical_encoding() {
    let (node, bytes, request, reference) = leaf(LargeValueKind::Bytes, b"payload");
    let (provider, _) = TestChunkProvider::controlled([(request.clone(), bytes)]);
    let provider = OwnedChunkProvider::new(Rc::new(provider));
    let mut inputs = EvaluationInputs::default();
    inputs.install_chunk_from_provider(request.clone(), block_on(provider.get(request)).unwrap());
    hashes();
    assert!(matches!(
        load_authenticated_node_attempt(
            2,
            LargeValueKind::Bytes,
            &reference,
            node_logical_hash(&node),
            &mut inputs
        ),
        Err(IvmRuntimeError::LargeValue(Error::UnsupportedFormat(2)))
    ));
    assert!(matches!(
        load_authenticated_node_attempt(
            FORMAT_VERSION,
            LargeValueKind::String,
            &reference,
            node_logical_hash(&node),
            &mut inputs
        ),
        Err(IvmRuntimeError::LargeValue(Error::DescriptorMismatch))
    ));
    assert!(matches!(
        load_authenticated_node_attempt(
            FORMAT_VERSION,
            LargeValueKind::Bytes,
            &reference,
            ContentHash([0; 32]),
            &mut inputs
        ),
        Err(IvmRuntimeError::LargeValue(Error::DescriptorMismatch))
    ));
    assert_eq!(hashes(), 0);

    // A truncated leaf header is hash-valid but cannot bind its required kind.
    let malformed = Bytes::from_static(&[0, FORMAT_VERSION]);
    let reference = NodeRef {
        object_hash: object_hash(&malformed),
        locator: Locator::from_seed(b"malformed-proof"),
    };
    let request = ChunkRequest {
        object_hash: reference.object_hash.0,
        locator: reference.locator,
    };
    let (provider, _) = TestChunkProvider::controlled([(request.clone(), malformed)]);
    let provider = OwnedChunkProvider::new(Rc::new(provider));
    inputs.install_chunk_from_provider(request.clone(), block_on(provider.get(request)).unwrap());
    hashes();
    assert!(matches!(
        load(&node, &reference, &mut inputs),
        Err(IvmRuntimeError::LargeValue(Error::MalformedNode))
    ));
    assert_eq!(hashes(), 0);
}

#[test]
fn coalesced_consumers_retain_independent_verified_leases() {
    let (node, bytes, request, reference) = leaf(LargeValueKind::Bytes, b"coalesced");
    let (provider, control) = TestChunkProvider::controlled([(request.clone(), bytes)]);
    let provider = OwnedChunkProvider::new_with_budget(Rc::new(provider), 0);
    control.pause();
    let mut first = provider.get(request.clone());
    let mut second = provider.get(request.clone());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(first.as_mut().poll(&mut cx), Poll::Pending));
    assert!(matches!(second.as_mut().poll(&mut cx), Poll::Pending));
    hashes();
    control.release_one();
    let Poll::Ready(Ok(first)) = first.as_mut().poll(&mut cx) else {
        panic!("first consumer must complete")
    };
    let Poll::Ready(Ok(second)) = second.as_mut().poll(&mut cx) else {
        panic!("second consumer must complete")
    };
    assert_eq!(hashes(), 1);
    for lease in [first, second] {
        let mut inputs = EvaluationInputs::default();
        inputs.install_chunk_from_provider(request.clone(), lease);
        assert_eq!(load(&node, &reference, &mut inputs).unwrap(), node);
        assert_eq!(hashes(), 0);
    }
    assert_eq!(control.observed().len(), 1);
    assert_eq!(provider.cache_stats().active_leases, 0);
    assert_eq!(provider.cache_stats().owned_bytes, 0);
}
