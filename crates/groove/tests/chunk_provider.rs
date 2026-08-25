use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::task::noop_waker;
use groove::chunks::{ChunkError, ChunkProvider, ChunkRequest, TestChunkProvider};

fn request(seed: u8) -> ChunkRequest {
    ChunkRequest {
        object_hash: [seed; 32],
        locator: groove::large_values::Locator::random(),
    }
}

#[test]
fn controlled_provider_retains_paused_request_until_permitted() {
    let key = request(7);
    let (provider, control) =
        TestChunkProvider::controlled([(key.clone(), Bytes::from_static(b"payload"))]);
    control.pause();
    let mut future = provider.get(key.clone());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut future).poll(&mut context), Poll::Pending);
    assert_eq!(Pin::new(&mut future).poll(&mut context), Poll::Pending);
    assert_eq!(control.observed(), vec![key]);

    control.release_one();
    assert_eq!(
        Pin::new(&mut future).poll(&mut context),
        Poll::Ready(Ok(Bytes::from_static(b"payload")))
    );
}

#[test]
fn controlled_provider_distinguishes_injected_failure_from_absence() {
    let present = request(3);
    let absent = request(4);
    let (provider, control) =
        TestChunkProvider::controlled([(present.clone(), Bytes::from_static(b"present"))]);
    control.fail_next(ChunkError::Backend("injected".to_owned()));

    assert_eq!(
        futures::executor::block_on(provider.get(present)),
        Err(ChunkError::Backend("injected".to_owned()))
    );
    assert_eq!(
        futures::executor::block_on(provider.get(absent)),
        Err(ChunkError::Unavailable)
    );
}
