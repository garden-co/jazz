//! Private stores shared by the client and binding tests.

use std::rc::Rc;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use jazz_e2ee::Zeroizing;

use crate::{MemoryStore, PrivateStore, StoreError, Stored};

/// Answer a binding's store request from memory on the caller's thread.
pub fn answer_memory_store(store: &MemoryStore, request: String) -> (bool, String) {
    crate::host::bridge::serve(Rc::new(store.clone()))(request)
        .now_or_never()
        .expect("memory store requests finish synchronously")
}

type SaveHook = dyn for<'a> Fn(
    &'a str,
    &'a str,
    &'a [u8],
    Option<u64>,
) -> LocalBoxFuture<'a, Result<(), StoreError>>;

/// A memory store with a hook that can delay or refuse a save.
pub struct StoreWithSaveHook {
    inner: MemoryStore,
    hook: Box<SaveHook>,
}

impl StoreWithSaveHook {
    pub fn new(
        inner: MemoryStore,
        hook: impl for<'a> Fn(
            &'a str,
            &'a str,
            &'a [u8],
            Option<u64>,
        ) -> LocalBoxFuture<'a, Result<(), StoreError>>
        + 'static,
    ) -> Self {
        Self {
            inner,
            hook: Box::new(hook),
        }
    }
}

impl PrivateStore for StoreWithSaveHook {
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> LocalBoxFuture<'a, Result<Option<Stored>, StoreError>> {
        self.inner.load(scope, doc)
    }

    fn open_scope<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        self.inner.open_scope(scope)
    }

    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            (self.hook)(scope, doc, bytes, expected).await?;
            self.inner
                .save(scope, incarnation, doc, bytes, expected)
                .await
        })
    }

    fn list<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<Vec<String>, StoreError>> {
        self.inner.list(scope)
    }

    fn delete<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<(), StoreError>> {
        self.inner.delete(scope)
    }

    fn protect<'a>(&'a self, secret: &'a [u8]) -> LocalBoxFuture<'a, Result<Vec<u8>, StoreError>> {
        self.inner.protect(secret)
    }

    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>> {
        self.inner.unprotect(blob)
    }
}
