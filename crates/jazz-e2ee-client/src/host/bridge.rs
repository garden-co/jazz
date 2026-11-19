//! The private store an app supplies from JavaScript.
//!
//! The runtime awaits its store; the store is JavaScript, which Rust cannot
//! call and wait for in the middle of a tick. So each store call becomes a
//! request in a queue that JavaScript drains (`takeStoreRequests`), runs
//! against the app's store, and answers (`replyStore`); the runtime's future
//! is polled again by the host and finds its answer. Bytes travel as base64,
//! numbers that may exceed 2^53 as decimal text.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::task::Poll;
use std::time::Duration;
use web_time::Instant;

use crate::{PrivateStore, StoreError, Stored};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::future::LocalBoxFuture;
use jazz_e2ee::Zeroizing;
use serde_json::{Value, json};

/// How long a store call may wait for JavaScript's answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// A store call waiting for JavaScript.
pub struct Request {
    id: u64,
    pub text: String,
}

#[derive(Default)]
pub struct JsStore {
    next: Cell<u64>,
    queue: RefCell<VecDeque<Request>>,
    /// The requests issued and not yet answered, refused or abandoned: only
    /// these accept an answer.
    outstanding: RefCell<HashSet<u64>>,
    replies: RefCell<HashMap<u64, Result<Value, StoreError>>>,
    /// Overrides [`ANSWER_TIMEOUT`] (tests).
    timeout: Cell<Option<Duration>>,
    /// Called when a request is queued, so the host polls the queue soon.
    wake: RefCell<Option<Box<dyn Fn()>>>,
}

impl JsStore {
    pub fn set_wake(&self, wake: Box<dyn Fn()>) {
        *self.wake.borrow_mut() = Some(wake);
    }

    /// Asks the host to tick now (the same call as when a request is queued).
    pub fn request_wake(&self) {
        if let Some(wake) = self.wake.borrow().as_ref() {
            wake();
        }
    }

    /// The requests queued since the last call, as JSON texts
    /// `{ "id", "op", ... }`.
    pub fn take_requests(&self) -> Vec<String> {
        self.queue
            .borrow_mut()
            .drain(..)
            .map(|request| request.text)
            .collect()
    }

    /// JavaScript's answer: `ok` with a JSON payload, or an error whose text
    /// is `conflict`, `wiped` or a message. An answer to a request that is
    /// not outstanding (never issued, answered, timed out or abandoned) is
    /// refused and dropped: it returns `false`.
    pub fn reply(&self, id: u64, ok: bool, payload: &str) -> bool {
        if !self.outstanding.borrow().contains(&id) {
            return false;
        }
        let result = if ok {
            serde_json::from_str(payload).map_err(|error| StoreError::Failed(error.to_string()))
        } else {
            Err(match payload {
                "conflict" => StoreError::Conflict,
                "wiped" => StoreError::Wiped,
                other => StoreError::Failed(other.to_owned()),
            })
        };
        self.replies.borrow_mut().insert(id, result);
        true
    }

    #[cfg(test)]
    pub fn set_timeout(&self, timeout: Duration) {
        self.timeout.set(Some(timeout));
    }

    /// Queues the request and waits for its answer. Every caller awaits one
    /// request before its next step, so a call that is dropped (cancelled)
    /// while its request is still queued never began that step's effect:
    /// the request is removed and never reaches JavaScript. One already
    /// sent may have taken effect; its late answer is refused.
    async fn call(&self, op: &str, mut args: Value) -> Result<Value, StoreError> {
        struct Abandon<'a>(&'a JsStore, u64);
        impl Drop for Abandon<'_> {
            fn drop(&mut self) {
                self.0
                    .queue
                    .borrow_mut()
                    .retain(|request| request.id != self.1);
                self.0.outstanding.borrow_mut().remove(&self.1);
                self.0.replies.borrow_mut().remove(&self.1);
            }
        }
        let id = self.next.get() + 1;
        self.next.set(id);
        args["id"] = json!(id.to_string());
        args["op"] = json!(op);
        self.outstanding.borrow_mut().insert(id);
        self.queue.borrow_mut().push_back(Request {
            id,
            text: args.to_string(),
        });
        let _abandon = Abandon(self, id);
        if let Some(wake) = self.wake.borrow().as_ref() {
            wake();
        }
        let deadline = Instant::now() + self.timeout.get().unwrap_or(ANSWER_TIMEOUT);
        std::future::poll_fn(|_| match self.replies.borrow_mut().remove(&id) {
            Some(result) => Poll::Ready(result),
            None if Instant::now() >= deadline => Poll::Ready(Err(StoreError::Failed(format!(
                "the private store did not answer `{op}` in time"
            )))),
            None => Poll::Pending,
        })
        .await
    }
}

fn bytes_of(value: &Value) -> Result<Vec<u8>, StoreError> {
    let text = value
        .as_str()
        .ok_or_else(|| StoreError::Failed("the store answered without bytes".to_owned()))?;
    STANDARD
        .decode(text)
        .map_err(|_| StoreError::Failed("the store answered with damaged bytes".to_owned()))
}

fn number_of(value: &Value) -> Result<u64, StoreError> {
    value
        .as_str()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| StoreError::Failed("the store answered without a number".to_owned()))
}

impl PrivateStore for JsStore {
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> LocalBoxFuture<'a, Result<Option<Stored>, StoreError>> {
        Box::pin(async move {
            let answer = self
                .call("load", json!({ "scope": scope, "doc": doc }))
                .await?;
            if answer.is_null() {
                return Ok(None);
            }
            Ok(Some(Stored {
                bytes: bytes_of(&answer["bytes"])?,
                revision: number_of(&answer["revision"])?,
            }))
        })
    }

    fn open_scope<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(
            async move { number_of(&self.call("openScope", json!({ "scope": scope })).await?) },
        )
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
            let args = json!({
                "scope": scope,
                "incarnation": incarnation.to_string(),
                "doc": doc,
                "bytes": STANDARD.encode(bytes),
                "expected": expected.map(|revision| revision.to_string()),
            });
            number_of(&self.call("save", args).await?)
        })
    }

    fn list<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            let answer = self.call("list", json!({ "scope": scope })).await?;
            answer
                .as_array()
                .ok_or_else(|| StoreError::Failed("the store answered without a list".to_owned()))?
                .iter()
                .map(|name| {
                    name.as_str().map(str::to_owned).ok_or_else(|| {
                        StoreError::Failed("the store listed a name that is not text".to_owned())
                    })
                })
                .collect()
        })
    }

    fn delete<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.call("delete", json!({ "scope": scope })).await?;
            Ok(())
        })
    }

    fn protect<'a>(&'a self, secret: &'a [u8]) -> LocalBoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            bytes_of(
                &self
                    .call("protect", json!({ "secret": STANDARD.encode(secret) }))
                    .await?,
            )
        })
    }

    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            Ok(Zeroizing::new(bytes_of(
                &self
                    .call("unprotect", json!({ "blob": STANDARD.encode(blob) }))
                    .await?,
            )?))
        })
    }
}

// Lower-level than the usual black-box tests, because the bridge's queue and
// its refusal of stray answers are only observable by playing JavaScript's
// side by hand; the TypeScript suite runs the same bridge end to end.
#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Waker};

    fn poll<T>(future: &mut std::pin::Pin<Box<dyn Future<Output = T> + '_>>) -> Poll<T> {
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    fn id_of(text: &str) -> u64 {
        let request: Value = serde_json::from_str(text).unwrap();
        request["id"].as_str().unwrap().parse().unwrap()
    }

    /// Contract: an answer is accepted only for a request that was issued and
    /// is waiting. One for an id not yet issued is refused and must not
    /// answer the request that later gets that id; one for an answered
    /// request is refused too.
    #[test]
    fn only_an_outstanding_request_accepts_an_answer() {
        let store = JsStore::default();
        // Early answer for the id the next request will get.
        assert!(!store.reply(1, true, "\"early\""));
        let mut first: std::pin::Pin<Box<dyn Future<Output = _>>> =
            Box::pin(store.call("openScope", json!({ "scope": "s" })));
        assert!(poll(&mut first).is_pending());
        let sent = store.take_requests();
        assert_eq!(sent.len(), 1);
        assert!(
            poll(&mut first).is_pending(),
            "the early answer was dropped"
        );
        assert!(store.reply(id_of(&sent[0]), true, "\"7\""));
        assert_eq!(poll(&mut first), Poll::Ready(Ok(json!("7"))));
        assert!(
            !store.reply(id_of(&sent[0]), true, "\"8\""),
            "answered once"
        );
        assert!(!store.reply(99, true, "null"), "never issued");
    }

    /// Contract: a store that never answers fails that call with a store
    /// error and blocks nothing else; its late answer is refused.
    #[test]
    fn a_silent_store_fails_the_call_in_time() {
        let store = JsStore::default();
        store.set_timeout(Duration::from_millis(20));
        let mut silent: std::pin::Pin<Box<dyn Future<Output = _>>> =
            Box::pin(store.call("load", json!({ "scope": "s", "doc": "d" })));
        assert!(poll(&mut silent).is_pending());
        let sent = store.take_requests();
        let mut other: std::pin::Pin<Box<dyn Future<Output = _>>> =
            Box::pin(store.call("list", json!({ "scope": "s" })));
        assert!(poll(&mut other).is_pending());
        let other_sent = store.take_requests();
        assert!(store.reply(id_of(&other_sent[0]), true, "[]"));
        assert_eq!(poll(&mut other), Poll::Ready(Ok(json!([]))));
        std::thread::sleep(Duration::from_millis(40));
        assert!(matches!(
            poll(&mut silent),
            Poll::Ready(Err(StoreError::Failed(_)))
        ));
        assert!(
            !store.reply(id_of(&sent[0]), true, "null"),
            "late answer refused"
        );
    }

    /// Contract: a call dropped while its request is still queued never
    /// reaches JavaScript (its step had not begun); one already sent is
    /// left to JavaScript and its late answer is refused.
    #[test]
    fn a_cancelled_call_withdraws_a_request_not_yet_sent() {
        let store = JsStore::default();
        let mut queued: std::pin::Pin<Box<dyn Future<Output = _>>> =
            Box::pin(store.call("delete", json!({ "scope": "s" })));
        assert!(poll(&mut queued).is_pending());
        drop(queued);
        assert!(store.take_requests().is_empty(), "the delete never ran");

        let mut sent: std::pin::Pin<Box<dyn Future<Output = _>>> =
            Box::pin(store.call("delete", json!({ "scope": "s" })));
        assert!(poll(&mut sent).is_pending());
        let taken = store.take_requests();
        drop(sent);
        assert!(!store.reply(id_of(&taken[0]), true, "null"));
    }
}
