//! The private store an app supplies from JavaScript.
//!
//! The runtime awaits its store; the store is JavaScript, which Rust cannot
//! call and wait for in the middle of a tick. So each store call becomes a
//! request in a queue that JavaScript drains (`takeStoreRequests`), runs
//! against the app's store, and answers (`replyStore`); the runtime's future
//! is polled again by the host and finds its answer. Bytes travel as base64,
//! numbers that may exceed 2^53 as decimal text.
//!
//! A host that can answer on the spot (a native app, a foreign-language
//! callback) gives the store a [`Handler`] instead ([`JsStore::served_by`]):
//! the same request texts and answers, with no queue to drain. [`serve`]
//! makes a handler of a Rust [`PrivateStore`].
//!
//! Request and answer texts carry key bytes (`protect`'s secret,
//! `unprotect`'s answer, the device's saved documents). What Rust holds of
//! them is wiped once used: the texts it writes and reads, and the JSON
//! values they come from or go to. A handler that takes a request text owns
//! it and wipes it ([`serve`] does); what crosses into JavaScript cannot be.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;
use web_time::Instant;

use crate::{PrivateStore, StoreError, Stored};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::future::LocalBoxFuture;
use jazz_e2ee::Zeroizing;
use serde_json::{Value, json};
use zeroize::Zeroize;

/// How long a store call may wait for its answer (JavaScript's, or a
/// [`Handler`]'s).
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// Answers a request text on the spot: `ok` and the JSON payload, or the
/// error text, as [`JsStore::reply`] takes them. An answer that has not come
/// by the deadline fails the call; one that is synchronous cannot be cut
/// short, so a handler that never returns blocks its thread.
pub type Handler = Rc<dyn Fn(String) -> LocalBoxFuture<'static, (bool, String)>>;

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
    /// Answers requests instead of the queue (see [`JsStore::served_by`]).
    handler: Option<Handler>,
}

impl JsStore {
    /// A store whose requests `handler` answers where they are made: nothing
    /// is queued and nothing waits for a host turn.
    pub fn served_by(handler: Handler) -> Self {
        Self {
            handler: Some(handler),
            ..Self::default()
        }
    }

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
        self.replies.borrow_mut().insert(id, decode(ok, payload));
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
        let late = || {
            Err(StoreError::Failed(format!(
                "the private store did not answer `{op}` in time"
            )))
        };
        if let Some(handler) = &self.handler {
            args["op"] = json!(op);
            // The same deadline as a queued request. A handler that runs
            // synchronously (a foreign callback) has answered before this
            // point and cannot be interrupted; an asynchronous one that never
            // answers, or never wakes its task, fails here, its late answer
            // dropped with its future. While it waits, the call is polled
            // when the handler wakes it or at the deadline, never in a loop.
            let deadline = Instant::now() + self.timeout.get().unwrap_or(ANSWER_TIMEOUT);
            let text = text_of(&args);
            wipe(&mut args);
            let mut answer = handler(text);
            let mut timed: Option<Waker> = None;
            return std::future::poll_fn(|context| {
                if let Poll::Ready((ok, mut payload)) = answer.as_mut().poll(context) {
                    let result = decode(ok, &payload);
                    payload.zeroize();
                    return Poll::Ready(result);
                }
                if Instant::now() >= deadline {
                    return Poll::Ready(late());
                }
                // Nothing else is bound to wake this call at its deadline.
                if timed
                    .as_ref()
                    .is_none_or(|waker| !waker.will_wake(context.waker()))
                {
                    if wake_at(deadline, context.waker().clone()) {
                        timed = Some(context.waker().clone());
                    } else {
                        context.waker().wake_by_ref();
                    }
                }
                Poll::Pending
            })
            .await;
        }
        let id = self.next.get() + 1;
        self.next.set(id);
        args["id"] = json!(id.to_string());
        args["op"] = json!(op);
        self.outstanding.borrow_mut().insert(id);
        self.queue.borrow_mut().push_back(Request {
            id,
            text: text_of(&args),
        });
        wipe(&mut args);
        let _abandon = Abandon(self, id);
        if let Some(wake) = self.wake.borrow().as_ref() {
            wake();
        }
        let deadline = Instant::now() + self.timeout.get().unwrap_or(ANSWER_TIMEOUT);
        std::future::poll_fn(|_| match self.replies.borrow_mut().remove(&id) {
            Some(result) => Poll::Ready(result),
            None if Instant::now() >= deadline => Poll::Ready(late()),
            None => Poll::Pending,
        })
        .await
    }
}

/// Has `waker` woken at `deadline` by the one thread that keeps the
/// deadlines of every store call waiting on a handler. `false` when there is
/// no such thread (a browser, or a thread that could not be started): the
/// caller then wakes itself, as it must to meet its deadline.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn wake_at(deadline: Instant, waker: Waker) -> bool {
    use std::sync::OnceLock;
    use std::sync::mpsc::{RecvTimeoutError, Sender, channel};
    static TIMER: OnceLock<Option<Sender<(Instant, Waker)>>> = OnceLock::new();
    let timer = TIMER.get_or_init(|| {
        let (sender, receiver) = channel::<(Instant, Waker)>();
        std::thread::Builder::new()
            .name("jazz-store-deadlines".to_owned())
            .spawn(move || {
                let mut due: Vec<(Instant, Waker)> = Vec::new();
                loop {
                    let now = Instant::now();
                    due.retain(|(at, waker)| {
                        let passed = *at <= now;
                        if passed {
                            waker.wake_by_ref();
                        }
                        !passed
                    });
                    let next = due.iter().map(|(at, _)| *at).min();
                    let received = match next {
                        Some(at) => receiver.recv_timeout(at.saturating_duration_since(now)),
                        None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
                    };
                    match received {
                        Ok(entry) => due.push(entry),
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
            })
            .ok()
            .map(|_| sender)
    });
    timer
        .as_ref()
        .is_some_and(|sender| sender.send((deadline, waker)).is_ok())
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn wake_at(_deadline: Instant, _waker: Waker) -> bool {
    false
}

/// `value` as JSON text, written into a buffer of its exact size so that
/// no partial copy of it is left behind by a growing buffer.
fn text_of(value: &Value) -> String {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).expect("a JSON value always serializes");
    let mut text = Vec::with_capacity(count.0);
    serde_json::to_writer(&mut text, value).expect("a JSON value always serializes");
    String::from_utf8(text).expect("JSON text is UTF-8")
}

/// Wipe every text in `value`: the base64 of key bytes a request or an
/// answer carried.
fn wipe(value: &mut Value) {
    match value {
        Value::String(text) => text.zeroize(),
        Value::Array(items) => items.iter_mut().for_each(wipe),
        Value::Object(fields) => fields.values_mut().for_each(wipe),
        _ => {}
    }
}

/// A host's answer as the store call's result: `ok` with a JSON payload, or
/// an error text (`conflict`, `wiped`, or a message).
fn decode(ok: bool, payload: &str) -> Result<Value, StoreError> {
    if ok {
        serde_json::from_str(payload).map_err(|error| StoreError::Failed(error.to_string()))
    } else {
        Err(match payload {
            "conflict" => StoreError::Conflict,
            "wiped" => StoreError::Wiped,
            other => StoreError::Failed(other.to_owned()),
        })
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
            let mut answer = self
                .call("load", json!({ "scope": scope, "doc": doc }))
                .await?;
            if answer.is_null() {
                return Ok(None);
            }
            let stored = (|| {
                Ok(Some(Stored {
                    bytes: bytes_of(&answer["bytes"])?,
                    revision: number_of(&answer["revision"])?,
                }))
            })();
            wipe(&mut answer);
            stored
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
            let mut answer = self
                .call("protect", json!({ "secret": STANDARD.encode(secret) }))
                .await?;
            let blob = bytes_of(&answer);
            wipe(&mut answer);
            blob
        })
    }

    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            let mut answer = self
                .call("unprotect", json!({ "blob": STANDARD.encode(blob) }))
                .await?;
            let secret = bytes_of(&answer).map(Zeroizing::new);
            wipe(&mut answer);
            secret
        })
    }
}

/// A [`Handler`] that runs each request against a Rust store, for a host
/// whose app supplies a [`PrivateStore`] directly.
pub fn serve(store: Rc<dyn PrivateStore>) -> Handler {
    Rc::new(move |mut text: String| {
        let store = Rc::clone(&store);
        Box::pin(async move {
            let result = run(&*store, &text).await;
            text.zeroize();
            match result {
                Ok(mut value) => {
                    let answer = text_of(&value);
                    wipe(&mut value);
                    (true, answer)
                }
                Err(StoreError::Conflict) => (false, "conflict".to_owned()),
                Err(StoreError::Wiped) => (false, "wiped".to_owned()),
                Err(error) => (false, error.to_string()),
            }
        })
    })
}

async fn run(store: &dyn PrivateStore, text: &str) -> Result<Value, StoreError> {
    let bad = |what: &str| StoreError::Failed(format!("a store request {what}"));
    let mut request: Value = serde_json::from_str(text).map_err(|_| bad("is not JSON"))?;
    let answer = answer(store, &request).await;
    wipe(&mut request);
    answer
}

async fn answer(store: &dyn PrivateStore, request: &Value) -> Result<Value, StoreError> {
    let bad = |what: &str| StoreError::Failed(format!("a store request {what}"));
    let text_of = |name: &str| -> Result<&str, StoreError> {
        request[name].as_str().ok_or_else(|| bad("lacks a field"))
    };
    let number = |name: &str| -> Result<u64, StoreError> { number_of(&request[name]) };
    let bytes = |name: &str| -> Result<Zeroizing<Vec<u8>>, StoreError> {
        bytes_of(&request[name]).map(Zeroizing::new)
    };
    Ok(match request["op"].as_str() {
        Some("openScope") => json!(store.open_scope(text_of("scope")?).await?.to_string()),
        Some("load") => match store.load(text_of("scope")?, text_of("doc")?).await? {
            None => Value::Null,
            Some(found) => json!({
                "bytes": STANDARD.encode(&*Zeroizing::new(found.bytes)),
                "revision": found.revision.to_string(),
            }),
        },
        Some("save") => {
            let expected = match request["expected"].as_str() {
                None => None,
                Some(_) => Some(number("expected")?),
            };
            let revision = store
                .save(
                    text_of("scope")?,
                    number("incarnation")?,
                    text_of("doc")?,
                    &bytes("bytes")?,
                    expected,
                )
                .await?;
            json!(revision.to_string())
        }
        Some("list") => json!(store.list(text_of("scope")?).await?),
        Some("delete") => {
            store.delete(text_of("scope")?).await?;
            Value::Null
        }
        Some("protect") => json!(STANDARD.encode(store.protect(&bytes("secret")?).await?)),
        Some("unprotect") => {
            json!(STANDARD.encode(&*store.unprotect(&bytes("blob")?).await?))
        }
        _ => return Err(bad("names an unknown operation")),
    })
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

    /// Contract: a handler that never answers fails the call at the same
    /// deadline, even under an executor that polls only when woken and a
    /// handler that never wakes it.
    #[test]
    fn a_silent_handler_fails_the_call_in_time() {
        let (done, outcome) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let store = JsStore::served_by(Rc::new(|_| Box::pin(std::future::pending())));
            store.set_timeout(Duration::from_millis(20));
            let result = futures::executor::block_on(
                store.call("load", json!({ "scope": "s", "doc": "d" })),
            );
            let _ = done.send(result);
        });
        let result = outcome
            .recv_timeout(Duration::from_secs(5))
            .expect("the call ends at its deadline");
        assert!(
            matches!(&result, Err(StoreError::Failed(message)) if message.contains("did not answer")),
            "{result:?}"
        );
    }

    /// Contract: a call waiting on a slow asynchronous handler is polled
    /// when the handler wakes it, not in a loop: its thread does not spin
    /// while the store works.
    ///
    /// ```text
    /// call ──► handler: pending (answers in 200 ms, then wakes the call)
    /// block_on polls: once at the start, once when woken ──► the answer
    /// ```
    #[test]
    fn a_slow_handler_does_not_spin_its_thread() {
        struct Slow {
            polls: Rc<Cell<usize>>,
            answered: std::sync::Arc<std::sync::atomic::AtomicBool>,
            started: bool,
        }
        impl Future for Slow {
            type Output = (bool, String);
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                context: &mut std::task::Context<'_>,
            ) -> Poll<Self::Output> {
                self.polls.set(self.polls.get() + 1);
                if self.answered.load(std::sync::atomic::Ordering::SeqCst) {
                    return Poll::Ready((true, "null".to_owned()));
                }
                if !self.started {
                    self.started = true;
                    let answered = std::sync::Arc::clone(&self.answered);
                    let waker = context.waker().clone();
                    std::thread::spawn(move || {
                        std::thread::sleep(Duration::from_millis(200));
                        answered.store(true, std::sync::atomic::Ordering::SeqCst);
                        waker.wake();
                    });
                }
                Poll::Pending
            }
        }
        let polls = Rc::new(Cell::new(0));
        let counted = Rc::clone(&polls);
        let store = JsStore::served_by(Rc::new(move |_| {
            Box::pin(Slow {
                polls: Rc::clone(&counted),
                answered: Default::default(),
                started: false,
            })
        }));
        let result =
            futures::executor::block_on(store.call("load", json!({ "scope": "s", "doc": "d" })));
        assert_eq!(result, Ok(Value::Null));
        assert!(polls.get() <= 3, "polled {} times", polls.get());
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

    /// Contract: a store served on the spot behaves as the store it wraps:
    /// what one call saves the next loads, a stale save conflicts, and a
    /// request that is not one is refused, not answered with a guess.
    #[test]
    fn a_served_store_is_the_store_it_wraps() {
        fn block<T>(future: impl Future<Output = T>) -> T {
            jazz::db::block_on(future)
        }
        let store = JsStore::served_by(serve(Rc::new(crate::MemoryStore::new())));
        let scope = block(store.open_scope("device")).unwrap();
        assert_eq!(block(store.load("device", "doc")).unwrap(), None);
        let first = block(store.save("device", scope, "doc", b"one", None)).unwrap();
        let found = block(store.load("device", "doc")).unwrap().unwrap();
        assert_eq!((found.bytes, found.revision), (b"one".to_vec(), first));
        assert!(matches!(
            block(store.save("device", scope, "doc", b"two", None)),
            Err(StoreError::Conflict)
        ));
        assert_eq!(block(store.list("device")).unwrap(), vec!["doc".to_owned()]);
        let handler = serve(Rc::new(crate::MemoryStore::new()));
        let (ok, _) = block(handler("{\"op\":\"nothing\"}".to_owned()));
        assert!(!ok);
        let (ok, _) = block(handler("not json".to_owned()));
        assert!(!ok);
    }
}
