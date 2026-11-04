//! Large-value bytes in a Vercel Blob store, written and read by the core
//! itself (nothing is uploaded from the browser).
//!
//! Each settled leaf is one private blob, named by its node hash under a
//! prefix. Vercel Blob reports no checksum of what it stored, so every read
//! is checked against the node's hash here: a blob that is not its node is
//! an error, never a value.
//!
//! The store authenticates with a Vercel OIDC token and the store's id
//! (`BLOB_STORE_ID`), as `@vercel/blob` does: in a `vercel()` Sandbox that
//! is the deploying function's own token, presented on each request, so the
//! Sandbox holds no secret. A read-write token (`BLOB_READ_WRITE_TOKEN`)
//! works too.
//!
//! The REST calls are the ones `@vercel/blob` makes: `PUT /?pathname=` and
//! `POST /delete` against the Blob API, and reads from the store's own
//! private host.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use jazz::chunk_store::{ChunkStore, NodeKey, StoreFuture};
use jazz::db::{Error, ErrorCode};
use sha2::{Digest, Sha256};

use crate::s3::{WebIdentityToken, uri_encode};

/// The Blob API `@vercel/blob` talks to.
pub const DEFAULT_API_URL: &str = "https://vercel.com/api/blob";
/// The Blob API version these requests are written for.
const API_VERSION: &str = "13";
/// An operation that takes longer fails, and is retried in a later batch.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// A failed claim answers every operation for this long, so a batch reads
/// the owner blob once, not once per operation.
const CLAIM_FAILURE_HOLD: Duration = Duration::from_secs(5);
/// Under the prefix: the id of the store that owns it.
const OWNER_OBJECT: &str = ".jazz-owner";
/// At the Blob store's root: the one environment whose cores may use it.
pub(crate) const ENVIRONMENT_OBJECT: &str = crate::s3::ENVIRONMENT_OBJECT;

/// How the store authenticates to Vercel Blob.
#[derive(Clone)]
pub enum VercelBlobCredentials {
    /// A Vercel OIDC token, read afresh for every request.
    Oidc(WebIdentityToken),
    /// A store's read-write token (`BLOB_READ_WRITE_TOKEN`).
    ReadWriteToken(String),
}

impl std::fmt::Debug for VercelBlobCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Oidc(token) => f.debug_tuple("Oidc").field(token).finish(),
            Self::ReadWriteToken(_) => f.write_str("ReadWriteToken"),
        }
    }
}

/// A Vercel Blob store for large-value bytes.
#[derive(Clone, Debug)]
pub struct VercelBlobOptions {
    /// The store's id, without its `store_` prefix.
    pub store_id: String,
    /// Prepended to every blob's pathname. Empty is the store's own
    /// default, `jazz/<store id>/`, which the server fills in. The
    /// prefix is claimed for one store by an owner blob.
    pub prefix: String,
    pub credentials: VercelBlobCredentials,
    /// The Blob API (`VERCEL_BLOB_API_URL`), by default [`DEFAULT_API_URL`].
    pub api_url: String,
    /// Where the store's private blobs are read; `None` is
    /// `https://<store id>.private.blob.vercel-storage.com`.
    pub blob_url: Option<String>,
    /// The deployment environment (`production`, `preview`, …) this
    /// store runs in. When set, the Blob store is claimed for that
    /// environment by a marker blob at its root, and a store of any other
    /// environment is refused: one environment's OIDC token reaches the
    /// whole Blob store, so environments never share one (a `vercel()`
    /// core sets it).
    pub environment: Option<String>,
}

impl VercelBlobOptions {
    /// A store named by the environment, filling in what the arguments
    /// leave out: `BLOB_STORE_ID` (or the store a read-write token names),
    /// and `credentials`, by default `VERCEL_OIDC_TOKEN` or else
    /// `BLOB_READ_WRITE_TOKEN`.
    pub fn from_env(
        store_id: Option<String>,
        prefix: Option<String>,
        credentials: Option<VercelBlobCredentials>,
    ) -> Result<Self, String> {
        let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let read_write = env("BLOB_READ_WRITE_TOKEN");
        let store_id = store_id
            .or_else(|| env("BLOB_STORE_ID"))
            .or_else(|| read_write.as_deref().and_then(store_of_read_write_token))
            .ok_or(
                "vercel blobs: no store (set BLOB_STORE_ID, or connect a Blob store to the project)",
            )?;
        let credentials = match (credentials, env("VERCEL_OIDC_TOKEN"), read_write) {
            (Some(credentials), _, _) => credentials,
            (None, Some(_), _) => {
                VercelBlobCredentials::Oidc(WebIdentityToken::Env("VERCEL_OIDC_TOKEN".to_owned()))
            }
            (None, None, Some(token)) => VercelBlobCredentials::ReadWriteToken(token),
            (None, None, None) => {
                return Err(
                    "vercel blobs: no credentials (set VERCEL_OIDC_TOKEN or BLOB_READ_WRITE_TOKEN)"
                        .to_owned(),
                );
            }
        };
        Ok(Self {
            store_id: normalize_store_id(&store_id),
            prefix: prefix.unwrap_or_default(),
            credentials,
            api_url: env("VERCEL_BLOB_API_URL").unwrap_or_else(|| DEFAULT_API_URL.to_owned()),
            blob_url: None,
            environment: None,
        })
    }

    /// Where the store's blobs are, for the blob-store record and logs.
    pub fn describe(&self) -> String {
        format!("vercel-blob {}/{}", self.store_id, self.prefix)
    }

    fn blob_base(&self) -> String {
        match &self.blob_url {
            Some(url) => url.trim_end_matches('/').to_owned(),
            // Host names are lower case, as the URLs Blob hands out are.
            None => format!(
                "https://{}.private.blob.vercel-storage.com",
                self.store_id.to_ascii_lowercase()
            ),
        }
    }
}

/// `store_abc` and `abc` name the same store.
fn normalize_store_id(id: &str) -> String {
    id.strip_prefix("store_").unwrap_or(id).to_owned()
}

/// The store a read-write token is for: `vercel_blob_rw_<store>_<secret>`.
fn store_of_read_write_token(token: &str) -> Option<String> {
    token
        .split('_')
        .nth(3)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

/// Large-value bytes in a Vercel Blob store ([`VercelBlobOptions`]).
pub struct VercelBlobChunkStore {
    inner: Arc<Inner>,
    runtime: Option<tokio::runtime::Runtime>,
}

struct Inner {
    options: VercelBlobOptions,
    http: reqwest::Client,
    /// The store id this prefix is claimed for, and whether the claim has
    /// been checked (once per process, before the first operation).
    owner: String,
    claimed: tokio::sync::OnceCell<()>,
    claim_failed: Mutex<Option<(Instant, String)>>,
}

impl VercelBlobChunkStore {
    /// A store whose blobs live under `options.prefix`, claimed for the
    /// store `owner` (a data directory's id): a prefix another store
    /// claimed is refused on first use.
    pub fn new(options: VercelBlobOptions, owner: String) -> Result<Self, String> {
        if options.prefix.is_empty() {
            return Err("vercel blobs: no prefix; every store needs one of its own".to_owned());
        }
        if options.store_id.is_empty() {
            return Err("vercel blobs: no store id".to_owned());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("jazz-vercel-blob")
            .enable_all()
            .build()
            .map_err(|error| format!("vercel blobs: start runtime: {error}"))?;
        let http = reqwest::Client::builder()
            .timeout(OPERATION_TIMEOUT)
            .build()
            .map_err(|error| format!("vercel blobs: http client: {error}"))?;
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                http,
                owner,
                claimed: tokio::sync::OnceCell::new(),
                claim_failed: Mutex::new(None),
            }),
            runtime: Some(runtime),
        }
        .claiming_now())
    }

    /// Check the store's claims (environment, then prefix) right away, so
    /// a refused store says so at startup rather than at its first large
    /// value. A core whose only credential is a workload token that has not
    /// arrived yet checks them on first use instead.
    fn claiming_now(self) -> Self {
        let inner = Arc::clone(&self.inner);
        let waits_for_token = matches!(
            &inner.options.credentials,
            VercelBlobCredentials::Oidc(WebIdentityToken::Workload)
        ) && crate::middleware::workload::latest_workload_token().is_none();
        if !waits_for_token && let Some(runtime) = &self.runtime {
            runtime.spawn(async move {
                if let Err(error) = inner.claim().await {
                    tracing::error!(%error, "vercel blobs: checking the large-value store at startup failed");
                }
            });
        }
        self
    }

    pub fn options(&self) -> &VercelBlobOptions {
        &self.inner.options
    }

    /// Run `op` on the store's runtime and await it from any executor.
    /// Dropping the future aborts the operation.
    fn spawn<T: Send + 'static>(
        &self,
        op: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
    ) -> StoreFuture<'_, T> {
        struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
        impl<T> Drop for AbortOnDrop<T> {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let mut handle = AbortOnDrop(
            self.runtime
                .as_ref()
                .expect("the runtime lives as long as the store")
                .spawn(op),
        );
        Box::pin(async move {
            match (&mut handle.0).await {
                Ok(result) => result.map_err(storage),
                Err(error) => Err(storage(format!("vercel blobs: operation failed: {error}"))),
            }
        })
    }
}

impl Drop for VercelBlobChunkStore {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn storage(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Storage, message)
}

impl ChunkStore for VercelBlobChunkStore {
    fn put<'a>(&'a self, node: &'a NodeKey, bytes: &'a [u8]) -> StoreFuture<'a, ()> {
        let inner = Arc::clone(&self.inner);
        let (node, bytes) = (*node, Bytes::copy_from_slice(bytes));
        self.spawn(async move { inner.put(&node, bytes).await })
    }

    fn get<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, Option<Bytes>> {
        let inner = Arc::clone(&self.inner);
        let node = *node;
        self.spawn(async move { inner.get(&node).await })
    }

    fn delete<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, ()> {
        let inner = Arc::clone(&self.inner);
        let node = *node;
        self.spawn(async move { inner.delete(&node).await })
    }
}

impl Inner {
    fn pathname(&self, name: &str) -> String {
        format!("{}{name}", self.options.prefix)
    }

    /// `name` under the prefix, or at the store's root.
    fn located(&self, name: &str, rooted: bool) -> String {
        if rooted {
            name.to_owned()
        } else {
            self.pathname(name)
        }
    }

    /// The blob's URL, where it is read.
    fn blob_url(&self, pathname: &str) -> String {
        format!(
            "{}/{}",
            self.options.blob_base(),
            uri_encode(pathname, false)
        )
    }

    fn token(&self) -> Result<String, String> {
        match &self.options.credentials {
            VercelBlobCredentials::ReadWriteToken(token) => Ok(token.clone()),
            VercelBlobCredentials::Oidc(token) => token
                .read()
                .map_err(|error| error.replace("web identity token", "vercel blobs: OIDC token")),
        }
    }

    /// A request to the Blob API, authenticated.
    fn api(&self, method: reqwest::Method, path: &str) -> Result<reqwest::RequestBuilder, String> {
        Ok(self
            .http
            .request(
                method,
                format!("{}{path}", self.options.api_url.trim_end_matches('/')),
            )
            .bearer_auth(self.token()?)
            .header("x-api-version", API_VERSION)
            .header("x-vercel-blob-store-id", &self.options.store_id))
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<reqwest::Response, String> {
        request
            .send()
            .await
            .map_err(|error| format!("vercel blobs: {what}: {}", error.without_url()))
    }

    /// Store `bytes` as the private blob `name`; with `overwrite` false,
    /// only if there is none yet.
    async fn upload(
        &self,
        name: &str,
        rooted: bool,
        bytes: Bytes,
        overwrite: bool,
    ) -> Result<(), String> {
        let path = format!(
            "/?pathname={}",
            uri_encode(&self.located(name, rooted), true)
        );
        let request = self
            .api(reqwest::Method::PUT, &path)?
            .header("x-vercel-blob-access", "private")
            .header("x-add-random-suffix", "0")
            .header("x-allow-overwrite", if overwrite { "1" } else { "0" })
            .header("x-content-type", "application/octet-stream")
            .header("x-content-length", bytes.len().to_string())
            .body(bytes);
        let response = self.send(request, &format!("put {name}")).await?;
        expect_ok(response, "put", name).await
    }

    /// The blob `name`, or `None` if there is none. Reads of the owner
    /// blob skip Vercel's cache (`?cache=0`, what `@vercel/blob`'s
    /// `useCache: false` sends); leaves never change once written.
    async fn download(
        &self,
        name: &str,
        rooted: bool,
        limit: usize,
        uncached: bool,
    ) -> Result<Option<Bytes>, String> {
        let mut url = self.blob_url(&self.located(name, rooted));
        if uncached {
            url.push_str("?cache=0");
        }
        let request = self.http.get(url).bearer_auth(self.token()?);
        let response = self.send(request, &format!("get {name}")).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(failure(status, &body, "get", name));
        }
        read_bounded(response, limit, name).await.map(Some)
    }

    /// Check, once per process, that the prefix belongs to this store,
    /// claiming it if no store has yet. A failed check answers every
    /// operation for a moment and is then tried again.
    ///
    /// The claim is a put with `x-allow-overwrite: 0`, read back. Unlike
    /// S3's `If-None-Match: *`, Vercel does not document that put as an
    /// atomic create-if-absent, so two stores that claim one prefix in the
    /// same instant might both pass. That needs two data dirs given the
    /// same explicit prefix and started together: the default prefix holds
    /// the data dir's own id. The claim is there to catch a prefix reused
    /// by mistake, which it does whatever the put's semantics.
    async fn claim(&self) -> Result<(), String> {
        if let Some((at, error)) = self.claim_failed.lock().unwrap().as_ref()
            && at.elapsed() < CLAIM_FAILURE_HOLD
        {
            return Err(error.clone());
        }
        let claimed = self
            .claimed
            .get_or_try_init(|| async {
                self.claim_environment().await?;
                match self.read_owner().await? {
                    Some(owner) => self.check_owner(&owner),
                    None => {
                        let mine = Bytes::from(format!("{}\n", self.owner));
                        // Without overwrite, a store that claimed it first
                        // keeps it; whoever's id is there afterwards owns it.
                        let put = self.upload(OWNER_OBJECT, false, mine, false).await;
                        match self.read_owner().await? {
                            Some(owner) => self.check_owner(&owner),
                            None => Err(put.err().unwrap_or_else(|| {
                                "vercel blobs: the owner blob was written but cannot be read"
                                    .to_owned()
                            })),
                        }
                    }
                }
            })
            .await
            .map(|_| ());
        if let Err(error) = &claimed {
            *self.claim_failed.lock().unwrap() = Some((Instant::now(), error.clone()));
        }
        claimed
    }

    fn check_owner(&self, owner: &str) -> Result<(), String> {
        if owner == self.owner {
            Ok(())
        } else {
            Err(format!(
                "vercel blobs: {} belongs to store {owner}, not {}; give each store its own prefix",
                self.pathname(""),
                self.owner
            ))
        }
    }

    async fn read_owner(&self) -> Result<Option<String>, String> {
        self.read_marker(OWNER_OBJECT, false).await
    }

    /// A small marker blob's contents, read past the cache.
    async fn read_marker(&self, name: &str, rooted: bool) -> Result<Option<String>, String> {
        Ok(self
            .download(name, rooted, 1024, true)
            .await?
            .map(|body| String::from_utf8_lossy(&body).trim().to_owned()))
    }

    /// With [`VercelBlobOptions::environment`] set, check that the Blob
    /// store serves that environment only, claiming it if none has yet.
    ///
    /// The claim is a put with `x-allow-overwrite: 0`, read back. Vercel
    /// does not document that put as an atomic create-if-absent, so cores
    /// of two environments that start against a fresh Blob store in the
    /// same instant might both pass. The marker then holds one of them,
    /// and the other is refused the next time it starts (a `vercel()`
    /// core restarts with every deployment and every idle recycle), before
    /// it reads or writes anything. Until then it only writes under its
    /// own prefix, which the other environment never reads or deletes.
    async fn claim_environment(&self) -> Result<(), String> {
        let Some(environment) = &self.options.environment else {
            return Ok(());
        };
        let check = |claimed: String| {
            if &claimed == environment {
                Ok(())
            } else {
                Err(format!(
                    "vercel blobs: Blob store {} serves the {claimed} environment, not {environment}; \
                     connect a separate Blob store to each environment",
                    self.options.store_id
                ))
            }
        };
        if let Some(claimed) = self.read_marker(ENVIRONMENT_OBJECT, true).await? {
            return check(claimed);
        }
        let mine = Bytes::from(format!("{environment}\n"));
        let put = self.upload(ENVIRONMENT_OBJECT, true, mine, false).await;
        match self.read_marker(ENVIRONMENT_OBJECT, true).await? {
            Some(claimed) => check(claimed),
            None => Err(put.err().unwrap_or_else(|| {
                "vercel blobs: the environment blob was written but cannot be read".to_owned()
            })),
        }
    }

    async fn put(&self, node: &NodeKey, bytes: Bytes) -> Result<(), String> {
        self.claim().await?;
        self.upload(&node.object_name(), false, bytes, true).await
    }

    async fn get(&self, node: &NodeKey) -> Result<Option<Bytes>, String> {
        self.claim().await?;
        let name = node.object_name();
        let Some(bytes) = self
            .download(
                &name,
                false,
                jazz::large_values::MAX_ENCODED_NODE_BYTES,
                false,
            )
            .await?
        else {
            return Ok(None);
        };
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != node.hash {
            return Err(format!(
                "vercel blobs: {} does not hold its node (its SHA-256 differs)",
                self.pathname(&name)
            ));
        }
        Ok(Some(bytes))
    }

    async fn delete(&self, node: &NodeKey) -> Result<(), String> {
        self.claim().await?;
        let name = node.object_name();
        // The API takes pathnames as well as URLs (`@vercel/blob`'s `del`
        // passes either through), so no host has to match.
        let request = self
            .api(reqwest::Method::POST, "/delete")?
            .json(&serde_json::json!({ "urls": [self.pathname(&name)] }));
        let response = self.send(request, &format!("delete {name}")).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        expect_ok(response, "delete", &name).await
    }
}

async fn expect_ok(response: reqwest::Response, what: &str, name: &str) -> Result<(), String> {
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(failure(status, &body, what, name))
}

/// What a failed request comes to, from the Blob API's
/// `{"error":{"code","message"}}`.
fn failure(status: reqwest::StatusCode, body: &str, what: &str, name: &str) -> String {
    let error = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("error").cloned());
    let field = |key: &str| {
        error
            .as_ref()
            .and_then(|error| error.get(key))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    format!(
        "vercel blobs: {what} {name}: {status} {} {}",
        field("code"),
        field("message")
    )
    .trim_end()
    .to_owned()
}

/// A response body of at most `limit` bytes.
async fn read_bounded(
    mut response: reqwest::Response,
    limit: usize,
    name: &str,
) -> Result<Bytes, String> {
    let too_large = || format!("vercel blobs: {name} is larger than any node");
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(too_large());
    }
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("vercel blobs: read {name}: {}", error.without_url()))?
    {
        if body.len() + chunk.len() > limit {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block<T>(future: StoreFuture<'_, T>) -> Result<T, Error> {
        jazz::db::block_on(future)
    }

    fn node(seed: u8) -> (NodeKey, Vec<u8>) {
        let bytes = vec![seed; 1000];
        (
            NodeKey {
                hash: Sha256::digest(&bytes).into(),
            },
            bytes,
        )
    }

    #[test]
    fn names_the_store_of_a_read_write_token() {
        assert_eq!(
            store_of_read_write_token("vercel_blob_rw_AbC123_secret").as_deref(),
            Some("AbC123")
        );
        assert_eq!(store_of_read_write_token("vercel_blob_rw"), None);
        assert_eq!(normalize_store_id("store_AbC123"), "AbC123");
        assert_eq!(normalize_store_id("AbC123"), "AbC123");
    }

    /// Leaves are private blobs at exactly their pathname, put with the
    /// headers `@vercel/blob` sends and a bearer OIDC token, and read back
    /// from the store's own host.
    #[test]
    fn puts_and_reads_private_blobs_at_their_pathname() {
        use std::sync::atomic::Ordering;
        let blob = mock::MockBlob::start();
        let store = VercelBlobChunkStore::new(blob.options("jazz/app/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(1);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        assert_eq!(blob.names("jazz/app/"), vec![key.object_name()]);
        assert_eq!(
            blob.owner("jazz/app/").as_deref(),
            Some("me"),
            "the prefix is claimed"
        );
        // The claim's put, then the leaf's.
        assert_eq!(blob.state.calls.puts.load(Ordering::Relaxed), 2);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(
            blob.state.calls.puts.load(Ordering::Relaxed),
            3,
            "claimed once"
        );
    }

    /// Deletes reach the store; deleting or reading an absent leaf
    /// succeeds; a wrong token is refused without being echoed.
    #[test]
    fn deletes_reach_the_store_and_wrong_tokens_do_not() {
        let blob = mock::MockBlob::start();
        let store = VercelBlobChunkStore::new(blob.options("p/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(2);
        block(store.put(&key, &bytes)).unwrap();
        block(store.delete(&key)).unwrap();
        assert!(blob.names("p/").is_empty());
        block(store.delete(&key)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap(), None);

        let mut forged = blob.options("p/");
        forged.credentials = VercelBlobCredentials::ReadWriteToken("guess".to_owned());
        let forged = VercelBlobChunkStore::new(forged, "me".to_owned()).unwrap();
        let error = block(forged.put(&key, &bytes)).unwrap_err();
        assert!(error.message.contains("forbidden"), "{error}");
        assert!(!error.message.contains("guess"), "{error}");
    }

    /// Vercel Blob checks no checksum, so a blob that is not its node is
    /// refused on read.
    #[test]
    fn a_blob_that_is_not_its_node_is_refused() {
        let blob = mock::MockBlob::start();
        let store = VercelBlobChunkStore::new(blob.options("p/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(3);
        block(store.put(&key, &bytes)).unwrap();
        blob.state
            .blobs
            .lock()
            .unwrap()
            .insert(format!("p/{}", key.object_name()), b"tampered".to_vec());
        let error = block(store.get(&key)).unwrap_err();
        assert!(error.message.contains("does not hold its node"), "{error}");
    }

    /// A prefix is claimed by the first store to use it; another store is
    /// refused there before it writes or deletes anything.
    #[test]
    fn two_stores_cannot_share_a_prefix() {
        let blob = mock::MockBlob::start();
        let (key, bytes) = node(4);
        let first = VercelBlobChunkStore::new(blob.options("p/"), "store-a".to_owned()).unwrap();
        block(first.put(&key, &bytes)).unwrap();
        let second = VercelBlobChunkStore::new(blob.options("p/"), "store-b".to_owned()).unwrap();
        let error = block(second.delete(&key)).unwrap_err();
        assert!(
            error.message.contains("belongs to store store-a"),
            "{error}"
        );
        assert_eq!(blob.names("p/"), vec![key.object_name()], "nothing deleted");
        let beside = VercelBlobChunkStore::new(blob.options("q/"), "store-b".to_owned()).unwrap();
        block(beside.put(&key, &bytes)).unwrap();
    }

    /// A Blob store serves one environment: the first store with an
    /// environment claims it at the store's root, and a store of another
    /// environment is refused under any prefix, before it touches a blob.
    #[test]
    fn a_blob_store_serves_one_environment() {
        let blob = mock::MockBlob::start();
        let (key, bytes) = node(5);
        let in_env = |prefix: &str, environment: &str| {
            let mut options = blob.options(prefix);
            options.environment = Some(environment.to_owned());
            options
        };
        let production =
            VercelBlobChunkStore::new(in_env("prod/", "production"), "store-a".to_owned()).unwrap();
        block(production.put(&key, &bytes)).unwrap();
        assert_eq!(
            blob.state
                .blobs
                .lock()
                .unwrap()
                .get(ENVIRONMENT_OBJECT)
                .map(|b| b.as_slice()),
            Some(&b"production\n"[..])
        );
        let preview =
            VercelBlobChunkStore::new(in_env("preview/", "preview"), "store-b".to_owned()).unwrap();
        let error = block(preview.put(&key, &bytes)).unwrap_err();
        assert!(
            error
                .message
                .contains("serves the production environment, not preview"),
            "{error}"
        );
        assert!(blob.names("preview/").is_empty(), "nothing written");
        assert!(blob.owner("preview/").is_none(), "no prefix claimed");
        // Another production store shares it.
        let other = VercelBlobChunkStore::new(in_env("other/", "production"), "store-c".to_owned())
            .unwrap();
        block(other.put(&key, &bytes)).unwrap();
    }

    /// Of two stores claiming one prefix at once, one gets it; the owner
    /// is read past the CDN, which may still remember there was none.
    #[test]
    fn of_two_stores_claiming_a_prefix_at_once_one_gets_it() {
        for _ in 0..20 {
            let blob = mock::MockBlob::start();
            let (key, bytes) = node(6);
            // The CDN has seen the prefix unclaimed.
            let owner = format!("p/{OWNER_OBJECT}");
            blob.state.cached.lock().unwrap().insert(owner, None);
            let a = VercelBlobChunkStore::new(blob.options("p/"), "store-a".to_owned()).unwrap();
            let b = VercelBlobChunkStore::new(blob.options("p/"), "store-b".to_owned()).unwrap();
            let (first, second) = jazz::db::block_on(futures::future::join(
                a.put(&key, &bytes),
                b.put(&key, &bytes),
            ));
            assert!(first.is_ok() != second.is_ok(), "{first:?} {second:?}");
            let winner = if first.is_ok() { "store-a" } else { "store-b" };
            assert_eq!(blob.owner("p/").as_deref(), Some(winner));
            let loser = first.err().or(second.err()).unwrap();
            assert!(
                loser
                    .message
                    .contains(&format!("belongs to store {winner}")),
                "{loser}"
            );
        }
    }

    /// The OIDC token is read for every request, so a fresh one (in a
    /// Sandbox, the latest the function presented) is used as it arrives.
    #[test]
    fn reads_the_oidc_token_afresh_for_every_request() {
        let blob = mock::MockBlob::start();
        let token = Arc::new(Mutex::new(Some("old".to_owned())));
        let mut options = blob.options("p/");
        options.credentials =
            VercelBlobCredentials::Oidc(WebIdentityToken::Shared(Arc::clone(&token)));
        *blob.state.token.lock().unwrap() = "old".to_owned();
        let store = VercelBlobChunkStore::new(options, "me".to_owned()).unwrap();
        let (key, bytes) = node(5);
        block(store.put(&key, &bytes)).unwrap();
        *blob.state.token.lock().unwrap() = "new".to_owned();
        assert!(block(store.get(&key)).is_err(), "the old token has expired");
        *token.lock().unwrap() = Some("new".to_owned());
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));

        let mut none = blob.options("r/");
        none.credentials =
            VercelBlobCredentials::Oidc(WebIdentityToken::Shared(Arc::new(Mutex::new(None))));
        let none = VercelBlobChunkStore::new(none, "me".to_owned()).unwrap();
        let error = block(none.get(&key)).unwrap_err();
        assert!(error.message.contains("OIDC token"), "{error}");
    }

    #[test]
    fn refuses_a_store_without_a_prefix() {
        let blob = mock::MockBlob::start();
        assert!(VercelBlobChunkStore::new(blob.options(""), "me".to_owned()).is_err());
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    pub(crate) struct Calls {
        pub gets: AtomicU64,
        pub puts: AtomicU64,
    }

    pub(crate) struct State {
        /// Blobs by pathname.
        pub blobs: Mutex<BTreeMap<String, Vec<u8>>>,
        pub calls: Calls,
        /// What the CDN serves for reads without `?cache=0`: the first
        /// answer for each pathname, stale or not.
        pub cached: Mutex<BTreeMap<String, Option<Vec<u8>>>>,
        /// The one bearer token accepted.
        pub token: Mutex<String>,
    }

    pub(crate) struct MockBlob {
        pub state: Arc<State>,
        pub url: String,
        _runtime: tokio::runtime::Runtime,
    }

    impl MockBlob {
        pub(crate) fn start() -> Self {
            let state = Arc::new(State {
                blobs: Mutex::default(),
                calls: Calls::default(),
                cached: Mutex::default(),
                token: Mutex::new("vercel_blob_rw_mockstore_secret".to_owned()),
            });
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let listener = runtime
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let served = Arc::clone(&state);
            let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
                let state = Arc::clone(&served);
                async move { serve(&state, request).await }
            });
            runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                state,
                url,
                _runtime: runtime,
            }
        }

        /// Options for this store under `prefix`, with its read-write token.
        pub(crate) fn options(&self, prefix: &str) -> VercelBlobOptions {
            VercelBlobOptions {
                store_id: "mockstore".to_owned(),
                prefix: prefix.to_owned(),
                credentials: VercelBlobCredentials::ReadWriteToken(
                    self.state.token.lock().unwrap().clone(),
                ),
                api_url: format!("{}/api/blob", self.url),
                blob_url: Some(format!("{}/blobs", self.url)),
                environment: None,
            }
        }

        /// Blob names under `prefix` (without it), owner blob aside.
        pub(crate) fn names(&self, prefix: &str) -> Vec<String> {
            self.state
                .blobs
                .lock()
                .unwrap()
                .keys()
                .filter_map(|key| key.strip_prefix(prefix))
                .filter(|name| *name != OWNER_OBJECT && !name.contains('/'))
                .map(str::to_owned)
                .collect()
        }

        pub(crate) fn owner(&self, prefix: &str) -> Option<String> {
            self.state
                .blobs
                .lock()
                .unwrap()
                .get(&format!("{prefix}{OWNER_OBJECT}"))
                .map(|body| String::from_utf8_lossy(body).trim().to_owned())
        }
    }

    type Reply = (axum::http::StatusCode, Vec<u8>);

    fn error(status: axum::http::StatusCode, code: &str) -> Reply {
        (
            status,
            format!(r#"{{"error":{{"code":"{code}","message":"mock"}}}}"#).into_bytes(),
        )
    }

    fn decode(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    async fn serve(state: &State, request: axum::extract::Request) -> Reply {
        use axum::http::StatusCode;
        let (parts, body) = request.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
        };
        let expected = format!("Bearer {}", state.token.lock().unwrap());
        if header("authorization") != Some(expected.as_str()) {
            return error(StatusCode::FORBIDDEN, "forbidden");
        }
        let path = parts.uri.path();
        let mut blobs = state.blobs.lock().unwrap();
        if let Some(pathname) = path.strip_prefix("/blobs/") {
            state.calls.gets.fetch_add(1, Ordering::Relaxed);
            let pathname = decode(pathname);
            let current = blobs.get(&pathname).cloned();
            let served = if parts.uri.query() == Some("cache=0") {
                current
            } else {
                state
                    .cached
                    .lock()
                    .unwrap()
                    .entry(pathname)
                    .or_insert(current)
                    .clone()
            };
            return match served {
                Some(bytes) => (StatusCode::OK, bytes),
                None => (StatusCode::NOT_FOUND, b"Not Found".to_vec()),
            };
        }
        if header("x-api-version") != Some(API_VERSION)
            || header("x-vercel-blob-store-id") != Some("mockstore")
        {
            return error(StatusCode::BAD_REQUEST, "bad_request");
        }
        match (parts.method.as_str(), path) {
            ("PUT", "/api/blob/") => {
                state.calls.puts.fetch_add(1, Ordering::Relaxed);
                let query = parts.uri.query().unwrap_or_default();
                let Some(pathname) = query.strip_prefix("pathname=") else {
                    return error(StatusCode::BAD_REQUEST, "bad_request");
                };
                let pathname = decode(pathname);
                if header("x-vercel-blob-access") != Some("private")
                    || header("x-add-random-suffix") != Some("0")
                    || header("x-content-length") != Some(body.len().to_string().as_str())
                {
                    return error(StatusCode::BAD_REQUEST, "bad_request");
                }
                // The status Vercel answers here is undocumented; the
                // claim reads the owner back rather than rely on it.
                if header("x-allow-overwrite") != Some("1") && blobs.contains_key(&pathname) {
                    return error(StatusCode::CONFLICT, "blob_already_exists");
                }
                blobs.insert(pathname.clone(), body.to_vec());
                (
                    StatusCode::OK,
                    serde_json::json!({ "pathname": pathname })
                        .to_string()
                        .into_bytes(),
                )
            }
            ("POST", "/api/blob/delete") => {
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                for pathname in request["urls"].as_array().unwrap() {
                    blobs.remove(pathname.as_str().unwrap());
                }
                (StatusCode::OK, b"{}".to_vec())
            }
            _ => error(StatusCode::NOT_FOUND, "not_found"),
        }
    }
}
