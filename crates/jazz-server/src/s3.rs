//! Large-value bytes in an S3-compatible bucket: AWS S3 (on Vercel through
//! the Marketplace's OIDC role), Cloudflare R2, MinIO.
//!
//! Each settled leaf is one object, named by its node hash under a prefix.
//! A node's hash is the SHA-256 of exactly the bytes stored, so every put
//! carries `x-amz-checksum-sha256` and the bucket itself rejects a body
//! that is not the node. Every object is immutable: a put of a key again
//! writes the same bytes.
//!
//! The store's operations run on its own small Tokio runtime and are
//! awaited from wherever the database awaits them (a tick on the store
//! thread, or a host's chunk I/O thread), so no caller needs a reactor.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use jazz::chunk_store::{ChunkStore, NodeKey, StoreFuture};
use jazz::db::{Error, ErrorCode};
use sha2::{Digest, Sha256};

/// Where a web identity token for `AssumeRoleWithWebIdentity` comes from.
#[derive(Clone)]
pub enum WebIdentityToken {
    /// Read afresh from this file on each refresh
    /// (`AWS_WEB_IDENTITY_TOKEN_FILE`).
    File(PathBuf),
    /// This environment variable, read on each refresh
    /// (`VERCEL_OIDC_TOKEN`).
    Env(String),
    /// The latest token a host handed over.
    Shared(Arc<Mutex<Option<String>>>),
    /// The trusted workload's latest verified token: in a `vercel()`
    /// Sandbox, the deploying function's OIDC token, presented on each
    /// request, which the Marketplace's AWS role trusts.
    Workload,
}

impl std::fmt::Debug for WebIdentityToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "File({})", path.display()),
            Self::Env(name) => write!(f, "Env({name})"),
            Self::Shared(_) => f.write_str("Shared"),
            Self::Workload => f.write_str("Workload"),
        }
    }
}

impl WebIdentityToken {
    pub(crate) fn read(&self) -> Result<String, String> {
        match self {
            Self::File(path) => std::fs::read_to_string(path)
                .map(|token| token.trim().to_owned())
                .map_err(|error| format!("read web identity token {}: {error}", path.display())),
            Self::Env(name) => {
                std::env::var(name).map_err(|_| format!("web identity token: {name} is not set"))
            }
            Self::Shared(token) => token
                .lock()
                .ok()
                .and_then(|token| token.clone())
                .ok_or_else(|| "web identity token: none handed over yet".to_owned()),
            Self::Workload => {
                crate::middleware::workload::latest_workload_token().ok_or_else(|| {
                    "web identity token: no trusted workload token arrived yet".to_owned()
                })
            }
        }
    }
}

/// How the store authenticates to the bucket.
#[derive(Clone, Debug)]
pub enum S3Credentials {
    /// Long-lived or session keys (`AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
    Static(Keys),
    /// Short-lived keys for `role_arn`, assumed with an OIDC token (the
    /// Vercel Marketplace's AWS integration sets `AWS_ROLE_ARN`).
    WebIdentity {
        role_arn: String,
        token: WebIdentityToken,
    },
}

/// One set of AWS keys.
#[derive(Clone)]
pub struct Keys {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// An S3-compatible bucket for large-value bytes.
#[derive(Clone, Debug)]
pub struct S3Options {
    pub bucket: String,
    /// The bucket's region (`auto` for R2).
    pub region: String,
    /// The service endpoint, e.g. `https://<account>.r2.cloudflarestorage.com`.
    /// `None` is AWS S3 in `region`.
    pub endpoint: Option<String>,
    /// Prepended to every object name. Empty is the store's own default,
    /// `jazz/<app id>/<store id>/`, which the server fills in: two stores
    /// never share a prefix by accident, since each deletes what it
    /// reclaims. The prefix is claimed for one store by an owner object.
    pub prefix: String,
    /// Address the bucket in the path (`endpoint/bucket/key`) rather than
    /// the host name. The default for custom endpoints, and always used
    /// for a bucket whose name has a dot (its host would not match AWS's
    /// TLS certificate).
    pub path_style: bool,
    pub credentials: S3Credentials,
    /// The STS endpoint web identity roles are assumed at
    /// (`AWS_ENDPOINT_URL_STS`); `None` is AWS's, in `region`.
    pub sts_endpoint: Option<String>,
    /// The deployment environment (`production`, `preview`, …) this
    /// store runs in. When set, the bucket is claimed for that environment
    /// by a marker at its root, and a store of any other environment is
    /// refused: one environment's credentials then never reach another's
    /// large values (a `vercel()` core sets it).
    pub environment: Option<String>,
}

impl S3Options {
    /// A bucket named by the environment, filling in what the arguments
    /// leave out: `S3_BUCKET_NAME`, `AWS_REGION`, `AWS_ROLE_ARN` with
    /// `AWS_WEB_IDENTITY_TOKEN_FILE` or `VERCEL_OIDC_TOKEN` (the Vercel
    /// Marketplace's AWS integration), or else `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY`.
    pub fn from_env(
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        prefix: Option<String>,
    ) -> Result<Self, String> {
        let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let bucket = bucket
            .or_else(|| env("S3_BUCKET_NAME"))
            .ok_or("s3 blobs: no bucket (set S3_BUCKET_NAME or pass one)")?;
        let region = region
            .or_else(|| env("AWS_REGION"))
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .or_else(|| endpoint.as_ref().map(|_| "auto".to_owned()))
            .ok_or("s3 blobs: no region (set AWS_REGION or pass one)")?;
        let credentials = match (env("AWS_ROLE_ARN"), env("AWS_ACCESS_KEY_ID")) {
            (Some(role_arn), _) => S3Credentials::WebIdentity {
                role_arn,
                token: match env("AWS_WEB_IDENTITY_TOKEN_FILE") {
                    Some(file) => WebIdentityToken::File(file.into()),
                    None => WebIdentityToken::Env("VERCEL_OIDC_TOKEN".to_owned()),
                },
            },
            (None, Some(access_key_id)) => S3Credentials::Static(Keys {
                access_key_id,
                secret_access_key: env("AWS_SECRET_ACCESS_KEY")
                    .ok_or("s3 blobs: AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY")?,
                session_token: env("AWS_SESSION_TOKEN"),
            }),
            (None, None) => {
                return Err(
                    "s3 blobs: no credentials (set AWS_ROLE_ARN for an OIDC role, \
                     or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)"
                        .to_owned(),
                );
            }
        };
        Ok(Self {
            bucket,
            region,
            path_style: endpoint.is_some(),
            endpoint,
            prefix: prefix.unwrap_or_default(),
            credentials,
            sts_endpoint: env("AWS_ENDPOINT_URL_STS"),
            environment: None,
        })
    }

    /// Where the store's objects are, for the blob-store record and logs.
    pub fn describe(&self) -> String {
        let at = self.endpoint.as_deref().unwrap_or("aws");
        format!("s3 {at} {}/{}", self.bucket, self.prefix)
    }
}

/// Assumed keys stay in use until this long before they expire.
const REFRESH_EARLY: Duration = Duration::from_secs(5 * 60);
/// How long assumed keys are asked to last.
const ASSUMED_FOR: Duration = Duration::from_secs(60 * 60);
/// An operation that takes longer fails, and is retried in a later batch.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// A failed role assumption answers every operation for this long, so a
/// batch asks STS once, not once per operation.
const ASSUME_FAILURE_HOLD: Duration = Duration::from_secs(5);
/// Under the prefix: the id of the store that owns it.
const OWNER_OBJECT: &str = ".jazz-owner";
/// At the bucket's root: the one environment (production, preview, …)
/// whose stores may use the bucket.
pub(crate) const ENVIRONMENT_OBJECT: &str = ".jazz-environment";

/// What reading a marker object found.
enum Marker {
    Found(String),
    /// 404: no such object.
    Missing,
    /// 403 AccessDenied: absent to keys without s3:ListBucket, or not
    /// readable with these keys.
    Denied,
}

/// Large-value bytes in an S3-compatible bucket ([`S3Options`]).
pub struct S3ChunkStore {
    inner: Arc<Inner>,
    runtime: Option<tokio::runtime::Runtime>,
}

struct Inner {
    options: S3Options,
    http: reqwest::Client,
    keys: tokio::sync::Mutex<KeyState>,
    /// The store id this prefix is claimed for, and whether the claim
    /// has been checked (once per process, before the first operation).
    owner: String,
    claimed: tokio::sync::OnceCell<()>,
    /// The last claim that failed, and when: it answers every operation
    /// for a while, as a failed role assumption does.
    claim_failed: Mutex<Option<(Instant, String)>>,
}

#[derive(Default)]
struct KeyState {
    /// Keys in use, and when assumed ones must be replaced.
    keys: Option<(Keys, Option<SystemTime>)>,
    /// The last role assumption that failed, and when.
    failed: Option<(Instant, String)>,
}

impl S3ChunkStore {
    /// A store whose objects live under `options.prefix`, claimed for the
    /// store `owner` (a data directory's id): a prefix another store
    /// claimed is refused on first use.
    pub fn new(options: S3Options, owner: String) -> Result<Self, String> {
        if options.prefix.is_empty() {
            return Err("s3 blobs: no prefix; every store needs one of its own".to_owned());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("jazz-s3")
            .enable_all()
            .build()
            .map_err(|error| format!("s3 blobs: start runtime: {error}"))?;
        let http = reqwest::Client::builder()
            .timeout(OPERATION_TIMEOUT)
            .build()
            .map_err(|error| format!("s3 blobs: http client: {error}"))?;
        let keys = KeyState {
            keys: match &options.credentials {
                S3Credentials::Static(keys) => Some((keys.clone(), None)),
                S3Credentials::WebIdentity { .. } => None,
            },
            failed: None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                http,
                keys: tokio::sync::Mutex::new(keys),
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
            S3Credentials::WebIdentity {
                token: WebIdentityToken::Workload,
                ..
            }
        ) && crate::middleware::workload::latest_workload_token().is_none();
        if !waits_for_token && let Some(runtime) = &self.runtime {
            runtime.spawn(async move {
                if let Err(error) = inner.claim().await {
                    tracing::error!(%error, "s3 blobs: checking the large-value store at startup failed");
                }
            });
        }
        self
    }

    pub fn options(&self) -> &S3Options {
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
                Err(error) => Err(storage(format!("s3 blobs: operation failed: {error}"))),
            }
        })
    }
}

impl Drop for S3ChunkStore {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn storage(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Storage, message)
}

impl ChunkStore for S3ChunkStore {
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

/// What one request to the bucket was.
struct Request<'a> {
    method: reqwest::Method,
    /// The object's name under the prefix.
    name: &'a str,
    body: Option<Bytes>,
    /// The SHA-256 of `body`, if there is one.
    body_hash: Option<[u8; 32]>,
    /// Put only if nothing is stored under the name.
    if_none_match: bool,
    /// `name` is at the bucket's root, not under the prefix.
    rooted: bool,
}

impl Inner {
    /// The object's URL and host, and its path as signed.
    fn locate(&self, name: &str, rooted: bool) -> (String, String, String) {
        let o = &self.options;
        let key = if rooted {
            name.to_owned()
        } else {
            format!("{}{name}", o.prefix)
        };
        let base = o
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", o.region));
        let base = base.trim_end_matches('/');
        let (scheme, authority) = base.split_once("://").unwrap_or(("https", base));
        let (host, path) = if o.path_style || o.bucket.contains('.') {
            (
                authority.to_owned(),
                format!(
                    "/{}/{}",
                    uri_encode(&o.bucket, true),
                    uri_encode(&key, false)
                ),
            )
        } else {
            (
                format!("{}.{authority}", o.bucket),
                format!("/{}", uri_encode(&key, false)),
            )
        };
        (format!("{scheme}://{host}{path}"), host, path)
    }

    async fn keys(&self) -> Result<Keys, String> {
        let mut state = self.keys.lock().await;
        if let Some((keys, until)) = state.keys.as_ref()
            && until.is_none_or(|until| SystemTime::now() < until)
        {
            return Ok(keys.clone());
        }
        if let Some((at, error)) = &state.failed
            && at.elapsed() < ASSUME_FAILURE_HOLD
        {
            return Err(error.clone());
        }
        let S3Credentials::WebIdentity { role_arn, token } = &self.options.credentials else {
            unreachable!("static keys never expire");
        };
        let assumed = match token.read() {
            Ok(token) => self.assume_role(role_arn, &token).await,
            Err(error) => Err(format!("s3 blobs: {error}")),
        };
        match assumed {
            Ok((keys, expires)) => {
                let until = expires.unwrap_or_else(|| SystemTime::now() + ASSUMED_FOR);
                state.keys = Some((keys.clone(), Some(until - REFRESH_EARLY)));
                state.failed = None;
                Ok(keys)
            }
            Err(error) => {
                state.failed = Some((Instant::now(), error.clone()));
                Err(error)
            }
        }
    }

    /// Short-lived keys for `role_arn` (STS `AssumeRoleWithWebIdentity`,
    /// which the token alone authenticates), and when they expire. The
    /// token goes in a form body, never in a URL that errors and logs
    /// could repeat.
    async fn assume_role(
        &self,
        role_arn: &str,
        token: &str,
    ) -> Result<(Keys, Option<SystemTime>), String> {
        let sts = match (&self.options.sts_endpoint, self.options.region.as_str()) {
            (Some(endpoint), _) => endpoint.clone(),
            (None, "auto" | "") => "https://sts.amazonaws.com".to_owned(),
            (None, region) => format!("https://sts.{region}.amazonaws.com"),
        };
        let response = self
            .http
            .post(sts)
            .form(&[
                ("Action", "AssumeRoleWithWebIdentity"),
                ("Version", "2011-06-15"),
                ("DurationSeconds", &ASSUMED_FOR.as_secs().to_string()),
                ("RoleArn", role_arn),
                ("RoleSessionName", "jazz-blobs"),
                ("WebIdentityToken", token),
            ])
            .send()
            .await
            .map_err(|error| format!("s3 blobs: assume {role_arn}: {}", error.without_url()))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!(
                "s3 blobs: assume {role_arn}: {status} {} {}",
                xml_field(&body, "Code").unwrap_or_default(),
                xml_field(&body, "Message").unwrap_or_default()
            ));
        }
        let field = |name| {
            xml_field(&body, name).ok_or_else(|| format!("s3 blobs: assume {role_arn}: no {name}"))
        };
        let keys = Keys {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("SecretAccessKey")?,
            session_token: Some(field("SessionToken")?),
        };
        let expires = xml_field(&body, "Expiration").and_then(|at| parse_iso8601(&at));
        Ok((keys, expires))
    }

    /// Forget assumed keys the bucket called expired or invalid.
    async fn drop_keys(&self) {
        if matches!(self.options.credentials, S3Credentials::WebIdentity { .. }) {
            self.keys.lock().await.keys = None;
        }
    }

    async fn send(&self, request: Request<'_>) -> Result<reqwest::Response, String> {
        let (url, host, path) = self.locate(request.name, request.rooted);
        let keys = self.keys().await?;
        let payload = hex::encode(
            request
                .body_hash
                .unwrap_or_else(|| Sha256::digest(b"").into()),
        );
        let mut headers = vec![
            ("host".to_owned(), host),
            ("x-amz-content-sha256".to_owned(), payload.clone()),
            ("x-amz-date".to_owned(), amz_date(SystemTime::now())),
        ];
        if let Some(hash) = request.body_hash {
            headers.push((
                "x-amz-checksum-sha256".to_owned(),
                base64::engine::general_purpose::STANDARD.encode(hash),
            ));
        }
        if request.if_none_match {
            headers.push(("if-none-match".to_owned(), "*".to_owned()));
        }
        if let Some(token) = &keys.session_token {
            headers.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        let method = request.method;
        let authorization = sign(
            method.as_str(),
            &path,
            "",
            &mut headers,
            &payload,
            &keys,
            &self.options.region,
        );
        let mut http = self.http.request(method, url);
        for (name, value) in headers.iter().filter(|(name, _)| name != "host") {
            http = http.header(name, value);
        }
        http = http.header("authorization", authorization);
        if let Some(body) = request.body {
            http = http.body(body);
        }
        http.send()
            .await
            .map_err(|error| format!("s3 blobs: {}", error.without_url()))
    }

    /// Check (once per process) that this store owns the prefix, claiming
    /// it if no store does yet.
    async fn claim(&self) -> Result<(), String> {
        if self.claimed.initialized() {
            return Ok(());
        }
        if let Some((at, error)) = &*self.claim_failed.lock().expect("claim state")
            && at.elapsed() < ASSUME_FAILURE_HOLD
        {
            return Err(error.clone());
        }
        let claimed = self.claimed
            .get_or_try_init(|| async {
                let refused = |other: &str| {
                    format!(
                        "s3 blobs: {} belongs to store {other}, not this one ({}); give each store its own bucket or prefix",
                        self.options.describe(),
                        self.owner
                    )
                };
                self.claim_environment().await?;
                match self.read_owner().await? {
                    Some(owner) if owner == self.owner => return Ok(()),
                    Some(owner) => return Err(refused(&owner)),
                    None => {}
                }
                let body = Bytes::from(format!("{}\n", self.owner));
                let response = self
                    .send(Request {
                        method: reqwest::Method::PUT,
                        name: OWNER_OBJECT,
                        body_hash: Some(Sha256::digest(&body).into()),
                        body: Some(body),
                        if_none_match: true,
                        rooted: false,
                    })
                    .await?;
                if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
                    // Another store claimed it first.
                    return match self.read_owner().await? {
                        Some(owner) if owner == self.owner => Ok(()),
                        Some(owner) => Err(refused(&owner)),
                        None => Err("s3 blobs: the owner object vanished while claiming".to_owned()),
                    };
                }
                self.expect_ok(response, "claim", OWNER_OBJECT).await.map(|_| ())
            })
            .await
            .map(|_| ());
        if let Err(error) = &claimed {
            *self.claim_failed.lock().expect("claim state") = Some((Instant::now(), error.clone()));
        }
        claimed
    }

    async fn read_owner(&self) -> Result<Option<String>, String> {
        self.read_marker(OWNER_OBJECT, false).await
    }

    /// A small marker object's contents, or `None` if there is none.
    async fn read_marker(&self, name: &str, rooted: bool) -> Result<Option<String>, String> {
        Ok(match self.read_marker_status(name, rooted).await? {
            Marker::Found(contents) => Some(contents),
            Marker::Missing | Marker::Denied => None,
        })
    }

    /// A small marker object's contents, or which answer said it is absent:
    /// a 404, or a 403 AccessDenied (absence to keys without s3:ListBucket,
    /// or a key that may not read it).
    async fn read_marker_status(&self, name: &str, rooted: bool) -> Result<Marker, String> {
        let response = self
            .send(Request {
                method: reqwest::Method::GET,
                name,
                body: None,
                body_hash: None,
                if_none_match: false,
                rooted,
            })
            .await?;
        let status = response.status();
        if status.is_success() {
            let body = read_bounded(response, 1024, name).await?;
            return Ok(Marker::Found(
                String::from_utf8_lossy(&body).trim().to_owned(),
            ));
        }
        let body = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(Marker::Missing);
        }
        if status == reqwest::StatusCode::FORBIDDEN
            && xml_field(&body, "Code").as_deref() == Some("AccessDenied")
        {
            return Ok(Marker::Denied);
        }
        Err(self.failure(status, &body, "read", name).await)
    }

    /// With [`S3Options::environment`] set, check that the bucket serves
    /// that environment only, claiming it for it if none has yet: a bucket
    /// another environment claimed is refused. The marker sits at the
    /// bucket's root, not under the prefix, since the whole bucket is what
    /// one environment's credentials reach.
    async fn claim_environment(&self) -> Result<(), String> {
        let Some(environment) = &self.options.environment else {
            return Ok(());
        };
        let unreachable_marker = || {
            format!(
                "s3 blobs: cannot claim bucket {bucket} for the {environment} environment: \
                 the credentials may not read or write s3://{bucket}/{ENVIRONMENT_OBJECT}. \
                 Allow s3:GetObject and s3:PutObject on arn:aws:s3:::{bucket}/{ENVIRONMENT_OBJECT} \
                 as well as on the prefix",
                bucket = self.options.bucket,
            )
        };
        let refused = |other: &str| {
            format!(
                "s3 blobs: bucket {} serves the {other} environment, not {environment}; \
                 give each environment its own bucket",
                self.options.bucket
            )
        };
        match self.read_marker(ENVIRONMENT_OBJECT, true).await? {
            Some(claimed) if &claimed == environment => return Ok(()),
            Some(claimed) => return Err(refused(&claimed)),
            None => {}
        }
        let body = Bytes::from(format!("{environment}\n"));
        let response = self
            .send(Request {
                method: reqwest::Method::PUT,
                name: ENVIRONMENT_OBJECT,
                body_hash: Some(Sha256::digest(&body).into()),
                body: Some(body),
                if_none_match: true,
                rooted: true,
            })
            .await?;
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            // A policy scoped to the prefix (`prefix/*`) reaches every
            // object but this one: its read answered AccessDenied, which
            // reads as absent, and its put is refused.
            let body = response.text().await.unwrap_or_default();
            if xml_field(&body, "Code").as_deref() == Some("AccessDenied") {
                return Err(unreachable_marker());
            }
            return Err(self
                .failure(
                    reqwest::StatusCode::FORBIDDEN,
                    &body,
                    "claim",
                    ENVIRONMENT_OBJECT,
                )
                .await);
        }
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return match self.read_marker_status(ENVIRONMENT_OBJECT, true).await? {
                Marker::Found(claimed) if &claimed == environment => Ok(()),
                Marker::Found(claimed) => Err(refused(&claimed)),
                // Its put says it exists, and its read was denied: the keys
                // may not read it (s3:GetObject).
                Marker::Denied => Err(unreachable_marker()),
                // Its put says it exists, and its read found none: deleted
                // in between. A later claim starts over.
                Marker::Missing => Err(format!(
                    "s3 blobs: the environment marker s3://{}/{ENVIRONMENT_OBJECT} vanished \
                     while claiming it for the {environment} environment",
                    self.options.bucket
                )),
            };
        }
        self.expect_ok(response, "claim", ENVIRONMENT_OBJECT)
            .await
            .map(|_| ())
    }

    async fn request(
        &self,
        method: reqwest::Method,
        name: &str,
    ) -> Result<reqwest::Response, String> {
        self.send(Request {
            method,
            name,
            body: None,
            body_hash: None,
            if_none_match: false,
            rooted: false,
        })
        .await
    }

    async fn put(&self, node: &NodeKey, bytes: Bytes) -> Result<(), String> {
        self.claim().await?;
        let name = node.object_name();
        let response = self
            .send(Request {
                method: reqwest::Method::PUT,
                name: &name,
                body: Some(bytes),
                // The node's hash is the SHA-256 of its bytes.
                body_hash: Some(node.hash),
                if_none_match: false,
                rooted: false,
            })
            .await?;
        self.expect_ok(response, "put", &name).await.map(|_| ())
    }

    async fn get(&self, node: &NodeKey) -> Result<Option<Bytes>, String> {
        self.claim().await?;
        let name = node.object_name();
        let response = self.request(reqwest::Method::GET, &name).await?;
        let Some(response) = self.found(response, "get", &name).await? else {
            return Ok(None);
        };
        read_bounded(response, jazz::large_values::MAX_ENCODED_NODE_BYTES, &name)
            .await
            .map(Some)
    }

    /// A successful GET's response, or `None` if the object is absent. A
    /// key that is missing answers 403 AccessDenied rather than 404 to keys
    /// without s3:ListBucket; reading is allowed under the prefix (or the
    /// owner object could not have been read), so that is absence too.
    async fn found(
        &self,
        response: reqwest::Response,
        what: &str,
        name: &str,
    ) -> Result<Option<reqwest::Response>, String> {
        let status = response.status();
        if status.is_success() {
            return Ok(Some(response));
        }
        let body = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::NOT_FOUND
            || (status == reqwest::StatusCode::FORBIDDEN
                && xml_field(&body, "Code").as_deref() == Some("AccessDenied"))
        {
            return Ok(None);
        }
        Err(self.failure(status, &body, what, name).await)
    }

    async fn delete(&self, node: &NodeKey) -> Result<(), String> {
        self.claim().await?;
        let name = node.object_name();
        let response = self.request(reqwest::Method::DELETE, &name).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.expect_ok(response, "delete", &name).await.map(|_| ())
    }

    async fn expect_ok(
        &self,
        response: reqwest::Response,
        what: &str,
        name: &str,
    ) -> Result<reqwest::Response, String> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(self.failure(status, &body, what, name).await)
    }

    /// The error a failed request comes to; assumed keys the bucket no
    /// longer takes are dropped, so the next request assumes the role again.
    async fn failure(
        &self,
        status: reqwest::StatusCode,
        body: &str,
        what: &str,
        name: &str,
    ) -> String {
        let code = xml_field(body, "Code").unwrap_or_default();
        if matches!(
            code.as_str(),
            "ExpiredToken" | "InvalidToken" | "TokenRefreshRequired"
        ) {
            self.drop_keys().await;
        }
        let message = xml_field(body, "Message").unwrap_or_default();
        format!("s3 blobs: {what} {name}: {status} {code} {message}")
    }
}

/// A response body of at most `limit` bytes.
async fn read_bounded(
    mut response: reqwest::Response,
    limit: usize,
    name: &str,
) -> Result<Bytes, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(format!("s3 blobs: {name} is larger than any node"));
    }
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("s3 blobs: read {name}: {}", error.without_url()))?
    {
        if body.len() + chunk.len() > limit {
            return Err(format!("s3 blobs: {name} is larger than any node"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// `YYYY-MM-DDTHH:MM:SS[.fff]Z`, as STS writes `Expiration`.
fn parse_iso8601(at: &str) -> Option<SystemTime> {
    let at = at.trim();
    let number = |range: std::ops::Range<usize>| at.get(range)?.parse::<i64>().ok();
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Civil date to days (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    u64::try_from(secs)
        .ok()
        .map(|secs| UNIX_EPOCH + Duration::from_secs(secs))
}

/// The text of the first `<name>` element: enough for STS and S3 replies.
fn xml_field(xml: &str, name: &str) -> Option<String> {
    let start = xml.find(&format!("<{name}>"))? + name.len() + 2;
    let end = start + xml[start..].find(&format!("</{name}>"))?;
    Some(
        xml[start..end]
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}

/// RFC 3986 encoding as SigV4 wants it; `/` kept unless `slash`.
pub(crate) fn uri_encode(value: &str, slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b'/' if !slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `YYYYMMDDTHHMMSSZ` in UTC.
fn amz_date(at: SystemTime) -> String {
    let secs = at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// The SigV4 `Authorization` header for a request to S3. `headers` holds
/// every header to sign, `x-amz-date` among them, and is sorted in place;
/// `query` is the canonical query string.
pub(crate) fn sign(
    method: &str,
    path: &str,
    query: &str,
    headers: &mut [(String, String)],
    payload_hash: &str,
    keys: &Keys,
    region: &str,
) -> String {
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    let date_time = headers
        .iter()
        .find(|(name, _)| name == "x-amz-date")
        .map(|(_, value)| value.clone())
        .expect("x-amz-date is signed");
    let date = &date_time[..8];
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date_time}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let key = [region.as_bytes(), b"s3", b"aws4_request"].iter().fold(
        hmac(
            format!("AWS4{}", keys.secret_access_key).as_bytes(),
            date.as_bytes(),
        ),
        |key, part| hmac(&key, part),
    );
    let signature = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        keys.access_key_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The "GET Object" example of the SigV4 documentation for S3.
    #[test]
    fn signs_the_documented_get_object_example() {
        let keys = Keys {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
            session_token: None,
        };
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let mut headers = vec![
            ("x-amz-date".to_owned(), "20130524T000000Z".to_owned()),
            ("range".to_owned(), "bytes=0-9".to_owned()),
            (
                "host".to_owned(),
                "examplebucket.s3.amazonaws.com".to_owned(),
            ),
            ("x-amz-content-sha256".to_owned(), empty.to_owned()),
        ];
        let authorization = sign(
            "GET",
            "/test.txt",
            "",
            &mut headers,
            empty,
            &keys,
            "us-east-1",
        );
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn parses_sts_expirations() {
        let at = |secs| Some(UNIX_EPOCH + Duration::from_secs(secs));
        assert_eq!(parse_iso8601("2013-05-24T00:00:00Z"), at(1_369_353_600));
        assert_eq!(
            parse_iso8601("2000-02-29T01:02:03.000Z"),
            at(951_782_400 + 3_723)
        );
        assert_eq!(parse_iso8601("soon"), None);
    }

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

    fn web_identity(bucket: &mock::MockS3, token: &str) -> S3Options {
        S3Options {
            credentials: S3Credentials::WebIdentity {
                role_arn: "arn:aws:iam::1:role/jazz".to_owned(),
                token: WebIdentityToken::Shared(Arc::new(Mutex::new(Some(token.to_owned())))),
            },
            ..bucket.options("app/")
        }
    }

    /// A web identity role is assumed once, with the token in a POST form
    /// (never in a URL), and its session keys sign what follows.
    #[test]
    fn assumes_a_web_identity_role_once_with_the_token_in_a_form_body() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let store =
            S3ChunkStore::new(web_identity(&bucket, "oidc-token"), "me".to_owned()).unwrap();
        let (key, bytes) = node(1);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        assert_eq!(bucket.state.calls.sts.load(Ordering::Relaxed), 1);
        assert_eq!(*bucket.state.sts_query.lock().unwrap(), None);
        assert_eq!(
            bucket.state.sts_token.lock().unwrap().as_deref(),
            Some("oidc-token")
        );
    }

    /// A role that cannot be assumed fails a whole batch with one STS call,
    /// and the error names no token.
    #[test]
    fn a_failed_assumption_answers_the_whole_batch() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let store = S3ChunkStore::new(web_identity(&bucket, "wrong"), "me".to_owned()).unwrap();
        let nodes: Vec<_> = (0..8).map(node).collect();
        let results = jazz::db::block_on(futures::future::join_all(
            nodes.iter().map(|(key, bytes)| store.put(key, bytes)),
        ));
        for result in results {
            let error = result.unwrap_err();
            assert!(error.message.contains("InvalidIdentityToken"), "{error}");
            assert!(!error.message.contains("wrong"), "{error}");
        }
        assert_eq!(bucket.state.calls.sts.load(Ordering::Relaxed), 1);
    }

    /// Deletes reach the bucket, and deleting an absent key succeeds; a
    /// request signed with the wrong secret is refused.
    #[test]
    fn deletes_reach_the_bucket_and_bad_signatures_do_not() {
        let bucket = mock::MockS3::start();
        let store = S3ChunkStore::new(bucket.options("app/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(2);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(bucket.names("app/"), vec![key.object_name()]);
        block(store.delete(&key)).unwrap();
        assert!(bucket.names("app/").is_empty());
        block(store.delete(&key)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap(), None);

        let mut forged = bucket.options("app/");
        forged.credentials = S3Credentials::Static(Keys {
            access_key_id: "test".to_owned(),
            secret_access_key: "guess".to_owned(),
            session_token: None,
        });
        let forged = S3ChunkStore::new(forged, "me".to_owned()).unwrap();
        let error = block(forged.put(&key, &bytes)).unwrap_err();
        assert!(error.message.contains("SignatureDoesNotMatch"), "{error}");
    }

    /// A prefix is claimed by the first store to use it; another store is
    /// refused there before it writes or deletes anything.
    #[test]
    fn two_stores_cannot_share_a_prefix() {
        let bucket = mock::MockS3::start();
        let (key, bytes) = node(3);
        let first = S3ChunkStore::new(bucket.options("app/"), "store-a".to_owned()).unwrap();
        block(first.put(&key, &bytes)).unwrap();
        let second = S3ChunkStore::new(bucket.options("app/"), "store-b".to_owned()).unwrap();
        let error = block(second.delete(&key)).unwrap_err();
        assert!(
            error.message.contains("belongs to store store-a"),
            "{error}"
        );
        assert_eq!(
            bucket.names("app/"),
            vec![key.object_name()],
            "nothing deleted"
        );
        let beside = S3ChunkStore::new(bucket.options("other/"), "store-b".to_owned()).unwrap();
        block(beside.put(&key, &bytes)).unwrap();
        let again = S3ChunkStore::new(bucket.options("app/"), "store-a".to_owned()).unwrap();
        assert_eq!(block(again.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
    }

    /// A bucket serves one environment: the first store with an
    /// environment claims it at the bucket's root (create-if-absent), and
    /// a store of another environment is refused under any prefix.
    #[test]
    fn a_bucket_serves_one_environment() {
        let bucket = mock::MockS3::start();
        let (key, bytes) = node(5);
        let in_env = |prefix: &str, environment: &str| {
            let mut options = bucket.options(prefix);
            options.environment = Some(environment.to_owned());
            options
        };
        let production =
            S3ChunkStore::new(in_env("prod/", "production"), "store-a".to_owned()).unwrap();
        block(production.put(&key, &bytes)).unwrap();
        assert_eq!(
            bucket
                .state
                .objects
                .lock()
                .unwrap()
                .get(&format!("bucket/{ENVIRONMENT_OBJECT}"))
                .map(|b| b.as_slice()),
            Some(&b"production\n"[..])
        );
        let preview =
            S3ChunkStore::new(in_env("preview/", "preview"), "store-b".to_owned()).unwrap();
        let error = block(preview.get(&key)).unwrap_err();
        assert!(
            error
                .message
                .contains("serves the production environment, not preview"),
            "{error}"
        );
        assert!(bucket.names("preview/").is_empty(), "nothing written");
        // Without an environment, nothing is checked (self-hosted stores).
        let plain = S3ChunkStore::new(bucket.options("plain/"), "store-c".to_owned()).unwrap();
        block(plain.put(&key, &bytes)).unwrap();
        let other =
            S3ChunkStore::new(in_env("other/", "production"), "store-d".to_owned()).unwrap();
        block(other.put(&key, &bytes)).unwrap();
    }

    /// Keys scoped to the prefix cannot reach the environment marker at
    /// the bucket's root: the claim says which permission is missing.
    #[test]
    fn a_prefix_scoped_policy_names_the_marker_it_cannot_reach() {
        let bucket = mock::MockS3::start();
        *bucket.state.only_prefix.lock().unwrap() = Some("prod/".to_owned());
        let mut options = bucket.options("prod/");
        options.environment = Some("production".to_owned());
        let store = S3ChunkStore::new(options, "store-a".to_owned()).unwrap();
        let (key, bytes) = node(6);
        let error = block(store.put(&key, &bytes)).unwrap_err();
        assert!(
            error
                .message
                .contains("s3:GetObject and s3:PutObject on arn:aws:s3:::bucket/.jazz-environment"),
            "{error}"
        );
        assert!(bucket.names("prod/").is_empty());
    }

    /// Keys that may put the root marker but not read it see an existing
    /// marker as absent and their create-if-absent refused: the claim says
    /// which permission is missing, not that the marker vanished.
    #[test]
    fn a_marker_that_cannot_be_read_names_the_missing_permission() {
        let bucket = mock::MockS3::start();
        let in_env = |prefix: &str| {
            let mut options = bucket.options(prefix);
            options.environment = Some("production".to_owned());
            options
        };
        let (key, bytes) = node(7);
        let first = S3ChunkStore::new(in_env("a/"), "store-a".to_owned()).unwrap();
        block(first.put(&key, &bytes)).unwrap();
        *bucket.state.only_prefix_reads.lock().unwrap() = Some("b/".to_owned());
        let second = S3ChunkStore::new(in_env("b/"), "store-b".to_owned()).unwrap();
        let error = block(second.put(&key, &bytes)).unwrap_err();
        assert!(
            error
                .message
                .contains("s3:GetObject and s3:PutObject on arn:aws:s3:::bucket/.jazz-environment"),
            "{error}"
        );
        assert!(bucket.names("b/").is_empty());
    }

    /// A marker created by someone else and deleted before our read: the
    /// claim says it vanished, not that a permission is missing.
    #[test]
    fn a_marker_deleted_while_claiming_says_it_vanished() {
        let bucket = mock::MockS3::start();
        let mut options = bucket.options("a/");
        options.environment = Some("production".to_owned());
        *bucket.state.conflict_then_vanish.lock().unwrap() =
            Some("bucket/.jazz-environment".to_owned());
        let store = S3ChunkStore::new(options, "store-a".to_owned()).unwrap();
        let (key, bytes) = node(8);
        let error = block(store.put(&key, &bytes)).unwrap_err();
        assert!(error.message.contains("vanished"), "{error}");
        assert!(!error.message.contains("s3:GetObject"), "{error}");
        assert!(bucket.names("a/").is_empty());
    }

    /// Without s3:ListBucket a missing key answers 403: it reads as absent.
    /// A body larger than any node is refused.
    #[test]
    fn reads_of_missing_or_oversized_objects() {
        let bucket = mock::MockS3::start();
        let store = S3ChunkStore::new(bucket.options("app/"), "me".to_owned()).unwrap();
        bucket
            .state
            .deny_missing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (key, _) = node(4);
        assert_eq!(block(store.get(&key)).unwrap(), None);
        let (big, _) = node(5);
        bucket.state.objects.lock().unwrap().insert(
            format!("bucket/app/{}", big.object_name()),
            vec![0; jazz::large_values::MAX_ENCODED_NODE_BYTES + 1],
        );
        let error = block(store.get(&big)).unwrap_err();
        assert!(error.message.contains("larger than any node"), "{error}");
    }

    #[test]
    fn formats_amz_dates() {
        let at = UNIX_EPOCH + Duration::from_secs(1_369_353_600);
        assert_eq!(amz_date(at), "20130524T000000Z");
        let leap = UNIX_EPOCH + Duration::from_secs(951_782_400 + 3_723);
        assert_eq!(amz_date(leap), "20000229T010203Z");
    }

    #[test]
    fn reads_assumed_keys_from_an_sts_reply() {
        let reply = "<AssumeRoleWithWebIdentityResponse><Credentials>\
            <AccessKeyId>ASIA1</AccessKeyId><SecretAccessKey>s&amp;k</SecretAccessKey>\
            <SessionToken>tok</SessionToken></Credentials></AssumeRoleWithWebIdentityResponse>";
        assert_eq!(xml_field(reply, "AccessKeyId").as_deref(), Some("ASIA1"));
        assert_eq!(xml_field(reply, "SecretAccessKey").as_deref(), Some("s&k"));
        assert_eq!(xml_field(reply, "Expiration"), None);
    }

    #[test]
    fn locates_objects_by_host_or_by_path() {
        let options = |endpoint: Option<&str>| S3Options {
            bucket: "jazz-app".to_owned(),
            region: "us-east-1".to_owned(),
            path_style: endpoint.is_some(),
            endpoint: endpoint.map(str::to_owned),
            prefix: "blobs/".to_owned(),
            credentials: S3Credentials::Static(Keys {
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: None,
            }),
            sts_endpoint: None,
            environment: None,
        };
        let node = NodeKey { hash: [0xab; 32] }.object_name();
        let store = S3ChunkStore::new(options(None), "me".to_owned()).unwrap();
        let (url, host, path) = store.inner.locate(&node, false);
        assert_eq!(host, "jazz-app.s3.us-east-1.amazonaws.com");
        assert_eq!(path, format!("/blobs/{}", "ab".repeat(32)));
        assert_eq!(url, format!("https://{host}{path}"));
        let store =
            S3ChunkStore::new(options(Some("http://127.0.0.1:9000/")), "me".to_owned()).unwrap();
        let (url, host, path) = store.inner.locate(&node, false);
        assert_eq!(host, "127.0.0.1:9000");
        assert_eq!(path, format!("/jazz-app/blobs/{}", "ab".repeat(32)));
        assert_eq!(url, format!("http://{host}{path}"));
        // A dotted bucket name would not match AWS's certificate as a host.
        let mut dotted = options(None);
        dotted.bucket = "jazz.app".to_owned();
        let store = S3ChunkStore::new(dotted, "me".to_owned()).unwrap();
        let (_, host, path) = store.inner.locate(&node, false);
        assert_eq!(host, "s3.us-east-1.amazonaws.com");
        assert!(path.starts_with("/jazz.app/blobs/"), "{path}");
    }
}

/// An S3 and STS stand-in over HTTP that checks what the real ones check:
/// every request's SigV4 signature (against the keys it issued), a put's
/// body against its signed `x-amz-checksum-sha256`, `If-None-Match`, and
/// that STS gets its token in a form body.
#[cfg(test)]
pub(crate) mod mock {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    #[derive(Default)]
    pub(crate) struct Calls {
        pub gets: AtomicU64,
        pub puts: AtomicU64,
        pub deletes: AtomicU64,
        pub sts: AtomicU64,
        pub requests: AtomicU64,
    }

    #[derive(Default)]
    pub(crate) struct State {
        pub objects: Mutex<BTreeMap<String, Vec<u8>>>,
        pub calls: Calls,
        /// Every request fails with 503 while set.
        pub failing: AtomicBool,
        /// A missing key answers 403 AccessDenied, as without s3:ListBucket.
        pub deny_missing: AtomicBool,
        /// Set: the keys may only touch objects under this prefix (an IAM
        /// policy on `bucket/prefix/*`); anything else answers 403.
        pub only_prefix: Mutex<Option<String>>,
        /// Set: reads (GET) only under this prefix; puts anywhere.
        pub only_prefix_reads: Mutex<Option<String>>,
        /// Set: a create-if-absent put of this key answers 412 while the key
        /// stays absent, as when another writer created it and it was
        /// deleted before our read.
        pub conflict_then_vanish: Mutex<Option<String>>,
        /// Keys by access key id; `test`/`test` is always valid.
        issued: Mutex<BTreeMap<String, (String, String)>>,
        /// What STS was last asked to accept.
        pub sts_token: Mutex<Option<String>>,
        pub sts_query: Mutex<Option<String>>,
    }

    pub(crate) struct MockS3 {
        pub state: Arc<State>,
        pub endpoint: String,
        _runtime: tokio::runtime::Runtime,
    }

    impl MockS3 {
        pub(crate) fn start() -> Self {
            let state = Arc::new(State::default());
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let listener = runtime
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let served = Arc::clone(&state);
            let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
                let state = Arc::clone(&served);
                async move { serve(&state, request).await }
            });
            runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                state,
                endpoint,
                _runtime: runtime,
            }
        }

        /// Options for this bucket under `prefix`, with static test keys.
        pub(crate) fn options(&self, prefix: &str) -> S3Options {
            S3Options {
                bucket: "bucket".to_owned(),
                region: "auto".to_owned(),
                endpoint: Some(self.endpoint.clone()),
                prefix: prefix.to_owned(),
                path_style: true,
                credentials: S3Credentials::Static(Keys {
                    access_key_id: "test".to_owned(),
                    secret_access_key: "test".to_owned(),
                    session_token: None,
                }),
                sts_endpoint: Some(format!("{}/sts", self.endpoint)),
                environment: None,
            }
        }

        /// Object names under `prefix` (without it), owner object aside.
        pub(crate) fn names(&self, prefix: &str) -> Vec<String> {
            let full = format!("bucket/{prefix}");
            self.state
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter_map(|key| key.strip_prefix(&full))
                .filter(|name| *name != OWNER_OBJECT)
                .map(str::to_owned)
                .collect()
        }
    }

    type Reply = (axum::http::StatusCode, Vec<u8>);

    fn error(status: axum::http::StatusCode, code: &str) -> Reply {
        (
            status,
            format!("<Error><Code>{code}</Code><Message>mock</Message></Error>").into_bytes(),
        )
    }

    async fn serve(state: &State, request: axum::extract::Request) -> Reply {
        use axum::http::StatusCode;
        let (parts, body) = request.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        state.calls.requests.fetch_add(1, Ordering::Relaxed);
        if state.failing.load(Ordering::Relaxed) {
            return error(StatusCode::SERVICE_UNAVAILABLE, "SlowDown");
        }
        if parts.uri.path() == "/sts" {
            return sts(state, &parts, &body);
        }
        if let Err(code) = verify(state, &parts) {
            return error(StatusCode::FORBIDDEN, code);
        }
        let key = parts.uri.path().trim_start_matches('/').to_owned();
        if let Some(prefix) = state.only_prefix.lock().unwrap().as_deref()
            && !key.starts_with(&format!("bucket/{prefix}"))
        {
            return error(StatusCode::FORBIDDEN, "AccessDenied");
        }
        if parts.method == axum::http::Method::GET
            && let Some(prefix) = state.only_prefix_reads.lock().unwrap().as_deref()
            && !key.starts_with(&format!("bucket/{prefix}"))
        {
            return error(StatusCode::FORBIDDEN, "AccessDenied");
        }
        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
        };
        let mut objects = state.objects.lock().unwrap();
        match parts.method.as_str() {
            "PUT" => {
                state.calls.puts.fetch_add(1, Ordering::Relaxed);
                let digest =
                    base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body));
                if header("x-amz-checksum-sha256") != Some(digest.as_str()) {
                    return error(StatusCode::BAD_REQUEST, "BadDigest");
                }
                if header("if-none-match") == Some("*")
                    && state.conflict_then_vanish.lock().unwrap().as_deref() == Some(key.as_str())
                {
                    return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                }
                if header("if-none-match") == Some("*") && objects.contains_key(&key) {
                    return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                }
                objects.insert(key, body.to_vec());
                (StatusCode::OK, Vec::new())
            }
            "GET" => {
                state.calls.gets.fetch_add(1, Ordering::Relaxed);
                match objects.get(&key) {
                    Some(bytes) => (StatusCode::OK, bytes.clone()),
                    None if state.deny_missing.load(Ordering::Relaxed) => {
                        error(StatusCode::FORBIDDEN, "AccessDenied")
                    }
                    None => error(StatusCode::NOT_FOUND, "NoSuchKey"),
                }
            }
            "DELETE" => {
                state.calls.deletes.fetch_add(1, Ordering::Relaxed);
                objects.remove(&key);
                (StatusCode::NO_CONTENT, Vec::new())
            }
            _ => error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
        }
    }

    /// Recompute the request's signature from what arrived.
    fn verify(state: &State, parts: &axum::http::request::Parts) -> Result<(), &'static str> {
        let authorization = parts
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or("AccessDenied")?;
        let field = |name: &str| {
            authorization
                .split([' ', ','])
                .find_map(|part| part.strip_prefix(name))
                .ok_or("AuthorizationHeaderMalformed")
        };
        let credential = field("Credential=")?;
        let signed = field("SignedHeaders=")?;
        let mut scope = credential.split('/');
        let access_key_id = scope.next().ok_or("AuthorizationHeaderMalformed")?;
        let region = scope.nth(1).ok_or("AuthorizationHeaderMalformed")?;
        let keys = if access_key_id == "test" {
            Keys {
                access_key_id: "test".to_owned(),
                secret_access_key: "test".to_owned(),
                session_token: None,
            }
        } else {
            let issued = state.issued.lock().unwrap();
            let (secret, token) = issued.get(access_key_id).ok_or("InvalidAccessKeyId")?;
            let sent = parts
                .headers
                .get("x-amz-security-token")
                .and_then(|value| value.to_str().ok());
            if sent != Some(token.as_str()) {
                return Err("InvalidToken");
            }
            Keys {
                access_key_id: access_key_id.to_owned(),
                secret_access_key: secret.clone(),
                session_token: Some(token.clone()),
            }
        };
        let mut headers = Vec::new();
        for name in signed.split(';') {
            let value = parts
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .ok_or("SignatureDoesNotMatch")?;
            headers.push((name.to_owned(), value.to_owned()));
        }
        let payload = parts
            .headers
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .ok_or("SignatureDoesNotMatch")?;
        let expected = sign(
            parts.method.as_str(),
            parts.uri.path(),
            "",
            &mut headers,
            payload,
            &keys,
            region,
        );
        if expected == authorization {
            Ok(())
        } else {
            Err("SignatureDoesNotMatch")
        }
    }

    fn decode(value: &str) -> String {
        let bytes = value.replace('+', " ").into_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%'
                && i + 2 < bytes.len()
                && let Ok(byte) =
                    u8::from_str_radix(&String::from_utf8_lossy(&bytes[i + 1..i + 3]), 16)
            {
                out.push(byte);
                i += 3;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn sts(state: &State, parts: &axum::http::request::Parts, body: &[u8]) -> Reply {
        use axum::http::StatusCode;
        state.calls.sts.fetch_add(1, Ordering::Relaxed);
        *state.sts_query.lock().unwrap() = parts.uri.query().map(str::to_owned);
        let form: BTreeMap<String, String> = String::from_utf8_lossy(body)
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .map(|(name, value)| (decode(name), decode(value)))
            .collect();
        *state.sts_token.lock().unwrap() = form.get("WebIdentityToken").cloned();
        if parts.method != axum::http::Method::POST
            || form.get("Action").map(String::as_str) != Some("AssumeRoleWithWebIdentity")
            || form
                .get("WebIdentityToken")
                .is_none_or(|token| token != "oidc-token")
        {
            return error(StatusCode::FORBIDDEN, "InvalidIdentityToken");
        }
        let n = state.calls.sts.load(Ordering::Relaxed);
        let (id, secret, token) = (
            format!("ASIA{n}"),
            format!("secret{n}"),
            format!("session{n}"),
        );
        state
            .issued
            .lock()
            .unwrap()
            .insert(id.clone(), (secret.clone(), token.clone()));
        let reply = format!(
            "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
             <AccessKeyId>{id}</AccessKeyId><SecretAccessKey>{secret}</SecretAccessKey>\
             <SessionToken>{token}</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration>\
             </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
        );
        (StatusCode::OK, reply.into_bytes())
    }
}
