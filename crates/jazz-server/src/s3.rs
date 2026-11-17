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
            Self::Workload => crate::middleware::workload::current_workload_token()
                .map_err(|why| format!("web identity token: {why}")),
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
    /// The task or pod role, from the container credentials endpoint
    /// that ECS, Fargate, EKS Pod Identity and Lambda point
    /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
    /// `AWS_CONTAINER_CREDENTIALS_FULL_URI` at.
    Container {
        /// The endpoint's full URL.
        uri: String,
        /// Sent as `Authorization` on each fetch
        /// (`AWS_CONTAINER_AUTHORIZATION_TOKEN[_FILE]`).
        authorization: Option<ContainerToken>,
    },
    /// The EC2 instance profile's role, from the instance metadata service
    /// (IMDSv2: a session token first, then the role's keys).
    InstanceMetadata {
        /// `http://169.254.169.254`, or `AWS_EC2_METADATA_SERVICE_ENDPOINT`.
        endpoint: String,
    },
}

/// The token a container credentials endpoint wants as `Authorization`.
#[derive(Clone)]
pub enum ContainerToken {
    /// Read afresh from this file on each fetch, since EKS Pod Identity
    /// rotates it (`AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`).
    File(PathBuf),
    /// This value (`AWS_CONTAINER_AUTHORIZATION_TOKEN`).
    Value(String),
}

impl std::fmt::Debug for ContainerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "File({})", path.display()),
            Self::Value(_) => f.write_str("Value"),
        }
    }
}

impl ContainerToken {
    fn read(&self) -> Result<String, String> {
        match self {
            Self::File(path) => std::fs::read_to_string(path)
                .map(|token| token.trim().to_owned())
                .map_err(|error| {
                    format!(
                        "s3 blobs: read container authorization token {}: {error}",
                        path.display()
                    )
                }),
            Self::Value(token) => Ok(token.clone()),
        }
    }
}

impl S3Credentials {
    /// Keys that are fetched, and fetched again before they expire.
    fn refreshed(&self) -> bool {
        !matches!(self, Self::Static(_))
    }

    /// Credentials the host attaches (static keys, the container's role,
    /// the instance's), there from the start: unlike an OIDC token a host
    /// may hand over later, a store can check them at startup.
    fn attached(&self) -> bool {
        !matches!(self, Self::WebIdentity { .. })
    }
}

/// Where the default chain looks, in order, for an error that found none.
const SOURCES_TRIED: &str = "AWS_ACCESS_KEY_ID with AWS_SECRET_ACCESS_KEY; \
     AWS_ROLE_ARN with AWS_WEB_IDENTITY_TOKEN_FILE; \
     AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or AWS_CONTAINER_CREDENTIALS_FULL_URI \
     (ECS, Fargate, EKS Pod Identity)";

/// The ECS / Fargate container credentials endpoint a relative URI is on.
const CONTAINER_HOST: &str = "http://169.254.170.2";
/// The EC2 instance metadata service.
const IMDS_ENDPOINT: &str = "http://169.254.169.254";
/// How long an instance metadata request may take. The service answers in
/// well under a millisecond on EC2; elsewhere nothing answers at all, and
/// this is only waited for when no other source was found.
const IMDS_TIMEOUT: Duration = Duration::from_secs(1);
/// How long an IMDSv2 session token is asked to last. A new one is
/// fetched with each set of keys, so it never needs to outlive them.
const IMDS_TOKEN_TTL: &str = "21600";
/// How often a metadata request (container endpoint or IMDS) is tried
/// when it fails transiently, as in the AWS SDKs.
const METADATA_ATTEMPTS: u32 = 3;
/// The pause between such tries.
const METADATA_RETRY_PAUSE: Duration = Duration::from_millis(100);
/// A credentials reply is a few hundred bytes; a longer one is refused.
const METADATA_BODY_LIMIT: usize = 64 * 1024;
/// How long a container credentials request may take.
const CONTAINER_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether a full container credentials URI may be sent the role's keys
/// over plain HTTP: only to this machine, or to the link-local addresses
/// of ECS and EKS Pod Identity, as the AWS SDKs allow.
fn container_uri_allowed(uri: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(uri) else {
        return false;
    };
    // `http://127.0.0.1@elsewhere/` is a request to `elsewhere`.
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    match url.scheme() {
        "https" => true,
        "http" if host.eq_ignore_ascii_case("localhost") => true,
        // The parsed host is normalised: an IPv6 address is bracketed.
        "http" => match host.trim_start_matches('[').trim_end_matches(']').parse() {
            Ok(std::net::IpAddr::V4(ip)) => {
                ip.is_loopback()
                    || ip == std::net::Ipv4Addr::new(169, 254, 170, 2)
                    || ip == std::net::Ipv4Addr::new(169, 254, 170, 23)
            }
            Ok(std::net::IpAddr::V6(ip)) => {
                ip.is_loopback()
                    || ip == std::net::Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x23)
            }
            Err(_) => false,
        },
        _ => false,
    }
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
    /// `jazz/<store id>/`, which the server fills in: two stores
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

impl S3Credentials {
    /// Credentials from the environment, in the order of the AWS SDKs'
    /// default chain (the parts of it that apply to a server):
    ///
    /// 1. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (with
    ///    `AWS_SESSION_TOKEN`): keys set on purpose win over anything
    ///    the host attaches.
    /// 2. `AWS_ROLE_ARN`, assumed with `AWS_WEB_IDENTITY_TOKEN_FILE` (EKS
    ///    IRSA, any OIDC provider) or else `VERCEL_OIDC_TOKEN`.
    /// 3. The container credentials endpoint
    ///    (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, else `_FULL_URI`, with
    ///    `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or `_TOKEN`).
    /// 4. The EC2 instance profile through IMDSv2, unless
    ///    `AWS_EC2_METADATA_DISABLED=true`. It is the only source that
    ///    can't be seen in the environment, so it is the last fallback and
    ///    is not contacted here: the store's startup check finds out
    ///    whether anything answers, and fails naming every source tried
    ///    when nothing does.
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// [`Self::from_env`] with `var` as the environment.
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let env = |name: &str| var(name).filter(|value| !value.is_empty());
        if let Some(access_key_id) = env("AWS_ACCESS_KEY_ID") {
            return Ok(Self::Static(Keys {
                access_key_id,
                secret_access_key: env("AWS_SECRET_ACCESS_KEY")
                    .ok_or("s3 blobs: AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY")?,
                session_token: env("AWS_SESSION_TOKEN"),
            }));
        }
        if let Some(role_arn) = env("AWS_ROLE_ARN") {
            return Ok(Self::WebIdentity {
                role_arn,
                token: match env("AWS_WEB_IDENTITY_TOKEN_FILE") {
                    Some(file) => WebIdentityToken::File(file.into()),
                    None => WebIdentityToken::Env("VERCEL_OIDC_TOKEN".to_owned()),
                },
            });
        }
        let uri = match (
            env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"),
            env("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        ) {
            (Some(relative), _) => Some(format!(
                "{CONTAINER_HOST}/{}",
                relative.trim_start_matches('/')
            )),
            (None, Some(full)) if container_uri_allowed(&full) => Some(full),
            (None, Some(_)) => {
                return Err(
                    "s3 blobs: AWS_CONTAINER_CREDENTIALS_FULL_URI must be https, \
                     or http to this machine or to the ECS / EKS link-local address"
                        .to_owned(),
                );
            }
            (None, None) => None,
        };
        if let Some(uri) = uri {
            let authorization = env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
                .map(|file| ContainerToken::File(file.into()))
                .or_else(|| env("AWS_CONTAINER_AUTHORIZATION_TOKEN").map(ContainerToken::Value));
            return Ok(Self::Container { uri, authorization });
        }
        if env("AWS_EC2_METADATA_DISABLED").is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return Err(format!(
                "s3 blobs: no AWS credentials found; tried {SOURCES_TRIED}; \
                 and EC2 instance metadata, which AWS_EC2_METADATA_DISABLED turns off"
            ));
        }
        Ok(Self::InstanceMetadata {
            endpoint: env("AWS_EC2_METADATA_SERVICE_ENDPOINT")
                .unwrap_or_else(|| IMDS_ENDPOINT.to_owned())
                .trim_end_matches('/')
                .to_owned(),
        })
    }
}

impl S3Options {
    /// A bucket named by the environment, filling in what the arguments
    /// leave out: `S3_BUCKET_NAME`, `AWS_REGION`, and credentials found
    /// the way the AWS SDKs' default chain finds them
    /// ([`S3Credentials::from_env`]).
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
        let credentials = S3Credentials::from_env()?;
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
/// Fetched keys handed out close to their expiry, or even past it, are
/// still used this long before they are fetched again. During an outage
/// IMDS and the container endpoint keep handing out the last keys they
/// have, which may already be expired; fetching again on every request
/// would only hammer an endpoint that cannot help. If the bucket refuses
/// them as expired, [`Inner::failure`] drops them and the next request
/// fetches again anyway.
const MIN_KEY_USE: Duration = Duration::from_secs(30);
/// How long assumed keys are asked to last.
const ASSUMED_FOR: Duration = Duration::from_secs(60 * 60);
/// An operation that takes longer fails, and is retried in a later batch.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// How long startup waits for the claim ([`S3ChunkStore::check_at_startup`])
/// before starting anyway: short, as it delays every cold start.
const STARTUP_CHECK_TIMEOUT: Duration = Duration::from_secs(3);
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
    /// For the container credentials endpoint and instance metadata:
    /// never through a proxy (`HTTP_PROXY` must not see the role's keys,
    /// and could not reach a link-local address anyway), never redirected.
    local_http: reqwest::Client,
    keys: tokio::sync::Mutex<KeyState>,
    /// The store id this prefix is claimed for, and whether the claim
    /// has been checked (once per process, before the first operation).
    owner: String,
    claimed: tokio::sync::OnceCell<()>,
    /// The last claim that failed, and when: it answers every operation
    /// for a while, as a failed role assumption does.
    claim_failed: Mutex<Option<(Instant, String)>>,
    /// Set once the bucket refused a request for good, with what to fix:
    /// a 4xx other than a missing object, a timeout, a throttle or a
    /// conditional-write race (wrong keys, a policy that forbids it, no
    /// such bucket, a prefix another store owns). Retrying never helps.
    /// Any operation may set it; only the startup check reads it
    /// ([`S3ChunkStore::check_at_startup`]), right after a claim.
    refused: Mutex<Option<&'static str>>,
    /// The region requests are signed for: [`S3Options::region`], until
    /// AWS answers that the bucket is in another (on Vercel `AWS_REGION`
    /// is the function's own region, not necessarily the bucket's).
    region: Mutex<String>,
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
        // Redirects are not followed: a cross-host one would carry the
        // session token along, and a bucket's 301 names its region, which
        // `send` handles itself.
        let http = reqwest::Client::builder()
            .timeout(OPERATION_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("s3 blobs: http client: {error}"))?;
        let local_http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("s3 blobs: http client: {error}"))?;
        let keys = KeyState {
            keys: match &options.credentials {
                S3Credentials::Static(keys) => Some((keys.clone(), None)),
                _ => None,
            },
            failed: None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                region: Mutex::new(options.region.clone()),
                options,
                http,
                local_http,
                keys: tokio::sync::Mutex::new(keys),
                owner,
                claimed: tokio::sync::OnceCell::new(),
                claim_failed: Mutex::new(None),
                refused: Mutex::new(None),
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

    /// At startup, with credentials the host attaches (static keys, the
    /// container's or the instance's role): check the store's claims now
    /// and fail if the bucket refused them for good (wrong keys, a policy
    /// that forbids the prefix, a prefix another store owns) or no
    /// credentials were found at all (nothing answered as instance
    /// metadata, the chain's last fallback), so a server never
    /// runs with large values that can only stay on its own disk. A bucket
    /// that is unreachable, failing or slow (network, 5xx, over
    /// `STARTUP_CHECK_TIMEOUT`) is only logged: the store retries.
    pub async fn check_at_startup(&self) -> Result<(), String> {
        if !self.inner.options.credentials.attached() {
            return Ok(());
        }
        let inner = Arc::clone(&self.inner);
        // A join handle, not `spawn`'s future: hosts await `build` on a
        // multi-threaded runtime, so this future must be `Send`.
        self
            .runtime
            .as_ref()
            .expect("the runtime lives as long as the store")
            .spawn(async move {
                match tokio::time::timeout(STARTUP_CHECK_TIMEOUT, inner.claim()).await {
                    Ok(Err(error)) if inner.refusal().is_some() => {
                        let hint = inner.refusal().unwrap_or_default();
                        let at = inner.options.describe();
                        Err(if hint.is_empty() {
                            format!("{error} ({at})")
                        } else {
                            format!("{error} ({at}); {hint}")
                        })
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "s3 blobs: the large-value store is not reachable yet; starting anyway and retrying");
                        Ok(())
                    }
                    Ok(Ok(())) => Ok(()),
                    Err(_) => {
                        tracing::warn!(
                            "s3 blobs: the large-value store did not answer within {}s; starting anyway and retrying",
                            STARTUP_CHECK_TIMEOUT.as_secs()
                        );
                        Ok(())
                    }
                }
            })
            .await
            .map_err(|error| format!("s3 blobs: startup check failed: {error}"))?
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

/// A response rebuilt from its status, headers and body, once the body was
/// read.
fn rebuilt(
    status: reqwest::StatusCode,
    headers: reqwest::header::HeaderMap,
    body: &[u8],
) -> reqwest::Response {
    let mut response = http::Response::new(body.to_vec());
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    reqwest::Response::from(response)
}

/// The region a bucket's reply names (its `x-amz-bucket-region` header, or
/// a `<Region>` in the body), if that is a region name other than
/// `signed_for`. It goes into host names, so anything but a plain region
/// name (`[a-z0-9-]`, at most 32 long) is ignored.
fn named_region(header: Option<&str>, body: &[u8], signed_for: &str) -> Option<String> {
    let named = header
        .map(str::to_owned)
        .or_else(|| xml_field(&String::from_utf8_lossy(body), "Region"))?;
    let plain = (1..=32).contains(&named.len())
        && named
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    (plain && named != signed_for).then_some(named)
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
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.region()));
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
        let fetched = match &self.options.credentials {
            S3Credentials::Static(_) => unreachable!("static keys never expire"),
            S3Credentials::WebIdentity { role_arn, token } => match token.read() {
                Ok(token) => self.assume_role(role_arn, &token).await,
                Err(error) => Err(format!("s3 blobs: {error}")),
            },
            S3Credentials::Container { uri, authorization } => {
                self.container_keys(uri, authorization.as_ref()).await
            }
            S3Credentials::InstanceMetadata { endpoint } => self.instance_keys(endpoint).await,
        };
        match fetched {
            Ok((keys, expires)) => {
                let now = SystemTime::now();
                let until = expires.unwrap_or_else(|| now + ASSUMED_FOR);
                state.keys = Some((keys.clone(), Some(refresh_at(now, until))));
                state.failed = None;
                Ok(keys)
            }
            Err(error) => {
                state.failed = Some((Instant::now(), error.clone()));
                Err(error)
            }
        }
    }

    /// The task or pod role's keys from the container credentials
    /// endpoint, and when they expire.
    async fn container_keys(
        &self,
        uri: &str,
        authorization: Option<&ContainerToken>,
    ) -> Result<(Keys, Option<SystemTime>), String> {
        let at = endpoint_host(uri);
        let failed = |why: String| format!("s3 blobs: container credentials at {at}: {why}");
        let token = authorization.map(ContainerToken::read).transpose()?;
        let mut attempt = 0;
        let (status, body) = loop {
            attempt += 1;
            let mut request = self.local_http.get(uri).timeout(CONTAINER_TIMEOUT);
            if let Some(token) = &token {
                request = request.header("authorization", token);
            }
            match request.send().await {
                Ok(response) if retried(response.status()) && attempt < METADATA_ATTEMPTS => {}
                Ok(response) => {
                    break (
                        response.status(),
                        metadata_body(response).await.map_err(failed)?,
                    );
                }
                Err(_) if attempt < METADATA_ATTEMPTS => {}
                Err(error) => return Err(failed(error.without_url().to_string())),
            }
            tokio::time::sleep(METADATA_RETRY_PAUSE).await;
        };
        if !status.is_success() {
            return Err(failed(format!("{status} {}", json_message(&body))));
        }
        role_keys(&body).map_err(failed)
    }

    /// The instance profile role's keys through IMDSv2: a session token
    /// (PUT), the role's name, then its keys, each request carrying the
    /// token. Nothing answering the token request means this is no EC2
    /// instance, and since instance metadata is the chain's last source,
    /// that no credentials were found anywhere. That case is not retried,
    /// so a host without instance metadata learns it within one
    /// [`IMDS_TIMEOUT`]; a service that answers but fails (IMDS throttles
    /// with 429 and 503) is asked again, as the AWS SDKs do.
    async fn instance_keys(&self, endpoint: &str) -> Result<(Keys, Option<SystemTime>), String> {
        let imds = |why: String| format!("s3 blobs: EC2 instance metadata at {endpoint}: {why}");
        let mut attempt = 0;
        let token = loop {
            attempt += 1;
            let response = self
                .local_http
                .put(format!("{endpoint}/latest/api/token"))
                .header("x-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL)
                .timeout(IMDS_TIMEOUT)
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    break metadata_body(response).await.map_err(imds)?;
                }
                Ok(response) if retried(response.status()) && attempt < METADATA_ATTEMPTS => {}
                Ok(response) => return Err(imds(format!("token: {}", response.status()))),
                Err(error) if error.is_connect() || error.is_timeout() => {
                    self.refuse(
                        "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or AWS_ROLE_ARN, \
                         or run where the host attaches a role (an ECS task role, an EC2 \
                         instance profile; in a container on EC2, the instance's metadata \
                         hop limit must be at least 2)",
                    );
                    return Err(format!(
                        "s3 blobs: no AWS credentials found; tried {SOURCES_TRIED}; \
                         and EC2 instance metadata at {endpoint}, which did not answer within {}s",
                        IMDS_TIMEOUT.as_secs()
                    ));
                }
                Err(_) if attempt < METADATA_ATTEMPTS => {}
                Err(error) => return Err(imds(error.without_url().to_string())),
            }
            tokio::time::sleep(METADATA_RETRY_PAUSE).await;
        };
        let token = token.trim();
        let get = async |path: &str, what: &str| -> Result<String, String> {
            let mut attempt = 0;
            loop {
                attempt += 1;
                let response = self
                    .local_http
                    .get(format!(
                        "{endpoint}/latest/meta-data/iam/security-credentials/{path}"
                    ))
                    .header("x-aws-ec2-metadata-token", token)
                    .timeout(IMDS_TIMEOUT)
                    .send()
                    .await;
                match response {
                    Ok(response) if response.status().is_success() => {
                        return metadata_body(response).await.map_err(imds);
                    }
                    Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                        return Err(imds(format!(
                            "{what}: the instance has no instance profile (IAM role) attached"
                        )));
                    }
                    Ok(response) if retried(response.status()) && attempt < METADATA_ATTEMPTS => {}
                    Ok(response) => return Err(imds(format!("{what}: {}", response.status()))),
                    Err(_) if attempt < METADATA_ATTEMPTS => {}
                    Err(error) => {
                        return Err(imds(format!("{what}: {}", error.without_url())));
                    }
                }
                tokio::time::sleep(METADATA_RETRY_PAUSE).await;
            }
        };
        let roles = get("", "role").await?;
        let role = roles
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .ok_or_else(|| {
                imds("role: the instance has no instance profile (IAM role) attached".to_owned())
            })?
            .to_owned();
        let body = get(&uri_encode(&role, true), "keys").await?;
        role_keys(&body).map_err(|why| imds(format!("keys for {role}: {why}")))
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
        if self.options.credentials.refreshed() {
            self.keys.lock().await.keys = None;
        }
    }

    /// The region requests are signed for now.
    fn region(&self) -> String {
        self.region.lock().expect("region").clone()
    }

    /// Send `request`. A bucket that answers it is in another region than
    /// the one signed for (AWS does, with `x-amz-bucket-region` or a
    /// `<Region>`) is asked again in that one, which is kept from then on.
    async fn send(&self, request: Request<'_>) -> Result<reqwest::Response, String> {
        let signed_for = self.region();
        let response = self.send_once(&request).await?;
        let status = response.status();
        if status != reqwest::StatusCode::MOVED_PERMANENTLY
            && status != reqwest::StatusCode::BAD_REQUEST
        {
            return Ok(response);
        }
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|error| format!("s3 blobs: {}", error.without_url()))?;
        let header = headers
            .get("x-amz-bucket-region")
            .and_then(|value| value.to_str().ok());
        let Some(region) = named_region(header, &body, &signed_for) else {
            return Ok(rebuilt(status, headers, &body));
        };
        tracing::warn!(
            bucket = %self.options.bucket,
            configured = %signed_for,
            region = %region,
            "s3 blobs: bucket {} is in {region}, not {signed_for}; signing for {region} from now on. \
             Set the region explicitly (s3({{ region }}) or a project AWS_REGION) to skip this",
            self.options.bucket,
        );
        *self.region.lock().expect("region") = region;
        self.send_once(&request).await
    }

    async fn send_once(&self, request: &Request<'_>) -> Result<reqwest::Response, String> {
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
        let method = request.method.clone();
        let authorization = sign(
            method.as_str(),
            &path,
            "",
            &mut headers,
            &payload,
            &keys,
            &self.region(),
        );
        let mut http = self.http.request(method, url);
        for (name, value) in headers.iter().filter(|(name, _)| name != "host") {
            http = http.header(name, value);
        }
        http = http.header("authorization", authorization);
        if let Some(body) = &request.body {
            http = http.body(body.clone());
        }
        http.send()
            .await
            .map_err(|error| format!("s3 blobs: {}", error.without_url()))
    }

    /// Record that the bucket refused a request for good, with what to fix.
    fn refuse(&self, hint: &'static str) {
        *self.refused.lock().expect("refusal") = Some(hint);
    }

    fn refusal(&self) -> Option<&'static str> {
        *self.refused.lock().expect("refusal")
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
                    self.refuse("");
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
        if status == reqwest::StatusCode::NOT_FOUND
            && xml_field(&body, "Code").as_deref() != Some("NoSuchBucket")
        {
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
            self.refuse("");
            format!(
                "s3 blobs: cannot claim bucket {bucket} for the {environment} environment: \
                 the credentials may not read or write s3://{bucket}/{ENVIRONMENT_OBJECT}. \
                 Allow s3:GetObject and s3:PutObject on arn:aws:s3:::{bucket}/{ENVIRONMENT_OBJECT} \
                 as well as on the prefix",
                bucket = self.options.bucket,
            )
        };
        let refused = |other: &str| {
            self.refuse("");
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

    /// A successful GET's response, or `None` on a 404. A 403 is an error,
    /// not absence: only nodes this store put are read, so with the store's
    /// own keys a denied read is a permission problem (a role or bucket
    /// policy edited, an SSE-KMS key it may not use) that a fix ends. Keys
    /// without s3:ListBucket also answer 403 for a missing key; the error
    /// says so.
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
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::FORBIDDEN
            && xml_field(&body, "Code").as_deref() == Some("AccessDenied")
        {
            return Err(format!(
                "s3 blobs: reading s3://{}/{}{name} was denied (AccessDenied). Check that the \
                 credentials may s3:GetObject under the prefix, and kms:Decrypt for an SSE-KMS \
                 bucket; without s3:ListBucket S3 also answers so for a missing object",
                self.options.bucket, self.options.prefix
            ));
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
            if matches!(self.options.credentials, S3Credentials::Static(_)) {
                // Static keys are never assumed again: the token stays bad.
                self.refuse("check the S3 session token (AWS_SESSION_TOKEN)");
            } else {
                self.drop_keys().await;
            }
        } else if let Some(hint) = refusal_hint(status, &code) {
            self.refuse(hint);
        }
        let message = xml_field(body, "Message").unwrap_or_default();
        format!("s3 blobs: {what} {name}: {status} {code} {message}")
    }
}

/// What to fix when the bucket answered `status` with error `code` for
/// good; `None` when a later try may succeed (a missing object, a timeout,
/// a throttle, a conditional write racing another instance's).
fn refusal_hint(status: reqwest::StatusCode, code: &str) -> Option<&'static str> {
    use reqwest::StatusCode;
    if code == "NoSuchBucket" {
        return Some("check the bucket name and region");
    }
    if !status.is_client_error()
        || matches!(
            status,
            StatusCode::NOT_FOUND
                | StatusCode::REQUEST_TIMEOUT
                | StatusCode::CONFLICT
                | StatusCode::TOO_MANY_REQUESTS
        )
    {
        return None;
    }
    Some(match code {
        "InvalidAccessKeyId" | "SignatureDoesNotMatch" => "check the S3 access key and secret",
        "RequestTimeTooSkewed" => "check this host's clock",
        "AccessDenied" | "AllAccessDisabled" => {
            "check the bucket policy: the keys need s3:GetObject and s3:PutObject on the prefix"
        }
        _ => "check the S3 credentials and bucket settings",
    })
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

/// When keys valid `until` are replaced: [`REFRESH_EARLY`] before then,
/// or for keys handed out closer to their expiry than that (the instance
/// and container endpoints rotate their keys late), halfway there, and
/// never sooner than [`MIN_KEY_USE`] from `now`.
fn refresh_at(now: SystemTime, until: SystemTime) -> SystemTime {
    let left = until.duration_since(now).unwrap_or_default();
    let use_for = if left > REFRESH_EARLY * 2 {
        left - REFRESH_EARLY
    } else {
        (left / 2).max(MIN_KEY_USE)
    };
    now + use_for
}

/// `scheme://host[:port]` of `uri`: what errors name of an endpoint,
/// whose path may identify a task's credentials.
fn endpoint_host(uri: &str) -> String {
    match reqwest::Url::parse(uri) {
        Ok(url) => match (url.host_str(), url.port()) {
            (Some(host), Some(port)) => format!("{}://{host}:{port}", url.scheme()),
            (Some(host), None) => format!("{}://{host}", url.scheme()),
            (None, _) => "the endpoint".to_owned(),
        },
        Err(_) => "the endpoint".to_owned(),
    }
}

/// Whether a metadata reply with `status` is worth asking again for.
fn retried(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

/// A metadata reply's body, of at most [`METADATA_BODY_LIMIT`] bytes.
async fn metadata_body(mut response: reqwest::Response) -> Result<String, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("read reply: {}", error.without_url()))?
    {
        if body.len() + chunk.len() > METADATA_BODY_LIMIT {
            return Err(format!("reply longer than {METADATA_BODY_LIMIT} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// The `Code` and `message` / `Message` of a JSON error reply, if any.
fn json_message(body: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return String::new();
    };
    ["code", "Code", "message", "Message"]
        .iter()
        .filter_map(|field| value.get(field)?.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Keys from the JSON the container credentials endpoint and instance
/// metadata answer with (`AccessKeyId`, `SecretAccessKey`, `Token`,
/// `Expiration`; instance metadata adds `"Code": "Success"`).
fn role_keys(body: &str) -> Result<(Keys, Option<SystemTime>), String> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Reply {
        code: Option<String>,
        message: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        token: Option<String>,
        expiration: Option<String>,
    }
    let reply: Reply =
        serde_json::from_str(body).map_err(|_| "the reply is not credentials JSON".to_owned())?;
    if let Some(code) = reply.code.filter(|code| code != "Success") {
        return Err(format!("{code} {}", reply.message.unwrap_or_default()));
    }
    let (Some(access_key_id), Some(secret_access_key)) =
        (reply.access_key_id, reply.secret_access_key)
    else {
        return Err("no AccessKeyId and SecretAccessKey".to_owned());
    };
    let expires = match reply.expiration {
        Some(at) => Some(parse_iso8601(&at).ok_or_else(|| format!("unreadable Expiration {at}"))?),
        None => None,
    };
    Ok((
        Keys {
            access_key_id,
            secret_access_key,
            session_token: reply.token.filter(|token| !token.is_empty()),
        },
        expires,
    ))
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

    /// A bucket in another region than the store was given (on Vercel,
    /// `AWS_REGION` is the function's region) answers naming its own: the
    /// store signs for that one from then on.
    #[test]
    fn a_bucket_in_another_region_is_signed_for_its_own() {
        let bucket = mock::MockS3::start();
        *bucket.state.bucket_region.lock().unwrap() = Some("eu-west-1".to_owned());
        let options = bucket.options("app/");
        assert_eq!(options.region, "auto");
        let store = S3ChunkStore::new(options, "me".to_owned()).unwrap();
        let (key, bytes) = node(9);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        assert_eq!(store.inner.region(), "eu-west-1");
    }

    /// The same, when the bucket answers 301 with `x-amz-bucket-region`
    /// and no body.
    #[test]
    fn a_bucket_naming_its_region_in_a_header_is_signed_for_its_own() {
        let bucket = mock::MockS3::start();
        *bucket.state.bucket_region.lock().unwrap() = Some("ap-south-1".to_owned());
        bucket
            .state
            .region_by_header
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let store = S3ChunkStore::new(bucket.options("app/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(10);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        assert_eq!(store.inner.region(), "ap-south-1");
    }

    /// A reply naming something other than a plain region name is passed
    /// on as it came, and the region kept is unchanged: it would go into
    /// the host name. Its headers come along.
    #[test]
    fn a_reply_naming_no_plain_region_is_passed_on() {
        assert_eq!(
            named_region(Some("eu-west-1"), b"", "auto").as_deref(),
            Some("eu-west-1")
        );
        assert_eq!(
            named_region(None, b"<Error><Region>us-west-2</Region></Error>", "auto").as_deref(),
            Some("us-west-2")
        );
        assert_eq!(named_region(Some("auto"), b"", "auto"), None, "the same");
        for junk in [
            "",
            "evil.example.com/x",
            "EU-WEST-1",
            "a b",
            &"a".repeat(33),
        ] {
            assert_eq!(named_region(Some(junk), b"", "auto"), None, "{junk:?}");
            let body = format!("<Error><Region>{junk}</Region></Error>");
            assert_eq!(
                named_region(None, body.as_bytes(), "auto"),
                None,
                "{junk:?}"
            );
        }

        let bucket = mock::MockS3::start();
        *bucket.state.bucket_region.lock().unwrap() = Some("evil.example.com/x".to_owned());
        bucket
            .state
            .region_by_header
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let store = S3ChunkStore::new(bucket.options("app/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(11);
        let error = block(store.put(&key, &bytes)).unwrap_err();
        assert!(error.message.contains("301"), "{error}");
        assert_eq!(store.inner.region(), "auto");
    }

    /// With no endpoint, the host is built from the region kept, so a
    /// bucket found in another region is addressed there.
    #[test]
    fn the_aws_host_follows_the_region_kept() {
        let options = S3Options {
            bucket: "jazz-app".to_owned(),
            region: "us-east-1".to_owned(),
            path_style: false,
            endpoint: None,
            prefix: "blobs/".to_owned(),
            credentials: S3Credentials::Static(Keys {
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: None,
            }),
            sts_endpoint: None,
            environment: None,
        };
        let store = S3ChunkStore::new(options, "me".to_owned()).unwrap();
        *store.inner.region.lock().unwrap() = "eu-central-1".to_owned();
        let (url, host, _) = store.inner.locate("x", false);
        assert_eq!(host, "jazz-app.s3.eu-central-1.amazonaws.com");
        assert!(
            url.starts_with("https://jazz-app.s3.eu-central-1.amazonaws.com/"),
            "{url}"
        );
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

    /// A 404 is absence. A 403 is an error, not absence, whether the keys
    /// lack s3:ListBucket or the permission went away: once it is back,
    /// the same store reads again. A body larger than any node is refused.
    #[test]
    fn reads_of_missing_denied_or_oversized_objects() {
        let bucket = mock::MockS3::start();
        let store = S3ChunkStore::new(bucket.options("app/"), "me".to_owned()).unwrap();
        let (key, bytes) = node(4);
        assert_eq!(block(store.get(&key)).unwrap(), None);
        bucket
            .state
            .deny_missing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let error = block(store.get(&key)).unwrap_err();
        assert!(error.message.contains("AccessDenied"), "{error}");
        assert!(error.message.contains("s3:ListBucket"), "{error}");
        block(store.put(&key, &bytes)).unwrap();
        *bucket.state.only_prefix_reads.lock().unwrap() = Some("elsewhere/".to_owned());
        let error = block(store.get(&key)).unwrap_err();
        assert!(error.message.contains("s3:GetObject"), "{error}");
        *bucket.state.only_prefix_reads.lock().unwrap() = None;
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
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

    fn with(credentials: S3Credentials, bucket: &mock::MockS3) -> S3Options {
        S3Options {
            credentials,
            ..bucket.options("app/")
        }
    }

    fn container(
        metadata: &mock::MockMetadata,
        authorization: Option<ContainerToken>,
    ) -> S3Credentials {
        S3Credentials::Container {
            uri: format!("{}/v2/credentials/task-guid", metadata.endpoint),
            authorization,
        }
    }

    /// Pretend the keys in use reached their refresh time.
    fn age_keys(store: &S3ChunkStore) {
        let mut state = store.inner.keys.try_lock().unwrap();
        let (_, until) = state.keys.as_mut().expect("keys in use");
        *until = Some(SystemTime::now() - Duration::from_secs(1));
    }

    /// The container endpoint's keys sign requests, are fetched once for
    /// many operations, and again (with the token file read afresh, as EKS
    /// Pod Identity rotates it) once they near their expiry.
    #[test]
    fn container_keys_carry_the_token_file_and_refresh_before_expiry() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "pod-token-1\n").unwrap();
        *metadata.state.container_auth.lock().unwrap() = Some("pod-token-1".to_owned());
        let store = S3ChunkStore::new(
            with(
                container(&metadata, Some(ContainerToken::File(file.clone()))),
                &bucket,
            ),
            "me".to_owned(),
        )
        .unwrap();
        let (key, bytes) = node(1);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        assert_eq!(metadata.state.container_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            metadata
                .state
                .container_auth_seen
                .lock()
                .unwrap()
                .as_deref(),
            Some("pod-token-1")
        );

        std::fs::write(&file, "pod-token-2").unwrap();
        *metadata.state.container_auth.lock().unwrap() = Some("pod-token-2".to_owned());
        age_keys(&store);
        let (key, bytes) = node(2);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(metadata.state.container_calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            metadata
                .state
                .container_auth_seen
                .lock()
                .unwrap()
                .as_deref(),
            Some("pod-token-2")
        );
        // The new keys are the ones in use.
        let keys = jazz::db::block_on(store.inner.keys()).unwrap();
        assert_eq!(keys.access_key_id, "ASIAROLE2");
        assert_eq!(keys.session_token.as_deref(), Some("rsession2"));
    }

    /// An authorization token from the environment is sent as is; a wrong
    /// one fails naming the endpoint's host, never its path or the token.
    #[test]
    fn container_keys_carry_the_token_value() {
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        *metadata.state.container_auth.lock().unwrap() = Some("ecs-token".to_owned());
        let good = S3ChunkStore::new(
            with(
                container(
                    &metadata,
                    Some(ContainerToken::Value("ecs-token".to_owned())),
                ),
                &bucket,
            ),
            "me".to_owned(),
        )
        .unwrap();
        let (key, bytes) = node(1);
        block(good.put(&key, &bytes)).unwrap();
        let wrong = S3ChunkStore::new(
            with(
                container(&metadata, Some(ContainerToken::Value("stale".to_owned()))),
                &bucket,
            ),
            "other".to_owned(),
        )
        .unwrap();
        let error = block(wrong.get(&key)).unwrap_err().message;
        assert!(error.contains(&metadata.endpoint), "{error}");
        assert!(
            error.contains("403") && error.contains("AccessDenied"),
            "{error}"
        );
        assert!(
            !error.contains("task-guid") && !error.contains("stale"),
            "{error}"
        );
    }

    /// IMDSv2: a session token is PUT for first, and every metadata
    /// request carries it; the role is listed, then its keys fetched. A
    /// refresh asks for a new token.
    #[test]
    fn instance_keys_come_through_an_imdsv2_session_token() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        let store = S3ChunkStore::new(
            with(
                S3Credentials::InstanceMetadata {
                    endpoint: metadata.endpoint.clone(),
                },
                &bucket,
            ),
            "me".to_owned(),
        )
        .unwrap();
        jazz::db::block_on(store.check_at_startup()).unwrap();
        let (key, bytes) = node(1);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        let state = &metadata.state;
        assert_eq!(state.token_puts.load(Ordering::Relaxed), 1);
        assert_eq!(
            *state.token_ttls.lock().unwrap(),
            vec![IMDS_TOKEN_TTL.to_owned()]
        );
        assert_eq!(state.metadata_gets.load(Ordering::Relaxed), 2);
        assert_eq!(state.metadata_refused.load(Ordering::Relaxed), 0);

        age_keys(&store);
        let (key, bytes) = node(2);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(state.token_puts.load(Ordering::Relaxed), 2);
        assert_eq!(state.metadata_gets.load(Ordering::Relaxed), 4);
        assert_eq!(state.metadata_refused.load(Ordering::Relaxed), 0);
    }

    /// A throttling instance metadata service is asked again.
    #[test]
    fn a_throttling_instance_metadata_service_is_asked_again() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        metadata.state.fail_next.store(2, Ordering::Relaxed);
        let store = S3ChunkStore::new(
            with(
                S3Credentials::InstanceMetadata {
                    endpoint: metadata.endpoint.clone(),
                },
                &bucket,
            ),
            "me".to_owned(),
        )
        .unwrap();
        let (key, bytes) = node(1);
        block(store.put(&key, &bytes)).unwrap();
        assert_eq!(metadata.state.token_puts.load(Ordering::Relaxed), 1);
        assert_eq!(metadata.state.fail_next.load(Ordering::Relaxed), 0);
    }

    /// An instance without a role says so.
    #[test]
    fn an_instance_without_a_role_says_so() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        metadata.state.no_role.store(true, Ordering::Relaxed);
        let store = S3ChunkStore::new(
            with(
                S3Credentials::InstanceMetadata {
                    endpoint: metadata.endpoint.clone(),
                },
                &bucket,
            ),
            "me".to_owned(),
        )
        .unwrap();
        let (key, _) = node(1);
        let error = block(store.get(&key)).unwrap_err().message;
        assert!(error.contains("no instance profile"), "{error}");
    }

    /// Where nothing answers as instance metadata (no EC2 instance), the
    /// startup check fails within the metadata timeout, naming every
    /// source the chain tried.
    #[test]
    fn no_instance_metadata_fails_at_startup_naming_every_source() {
        let bucket = mock::MockS3::start();
        // A port nothing listens on.
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let store = S3ChunkStore::new(
            with(S3Credentials::InstanceMetadata { endpoint }, &bucket),
            "me".to_owned(),
        )
        .unwrap();
        let started = Instant::now();
        let error = jazz::db::block_on(store.check_at_startup()).unwrap_err();
        assert!(
            started.elapsed() < STARTUP_CHECK_TIMEOUT,
            "{:?}",
            started.elapsed()
        );
        for source in [
            "AWS_ACCESS_KEY_ID",
            "AWS_ROLE_ARN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "EC2 instance metadata",
        ] {
            assert!(error.contains(source), "{source} missing from {error}");
        }
        assert!(error.contains("hop limit"), "{error}");
    }

    /// A role endpoint that fails answers a whole batch with one fetch (of
    /// [`METADATA_ATTEMPTS`] tries),
    /// and every operation for [`ASSUME_FAILURE_HOLD`] after it.
    #[test]
    fn a_failing_role_endpoint_is_held_off() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        metadata.state.failing.store(true, Ordering::Relaxed);
        let store =
            S3ChunkStore::new(with(container(&metadata, None), &bucket), "me".to_owned()).unwrap();
        let nodes: Vec<_> = (0..8).map(node).collect();
        let results = jazz::db::block_on(futures::future::join_all(
            nodes.iter().map(|(key, bytes)| store.put(key, bytes)),
        ));
        for result in results {
            let error = result.unwrap_err().message;
            assert!(error.contains("500") && error.contains("Busy"), "{error}");
        }
        assert_eq!(
            metadata.state.container_calls.load(Ordering::Relaxed),
            METADATA_ATTEMPTS as u64
        );
        // Recovered, but still held off.
        metadata.state.failing.store(false, Ordering::Relaxed);
        let (key, bytes) = &nodes[0];
        block(store.put(key, bytes)).unwrap_err();
        assert_eq!(
            metadata.state.container_calls.load(Ordering::Relaxed),
            METADATA_ATTEMPTS as u64
        );
        // Once the hold is over, the endpoint is asked again.
        store
            .inner
            .keys
            .try_lock()
            .unwrap()
            .failed
            .as_mut()
            .unwrap()
            .0 -= ASSUME_FAILURE_HOLD;
        // (The prefix claim, which failed on the same keys, holds off too.)
        if let Some((at, _)) = store.inner.claim_failed.lock().unwrap().as_mut() {
            *at -= ASSUME_FAILURE_HOLD;
        }
        block(store.put(key, bytes)).unwrap();
        assert_eq!(
            metadata.state.container_calls.load(Ordering::Relaxed),
            METADATA_ATTEMPTS as u64 + 1
        );
    }

    /// Keys the bucket calls expired are fetched again from the role
    /// endpoint, as assumed keys are.
    #[test]
    fn keys_the_bucket_calls_expired_are_fetched_again() {
        use std::sync::atomic::Ordering;
        let bucket = mock::MockS3::start();
        let metadata = mock::MockMetadata::start(&bucket);
        let store =
            S3ChunkStore::new(with(container(&metadata, None), &bucket), "me".to_owned()).unwrap();
        let (key, bytes) = node(1);
        block(store.put(&key, &bytes)).unwrap();
        // The bucket forgets the keys, as when the role's session ends.
        bucket.state.issue("ASIAROLE1", "rsecret1", "revoked");
        block(store.get(&key)).unwrap_err();
        assert_eq!(block(store.get(&key)).unwrap().as_deref(), Some(&bytes[..]));
        assert_eq!(metadata.state.container_calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn refreshes_early_or_halfway_for_keys_near_their_expiry() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let hour = Duration::from_secs(3600);
        assert_eq!(refresh_at(now, now + hour), now + hour - REFRESH_EARLY);
        let six_minutes = Duration::from_secs(360);
        assert_eq!(refresh_at(now, now + six_minutes), now + six_minutes / 2);
        assert_eq!(
            refresh_at(now, now + Duration::from_secs(10)),
            now + MIN_KEY_USE
        );
        assert_eq!(refresh_at(now, now - hour), now + MIN_KEY_USE);
    }

    #[test]
    fn reads_role_keys_and_refuses_failures() {
        let (keys, expires) = role_keys(
            r#"{"Code":"Success","AccessKeyId":"ASIA1","SecretAccessKey":"s","Token":"t","Expiration":"2013-05-24T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(keys.access_key_id, "ASIA1");
        assert_eq!(keys.session_token.as_deref(), Some("t"));
        assert_eq!(
            expires,
            Some(UNIX_EPOCH + Duration::from_secs(1_369_353_600))
        );
        let error =
            role_keys(r#"{"Code":"AssumeRoleUnauthorizedAccess","Message":"no"}"#).unwrap_err();
        assert!(error.contains("AssumeRoleUnauthorizedAccess"), "{error}");
        assert!(role_keys("<html>").is_err());
        assert!(
            role_keys(r#"{"AccessKeyId":"a","SecretAccessKey":"s","Expiration":"soon"}"#).is_err()
        );
    }

    /// The default chain's order: keys, web identity, container, instance.
    #[test]
    fn finds_credentials_in_the_default_chain_order() {
        let chain = |vars: &[(&str, &str)]| {
            let vars: std::collections::BTreeMap<String, String> = vars
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect();
            S3Credentials::from_vars(|name| vars.get(name).cloned())
        };
        let keys = [
            ("AWS_ACCESS_KEY_ID", "AKIA"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
        ];
        let role = [
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/t"),
        ];
        let relative = (
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "/v2/credentials/x",
        );
        let full = (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://127.0.0.1:9/creds",
        );
        let all: Vec<_> = keys
            .iter()
            .chain(&role)
            .chain([&relative, &full])
            .copied()
            .collect();
        assert!(matches!(chain(&all), Ok(S3Credentials::Static(_))));
        assert!(matches!(
            chain(&all[2..]),
            Ok(S3Credentials::WebIdentity {
                token: WebIdentityToken::File(_),
                ..
            })
        ));
        assert!(matches!(
            chain(&all[4..]),
            Ok(S3Credentials::Container { uri, authorization: None })
                if uri == "http://169.254.170.2/v2/credentials/x"
        ));
        let Ok(S3Credentials::Container {
            uri,
            authorization: Some(ContainerToken::File(file)),
        }) = chain(&[
            full,
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "v"),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "/var/run/token"),
        ])
        else {
            panic!("a full URI with a token file");
        };
        assert_eq!(uri, "http://127.0.0.1:9/creds");
        assert_eq!(file, PathBuf::from("/var/run/token"));
        assert!(matches!(
            chain(&[full, ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "v")]),
            Ok(S3Credentials::Container {
                authorization: Some(ContainerToken::Value(_)),
                ..
            })
        ));
        assert!(matches!(
            chain(&[]),
            Ok(S3Credentials::InstanceMetadata { endpoint }) if endpoint == IMDS_ENDPOINT
        ));
        assert!(matches!(
            chain(&[("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://[fd00:ec2::254]/")]),
            Ok(S3Credentials::InstanceMetadata { endpoint }) if endpoint == "http://[fd00:ec2::254]"
        ));
        let error = chain(&[("AWS_EC2_METADATA_DISABLED", "true")]).unwrap_err();
        for source in [
            "AWS_ACCESS_KEY_ID",
            "AWS_ROLE_ARN",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_EC2_METADATA_DISABLED",
        ] {
            assert!(error.contains(source), "{source} missing from {error}");
        }
        assert!(
            chain(&[(
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "http://example.com/creds"
            )])
            .is_err()
        );
        // Empty values count as unset.
        assert!(matches!(
            chain(&[("AWS_ACCESS_KEY_ID", ""), relative]),
            Ok(S3Credentials::Container { .. })
        ));
    }

    #[test]
    fn sends_container_keys_over_http_only_to_local_or_link_local_hosts() {
        for allowed in [
            "https://creds.example.com/x",
            "http://127.0.0.1:8080/creds",
            "http://127.1.2.3/creds",
            "http://localhost/creds",
            "http://[::1]:80/creds",
            "http://169.254.170.2/v2/credentials/x",
            "http://169.254.170.23/v1/credentials",
            "http://[fd00:ec2::23]/v1/credentials",
        ] {
            assert!(container_uri_allowed(allowed), "{allowed}");
        }
        for refused in [
            "http://example.com/creds",
            "http://169.254.169.254/latest",
            "http://10.0.0.1/creds",
            "ftp://127.0.0.1/creds",
            "http://127.0.0.1.example.com/creds",
            "http://127.0.0.1:80@evil.example/creds",
            "http://user:pass@127.0.0.1/creds",
            "https://user@creds.example.com/x",
            "not a url",
        ] {
            assert!(!container_uri_allowed(refused), "{refused}");
        }
        // Errors name the endpoint by scheme, host and port only.
        assert_eq!(
            endpoint_host("http://user:secret@127.0.0.1:9/v2/credentials/guid?x"),
            "http://127.0.0.1:9"
        );
        assert_eq!(
            endpoint_host("http://169.254.170.2/v2/credentials/guid"),
            "http://169.254.170.2"
        );
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
        /// Every request answers 404 NoSuchBucket while set.
        pub no_bucket: AtomicBool,
        /// Requests answer only once this is unset.
        pub stalled: AtomicBool,
        /// GETs of data objects (not the claim markers) answer only once
        /// this is unset, as a bucket that stopped answering.
        pub stall_gets: AtomicBool,
        /// A missing key answers 403 AccessDenied, as without s3:ListBucket.
        pub deny_missing: AtomicBool,
        /// Set: the keys may only touch objects under this prefix (an IAM
        /// policy on `bucket/prefix/*`); anything else answers 403.
        pub only_prefix: Mutex<Option<String>>,
        /// Set: the bucket's region; a request signed for another is
        /// refused naming it, as AWS refuses one.
        pub bucket_region: Mutex<Option<String>>,
        /// Name `bucket_region` the way a 301 does (`x-amz-bucket-region`,
        /// no body), not as a 400's `<Region>`.
        pub region_by_header: AtomicBool,
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

    impl State {
        /// Make `id` valid keys, as STS or a role endpoint issuing them.
        pub(crate) fn issue(&self, id: &str, secret: &str, token: &str) {
            self.issued
                .lock()
                .unwrap()
                .insert(id.to_owned(), (secret.to_owned(), token.to_owned()));
        }
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
                async move {
                    use axum::response::IntoResponse as _;
                    match moved(&state, &request) {
                        Some(moved) => moved,
                        None => serve(&state, request).await.into_response(),
                    }
                }
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

    /// The 301 a bucket in `bucket_region` answers a request signed for
    /// another region with, when `region_by_header` is set.
    fn moved(state: &State, request: &axum::extract::Request) -> Option<axum::response::Response> {
        use axum::response::IntoResponse as _;
        if !state.region_by_header.load(Ordering::Relaxed) {
            return None;
        }
        let region = state.bucket_region.lock().unwrap().clone()?;
        let signed_for = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split("Credential=").nth(1))
            .and_then(|credential| credential.split('/').nth(2))
            .unwrap_or_default();
        (signed_for != region).then(|| {
            (
                axum::http::StatusCode::MOVED_PERMANENTLY,
                [("x-amz-bucket-region", region)],
            )
                .into_response()
        })
    }

    async fn serve(state: &State, request: axum::extract::Request) -> Reply {
        use axum::http::StatusCode;
        let (parts, body) = request.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        state.calls.requests.fetch_add(1, Ordering::Relaxed);
        if state.failing.load(Ordering::Relaxed) {
            return error(StatusCode::SERVICE_UNAVAILABLE, "SlowDown");
        }
        while state.stalled.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if state.no_bucket.load(Ordering::Relaxed) && parts.uri.path() != "/sts" {
            return error(StatusCode::NOT_FOUND, "NoSuchBucket");
        }
        if parts.method == axum::http::Method::GET && !parts.uri.path().contains("/.jazz-") {
            while state.stall_gets.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        if parts.uri.path() == "/sts" {
            return sts(state, &parts, &body);
        }
        if let Some(region) = state.bucket_region.lock().unwrap().clone() {
            let signed_for = parts
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split("Credential=").nth(1))
                .and_then(|credential| credential.split('/').nth(2))
                .unwrap_or_default()
                .to_owned();
            if signed_for != region {
                return (
                    StatusCode::BAD_REQUEST,
                    format!(
                        "<Error><Code>AuthorizationHeaderMalformed</Code><Message>the region \
                         '{signed_for}' is wrong; expecting '{region}'</Message>\
                         <Region>{region}</Region></Error>"
                    )
                    .into_bytes(),
                );
            }
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

    /// A container credentials endpoint and an IMDSv2 instance metadata
    /// service in one, issuing keys `bucket` accepts.
    pub(crate) struct MockMetadata {
        pub state: Arc<MetadataState>,
        pub endpoint: String,
        _runtime: tokio::runtime::Runtime,
    }

    #[derive(Default)]
    pub(crate) struct MetadataState {
        bucket: Arc<State>,
        /// The `Authorization` the container endpoint wants, if any.
        pub container_auth: Mutex<Option<String>>,
        /// The `Authorization` the container endpoint last got.
        pub container_auth_seen: Mutex<Option<String>>,
        pub container_calls: AtomicU64,
        /// The container endpoint and instance metadata answer 500 while set.
        pub failing: AtomicBool,
        /// The next this many requests answer 503, as a throttling IMDS.
        pub fail_next: AtomicU64,
        /// Keys expire this many seconds from when they are issued.
        pub expires_in: AtomicU64,
        /// The instance has no role attached.
        pub no_role: AtomicBool,
        pub token_puts: AtomicU64,
        /// The TTL each token PUT asked for.
        pub token_ttls: Mutex<Vec<String>>,
        /// Requests for role names or keys, and those of them refused for
        /// carrying no valid session token.
        pub metadata_gets: AtomicU64,
        pub metadata_refused: AtomicU64,
        tokens: Mutex<Vec<String>>,
        issued: AtomicU64,
    }

    impl MockMetadata {
        pub(crate) fn start(bucket: &MockS3) -> Self {
            let state = Arc::new(MetadataState {
                bucket: Arc::clone(&bucket.state),
                expires_in: AtomicU64::new(6 * 3600),
                ..MetadataState::default()
            });
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let listener = runtime
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let served = Arc::clone(&state);
            let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
                let state = Arc::clone(&served);
                async move { metadata(&state, request) }
            });
            runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                state,
                endpoint,
                _runtime: runtime,
            }
        }
    }

    /// `YYYY-MM-DDTHH:MM:SSZ`, as the role endpoints write `Expiration`.
    fn iso8601(at: SystemTime) -> String {
        let d = amz_date(at);
        format!(
            "{}-{}-{}T{}:{}:{}Z",
            &d[0..4],
            &d[4..6],
            &d[6..8],
            &d[9..11],
            &d[11..13],
            &d[13..15]
        )
    }

    fn keys_json(state: &MetadataState, imds: bool) -> String {
        let n = state.issued.fetch_add(1, Ordering::Relaxed) + 1;
        let (id, secret, token) = (
            format!("ASIAROLE{n}"),
            format!("rsecret{n}"),
            format!("rsession{n}"),
        );
        state.bucket.issue(&id, &secret, &token);
        let expires =
            SystemTime::now() + Duration::from_secs(state.expires_in.load(Ordering::Relaxed));
        let code = if imds {
            r#""Code":"Success","Type":"AWS-HMAC","#
        } else {
            ""
        };
        format!(
            r#"{{{code}"AccessKeyId":"{id}","SecretAccessKey":"{secret}","Token":"{token}","Expiration":"{}","RoleArn":"arn:aws:iam::1:role/task"}}"#,
            iso8601(expires)
        )
    }

    fn metadata(state: &MetadataState, request: axum::extract::Request) -> Reply {
        use axum::http::{Method, StatusCode};
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let path = request.uri().path().to_owned();
        let throttled = state
            .fail_next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok();
        if throttled {
            return (StatusCode::SERVICE_UNAVAILABLE, Vec::new());
        }
        if state.failing.load(Ordering::Relaxed) {
            if path.starts_with("/v2/credentials/") {
                state.container_calls.fetch_add(1, Ordering::Relaxed);
            }
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"code":"Busy","message":"try later"}"#.to_vec(),
            );
        }
        if path == "/latest/api/token" {
            if request.method() != Method::PUT {
                return (StatusCode::METHOD_NOT_ALLOWED, Vec::new());
            }
            let Some(ttl) = header("x-aws-ec2-metadata-token-ttl-seconds") else {
                return (StatusCode::BAD_REQUEST, Vec::new());
            };
            state.token_ttls.lock().unwrap().push(ttl);
            let n = state.token_puts.fetch_add(1, Ordering::Relaxed) + 1;
            let token = format!("imds-token-{n}");
            state.tokens.lock().unwrap().push(token.clone());
            return (StatusCode::OK, token.into_bytes());
        }
        if let Some(role) = path.strip_prefix("/latest/meta-data/iam/security-credentials/") {
            state.metadata_gets.fetch_add(1, Ordering::Relaxed);
            let token = header("x-aws-ec2-metadata-token");
            if request.method() != Method::GET
                || token.is_none_or(|token| !state.tokens.lock().unwrap().contains(&token))
            {
                state.metadata_refused.fetch_add(1, Ordering::Relaxed);
                return (StatusCode::UNAUTHORIZED, Vec::new());
            }
            if state.no_role.load(Ordering::Relaxed) {
                return (StatusCode::NOT_FOUND, Vec::new());
            }
            return match role {
                "" => (StatusCode::OK, b"jazz-instance\n".to_vec()),
                "jazz-instance" => (StatusCode::OK, keys_json(state, true).into_bytes()),
                _ => (StatusCode::NOT_FOUND, Vec::new()),
            };
        }
        if path.starts_with("/v2/credentials/") || path == "/get-credentials" {
            state.container_calls.fetch_add(1, Ordering::Relaxed);
            let auth = header("authorization");
            *state.container_auth_seen.lock().unwrap() = auth.clone();
            let wanted = state.container_auth.lock().unwrap().clone();
            if wanted.is_some() && auth != wanted {
                return (
                    StatusCode::FORBIDDEN,
                    br#"{"code":"AccessDenied","message":"bad token"}"#.to_vec(),
                );
            }
            return (StatusCode::OK, keys_json(state, false).into_bytes());
        }
        (StatusCode::NOT_FOUND, Vec::new())
    }
}
