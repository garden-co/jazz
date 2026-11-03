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
    fn read(&self) -> Result<String, String> {
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
    /// Prepended to every object name, e.g. `jazz/` to share a bucket.
    pub prefix: String,
    /// Address the bucket in the path (`endpoint/bucket/key`) rather than
    /// the host name. The default for custom endpoints.
    pub path_style: bool,
    pub credentials: S3Credentials,
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

/// Large-value bytes in an S3-compatible bucket ([`S3Options`]).
pub struct S3ChunkStore {
    inner: Arc<Inner>,
    runtime: Option<tokio::runtime::Runtime>,
}

struct Inner {
    options: S3Options,
    http: reqwest::Client,
    keys: tokio::sync::Mutex<Option<(Keys, Option<Instant>)>>,
}

impl S3ChunkStore {
    pub fn new(options: S3Options) -> Result<Self, String> {
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
        let keys = match &options.credentials {
            S3Credentials::Static(keys) => Some((keys.clone(), None)),
            S3Credentials::WebIdentity { .. } => None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                http,
                keys: tokio::sync::Mutex::new(keys),
            }),
            runtime: Some(runtime),
        })
    }

    pub fn options(&self) -> &S3Options {
        &self.inner.options
    }

    /// Run `op` on the store's runtime and await it from any executor.
    fn spawn<T: Send + 'static>(
        &self,
        op: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
    ) -> StoreFuture<'_, T> {
        let handle = self
            .runtime
            .as_ref()
            .expect("the runtime lives as long as the store")
            .spawn(op);
        Box::pin(async move {
            match handle.await {
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

impl Inner {
    /// The object's URL and host, and its path as signed.
    fn locate(&self, node: &NodeKey) -> (String, String, String) {
        let o = &self.options;
        let key = format!("{}{}", o.prefix, node.object_name());
        let base = o
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", o.region));
        let base = base.trim_end_matches('/');
        let (scheme, authority) = base.split_once("://").unwrap_or(("https", base));
        let (host, path) = if o.path_style {
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
        let mut cached = self.keys.lock().await;
        if let Some((keys, until)) = cached.as_ref()
            && until.is_none_or(|until| Instant::now() < until)
        {
            return Ok(keys.clone());
        }
        let S3Credentials::WebIdentity { role_arn, token } = &self.options.credentials else {
            unreachable!("static keys never expire");
        };
        let assumed =
            assume_role(&self.http, &self.options.region, role_arn, &token.read()?).await?;
        *cached = Some((
            assumed.clone(),
            Some(Instant::now() + ASSUMED_FOR - REFRESH_EARLY),
        ));
        Ok(assumed)
    }

    async fn send(
        &self,
        method: reqwest::Method,
        node: &NodeKey,
        body: Option<Bytes>,
    ) -> Result<reqwest::Response, String> {
        let (url, host, path) = self.locate(node);
        let keys = self.keys().await?;
        let payload = match &body {
            // The node's hash is the SHA-256 of its bytes.
            Some(_) => hex::encode(node.hash),
            None => hex::encode(Sha256::digest(b"")),
        };
        let mut headers = vec![
            ("host".to_owned(), host),
            ("x-amz-content-sha256".to_owned(), payload.clone()),
            ("x-amz-date".to_owned(), amz_date(SystemTime::now())),
        ];
        if body.is_some() {
            headers.push((
                "x-amz-checksum-sha256".to_owned(),
                base64::engine::general_purpose::STANDARD.encode(node.hash),
            ));
        }
        if let Some(token) = &keys.session_token {
            headers.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        let authorization = sign(
            method.as_str(),
            &path,
            "",
            &mut headers,
            &payload,
            &keys,
            &self.options.region,
        );
        let mut request = self.http.request(method, url);
        for (name, value) in headers.iter().filter(|(name, _)| name != "host") {
            request = request.header(name, value);
        }
        request = request.header("authorization", authorization);
        if let Some(body) = body {
            request = request.body(body);
        }
        request
            .send()
            .await
            .map_err(|error| format!("s3 blobs: {error}"))
    }

    async fn put(&self, node: &NodeKey, bytes: Bytes) -> Result<(), String> {
        let response = self.send(reqwest::Method::PUT, node, Some(bytes)).await?;
        expect_ok(response, "put", node).await.map(|_| ())
    }

    async fn get(&self, node: &NodeKey) -> Result<Option<Bytes>, String> {
        let response = self.send(reqwest::Method::GET, node, None).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect_ok(response, "get", node).await?;
        response
            .bytes()
            .await
            .map(Some)
            .map_err(|error| format!("s3 blobs: read {}: {error}", node.object_name()))
    }

    async fn delete(&self, node: &NodeKey) -> Result<(), String> {
        let response = self.send(reqwest::Method::DELETE, node, None).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        expect_ok(response, "delete", node).await.map(|_| ())
    }
}

async fn expect_ok(
    response: reqwest::Response,
    what: &str,
    node: &NodeKey,
) -> Result<reqwest::Response, String> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let code = xml_field(&body, "Code").unwrap_or_default();
    let message = xml_field(&body, "Message").unwrap_or_default();
    Err(format!(
        "s3 blobs: {what} {}: {status} {code} {message}",
        node.object_name()
    ))
}

/// Short-lived keys for `role_arn` (STS `AssumeRoleWithWebIdentity`, which
/// is authenticated by the token alone).
async fn assume_role(
    http: &reqwest::Client,
    region: &str,
    role_arn: &str,
    token: &str,
) -> Result<Keys, String> {
    let sts = match region {
        "auto" | "" => "https://sts.amazonaws.com".to_owned(),
        region => format!("https://sts.{region}.amazonaws.com"),
    };
    let response = http
        .get(sts)
        .query(&[
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("DurationSeconds", &ASSUMED_FOR.as_secs().to_string()),
            ("RoleArn", role_arn),
            ("RoleSessionName", "jazz-blobs"),
            ("WebIdentityToken", token),
        ])
        .send()
        .await
        .map_err(|error| format!("s3 blobs: assume {role_arn}: {error}"))?;
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
    Ok(Keys {
        access_key_id: field("AccessKeyId")?,
        secret_access_key: field("SecretAccessKey")?,
        session_token: Some(field("SessionToken")?),
    })
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
fn uri_encode(value: &str, slash: bool) -> String {
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
fn sign(
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
        };
        let node = NodeKey { hash: [0xab; 32] };
        let store = S3ChunkStore::new(options(None)).unwrap();
        let (url, host, path) = store.inner.locate(&node);
        assert_eq!(host, "jazz-app.s3.us-east-1.amazonaws.com");
        assert_eq!(path, format!("/blobs/{}", "ab".repeat(32)));
        assert_eq!(url, format!("https://{host}{path}"));
        let store = S3ChunkStore::new(options(Some("http://127.0.0.1:9000/"))).unwrap();
        let (url, host, path) = store.inner.locate(&node);
        assert_eq!(host, "127.0.0.1:9000");
        assert_eq!(path, format!("/jazz-app/blobs/{}", "ab".repeat(32)));
        assert_eq!(url, format!("http://{host}{path}"));
    }
}
