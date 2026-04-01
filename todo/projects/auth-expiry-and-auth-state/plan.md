# Auth Expiry and Auth State Implementation Plan

**Goal:** Implement response-driven JWT auth loss handling so Jazz surfaces a typed unauthenticated state, pauses sync on `401`, and resumes when the app calls `db.updateAuth(jwtToken)`.

**Architecture:** Add a shared TypeScript auth-state store owned by `Db`, teach both sync servers to return structured unauthenticated `401` bodies, and route `401` failures from direct and worker transports into that store. Keep the last derived session during auth loss, update live runtime auth with `updateAuth`, and make framework providers read session state from `db` instead of a one-shot decode.

**Tech Stack:** Rust (`axum`, `jsonwebtoken`), TypeScript (`vitest`, React/Vue/Svelte adapters), browser workers, Turbo/PNPM.

---

## File Structure

- Create: `packages/jazz-tools/src/runtime/auth-state.ts`
  Responsibility: shared `AuthFailureReason`, `AuthState`, deduplicated listener store, bearer principal guard, initial-state derivation.
- Create: `packages/jazz-tools/src/runtime/auth-state.test.ts`
  Responsibility: unit tests for last-session preservation, dedupe, JWT-only renewal, and principal hot-swap rejection.
- Create: `packages/jazz-tools/src/runtime/db.auth-state.test.ts`
  Responsibility: `Db`-level tests for `getAuthState()`, `onAuthChanged()`, and `updateAuth(...)`.
- Create: `packages/jazz-tools/src/react-core/provider.test.tsx`
  Responsibility: verifies `JazzProvider` reuses the client on JWT-only changes and keeps `useSession()` reactive.
- Modify: `crates/jazz-tools/src/transport_protocol.rs:148-189`
  Responsibility: add the runtime-facing `UnauthenticatedCode` and `UnauthenticatedResponse` schema.
- Modify: `crates/jazz-tools/src/middleware/auth.rs:99-166`
  Responsibility: extend JWT validation types so expiry and disabled-auth cases are first-class.
- Modify: `crates/jazz-tools/src/middleware/auth.rs:544-629`
  Responsibility: enforce `exp` after signature verification and classify retryable vs fatal JWT errors.
- Modify: `crates/jazz-tools/src/middleware/auth.rs:739-829`
  Responsibility: return structured missing/invalid/expired/disabled auth failures from request extraction.
- Modify: `crates/jazz-tools/src/routes.rs:279-343`
  Responsibility: return structured JSON `401`s from `/events`.
- Modify: `crates/jazz-tools/src/routes.rs:505-534`
  Responsibility: return structured JSON `401`s from `/sync`.
- Modify: `crates/jazz-tools/src/routes.rs:1645-1669`
  Responsibility: assert `/sync` missing-auth responses return the new body shape.
- Modify: `crates/jazz-tools/tests/auth_test.rs:436-640`
  Responsibility: add external-auth integration coverage for expired, invalid, and missing bearer flows.
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts:43-68`
  Responsibility: add auth-failure callback plumbing and typed `SyncAuthError`.
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts:126-288`
  Responsibility: pause stream reconnects on structured `401`.
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts:508-624`
  Responsibility: parse structured `401` bodies for `/sync` POSTs.
- Modify: `packages/jazz-tools/src/runtime/sync-transport.test.ts:321-396`
  Responsibility: test stream pause/dedupe on auth loss.
- Modify: `packages/jazz-tools/src/runtime/sync-transport.test.ts:725-759`
  Responsibility: test `SyncAuthError` parsing for `/sync` POSTs.
- Modify: `packages/jazz-tools/src/runtime/client.ts:597-630`
  Responsibility: allow direct clients to notify auth failure and update their JWT/session in place.
- Modify: `packages/jazz-tools/src/runtime/client.ts:793-806`
  Responsibility: keep sync auth lookup JWT-driven and resumable.
- Modify: `packages/jazz-tools/src/runtime/client.ts:1469-1505`
  Responsibility: classify `/sync` auth failures separately from generic transport failures.
- Modify: `packages/jazz-tools/src/runtime/db.ts:54-90`
  Responsibility: expose the public auth-state API on `DbConfig`/`Db`.
- Modify: `packages/jazz-tools/src/runtime/db.ts:338-390`
  Responsibility: create and own the shared auth-state store.
- Modify: `packages/jazz-tools/src/runtime/db.ts:470-560`
  Responsibility: pass auth-failure callbacks into cached clients and worker bridges.
- Modify: `packages/jazz-tools/src/runtime/db.ts:957-960`
  Responsibility: keep cloned config in sync with live JWT updates.
- Modify: `packages/jazz-tools/src/runtime/db.ts:1588-1608`
  Responsibility: initialize auth state from resolved config and preserve `createDbFromClient(...)` behavior.
- Modify: `packages/jazz-tools/src/runtime/client-session.ts:282-351`
  Responsibility: reuse JWT session derivation from auth-state code without duplicating decode logic.
- Modify: `packages/jazz-tools/src/runtime/index.ts:20-32`
  Responsibility: export auth-state types from the runtime package.
- Modify: `packages/jazz-tools/src/worker/worker-protocol.ts:75-81`
  Responsibility: keep `update-auth` JWT-only and add a worker-to-main auth-failure message.
- Modify: `packages/jazz-tools/src/worker/worker-protocol.ts:185-193`
  Responsibility: include `auth-failed` in the worker protocol union.
- Modify: `packages/jazz-tools/src/runtime/worker-bridge.ts:70-211`
  Responsibility: surface worker auth failures to the main thread and `Db` auth store.
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:58-80`
  Responsibility: pause batched `/sync` retries on auth loss instead of looping.
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:390-480`
  Responsibility: pause `/events` reconnects on auth loss instead of looping.
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:581-594`
  Responsibility: clear auth pause only when `update-auth` arrives.
- Modify: `packages/jazz-tools/src/runtime/worker-bridge.race-harness.test.ts:1-260`
  Responsibility: test worker-to-main auth-failure forwarding.
- Modify: `packages/jazz-tools/tests/browser/support.ts:255-270`
  Responsibility: add a JWT-backed synced-db helper for browser tests.
- Modify: `packages/jazz-tools/tests/browser/testing-server.ts:20-24`
  Responsibility: reuse the existing JWT helper in the new browser flow.
- Modify: `packages/jazz-tools/tests/browser/worker-bridge.test.ts:174-260`
  Responsibility: add browser auth-loss and recovery coverage.
- Modify: `packages/jazz-tools/src/react/create-jazz-client.ts:1-39`
  Responsibility: stop decoding session separately; use `db.getAuthState().session`.
- Modify: `packages/jazz-tools/src/vue/create-jazz-client.ts:1-39`
  Responsibility: same as React wrapper.
- Modify: `packages/jazz-tools/src/svelte/create-jazz-client.ts:1-39`
  Responsibility: same as React wrapper.
- Modify: `packages/jazz-tools/src/react-native/create-jazz-client.ts:1-38`
  Responsibility: same as React wrapper.
- Modify: `packages/jazz-tools/src/react-core/provider.tsx:1-165`
  Responsibility: keep a stable client cache key without `jwtToken`, subscribe to `db.onAuthChanged`, and call `db.updateAuth(...)` for JWT-only changes.
- Modify: `packages/jazz-tools/src/vue/provider.ts:16-123`
  Responsibility: keep provided session in sync with `db.onAuthChanged`.
- Modify: `packages/jazz-tools/src/svelte/JazzSvelteProvider.svelte:1-50`
  Responsibility: keep Svelte context session in sync with `db.onAuthChanged`.
- Modify: `packages/jazz-tools/src/react/create-jazz-client.test.ts`
  Responsibility: replace `resolveClientSession(...)` expectations with `db.getAuthState()`.
- Modify: `packages/jazz-tools/src/vue/create-jazz-client.test.ts`
  Responsibility: replace `resolveClientSession(...)` expectations with `db.getAuthState()`.
- Modify: `packages/jazz-tools/src/svelte/create-jazz-client.test.ts`
  Responsibility: replace `resolveClientSession(...)` expectations with `db.getAuthState()`.
- Modify: `packages/jazz-tools/src/react-native/create-jazz-client.test.ts`
  Responsibility: replace `resolveClientSession(...)` expectations with `db.getAuthState()`.
- Modify: `crates/jazz-cloud-server/src/server.rs:3037-3185`
  Responsibility: mirror JWT expiry and structured unauthenticated classification for cloud apps.
- Modify: `crates/jazz-cloud-server/src/server.rs:3759-3788`
  Responsibility: return structured `401` bodies from cloud `/sync`.
- Modify: `crates/jazz-cloud-server/src/server.rs:4616-4620`
  Responsibility: return structured `401` bodies from cloud link-external JWT validation.
- Modify: `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts:1196-1222`
  Responsibility: assert cloud `401`s become typed auth failures in the TS client path.

### Task 1: Structured `401` Contract in the Self-hosted Sync Server

**Files:**

- Modify: `crates/jazz-tools/src/transport_protocol.rs:148-189`
- Modify: `crates/jazz-tools/src/middleware/auth.rs:99-166`
- Modify: `crates/jazz-tools/src/middleware/auth.rs:544-629`
- Modify: `crates/jazz-tools/src/middleware/auth.rs:739-829`
- Modify: `crates/jazz-tools/src/routes.rs:279-343`
- Modify: `crates/jazz-tools/src/routes.rs:505-534`
- Modify: `crates/jazz-tools/src/routes.rs:1645-1669`
- Modify: `crates/jazz-tools/tests/auth_test.rs:436-640`

- [ ] **Step 1: Write the failing Rust tests**

```rust
fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn make_jwt_with_kid_and_exp(
    sub: &str,
    kid: &str,
    secret: &str,
    exp: Option<u64>,
) -> String {
    let jwt_claims = serde_json::json!({
        "sub": sub,
        "claims": {},
        "exp": exp,
    });

    let key = EncodingKey::from_secret(secret.as_bytes());
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some(kid.to_string());
    encode(&header, &jwt_claims, &key).unwrap()
}

#[tokio::test]
async fn test_expired_bearer_returns_structured_401() {
    let server = TestServer::start_with_jwks_responses(vec![
        test_server::hs256_jwks("kid-expired", "secret-expired"),
    ])
    .await;

    let token = make_jwt_with_kid_and_exp(
        "user-expired",
        "kid-expired",
        "secret-expired",
        Some(now_unix_seconds() - 60),
    );

    let response = client()
        .post(format!("{}/sync", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .body(sync_body())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "unauthenticated");
    assert_eq!(body["code"], "expired");
    assert_eq!(body["message"], "JWT has expired");
}

#[tokio::test]
async fn sync_batch_requires_auth() {
    let app = make_sync_test_app("test-backend-secret").await;

    let body = serde_json::json!({
        "payloads": [object_updated_payload("00000000-0000-0000-0000-000000000001")],
        "client_id": ClientId::new(),
    });

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/sync")
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "unauthenticated");
    assert_eq!(json["code"], "missing");
}
```

- [ ] **Step 2: Run the Rust tests to verify they fail**

Run: `cargo test -p jazz-tools test_expired_bearer_returns_structured_401 --test auth_test`

Run: `cargo test -p jazz-tools sync_batch_requires_auth`

Expected: FAIL because `/sync` still returns the old generic unauthorized body and expired JWTs are still treated as generic invalid auth.

- [ ] **Step 3: Add the shared unauthenticated response type**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnauthenticatedCode {
    Expired,
    Missing,
    Invalid,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnauthenticatedResponse {
    pub error: &'static str,
    pub code: UnauthenticatedCode,
    pub message: String,
}

impl UnauthenticatedResponse {
    pub fn expired(message: impl Into<String>) -> Self {
        Self { error: "unauthenticated", code: UnauthenticatedCode::Expired, message: message.into() }
    }

    pub fn missing(message: impl Into<String>) -> Self {
        Self { error: "unauthenticated", code: UnauthenticatedCode::Missing, message: message.into() }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self { error: "unauthenticated", code: UnauthenticatedCode::Invalid, message: message.into() }
    }

    pub fn disabled(message: impl Into<String>) -> Self {
        Self { error: "unauthenticated", code: UnauthenticatedCode::Disabled, message: message.into() }
    }
}
```

- [ ] **Step 4: Classify JWT expiry, missing auth, and disabled auth in `extract_session(...)`**

```rust
#[derive(Debug, Clone)]
pub struct VerifiedJwt {
    pub subject: String,
    pub issuer: Option<String>,
    pub principal_id_claim: Option<String>,
    pub claims: serde_json::Value,
    pub exp: Option<u64>,
}

#[derive(Debug)]
pub enum JwtError {
    Disabled,
    Expired,
    Invalid(String),
}

fn ensure_token_not_expired(verified: &VerifiedJwt) -> Result<(), JwtError> {
    let Some(exp) = verified.exp else {
        return Ok(());
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if exp <= now {
        return Err(JwtError::Expired);
    }

    Ok(())
}

pub async fn extract_session(
    headers: &HeaderMap,
    app_id: AppId,
    config: &AuthConfig,
    external_identities: Option<&ExternalIdentityMap>,
    jwks_cache: Option<&JwksCache>,
) -> Result<Option<Session>, UnauthenticatedResponse> {
    if let Some(auth_value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok())
        && let Some(token) = auth_value.strip_prefix("Bearer ")
    {
        let token = token.trim();
        if token.is_empty() {
            return Err(UnauthenticatedResponse::invalid("Empty bearer token"));
        }

        let cache = jwks_cache.ok_or_else(|| {
            UnauthenticatedResponse::disabled("JWT auth is not enabled for this app")
        })?;

        let verified = validate_jwt_with_cache(token, cache)
            .await
            .map_err(|error| match error {
                JwtError::Disabled => UnauthenticatedResponse::disabled("JWT auth is not enabled for this app"),
                JwtError::Expired => UnauthenticatedResponse::expired("JWT has expired"),
                JwtError::Invalid(message) => UnauthenticatedResponse::invalid(message),
            })?;

        let session = resolve_verified_jwt_session(app_id, verified, external_identities)
            .map_err(|_| UnauthenticatedResponse::invalid("Invalid JWT subject"))?;
        return Ok(Some(session));
    }

    if let Some((mode, token)) = parse_local_auth_headers(headers)
        .map_err(|_| UnauthenticatedResponse::invalid("Invalid local auth headers"))?
    {
        if !config.is_local_mode_enabled(mode) {
            return Err(UnauthenticatedResponse::disabled(match mode {
                LocalAuthMode::Anonymous => "Anonymous auth disabled",
                LocalAuthMode::Demo => "Demo auth disabled",
            }));
        }

        let principal_id = derive_local_principal_id(app_id, mode, &token);
        return Ok(Some(Session {
            user_id: principal_id,
            claims: serde_json::json!({
                "auth_mode": "local",
                "local_mode": mode.as_str(),
            }),
        }));
    }

    Ok(None)
}
```

- [ ] **Step 5: Return structured JSON from `/events` and `/sync`**

```rust
async fn events_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<EventsParams>,
) -> Result<impl IntoResponse, Response> {
    // ...
    let session = match extract_session(
        &headers,
        state.app_id,
        &state.auth_config,
        Some(&external_identities),
        state.jwks_cache.as_ref(),
    )
    .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(UnauthenticatedResponse::missing(
                    "Session required for event stream. Provide JWT, local auth headers, or backend secret.",
                )),
            )
                .into_response());
        }
        Err(error) => {
            return Err((StatusCode::UNAUTHORIZED, Json(error)).into_response());
        }
    };
    // ...
}

// In sync_handler:
Ok(None) => {
    return (
        StatusCode::UNAUTHORIZED,
        Json(UnauthenticatedResponse::missing(
            "Session required for sync. Provide JWT, local auth headers, or backend secret.",
        )),
    )
        .into_response();
}
Err(error) => {
    return (StatusCode::UNAUTHORIZED, Json(error)).into_response();
}
```

- [ ] **Step 6: Run the Rust tests to verify they pass**

Run: `cargo test -p jazz-tools test_expired_bearer_returns_structured_401 --test auth_test`

Run: `cargo test -p jazz-tools sync_batch_requires_auth`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/jazz-tools/src/transport_protocol.rs crates/jazz-tools/src/middleware/auth.rs crates/jazz-tools/src/routes.rs crates/jazz-tools/tests/auth_test.rs
git commit -m "feat: return structured unauthenticated sync responses"
```

### Task 2: Parse Structured `401`s in the TS Transport Layer

**Files:**

- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts:43-68`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts:126-288`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts:508-624`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.test.ts:321-396`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.test.ts:725-759`
- Modify: `packages/jazz-tools/src/runtime/client.ts:622-629`
- Modify: `packages/jazz-tools/src/runtime/client.ts:1469-1505`

- [ ] **Step 1: Write the failing transport tests**

```ts
it("stops reconnecting and reports auth loss on structured 401 stream responses", async () => {
  vi.useFakeTimers();
  vi.spyOn(Math, "random").mockReturnValue(0);

  const fetchMock = vi.fn().mockResolvedValue({
    ok: false,
    status: 401,
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({
      error: "unauthenticated",
      code: "expired",
      message: "JWT has expired",
    }),
  } satisfies Partial<Response>);
  (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

  const onAuthFailure = vi.fn();
  const controller = new SyncStreamController({
    getAuth: () => ({ jwtToken: "expired-jwt" }),
    getClientId: () => "client-1",
    setClientId: vi.fn(),
    onConnected: vi.fn(),
    onDisconnected: vi.fn(),
    onSyncMessage: vi.fn(),
    onAuthFailure,
  });

  controller.start("http://localhost:3000");
  await Promise.resolve();

  expect(onAuthFailure).toHaveBeenCalledWith("expired");
  await vi.advanceTimersByTimeAsync(5_000);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("throws SyncAuthError for structured 401 sync POST responses", async () => {
  const fetchMock = vi.fn().mockResolvedValue({
    ok: false,
    status: 401,
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({
      error: "unauthenticated",
      code: "invalid",
      message: "JWT signature verification failed",
    }),
  } satisfies Partial<Response>);
  (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

  await expect(
    sendSyncPayloadBatch("http://localhost:3000", [playerPayload("id-1")], {
      jwtToken: "broken-jwt",
    }),
  ).rejects.toMatchObject({
    name: "SyncAuthError",
    reason: "invalid",
    message: "JWT signature verification failed",
  });
});
```

- [ ] **Step 2: Run the transport tests to verify they fail**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/sync-transport.test.ts`

Expected: FAIL because the controller still schedules reconnects on `401` and `sendSyncPayloadBatch(...)` still throws a generic `Error`.

- [ ] **Step 3: Introduce `SyncAuthError` and structured-401 parsing**

```ts
export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";

type UnauthenticatedPayload = {
  error?: unknown;
  code?: unknown;
  message?: unknown;
};

export class SyncAuthError extends Error {
  readonly name = "SyncAuthError";

  constructor(
    readonly reason: AuthFailureReason,
    message: string,
    readonly status = 401,
  ) {
    super(message);
  }
}

async function readSyncAuthError(response: Response): Promise<SyncAuthError | null> {
  if (response.status !== 401) {
    return null;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    return new SyncAuthError("invalid", `Sync POST failed: ${response.status}`);
  }

  const body = (await response.json()) as UnauthenticatedPayload;
  if (body.error !== "unauthenticated") {
    return new SyncAuthError("invalid", `Sync POST failed: ${response.status}`);
  }

  const code = body.code;
  const reason: AuthFailureReason =
    code === "expired" || code === "missing" || code === "invalid" || code === "disabled"
      ? code
      : "invalid";

  const message = typeof body.message === "string" ? body.message : `Unauthenticated (${reason})`;
  return new SyncAuthError(reason, message, response.status);
}
```

- [ ] **Step 4: Pause reconnect loops on auth loss and surface the reason**

```ts
export interface SyncStreamControllerOptions {
  logPrefix?: string;
  getAuth(): Pick<SyncAuth, "jwtToken" | "localAuthMode" | "localAuthToken" | "backendSecret">;
  getClientId(): string;
  setClientId(clientId: string): void;
  onConnected(catalogueStateHash?: string | null): void;
  onDisconnected(): void;
  onSyncMessage(payloadJson: string): void;
  onAuthFailure?(reason: AuthFailureReason): void;
}

export class SyncStreamController {
  private pausedForAuth = false;

  updateAuth(): void {
    this.pausedForAuth = false;
    this.abortStream();
    this.detachServer();
    if (this.activeServerUrl && !this.stopped) {
      this.scheduleReconnect();
    }
  }

  private async connectStream(): Promise<void> {
    // ...
    const response = await fetch(eventsUrl, { headers, signal: abortController.signal });
    if (!response.ok) {
      const authError = await readSyncAuthError(response);
      if (authError) {
        this.pausedForAuth = true;
        this.abortStream();
        this.detachServer();
        this.options.onAuthFailure?.(authError.reason);
        return;
      }

      this.detachServer();
      this.streamConnecting = false;
      this.scheduleReconnect();
      return;
    }
    // ...
  }
}

async function postSyncBatch(
  url: string,
  headers: Record<string, string>,
  body: string,
  logPrefix: string,
): Promise<void> {
  // ...
  if (!response.ok) {
    const authError = await readSyncAuthError(response);
    if (authError) {
      throw authError;
    }

    const statusText = response.statusText ? ` ${response.statusText}` : "";
    throw new Error(`${logPrefix}Sync POST failed: ${response.status}${statusText}`);
  }
}
```

- [ ] **Step 5: Route direct-client auth failures away from generic transport failure**

```ts
export interface ConnectSyncRuntimeOptions {
  useBinaryEncoding?: boolean;
  onAuthFailure?: (reason: AuthFailureReason) => void;
}

this.streamController = createRuntimeSyncStreamController({
  getRuntime: () => this.runtime,
  getAuth: () => this.getSyncAuth(),
  getClientId: () => this.serverClientId,
  setClientId: (clientId) => {
    this.serverClientId = clientId;
  },
  onAuthFailure: runtimeOptions?.onAuthFailure,
});

this.runtime.onSyncMessageToSend(
  createSyncOutboxRouter({
    logPrefix: "[client] ",
    retryServerPayloads: true,
    onServerPayload: (payload, isCatalogue) => this.sendSyncMessage(payload as string, isCatalogue),
    onServerPayloadError: (error) => {
      if (error instanceof SyncAuthError) {
        runtimeOptions?.onAuthFailure?.(error.reason);
        return;
      }

      const isExpectedAbort = isExpectedFetchAbortError(error);
      if (!isExpectedAbort) {
        console.error("Sync POST error:", error);
        this.streamController.notifyTransportFailure();
      }
    },
  }),
);
```

- [ ] **Step 6: Run the transport tests to verify they pass**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/sync-transport.test.ts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/jazz-tools/src/runtime/sync-transport.ts packages/jazz-tools/src/runtime/sync-transport.test.ts packages/jazz-tools/src/runtime/client.ts
git commit -m "feat: classify auth loss in sync transport"
```

### Task 3: Add the Shared Auth-state Store and `Db.updateAuth(...)`

**Files:**

- Create: `packages/jazz-tools/src/runtime/auth-state.ts`
- Create: `packages/jazz-tools/src/runtime/auth-state.test.ts`
- Create: `packages/jazz-tools/src/runtime/db.auth-state.test.ts`
- Modify: `packages/jazz-tools/src/runtime/db.ts:54-90`
- Modify: `packages/jazz-tools/src/runtime/db.ts:338-390`
- Modify: `packages/jazz-tools/src/runtime/db.ts:470-560`
- Modify: `packages/jazz-tools/src/runtime/db.ts:957-960`
- Modify: `packages/jazz-tools/src/runtime/db.ts:1588-1608`
- Modify: `packages/jazz-tools/src/runtime/client.ts:597-630`
- Modify: `packages/jazz-tools/src/runtime/client.ts:793-806`
- Modify: `packages/jazz-tools/src/runtime/client-session.ts:282-351`
- Modify: `packages/jazz-tools/src/runtime/index.ts:20-32`

- [ ] **Step 1: Write the failing auth-state tests**

```ts
function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

function makeClientWithContext(context: AppContext): JazzClient {
  let nextHandle = 0;
  const runtime: Runtime = {
    insert: () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    update: () => {},
    delete: () => {},
    query: async () => [],
    subscribe: () => nextHandle++,
    createSubscription: () => nextHandle++,
    executeSubscription: () => {},
    unsubscribe: () => {},
    insertDurable: async () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    updateDurable: async () => {},
    deleteDurable: async () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "worker" | "edge" | "global",
    ): JazzClient;
  };
  return new JazzClientCtor(runtime, context, "edge");
}

function makeDbWithJwt(jwtToken: string): Db {
  const client = makeClientWithContext({
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
    jwtToken,
  });

  return createDbFromClient({ appId: "test-app", jwtToken }, client);
}

it("keeps the last session while unauthenticated", () => {
  const store = createAuthStateStore({
    appId: "test-app",
    jwtToken: makeJwt({ sub: "alice", claims: { role: "reader" } }),
  });

  store.markUnauthenticated("expired");

  expect(store.getState()).toEqual({
    status: "unauthenticated",
    reason: "expired",
    session: {
      user_id: "alice",
      claims: {
        role: "reader",
        auth_mode: "external",
        subject: "alice",
      },
    },
  });
});

it("rejects principal hot-swap on a live client", () => {
  const store = createAuthStateStore({
    appId: "test-app",
    jwtToken: makeJwt({ sub: "alice" }),
  });

  expect(() => store.applyJwtToken(makeJwt({ sub: "bob" }))).toThrow(
    "Changing principals on a live client is not supported",
  );
});

it("db.updateAuth resumes same-principal bearer auth", () => {
  const aliceJwt = makeJwt({ sub: "alice", claims: { role: "reader" } });
  const refreshedJwt = makeJwt({ sub: "alice", claims: { role: "writer" } });
  const db = makeDbWithJwt(aliceJwt);
  const seen: AuthState[] = [];

  const stop = db.onAuthChanged((state) => seen.push(state));
  db.updateAuth(refreshedJwt);
  stop();

  expect(db.getAuthState()).toMatchObject({
    status: "authenticated",
    transport: "bearer",
    session: {
      user_id: "alice",
      claims: expect.objectContaining({ role: "writer" }),
    },
  });
  expect(seen.at(-1)).toMatchObject({ status: "authenticated" });
});
```

- [ ] **Step 2: Run the auth-state tests to verify they fail**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/auth-state.test.ts src/runtime/db.auth-state.test.ts`

Expected: FAIL because the store and the public `Db` auth API do not exist yet.

- [ ] **Step 3: Add the auth-state store**

```ts
import { resolveClientSessionSync, resolveJwtSession } from "./client-session.js";
import type { LocalAuthMode, Session } from "./context.js";

export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";

export type AuthState =
  | {
      status: "authenticated";
      transport: "bearer" | "local" | "backend";
      session: Session | null;
    }
  | {
      status: "unauthenticated";
      reason: AuthFailureReason;
      session: Session | null;
    };

type AuthStateListener = (state: AuthState) => void;

function deriveInitialAuthState(input: {
  appId: string;
  jwtToken?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
}): AuthState {
  if (input.jwtToken) {
    return {
      status: "authenticated",
      transport: "bearer",
      session: resolveJwtSession(input.jwtToken),
    };
  }

  const localSession = resolveClientSessionSync({
    appId: input.appId,
    localAuthMode: input.localAuthMode,
    localAuthToken: input.localAuthToken,
  });

  if (localSession) {
    return {
      status: "authenticated",
      transport: "local",
      session: localSession,
    };
  }

  return {
    status: "unauthenticated",
    reason: "missing",
    session: null,
  };
}

export function createAuthStateStore(input: {
  appId: string;
  jwtToken?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
}): {
  getState(): AuthState;
  onChange(listener: AuthStateListener): () => void;
  markUnauthenticated(reason: AuthFailureReason): void;
  applyJwtToken(jwtToken?: string): AuthState;
} {
  let state = deriveInitialAuthState(input);
  const listeners = new Set<AuthStateListener>();

  const emit = () => {
    for (const listener of listeners) {
      listener(state);
    }
  };

  return {
    getState: () => state,
    onChange(listener) {
      listeners.add(listener);
      listener(state);
      return () => listeners.delete(listener);
    },
    markUnauthenticated(reason) {
      if (state.status === "unauthenticated" && state.reason === reason) {
        return;
      }

      state = {
        status: "unauthenticated",
        reason,
        session: state.session,
      };
      emit();
    },
    applyJwtToken(jwtToken) {
      const nextSession =
        resolveJwtSession(jwtToken ?? "") ??
        resolveClientSessionSync({
          appId: input.appId,
          localAuthMode: input.localAuthMode,
          localAuthToken: input.localAuthToken,
        });

      if (
        jwtToken &&
        state.session &&
        nextSession &&
        state.session.user_id !== nextSession.user_id
      ) {
        throw new Error("Changing principals on a live client is not supported. Recreate the Db.");
      }

      state = jwtToken
        ? { status: "authenticated", transport: "bearer", session: nextSession }
        : nextSession
          ? { status: "authenticated", transport: "local", session: nextSession }
          : { status: "unauthenticated", reason: "missing", session: null };

      emit();
      return state;
    },
  };
}
```

- [ ] **Step 4: Wire the store into `Db` and live `JazzClient` instances**

```ts
export class Db {
  private readonly authState: ReturnType<typeof createAuthStateStore>;

  protected constructor(config: DbConfig, wasmModule: WasmModule | null) {
    this.config = config;
    this.wasmModule = wasmModule;
    this.authState = createAuthStateStore(config);
  }

  updateAuth(jwtToken?: string): void {
    this.config.jwtToken = jwtToken;
    this.authState.applyJwtToken(jwtToken);

    for (const client of this.clients.values()) {
      client.updateAuth(jwtToken);
    }

    this.workerBridge?.updateAuth({
      jwtToken,
      localAuthMode: this.config.localAuthMode,
      localAuthToken: this.config.localAuthToken,
    });
  }

  getAuthState(): AuthState {
    return this.authState.getState();
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.authState.onChange(listener);
  }

  protected getClient(schema: WasmSchema): JazzClient {
    // ...
    const client = JazzClient.connectSync(
      this.wasmModule,
      {
        appId: this.config.appId,
        schema,
        driver: this.config.driver,
        serverUrl: this.worker ? undefined : this.config.serverUrl,
        serverPathPrefix: this.worker ? undefined : this.config.serverPathPrefix,
        env: this.config.env,
        userBranch: this.config.userBranch,
        jwtToken: this.config.jwtToken,
        localAuthMode: this.config.localAuthMode,
        localAuthToken: this.config.localAuthToken,
        adminSecret: this.config.adminSecret,
        tier: this.worker ? undefined : "worker",
      },
      {
        useBinaryEncoding: this.worker !== null,
        onAuthFailure: (reason) => {
          this.authState.markUnauthenticated(reason);
        },
      },
    );
    // ...
  }
}

export class JazzClient {
  updateAuth(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = resolveClientSessionSync({
      appId: this.context.appId,
      jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
    });
    this.streamController.updateAuth();
  }
}
```

- [ ] **Step 5: Export the auth-state API**

```ts
export type { AuthFailureReason, AuthState } from "./auth-state.js";
export {
  createDb,
  Db,
  type DbConfig,
  type QueryBuilder,
  type QueryOptions,
  type TableProxy,
} from "./db.js";
```

- [ ] **Step 6: Run the auth-state tests to verify they pass**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/auth-state.test.ts src/runtime/db.auth-state.test.ts src/runtime/sync-transport.test.ts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/jazz-tools/src/runtime/auth-state.ts packages/jazz-tools/src/runtime/auth-state.test.ts packages/jazz-tools/src/runtime/db.auth-state.test.ts packages/jazz-tools/src/runtime/db.ts packages/jazz-tools/src/runtime/client.ts packages/jazz-tools/src/runtime/client-session.ts packages/jazz-tools/src/runtime/index.ts
git commit -m "feat: add db auth state and jwt refresh api"
```

### Task 4: Forward Worker Auth Loss and Resume After `updateAuth(...)`

**Files:**

- Modify: `packages/jazz-tools/src/worker/worker-protocol.ts:75-81`
- Modify: `packages/jazz-tools/src/worker/worker-protocol.ts:185-193`
- Modify: `packages/jazz-tools/src/runtime/worker-bridge.ts:70-211`
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:58-80`
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:390-480`
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:581-594`
- Modify: `packages/jazz-tools/src/runtime/worker-bridge.race-harness.test.ts:1-260`
- Modify: `packages/jazz-tools/tests/browser/support.ts:255-270`
- Modify: `packages/jazz-tools/tests/browser/worker-bridge.test.ts:174-260`

- [ ] **Step 1: Write the failing worker tests**

```ts
export async function createJwtSyncedDb(
  ctx: TestCleanup,
  label: string,
  jwtToken: string,
): Promise<Db> {
  const { appId, serverUrl, adminSecret } = await getTestingServerInfo();
  return ctx.track(
    await createDb({
      appId,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
      serverUrl,
      jwtToken,
      adminSecret,
    }),
  );
}

it("WB-U05 forwards auth-failed from worker to main thread", async () => {
  const worker = new FakeWorker();
  const { runtime } = createRuntimeHarness();
  const bridge = new WorkerBridge(worker as unknown as Worker, runtime);
  const onAuthFailure = vi.fn();

  bridge.onAuthFailure(onAuthFailure);
  const initPromise = bridge.init(makeBridgeOptions());
  worker.script.completeInit("worker-client-5");
  await initPromise;

  worker.emitToMain({ type: "auth-failed", reason: "expired" });

  expect(onAuthFailure).toHaveBeenCalledWith("expired");
});

it("pauses worker reconnects until db.updateAuth is called", async () => {
  const expiredJwt = await getTestingServerJwtForUser("alice", {
    exp: Math.floor(Date.now() / 1000) - 60,
  });
  const freshJwt = await getTestingServerJwtForUser("alice", {
    role: "writer",
  });

  const db = await createJwtSyncedDb(ctx, "auth-loss", expiredJwt);
  const authStates: AuthState[] = [];
  const stop = db.onAuthChanged((state) => authStates.push(state));

  await waitForCondition(
    async () =>
      authStates.some((state) => state.status === "unauthenticated" && state.reason === "expired"),
    12_000,
    "expected auth loss",
  );

  db.insert(todos, { title: "queued while signed out", done: false });
  db.updateAuth(freshJwt);

  await waitForQuery(db, allTodos, (rows) =>
    rows.some((row) => row.title === "queued while signed out"),
  );
  stop();
});
```

- [ ] **Step 2: Run the worker tests to verify they fail**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/worker-bridge.race-harness.test.ts`

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.browser.ts tests/browser/worker-bridge.test.ts`

Expected: FAIL because the worker protocol has no auth-failure message and the worker still schedules reconnects forever on `401`.

- [ ] **Step 3: Add a worker-to-main auth-failure message**

```ts
export interface WorkerAuthFailedMessage {
  type: "auth-failed";
  reason: AuthFailureReason;
}

export type WorkerToMainMessage =
  | ReadyMessage
  | InitOkMessage
  | SyncToMainMessage
  | PeerSyncToMainMessage
  | WorkerAuthFailedMessage
  | ErrorMessage
  | ShutdownOkMessage
  | DebugSchemaStateOkMessage
  | DebugSeedLiveSchemaOkMessage;
```

- [ ] **Step 4: Pause worker reconnects on auth failure and resume only on `update-auth`**

```ts
let authPaused = false;

const serverPayloadBatcher = new ServerPayloadBatcher(async (payloads) => {
  if (!activeServerUrl || authPaused) return;
  try {
    await sendSyncPayloadBatch(activeServerUrl, payloads, {
      jwtToken,
      localAuthMode,
      localAuthToken,
      adminSecret,
      clientId: serverClientId,
      pathPrefix: activeServerPathPrefix,
    }, "[worker] ");
  } catch (error) {
    if (error instanceof SyncAuthError) {
      authPaused = true;
      detachServer();
      self.postMessage({ type: "auth-failed", reason: error.reason } satisfies WorkerToMainMessage);
      return;
    }

    detachServer();
    scheduleReconnect();
  }
});

async function connectStream(): Promise<void> {
  if (streamConnecting || !activeServerUrl || isShuttingDown || authPaused) return;
  // ...
  if (!response.ok) {
    const authError = await readSyncAuthError(response);
    if (authError) {
      authPaused = true;
      detachServer();
      self.postMessage({ type: "auth-failed", reason: authError.reason } satisfies WorkerToMainMessage);
      streamConnecting = false;
      return;
    }
    detachServer();
    streamConnecting = false;
    scheduleReconnect();
    return;
  }
}

case "update-auth":
  jwtToken = msg.jwtToken;
  localAuthMode = msg.localAuthMode;
  localAuthToken = msg.localAuthToken;
  authPaused = false;
  if (streamAbortController) {
    streamAbortController.abort();
    streamAbortController = null;
  }
  detachServer();
  if (activeServerUrl && !isShuttingDown) {
    scheduleReconnect();
  }
  break;
```

- [ ] **Step 5: Surface worker auth failures to `Db`**

```ts
export class WorkerBridge {
  private authFailureListener: ((reason: AuthFailureReason) => void) | null = null;

  constructor(worker: Worker, runtime: Runtime) {
    // ...
    this.worker.onmessage = (event: MessageEvent<WorkerToMainMessage>) => {
      const msg = event.data;
      if (msg.type === "auth-failed") {
        this.authFailureListener?.(msg.reason);
        return;
      }
      if (msg.type === "sync") {
        for (const payload of msg.payload) {
          this.runtime.onSyncMessageReceived(payload);
        }
      } else if (msg.type === "peer-sync") {
        this.state.peerSyncListener?.({
          peerId: msg.peerId,
          term: msg.term,
          payload: msg.payload,
        });
      }
    };
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.authFailureListener = listener;
  }
}

// In Db.attachWorkerBridge(...)
bridge.onAuthFailure((reason) => {
  this.authState.markUnauthenticated(reason);
});
```

- [ ] **Step 6: Run the worker tests to verify they pass**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/worker-bridge.race-harness.test.ts`

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.browser.ts tests/browser/worker-bridge.test.ts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/jazz-tools/src/worker/worker-protocol.ts packages/jazz-tools/src/runtime/worker-bridge.ts packages/jazz-tools/src/worker/jazz-worker.ts packages/jazz-tools/src/runtime/worker-bridge.race-harness.test.ts packages/jazz-tools/tests/browser/support.ts packages/jazz-tools/tests/browser/worker-bridge.test.ts
git commit -m "feat: pause worker sync on auth loss"
```

### Task 5: Make Framework Session State Reactive and Stop Recreating the Client on JWT Refresh

**Files:**

- Create: `packages/jazz-tools/src/react-core/provider.test.tsx`
- Modify: `packages/jazz-tools/src/react/create-jazz-client.ts:1-39`
- Modify: `packages/jazz-tools/src/vue/create-jazz-client.ts:1-39`
- Modify: `packages/jazz-tools/src/svelte/create-jazz-client.ts:1-39`
- Modify: `packages/jazz-tools/src/react-native/create-jazz-client.ts:1-38`
- Modify: `packages/jazz-tools/src/react-core/provider.tsx:1-165`
- Modify: `packages/jazz-tools/src/vue/provider.ts:16-123`
- Modify: `packages/jazz-tools/src/svelte/JazzSvelteProvider.svelte:1-50`
- Modify: `packages/jazz-tools/src/react/create-jazz-client.test.ts`
- Modify: `packages/jazz-tools/src/vue/create-jazz-client.test.ts`
- Modify: `packages/jazz-tools/src/svelte/create-jazz-client.test.ts`
- Modify: `packages/jazz-tools/src/react-native/create-jazz-client.test.ts`

- [ ] **Step 1: Write the failing wrapper and provider tests**

```tsx
it("reuses the client for jwt-only config changes", async () => {
  const listeners = new Set<(state: AuthState) => void>();
  let authState: AuthState = {
    status: "authenticated",
    transport: "bearer",
    session: { user_id: "alice", claims: { auth_mode: "external" } },
  };

  const db = {
    getAuthState: () => authState,
    onAuthChanged: (listener: (state: AuthState) => void) => {
      listeners.add(listener);
      listener(authState);
      return () => listeners.delete(listener);
    },
    updateAuth: vi.fn((jwtToken?: string) => {
      authState = jwtToken
        ? {
            status: "authenticated",
            transport: "bearer",
            session: { user_id: "alice", claims: { auth_mode: "external", token: jwtToken } },
          }
        : { status: "unauthenticated", reason: "missing", session: null };
      for (const listener of listeners) listener(authState);
    }),
  };

  const createJazzClient = vi.fn().mockResolvedValue({
    db,
    manager: { init: vi.fn(), shutdown: vi.fn() },
    session: null,
    shutdown: vi.fn(),
  });

  function Probe() {
    const session = useSession();
    return <div data-user={session?.user_id ?? "none"} />;
  }

  const root = createRoot(document.createElement("div"));
  await act(async () => {
    root.render(
      <JazzProvider
        config={{ appId: "app-1", jwtToken: "jwt-1" }}
        createJazzClient={createJazzClient}
      >
        <Probe />
      </JazzProvider>,
    );
  });

  await act(async () => {
    root.render(
      <JazzProvider
        config={{ appId: "app-1", jwtToken: "jwt-2" }}
        createJazzClient={createJazzClient}
      >
        <Probe />
      </JazzProvider>,
    );
  });

  expect(createJazzClient).toHaveBeenCalledTimes(1);
  expect(db.updateAuth).toHaveBeenCalledWith("jwt-2");
});
```

- [ ] **Step 2: Run the framework tests to verify they fail**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/react-core/provider.test.tsx src/react/create-jazz-client.test.ts src/vue/create-jazz-client.test.ts src/react-native/create-jazz-client.test.ts`

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.svelte.ts src/svelte/create-jazz-client.test.ts`

Expected: FAIL because wrappers still call `resolveClientSession(...)` directly and the React provider still keys its cache by the full config JSON, including `jwtToken`.

- [ ] **Step 3: Stop decoding session separately in framework wrappers**

```ts
async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  const resolvedConfig = resolveLocalAuthDefaults(config);
  const db = await createDb(resolvedConfig);

  const manager = new SubscriptionsOrchestrator({ appId: resolvedConfig.appId }, db);
  await manager.init();

  return {
    db,
    session: db.getAuthState().session,
    manager,
    async shutdown() {
      await manager.shutdown();
      await db.shutdown();
    },
  };
}
```

- [ ] **Step 4: Keep provider session state subscribed to `db.onAuthChanged(...)` and exclude `jwtToken` from the cache key**

```tsx
function stableClientConfigKey(config: DbConfig): string {
  const { jwtToken: _jwtToken, ...stableConfig } = config;
  return JSON.stringify(stableConfig);
}

export function JazzClientProvider({ client, children }: JazzClientProviderProps) {
  const [session, setSession] = useState(() => client.db.getAuthState().session);

  useEffect(() => {
    return client.db.onAuthChanged((state) => {
      setSession(state.session);
    });
  }, [client]);

  return <JazzContext.Provider value={{ ...client, session }}>{children}</JazzContext.Provider>;
}

export function JazzProvider({ config, fallback, children, createJazzClient }: JazzProviderProps) {
  const configKey = stableClientConfigKey(config);
  const [client, setClient] = useState<CoreJazzClient | null>(null);
  const [error, setError] = useState<unknown>(null);

  useEffect(() => {
    let active = true;
    const pendingClient = acquireClient<CoreJazzClient>(configKey, config, createJazzClient);

    void pendingClient.then(
      (resolved) => {
        if (!active) return;
        resolved.db.updateAuth(config.jwtToken);
        setClient(resolved);
      },
      (reason) => {
        if (!active) return;
        setError(reason);
      },
    );

    return () => {
      active = false;
      releaseClient(configKey);
    };
  }, [config, configKey, createJazzClient]);
  // ...
}
```

- [ ] **Step 5: Mirror the same subscription pattern in Vue and Svelte providers**

```ts
// Vue
let stopAuthSync: (() => void) | null = null;

Promise.resolve(nextClient).then((client) => {
  stopAuthSync?.();
  stopAuthSync = client.db.onAuthChanged((state) => {
    clientRef.value = {
      ...client,
      session: state.session,
    };
  });
});

onUnmounted(() => {
  stopAuthSync?.();
  stopAuthSync = null;
  // existing shutdown path
});
```

```svelte
let stopAuthSync: (() => void) | null = null;

Promise.resolve(client)
  .then((c) => {
    if (cancelled) {
      c.shutdown();
      return;
    }
    resolvedClient = c;
    ctx.db = c.db;
    ctx.manager = c.manager;
    stopAuthSync = c.db.onAuthChanged((state) => {
      ctx.session = state.session;
    });
  })

onDestroy(() => {
  stopAuthSync?.();
  stopAuthSync = null;
  if (resolvedClient) {
    void resolvedClient.shutdown();
  }
});
```

- [ ] **Step 6: Run the framework tests to verify they pass**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/react-core/provider.test.tsx src/react/create-jazz-client.test.ts src/vue/create-jazz-client.test.ts src/react-native/create-jazz-client.test.ts`

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.svelte.ts src/svelte/create-jazz-client.test.ts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/jazz-tools/src/react/create-jazz-client.ts packages/jazz-tools/src/vue/create-jazz-client.ts packages/jazz-tools/src/svelte/create-jazz-client.ts packages/jazz-tools/src/react-native/create-jazz-client.ts packages/jazz-tools/src/react-core/provider.tsx packages/jazz-tools/src/react-core/provider.test.tsx packages/jazz-tools/src/vue/provider.ts packages/jazz-tools/src/svelte/JazzSvelteProvider.svelte packages/jazz-tools/src/react/create-jazz-client.test.ts packages/jazz-tools/src/vue/create-jazz-client.test.ts packages/jazz-tools/src/svelte/create-jazz-client.test.ts packages/jazz-tools/src/react-native/create-jazz-client.test.ts
git commit -m "feat: keep framework session state in sync with db auth"
```

### Task 6: Mirror the Contract in `jazz-cloud-server`

**Files:**

- Modify: `crates/jazz-cloud-server/src/server.rs:3037-3185`
- Modify: `crates/jazz-cloud-server/src/server.rs:3759-3788`
- Modify: `crates/jazz-cloud-server/src/server.rs:4616-4620`
- Modify: `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts:1196-1222`

- [ ] **Step 1: Write the failing cloud integration test**

```ts
function signJwt(
  sub: string,
  secret: string,
  options?: { principalId?: string; exp?: number },
): string {
  const now = Math.floor(Date.now() / 1000);
  return jwt.sign(
    {
      sub,
      exp: options?.exp ?? now + 3600,
      ...(options?.principalId ? { jazz_principal_id: options.principalId } : {}),
    },
    secret,
    { algorithm: "HS256" },
  );
}

it("surfaces expired cloud JWTs as SyncAuthError", async () => {
  const jwks = await JwksServer.start(JWT_SECRET);
  const dataRoot = allocTempDir("jazz-ts-cloud-server-auth-state-");
  const server = await startCloudServer({ dataRoot });

  try {
    const app = await createApp(server.baseUrl, jwks.url);
    const pathPrefix = `/apps/${app.app_id}`;
    const expiredJwt = signJwt("expired-user", JWT_SECRET, {
      exp: Math.floor(Date.now() / 1000) - 60,
    });

    await expect(
      sendSyncPayload(
        server.baseUrl,
        JSON.stringify(makeSyncPayload()),
        false,
        { jwtToken: expiredJwt, pathPrefix },
        "[expired] ",
      ),
    ).rejects.toMatchObject({
      name: "SyncAuthError",
      reason: "expired",
    });
  } finally {
    await stopProcess(server.child);
    await jwks.stop();
  }
}, 30000);
```

- [ ] **Step 2: Run the cloud integration test to verify it fails**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/cloud-server.integration.test.ts`

Expected: FAIL because the cloud server still returns the old generic unauthorized body.

- [ ] **Step 3: Reuse the same structured unauthenticated response in the cloud server**

```rust
async fn validate_jwt_with_jwks(
    state: &ServerState,
    app_id: AppId,
    app_config: &AppConfig,
    token: &str,
) -> Result<VerifiedJwt, UnauthenticatedResponse> {
    if app_config.jwks_endpoint.trim().is_empty() {
        return Err(UnauthenticatedResponse::disabled(
            "JWT auth is not enabled for this app",
        ));
    }

    let verified = /* existing signature verification */;
    ensure_token_not_expired(&verified)
        .map_err(|_| UnauthenticatedResponse::expired("JWT has expired"))?;
    Ok(verified)
}

match extract_session(&headers, app_id, &cfg, &state).await {
    Ok(Some(session)) => Some(session),
    Ok(None) => {
        return (
            StatusCode::UNAUTHORIZED,
            Json(UnauthenticatedResponse::missing(
                "session required for sync. provide JWT or backend secret",
            )),
        )
            .into_response();
    }
    Err(error) => {
        return (StatusCode::UNAUTHORIZED, Json(error)).into_response();
    }
}
```

- [ ] **Step 4: Run the cloud integration test to verify it passes**

Run: `cd packages/jazz-tools && pnpm exec vitest run --config vitest.config.ts src/runtime/cloud-server.integration.test.ts`

Run: `cargo test -p jazz-cloud-server`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-cloud-server/src/server.rs packages/jazz-tools/src/runtime/cloud-server.integration.test.ts
git commit -m "feat: mirror auth loss responses in cloud server"
```

## Final Verification

- [ ] Run: `cargo test -p jazz-tools`
- [ ] Run: `cargo test -p jazz-cloud-server`
- [ ] Run: `cd packages/jazz-tools && pnpm test`
- [ ] Run: `cd packages/jazz-tools && pnpm test:browser`
- [ ] Run: `pnpm build`

Expected: PASS.

## Coverage Check

- Structured unauthenticated `401` bodies for `/sync` and `/events`: covered by Task 1 and Task 6.
- Response-driven auth loss, no silent reconnect loop: covered by Task 2 and Task 4.
- Public `Db` auth API (`updateAuth`, `getAuthState`, `onAuthChanged`): covered by Task 3.
- Last known session preserved during auth loss: covered by Task 3 and Task 5.
- Same-principal renewal, no principal hot-swap: covered by Task 3.
- `JazzProvider` should not recreate the DB on JWT-only changes: covered by Task 5.
- Browser worker recovery after renewal: covered by Task 4.
- Local auth behavior remains intact when bearer auth is not used: preserved because Task 3 re-derives local auth when no JWT is present and does not change local-auth transport logic.
