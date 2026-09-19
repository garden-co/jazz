# Inspector session integration contract (issue #3156)

Implementation contract under development; this is not a receipt of deployed
Cloud support. Cloud must implement the tenant-manager adapter below before
production end-to-end acceptance. Main uses wire version 3; current Cloud wire
version 2 is not compatible with this rollout.

## Authority and capabilities

Inspector credentials are separate from account JWTs, backend secrets and root
admin secrets. Default `inspector:read` grants protected application data and
catalogue inspection. Explicit `inspector:edit` adds application data mutation;
explicit `inspector:admin` adds schema/permission/migration publication. Neither
capability admits account administration, shutdown, catalogue bootstrap,
replication, authority publications or client-selected policy impersonation.
An Edge may forward a server-selected protected Inspector query to its Core;
that does not authorize a browser to delegate arbitrary subjects.

Access tokens last at most 900 seconds, bind issuer, exact app, the
`jazz-inspector-v1` audience, operator and capabilities, and stay in browser memory.
Expiry terminates existing WebSocket authorization, including idle/live query
connections. Renewal creates a new token and connection. Stateless tokens have
no individual revocation: logout clears local authority immediately; stolen
copies/access removal remain usable until expiry (at most 900 seconds). Root
key rotation rejects existing tokens on new admission. ServerState is immutable;
rotation replaces/restarts the server, and deployment draining closes old sockets.
If an old process is left serving, its sockets retain authority only until their
original expiry (at most 900 seconds). No refresh token is
issued to Inspector.

## Jazz exchange

`POST /apps/{appId}/admin/inspector/sessions`, authenticated only by
`X-Jazz-Admin-Secret`, with JSON `{ "operator": "opaque-operator-id",
"capabilities": ["inspector:read"], "expiresIn": 900 }`. Tenant manager supplies an opaque
operator identity after checking current administrative permission for this
specific app. Self-hosted operators may exchange their root secret directly,
then discard it. Credentials never appear in URLs, logs or error messages.
The response is `Cache-Control: no-store` and contains `accessToken`,
`expiresAt` (Unix seconds), `appId` and granted `capabilities`. Invalid root
credentials are 401, malformed/unsupported capabilities are 400. The exchange
never accepts an Inspector token or ordinary account JWT as root authority.

## Tenant-manager browser adapter (required Cloud work)

Use a configured HTTPS dashboard origin, never an origin supplied by a callback.
Inspector opens a dashboard authorization popup; the dashboard's first-party
HttpOnly Secure login cookie authenticates this navigation. This does not depend
on cross-site cookies or iframe storage access.

`GET /inspector/authorize?app_id=...&redirect_uri=...&state=...&code_challenge=...&code_challenge_method=S256&capabilities=inspector%3Aread`
checks login and current app administration permission. The exact redirect URI
must be pre-registered (no wildcards or prefix matching). Dashboard Open Inspector
must first navigate to Inspector with connection metadata, so Inspector creates
the browser-bound state/verifier before authorization. The dashboard returns a
single-use code, valid for at most 60 seconds, bound to app/operator/redirect URI,
S256 challenge and requested capabilities. Callback contains only `code` and
`state`; Inspector removes them from history immediately and checks state and
popup source/origin before token exchange.

`POST /inspector/token` JSON `{ "code": "...", "code_verifier": "...",
"redirect_uri": "..." }` consumes the bound authorization code exactly once,
rechecks operator permission, and calls the Jazz exchange server-to-server. Its
response contains the Jazz token response plus canonical `serverUrl`. It must
not return root credentials. Use `Cache-Control: no-store`, exact configured
Inspector CORS origins, no credentials, and reject unsupported content types.
Failures return generic JSON `{ "error": "invalid_grant" }` (400) or
`{ "error": "access_denied" }` (403), never tokens, codes or raw upstream errors.
Validate the code/verifier/redirect binding atomically before consuming it;
failed checks never yield a token. Rate-limit exchange attempts.

For default same-site Cloud hosting, renewal is automatic through the authenticated
dashboard session. `GET /inspector/session` with `credentials: include` returns
`{ "csrfToken": "opaque-session-bound-token" }`, `Cache-Control: no-store`.
`POST /inspector/renew` with credentials, JSON `{ "appId": "...",
"capabilities": ["inspector:read"] }`, and `X-Inspector-CSRF` rechecks the
current dashboard login and current app administration permission, then returns
the same token response (including canonical `serverUrl`). For cross-origin
requests both endpoints require an exact allowed Inspector Origin, `Vary: Origin`,
exact `Access-Control-Allow-Origin`, `Access-Control-Allow-Credentials: true`, and
no-store. Same-origin GET requests may omit Origin: allow that case only with
an authenticated session, `Sec-Fetch-Site: same-origin` and trusted-host/SOP
checks; never return wildcard CORS. POST renewal still requires exact Origin and
the session-bound CSRF header. Preflight allows only GET/POST and Content-Type/X-Inspector-CSRF.
The CSRF token is bound to the login session, remains memory-only, and is not
itself authority without the HttpOnly cookie. Missing login returns 401
`{ "error": "login_required" }`; revoked permission or invalid CSRF returns 403
`{ "error": "access_denied" }`. Neither denial returns a replacement token.

Renew before expiry and replace the client connection. Reload retains only
app/server metadata and may reestablish via the same authenticated renewal path.
Cross-site cookie blocking, expired login or popup blocking shows explicit sign-in
UI; timer-based renewal must not attempt a browser-blocked popup. Explicit sign-in
repeats authorization with fresh PKCE/state. Denied renewal closes the active
client and clears its token; it must never fall back to root/admin admission.
Inspector logout closes the local client, clears token/state, and suppresses
automatic renewal until the operator explicitly reconnects. It does not invalidate
the dashboard's login or individually revoke a stateless access token.

Login cookies use Secure, HttpOnly and SameSite=Lax or stricter compatible
first-party settings. Dashboard login/permission-changing actions retain their
existing CSRF checks. Authorization validates redirect allowlist, state and PKCE;
code redemption is a non-cookie JSON endpoint with strict CORS. Cookie-based
renewal additionally validates the session-bound CSRF header and exact Origin. Do not allow
`*` with credentials, redirects to arbitrary caller origins, bearer access tokens
in callback URLs, or credential/error body logging.

## CLI handoff and rollout

CLI uses configured root authority to exchange a short-lived session, then
serves a loopback-only one-shot browser handoff. The launch fragment contains a
random 32-byte one-time bootstrap code, app metadata and loopback address. This
code is distinct from a bearer access/root token: the challenge endpoint consumes
it once while binding the browser-generated S256 proof, and the token endpoint
requires the matching verifier. Inspector immediately scrubs the fragment and
never persists the bootstrap code. Exact Origin and loopback Host checks remain
mandatory, but Origin alone is not proof that a caller owns the CLI launch.
Root/access tokens must not be placed in the launch URL. Only connection metadata may be
persisted by Inspector. CLI listener closes after successful redemption or a
short timeout.

Deploy matching Jazz server and client artifacts before enabling dashboard UI.
Old servers must fail closed on the new admission path; no automatic fallback to
admin-secret or account JWT admission. Direct manual admin mode remains an
explicit compatibility fallback. Verify currentCloud wire version separately;
this change does not deploy or negotiate away the wire2/wire3 difference.

## Token encoding and trust boundary

Tokens use the established `jsonwebtoken` HS256 implementation with fixed-algorithm
verification and zero expiry leeway. The signing key is SHA-256 of the fixed byte
prefix `jazz-inspector-signing-key-v1\0` followed by the configured admin-secret
bytes. A fixed prefix and one final variable component make this unambiguous;
this key is never reused for account JWT verification. Claims are `iss` =
`jazz-inspector-v1:<canonical-app-id>`, `aud` = `jazz-inspector-v1`, `app` = canonical
app ID, `sub` = opaque operator, `iat`/`exp` = Unix seconds, and `capabilities` =
unique recognized strings including `inspector:read`. Root credentials must have
normal production secret entropy. Token signing does not strengthen weak roots.

HTTP carries `X-Jazz-Inspector-Token`. The JSON WebSocket prelude carries
`auth.inspector_token` with canonical SYSTEM `peer_identity`; mixed credentials,
bootstrap and relay requests are rejected. This optional prelude field does not
change the binary sync-message encoding or its wire3 version. The server chooses
a distinct Inspector ingest mode; the client never receives ordinary trusted
backend/authority admission. The operator claim supplies accountability/cap
partitioning, not application-account impersonation.

Expiry also bounds the authenticated pre-Hello wait and queued runtime admission.
A cancelled runtime-open response owns cleanup until its caller accepts the
session, preventing timed-out upgrades from leaking server sessions. A frame
accepted before expiry may finish its durable transaction; expiry is not a
rollback mechanism. Dropping the outbound stream stops later frames in that
already-queued batch and suppresses all subsequent protected output.

## HTTP examples for tenant-manager implementation

Exchange (server-to-server):

```http
POST /apps/<canonical-app-id>/admin/inspector/sessions
Content-Type: application/json
X-Jazz-Admin-Secret: <server-held-root>

{"operator":"opaque-operator-id","capabilities":["inspector:read"],"expiresIn":900}
```

Successful exchange/renewal responses use the following shape; Cloud token and
renewal responses additionally include the app's canonical `serverUrl`:

```json
{
  "accessToken": "<short-lived-token>",
  "expiresAt": 1800000900,
  "appId": "<canonical-app-id>",
  "capabilities": ["inspector:read"],
  "serverUrl": "https://sync.example"
}
```

Jazz exchange uses `401 {"error":"invalid_admin_credential"}` for missing or
invalid root authority, `400 {"error":"invalid_request"}` for unsupported scopes,
operator/TTL bounds or malformed JSON request shapes, and
`500 {"error":"exchange_failed"}` for signing failure. These responses and tokens
use `Cache-Control: no-store`; errors never echo submitted credential material.
Wrong app routes return 404. Inspector HTTP admission uses
`401 {"error":"invalid_inspector_credential"}` for signature/claim/expiry failure,
`403 {"error":"inspector_capability_denied"}` for an absent required capability,
and `403 {"error":"inspector_operation_denied"}` for operations outside the
Inspector allowlist. Mixing root and Inspector headers is 400
`{"error":"ambiguous_credentials"}`. Existing catalogue-handler payload errors
retain their existing response contract after successful capability admission.

The browser adapter and CLI request shapes, configured dashboard origin and
callback setup are detailed in [Inspector session client](inspector-session-client.md).
