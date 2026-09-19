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
`jazz-inspector` audience, operator and capabilities, and stay in browser memory.
Expiry terminates existing WebSocket authorization, including idle/live query
connections. Renewal creates a new token and connection. Stateless tokens have
no individual revocation: logout clears local authority immediately; stolen
copies/access removal remain usable until expiry (at most 900 seconds). Root
key rotation invalidates all tokens derived from that key. No refresh token is
issued to Inspector.

## Jazz exchange

Proposed `POST /apps/{appId}/admin/inspector/sessions`, authenticated only by
`X-Jazz-Admin-Secret`, with JSON `{ "operator": "opaque-operator-id",
"capabilities": ["inspector:read"] }`. Tenant manager supplies an opaque
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

`GET /inspector/authorize?app_id=...&redirect_uri=...&state=...&code_challenge=...&code_challenge_method=S256`
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

Renewal repeats first-party popup authorization and the same PKCE exchange,
rechecking permission, without a browser-held refresh credential. Reload may
retain app/server/dashboard metadata only, then repeat authorization. Popup
blocking/login requirements need visible user action; denied renewal clears the
active client and token. Logout closes the local client and clears token/state;
it does not silently log the operator back in or invalidate the dashboard login.
Dashboard logout is a separate dashboard action.

Login cookies use Secure, HttpOnly and SameSite=Lax or stricter compatible
first-party settings. Dashboard login/permission-changing actions retain their
existing CSRF checks. Authorization validates redirect allowlist, state and PKCE;
code redemption is a non-cookie JSON endpoint with strict CORS. Do not allow
`*` with credentials, redirects to arbitrary caller origins, bearer access tokens
in callback URLs, or credential/error body logging.

## CLI handoff and rollout

CLI uses configured root authority to exchange a short-lived session, then
serves a loopback-only one-shot browser handoff. The browser handoff must verify
its initiating Inspector origin and browser-generated challenge; root/access
tokens must not be placed in the launch URL. Only connection metadata may be
persisted by Inspector. CLI listener closes after successful redemption or a
short timeout.

Deploy matching Jazz server and client artifacts before enabling dashboard UI.
Old servers must fail closed on the new admission path; no automatic fallback to
admin-secret or account JWT admission. Direct manual admin mode remains an
explicit compatibility fallback. Verify currentCloud wire version separately;
this change does not deploy or negotiate away the wire2/wire3 difference.
