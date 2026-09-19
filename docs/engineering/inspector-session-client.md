# Inspector session client

The browser and CLI implement the adapter contract in
[inspector-sessions.md](inspector-sessions.md). This is an integration contract,
not a claim that Cloud has deployed the tenant-manager endpoints or wire version 3. Cloud currently uses wire version 2; enable this only with matching server and
client artifacts.

## Dashboard deployment

Build standalone Inspector with `VITE_INSPECTOR_DASHBOARD_ORIGIN` set to its exact
trusted dashboard origin. The origin is deployment configuration; neither
callback parameters nor local storage can select the credential exchange host.
Register the exact `<Inspector origin>/inspector/callback` redirect with tenant
manager and serve that path through the standalone HTML entry point.

Dashboard links to `<Inspector URL>#login=dashboard&appId=<encoded app id>`.
Inspector requests read access by default. Checkboxes explicitly request data
editing and catalogue administration. Without `inspector:edit`, the data grid
shows a read-only session and disables insert, delete and cell editing controls. The dashboard must verify each requested
capability against the current operator's app permission and may reduce the
requested set. Sign-in opens a first-party popup using fresh state and S256 PKCE.
The callback removes its one-time code from history immediately, then delivers it
to the initiating popup owner. Root secrets and bearer tokens never go in URLs.

One minute before expiry, Inspector obtains a login-bound CSRF token from
`GET /inspector/session` and calls `POST /inspector/renew` with cookies, JSON
`{appId, capabilities}` and `X-Inspector-CSRF`. Both endpoints require credentials and `Cache-Control: no-store`. Cross-origin
requests require the exact configured Inspector CORS origin. Same-origin GET
requests omit Origin in browsers: `/inspector/session` may admit an authenticated
request with `Sec-Fetch-Site: same-origin` under the trusted-host/SOP policy.
When Origin is present it must match the exact allowlist; POST renewal always
requires exact Origin plus the login-bound CSRF token.
Tenant manager verifies Origin, the session-bound CSRF token, and current app
permission. It exchanges root authority only server-to-server. Default same-site
deployments renew without popups. Cross-site/private browser cookie blocking,
login expiry or access denial clears the active view and requires an explicit
sign-in gesture; timers never open popups.

Expiry, logout and denied renewal close the in-memory client and clear its active
view. Pending older setup/renewal callbacks cannot install a stale client after
logout or replacement. Closing a client cannot erase information an operator has
already seen or copied. Only app connection metadata is retained in local storage;
there is no browser refresh credential. Reload attempts cookie-backed restoration
with read access, without a popup; blocked or denied restoration offers explicit
sign-in. Local logout preserves only a signed-out metadata flag so reload cannot
silently sign the operator back in. Dashboard logout
is separate from local Inspector logout.

## CLI

Set `JAZZ_ADMIN_SECRET` in the process environment, then run:

```sh
jazz-tools inspect my-app --server-url http://127.0.0.1:4200 --inspector-url https://inspector.example
```

`JAZZ_INSPECTOR_URL` and the existing Jazz server environment variables can supply
the URLs. `--edit` and `--admin` explicitly request additional capabilities. The
command prints a URL containing app/loopback metadata and a random one-time launch
code in the fragment. The code is not an access credential and is removed from
history immediately. Open it
within 60 seconds. The root secret is exchanged on the dedicated session route;
it is never sent to Inspector or printed. The loopback listener validates exact
Origin and Host, consumes the launch code to bind a single-use redemption code to
a browser-generated S256 challenge,
then requires the matching verifier before returning the session. Knowing the
listener port and spoofing Origin is insufficient without the launch code. A
local process able to read the CLI output/browser memory is outside this handoff
boundary. It closes after
redemption or timeout. The CLI issues no browser refresh authority; on expiry, run
the command again. This path has no fallback to account JWT/root-secret admission
when a server does not support Inspector sessions. Manual admin mode remains an
explicit separate compatibility mode.

## Focused receipts

From `packages/inspector`:

```sh
pnpm exec vitest run --config vitest.config.ts src/session
pnpm exec playwright test --config playwright.sessions.config.ts
```

The standalone Playwright contract uses a real browser, a synthetic cookie-backed
tenant-manager HTTP adapter and the production browser/CLI handoff code. It tests
popup PKCE, same-site cookie/CSRF renewal, permission removal and loopback proof
redemption without requiring native artifacts. It does not claim deployment or
native server admission coverage; server admission and full Inspector data views
remain covered by their respective native/correctness gates.

From `packages/jazz-tools`:

```sh
pnpm exec vitest run --config vitest.config.ts src/runtime/inspector-auth.test.ts src/dev/inspector-handoff.test.ts
```
