# auth-workos-chat

A small React + Vite example that shows how to integrate [WorkOS AuthKit](https://workos.com/docs/user-management) with Jazz.

What it demonstrates:

- Using `@workos-inc/authkit-react` to handle the full OAuth / SSO sign-in flow
- Pointing the Jazz sync server at WorkOS's hosted JWKS endpoint — no local auth server needed
- Resolving ordinary WorkOS access tokens into opaque Jazz account handles
- Gracefully closing the old client before provider login/logout, with same-identity token refresh owned by the account manager
- Falling back to local-first auth when no WorkOS session exists
- Role-based UI gating derived from JWT claims (`admin` posts to Announcements; `member` posts to the general chat), with generic-chat message ownership enforced via `$createdBy` in `permissions.ts`

There is no local auth server in this example. WorkOS issues and signs all tokens;
the JWKS is fetched from `https://api.workos.com/sso/jwks/<clientId>`.

## Prerequisites

You need a [WorkOS account](https://workos.com/docs/authkit/client-only) with a configured AuthKit environment and a client ID.

## Setup

### 1. Configure WorkOS

1. Configure the `WORKOS_CLIENT_ID` in `constants.ts` with your WorkOS client ID.
2. Set `WORKOS_JWT_ISSUER` in `.env.local` to the exact `iss` claim WorkOS places in its access tokens:

   ```bash
   WORKOS_JWT_ISSUER=https://api.workos.com/
   ```

   Include the trailing slash when the token has one, and use the exact custom AuthKit domain when you have configured one. This is the server's static expected issuer; never derive it from an incoming token.

3. Configure redirect URI in the WorkOS dashboard to `http://127.0.0.1:5173`.
4. Under Authentication > Sessions > JWT Template add the following snippet to expose the Organization's role and an audience to the JWT claims:

```
{
  "role": "{{ organization_membership.role }}",
  "aud": "client_your_authkit_client_id"
}
```

Replace the `aud` value with the same AuthKit Client ID configured in
`constants.ts`. WorkOS access tokens do not include an audience claim by default,
so this template adds one. The quotes around the role template produce a JSON
string claim. Jazz verifies both the configured issuer and this audience before
admitting the token.

### 2. Start the Vite app

```bash
pnpm dev
```

The Jazz Vite plugin (`jazzPlugin` in `vite.config.ts`) automatically spawns a local Jazz sync
server pointed at the WorkOS JWKS URL, pushes the schema catalogue, and exposes
`VITE_JAZZ_APP_ID` / `VITE_JAZZ_SERVER_URL` to the app. The JWKS URL is assembled from
`WORKOS_CLIENT_ID` at config time, while the issuer comes from the configured
expected value:

```ts
const { WORKOS_JWT_ISSUER } = loadEnv(mode, process.cwd(), "");

jazzPlugin({
  server: {
    jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`,
    jwtIssuer: WORKOS_JWT_ISSUER,
    jwtAudience: WORKOS_CLIENT_ID,
  },
});
```

The sync server fetches that URL to verify every incoming Jazz JWT signature.

Open the URL Vite prints (default `http://127.0.0.1:5173`). Click **Continue with WorkOS** to be
redirected to the hosted login page; after sign-in WorkOS redirects back with an access token.

## How the WorkOS integration works

### Provider setup — `src/App.tsx`

The root wraps the whole app in `AuthKitProvider`, which manages the WorkOS session and OAuth
redirect lifecycle. The example enables `devMode` explicitly so the documented
`127.0.0.1` setup persists the refresh token locally and can restore the SPA session after the
redirect back from WorkOS:

```tsx
export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}
```

### Account admission — `src/App.tsx`

`JazzApp` gets an ordinary WorkOS JWT through `getAccessToken()` and calls
`accounts.loginJWT({ getToken })`. Login resolves an existing Jazz account; it
never registers an identity implicitly. The UI offers an explicit registration
action for a new provider identity. Without a WorkOS session, the app restores
or creates a local-first account.

The app creates a client from that opaque handle and provides the existing
client with `JazzClientProvider`. Client creation and cleanup run after React
commits, including Strict Mode cancellation. Before provider redirects it uses
ordinary `shutdown({ waitForSync: true })`; a failed synchronization barrier
preserves the existing client. Same-identity expiry refresh calls WorkOS through
the account credential's `getToken` callback.

Ownership policies compare `$createdBy.account` with `session.user.account`.
The provider's issuer and subject remain available under `session.user.identity`.
