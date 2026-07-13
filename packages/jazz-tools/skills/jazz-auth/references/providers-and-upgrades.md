# Providers and identity upgrades

## External JWT verification

An external provider must issue signed JWTs and expose a JWKS endpoint, or the server must be
configured with one static public key. Configure exactly one of `jwksUrl` and `jwtPublicKey`.

For a self-hosted sync server:

```bash
pnpm exec jazz-tools server <appId> --jwks-url https://auth.example.com/api/auth/jwks
```

For a TypeScript backend that resolves caller sessions:

```ts
const context = createJazzContext({
  appId,
  app,
  permissions,
  driver: { type: "persistent", dataPath: "./data" },
  serverUrl,
  backendSecret,
  jwksUrl,
});
```

The browser passes the provider token as `jwtToken`; the server validates its signature and uses
`sub` as `session.user_id`. Keep signing keys and Jazz backend/admin secrets on trusted servers.

Use signed custom claims in permissions only after confirming the provider's exact claim names and
shape. Author the policy itself through `jazz-schema-evolution`.

## Local-first to external upgrade

Preserving identity requires the external account to adopt the existing Jazz ID:

1. Start with the user's current local-first `Db` and secret.
2. Generate a short-lived identity proof with `db.getLocalFirstIdentityProof(...)`.
3. Send the proof alongside sign-up credentials over an authenticated application endpoint.
4. Verify it on the server with `verifyLocalFirstIdentityProof` from `jazz-napi`, using the same
   audience as the client.
5. Store the proven Jazz ID on the external account.
6. Issue future JWTs with that ID as `sub`.
7. Recreate `Db` or `JazzProvider` with the external token or cookie session.

If a provider fixes `sub` to another identifier and cannot be configured, mint the Jazz-facing JWT
through a server-controlled issuer. Do not silently accept a new `sub`; Jazz will treat it as a
different user.

Keep the local-first recovery route until the external provider reliably recovers the same Jazz ID.

## Better Auth as a JWT provider

Enable Better Auth's JWT support, expose its JWKS route, and use the corresponding client plugin to
obtain a token. Point Jazz verification at that JWKS URL. For try-before-signup, add the identity
proof hook before Better Auth creates the user so the account is assigned the proven Jazz ID.

## Better Auth as a Jazz database consumer

The `jazz-tools/better-auth-adapter` integration is a separate concern from merely accepting Better
Auth JWTs:

1. Generate the Better Auth schema module with the installed Better Auth CLI.
2. Spread its tables into the app's table map and build one merged Jazz `app`.
3. Merge the generated deny-by-default Better Auth permissions with the application's permissions.
4. Validate the combined schema.
5. Create one server-side Jazz context.
6. Pass `db: () => context.asBackend(app)` and the merged schema to `jazzAdapter(...)`.
7. Keep Better Auth experimental joins disabled unless the installed adapter explicitly supports
   them.
8. Deploy schema, permissions, and any required migration through the normal Jazz workflow.

Inspect the installed adapter's compatibility and generated schema before enabling Better Auth
plugins that add tables or custom fields. Regenerate and validate after such changes.

## Provider-change checklist

- Confirm token issuer, verification key source, and server reachability.
- Confirm `sub` is stable and matches existing Jazz identity where required.
- Confirm token refresh does not switch principals.
- Confirm sign-out replaces the Jazz client and clears provider state.
- Confirm permissions consume signed claims rather than unsigned UI state.
- Test account upgrade on the original device and recovery/sign-in on another device.
