# Authentication modes and lifecycle

Use the installed package's framework binding and types. The patterns below describe the current
public model; do not invent auth mutation APIs when a project uses another version.

## Choose one initial mode

| Mode          | Client configuration                              | Consequence                                                                                         |
| ------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Anonymous     | no `secret`, `jwtToken`, or `cookieSession`       | Reactive reads are possible; writes fail with `AnonymousWriteDeniedError` before policy evaluation. |
| Local-first   | `secret`                                          | Stable offline-capable identity derived from a device-held secret.                                  |
| External JWT  | `jwtToken`                                        | Server validates the token and uses its stable `sub` as `session.user_id`.                          |
| Cookie-backed | `cookieSession` plus an HttpOnly cookie transport | The cookie authenticates sync; the mirrored session drives local identity and permission state.     |

Do not combine `cookieSession` with `secret` or `jwtToken` on one client.

## Local-first identity

Use the framework's local-first auth helper or the platform's secret store:

- React, Vue, Solid, and Expo: `useLocalFirstAuth()`.
- Svelte: `LocalFirstAuth`.
- Browser TypeScript: `BrowserAuthSecretStore`.
- Expo: `ExpoAuthSecretStore`, backed by secure device storage.

The secret is a credential, not disposable cache data. The same secret always produces the same
Jazz ID. Anyone who obtains it can authenticate as that user, and losing it can make data owned by
that identity inaccessible.

For recovery:

- `RecoveryPhrase` from `jazz-tools/passphrase` encodes the same secret as 24 words. It is not a
  password or extra encryption layer.
- `BrowserPasskeyBackup` from `jazz-tools/passkey-backup` stores and releases the secret through a
  browser passkey. Keep another recovery route because passkey availability can be platform-bound.
- Restoring the secret must feed it back into the framework auth handle or secret store, then replace
  or reload the active client as required by that binding.

## External JWT identity

Jazz reads JWT `sub` verbatim as `session.user_id`. Use a stable provider user ID or a Jazz ID linked
to that account. Do not use an email address, mutable username, or session identifier.

Create the client with the current token:

```ts
const db = await createDb({ appId, serverUrl, jwtToken });
```

Refresh the same principal in place:

```ts
db.updateAuthToken(freshJwtForTheSameUser);
```

Recreate the client for sign-in, sign-out, or a principal change. Framework providers should be
keyed or otherwise replaced when their auth configuration changes. Calling
`db.updateAuthToken(null)` is not a logout or local-first fallback mechanism.

## Cookie-backed identity

`cookieSession` mirrors the already-resolved session into the Jazz client. The HttpOnly cookie
remains the actual transport credential.

```ts
const db = await createDb({
  appId,
  serverUrl,
  cookieSession: {
    user_id: "user_123",
    claims: { role: "member" },
    authMode: "external",
  },
});
```

Use `db.updateCookieSession(nextSession)` for claim or session changes affecting the same user.
Recreate the client when the user changes or signs out. Application servers must resolve cookies;
Jazz request auth does not parse arbitrary application cookies.

## Auth state and expiry

Read framework auth state or call `db.getAuthState()`. Subscribe outside bindings with
`db.onAuthChanged(listener)` and dispose the returned listener.

An auth state can retain its last-known `session` while carrying an `error` such as `expired`,
`missing`, `invalid`, or `disabled`. Check the error when deciding whether authenticated sync is
healthy. React can use `JazzProvider`'s `onJWTExpired` callback to serialize same-user refresh.

Backend-scoped wrappers created by `asBackend`, `forRequest`, or `forSession` do not own the shared
bearer token and are not the place to call `updateAuthToken(...)`.

## Logout and storage

Identity credentials and local database storage are separate:

| Operation                       | Client/runtime               | Browser OPFS database     | Local-first secret | Provider token or cookie |
| ------------------------------- | ---------------------------- | ------------------------- | ------------------ | ------------------------ |
| `db.logout()`                   | shuts down                   | preserved                 | preserved          | provider must clear      |
| `db.logout({ wipeData: true })` | shuts down                   | deleted for the namespace | preserved          | provider must clear      |
| `db.deleteClientStorage()`      | remains usable after reset   | deleted and reopened      | preserved          | preserved                |
| local-first `signOut()`         | provider/client flow changes | not inherently deleted    | cleared            | unrelated                |

`deleteClientStorage()` is browser worker-backed persistent storage only and coordinates the reset
across tabs. Do not present it as a universal runtime API.
