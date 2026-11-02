# @garden-co/local-first-auth

Sign users in before they sign up. The device keeps a random secret; the secret
derives an Ed25519 key and a stable user ID (the _subject_); the client signs a
short-lived token that any backend can verify with no auth server, no JWKS and
no network call. Works offline, and the same secret restores the same identity
on another device through a recovery phrase or a passkey.

Jazz uses this package for its guests, but nothing in it depends on Jazz.

```ts
import { identityOf, signToken } from "@garden-co/local-first-auth";
import { browserSecretStore } from "@garden-co/local-first-auth/browser";

const store = browserSecretStore({ scope: "my-app" });
const secret = await store.getOrCreate();

identityOf(secret); // { issuer: "urn:jazz:local-first", subject: "<uuid>", publicKey }

await fetch("/api/notes", {
  headers: {
    authorization: `Bearer ${signToken(secret, { audience: "https://app.example" })}`,
  },
});
```

On the server:

```ts
import { verifyRequest } from "@garden-co/local-first-auth/server";

const who = verifyRequest(request, { audience: "https://app.example" });
// null: no local-first bearer, try your other sign-in methods
// { issuer, subject, publicKey }: a verified local-first user
// throws LocalFirstTokenError: a local-first bearer that is invalid
```

`verifyRequest` takes a Fetch API `Request`, or anything with a Fetch `Headers`
object as `headers`. For Node's `IncomingMessage`, read the `authorization`
header yourself and call `verifyToken`.

## Entry points

| Import                        | What                                                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `@garden-co/local-first-auth` | `generateSecret`, `parseSecret`, `identityOf`, `signToken`, `verifyToken`, `createSecretStore`, `secretStorageKey` |
| `/browser`                    | `browserSecretStore({ scope?, profile? })` on localStorage                                                         |
| `/server`                     | `verifyRequest(request, { audience })`                                                                             |
| `/recovery-phrase`            | `RecoveryPhrase.fromSecret` / `toSecret` (24-word BIP39)                                                           |
| `/passkey`                    | `BrowserPasskeyBackup`: wrap the secret in a passkey                                                               |

`createSecretStore` takes any `{ getItem, setItem, removeItem }` storage, sync
or async, so React Native and Expo apps pass their secure store.

## Formats

The formats are fixed and versioned, and match Jazz's Rust core byte for byte
(`test-vectors.json` is checked by both test suites):

- Secret: `jazz-auth-v1:` + 43 unpadded base64url characters (32 random bytes).
- Signing key: `SHA-512("jazz-auth-sign-v1" || secret bytes)[0..32]` as an Ed25519 secret key.
- Subject: UUIDv5 of the public key in the namespace `"jazz-auth-key-v1"` (ASCII bytes).
- Token: compact JWT, header `{"alg":"EdDSA","typ":"JWT"}`, claims
  `{ iss: "urn:jazz:local-first", sub, aud, jazz_pub_key, iat, exp }`.
  Verification checks the strict Ed25519 signature, that `jazz_pub_key`
  derives `sub`, the `aud`, `iat` at most 60 s ahead, `exp` not passed,
  and a lifetime of at most 1 h unless configured. Unknown claims are rejected.
- Audience: both sides normalize it like Jazz's Rust core
  (`normalizeAudience`). A UUID in any common form becomes its lowercase
  hyphenated form; any other string becomes UUIDv5(DNS namespace, audience).
  The audience is required and must be non-empty.

## Threat model

- **Identities are self-asserted.** Anyone can create as many as they like, at
  no cost. A verified token proves "the same device as before", not a vetted
  person. Rate-limit by subject and IP, and gate anything valuable behind a
  real sign-up.
- **Tokens are bearer tokens.** They have no `jti` and are not bound to a
  request, so a leaked token replays until it expires, for up to 1 h by
  default. Send them only over TLS, keep them out of logs and URLs, and keep
  lifetimes short.
- **The secret is the account.** Whoever holds it is that user, on every
  device, with no revocation. `browserSecretStore` keeps it in plaintext
  `localStorage`, so any script on the origin (XSS) can read it. On React
  Native and Expo, pass a secure store such as `expo-secure-store`, never
  AsyncStorage. Offer a recovery phrase or passkey backup before anything
  valuable is tied to it.
- **Clocks.** `iat` may be up to 60 s in the future. `exp` gets no leeway, and
  a token is rejected from the second it expires.

## Scope

`scope` is any string that keeps one app's users apart from another's on the
same storage: an app ID, an endpoint, or nothing when the storage belongs to
one app. The library never interprets it; it only derives the storage key
(`secretStorageKey(scope, profile)`, a fixed format). The token audience is
separate and always passed to `signToken`.

Subscribing to a store (`subscribe`) creates a secret on first use, which is
the point: users are signed in before they sign up. `clear()` signs out, and
subscribers then see no secret until the app calls `getOrCreate()` again,
which creates a new identity.

Jazz passes its app ID as the scope, so a secret saved by any earlier Jazz
version is found again under the same key and the user keeps their identity.
