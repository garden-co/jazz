# Accounts and contexts

The core assigns each exact issuer/subject identity to one permanent account.
Linking adds a fresh identity to that account; it never merges existing accounts.
A context receives an opaque `AccountHandle`, and its acting identity stays fixed.

```ts
import { createAccountManager, createJazzClient } from "jazz-tools/client";

const config = { appId: "my-app", serverUrl: "https://core.example" };
const accounts = await createAccountManager(config);
const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
let client: Awaited<ReturnType<typeof createJazzClient>> | undefined = await createJazzClient({
  ...config,
  account,
});

async function signIn(getToken: () => Promise<string>) {
  // General graceful shutdown, before linking and outside the next context.
  // A sync-barrier failure leaves the existing context usable; a later
  // teardown failure does not. Release other shared holders first.
  await client?.shutdown({ waitForSync: true });
  client = undefined;
  try {
    await accounts.linkJWT({ getToken });
  } finally {
    // A failed link preserves selection unless another action logged out.
    const selected = accounts.getLoggedIn();
    if (selected) client = await createJazzClient({ ...config, account: selected });
  }
}
```

`registerJWT` explicitly creates an account for an unassigned external identity.
`loginJWT` requires an existing active assignment. Merely obtaining or decoding a
JWT does neither. Refresh callbacks must keep the exact issuer and subject.
Supply `getToken` on the account operation; the shared account context refreshes
credentials on expiry across every framework. There is no provider-level
`onJWTExpired` callback. A callback that takes longer than 30 seconds fails the
attempt; a later refresh may retry, and a late result cannot replace credentials.

For passphrase/passkey backup, explicitly call `exportLocalFirstSecret(handle)`.
Restore with `accounts.restoreLocalFirst(secret)` outside a context after normal
graceful shutdown. Export requires a live local-first handle; external handles
and logged-out handles cannot expose a recovery root. Secrets never appear in
`useAccountState` snapshots.

The handle retains its registry authority independently of context transport.
Omit `serverUrl` when opening a local-only context; supplying it enables the
configured upstream and must match the handle's authority. Local storage is
scoped by registry, application, environment, and account. Linked identities
share that durable root but always open distinct live authorization sessions.

Browser managers restore local selection from localStorage. Other hosts supply
an `AccountStore`. Local keys remain retained after logout; provider tokens are
never written to this store. Each server-rendering request owns its own manager.

React and Vue expose `useAccountState(accounts)`, Solid exposes
`createAccountState(accounts)`, and Svelte exposes `accountState(accounts)`.
These observe the same `{ account, pending, error }` state machine. Actions stay
on the manager. Render a context only when an account handle is available; show
account-operation errors beside the app's login controls.

`$createdBy` and `$updatedBy` are structured native records:

```ts
{ account: "account-uuid", identity: { issuer: "https://issuer.example", subject: "alice" } }
```

Compare `.account` for account ownership. Comparing the whole author also checks
issuer and subject. Linking therefore shares account-owned access without
pretending that the new identity authored earlier rows.

`AccountStore.update` must perform an atomic read/transform/write across all
managers using that store. Browser defaults use Web Locks with localStorage;
server/native hosts supply the equivalent transaction or process-wide lock.
Automatic `createJazzSession({ initial: "local-first" })` startup performs its
first-root ensure inside that transaction, then adopts the durable winner before
opening a client. Explicit `createLocalFirst`, recovery, and logout operations
keep their existing selection semantics.
Browser managers (`createAccountManager` with its default browser store) also
retain the selected external account's non-secret assignment,
`{ account, issuer, subject }`, never a token. Managers given an `AccountStore`
(Node, SSR, React Native) do not, and keep the ordinary teardown-and-reopen
login. On the next browser start `getLoggedIn()` returns that account at once,
and a context opens its local data before the provider answers. Until a
provider JWT arrives the context presents no credential upstream and local
policies see an empty `session.claims`: rules that depend on provider claims
deny until revalidation. In a shared browser worker, another tab's credential
for the same account may sync this tab's writes sooner. Logging in again as the
same identity (`loginJWT`, `loginOrRegisterJWT`, or the auth provider
connection) revalidates that account in place, without closing the context. A
different provider subject is detected before any registry call and switches
accounts as before, reusing the provider token already fetched; a provider that
hydrates signed out logs the retained account out. If the registry rejects the
revalidation, the context stays open with the error, and `session.retry()`
revalidates again rather than reopening the unconfirmed account. A logout that
starts while a revalidation is in flight wins: the account is never confirmed.
Writes made before revalidation stay in that account's local store and sync
once that identity is admitted again.
The shared helper merges retained roots inside that transaction, so a stale
manager cannot erase another manager's offline key. Selection follows the
last successful operation; external provider credentials are never persisted.
