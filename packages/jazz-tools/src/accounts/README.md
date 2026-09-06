# Accounts and contexts

The core assigns each exact issuer/subject identity to one permanent account.
Linking adds a fresh identity to that account; it never merges existing accounts.
A context receives an opaque `AccountHandle`, and its acting identity stays fixed.

```ts
import { createAccountManager, createJazzClient } from "jazz-tools/client";

const config = { appId: "my-app", serverUrl: "https://core.example" };
const accounts = await createAccountManager(config);
const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
let client = await createJazzClient({ ...config, account });

async function signIn(getToken: () => Promise<string>) {
  // General graceful shutdown, before linking and outside the next context.
  // Failure here leaves the existing context and selection usable.
  await client.shutdown({ waitForSync: true });
  try {
    await accounts.linkJWT({ getToken });
  } finally {
    // A failed link preserves the old handle. A successful link selects the
    // new acting identity, on the same account.
    client = await createJazzClient({ ...config, account: accounts.getLoggedIn()! });
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
The shared helper merges retained roots inside that transaction, so a stale
manager cannot erase another manager's offline key. Selection follows the
last successful operation; external provider credentials are never persisted.
