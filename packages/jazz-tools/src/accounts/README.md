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
The shared helper merges retained roots inside that transaction, so a stale
manager cannot erase another manager's offline key. Selection follows the
last successful operation; external provider credentials are never persisted.

New local-first roots retain private **generated-here provenance** and
first-founder eligibility in the same atomic write as the root. The versioned
`jazz-account-selection-v3` inventory preserves roots, selection and provenance,
and adds first-device claims scoped to the account root and application.
Managers sharing one atomic `AccountStore` cannot create competing automatic
offline founders. Logout invalidates the handle, not the durable claim.

Ordinary v1/v2 inventories remain readable and keep their roots and selection.
Migration preserves v2 generation provenance but does not infer original-device
ownership from it: these roots require online readiness for a new founder.
Candidate v2 inventories with a valid `founders` field retain their claims and
eligibility, including an empty claim list. Older writers refuse v3 rather than
discarding ownership. Independent stores still rely on Global authority.

With an authenticated application catalogue cached by the runtime host, a
newly eligible account can open an encrypted database and create its first
spaces offline without a separate initialisation call. Its original device
reserves ownership, durably journals the proposal, then binds the exact epoch
before publication. A missing or altered bound journal cannot be replaced;
the matching original journal can resume even through an imported account store.
These claims are not accepted membership. Device, account epoch, space roots and
explicit grants remain provisional until authority acceptance. The SDK journals
sealed envelopes and reserved transaction identities, not message or image payloads.
An explicit self grant is required for ordinary local reads and later writes:
possession of the author's sealed envelope alone is not membership.

Local durability acknowledges the original pending transaction, not eventual
authorisation. Restart checks those exact identities with the durable runtime
owner. A missing acknowledged transaction is corruption; an interrupted
unacknowledged reservation is never resubmitted under a replacement identity.
Rejection or known revocation disables dependent provisional key use. Unknown
explicit recipients produce retryable `e2ee_initialization_not_ready` before a
stream is consumed; requested recipients are never silently omitted.
