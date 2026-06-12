# E2EE Shared Key Management & Encryption Algorithms — Design

Status: approved design, pre-implementation.

## Goal

Per-column end-to-end encryption for Jazz2: developers mark columns as encrypted in
the schema, values are encrypted client-side before sync, and the server only ever
sees ciphertext. This document covers shared key management (DX + persistence) and
encryption algorithms. ACL remains the
job of row policies; E2EE protects data from the service provider, not from other
authorized members.

Scope for v1: LocalFirst Auth users only.

## Decisions at a glance

| Question                | Decision                                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Key scope               | Per **space row** — a row of a table marked `.encryptionSpace()` owns one shared key                                      |
| Key handoff             | Sealed at invite time via an explicit `shareKey` call                                                                     |
| Revocation              | Policy-only in v1 (no rotation); envelope carries `key_id` so epochs can be added later                                   |
| Public-key directory    | App-managed column in the app's own schema; framework never queries it                                                    |
| Schema DX               | `s.string().encrypted("refColumn")` — each encrypted column names the ref to its space                                    |
| Sealed-key persistence  | Framework-managed companion table `<table>$keys` per space table                                                          |
| Crypto venue            | Rust core (WASM / NAPI / RN) — one implementation, synchronous in-engine                                                  |
| Symmetric cipher        | XChaCha20-Poly1305                                                                                                        |
| Sealing                 | HPKE (RFC 9180), DHKEM(X25519) + HKDF-SHA256 + ChaCha20-Poly1305                                                          |
| Post-quantum            | Not in v1; algorithm-id byte in every envelope/sealed blob keeps the upgrade additive                                     |
| LoFi auth key algorithm | Unchanged (ed25519 already ships in the WASM engine)                                                                      |
| Rust API                | Same core feature surfaced on `JazzClient` (`share_key` / `unshare_key` / `key_holders`); schema via `TableSchemaBuilder` |

## 1. Identity & private keys

No new secret material and no new storage. The 32-byte LocalFirst Auth secret is the
single root. Alongside the existing `jazz-auth-sign-v1` ed25519 derivation
(`crates/jazz-tools/src/identity.rs`), add one domain-separated derivation:

- `jazz-e2ee-seal-v1` → X25519 keypair (the user's encryption identity).

Consequences:

- Everything is re-derivable from the auth secret, so existing backup/recovery paths
  (passphrase, passkey backup) automatically cover E2EE keys.
- Recovery = auth recovery. Lose the secret, lose the keys.
- `x25519-dalek` reuses the `curve25519-dalek` backend already pulled in by
  `ed25519-dalek`, so the bundle delta is minimal.

The public encryption key is exposed to apps as `db.e2ee.publicKey()`. The **app**
publishes it into its own users/profiles table (app-managed directory). The framework
never reads app tables for keys: `shareKey` takes the recipient's public key as an
argument, and the app looks it up with an ordinary query. Directory keys are trusted
on first use in v1; key transparency / verification is out of scope.

## 2. Schema DX

```ts
const schema = {
  users: s.table({
    name: s.string(),
    e2eeKey: s.string().optional(), // app-managed directory, ordinary column
  }),
  projects: s
    .table({
      name: s.string(),
    })
    .encryptionSpace(), // rows of this table can own a shared key
  todos: s.table({
    title: s.string().encrypted("projectId"),
    done: s.boolean(), // plaintext, queryable
    projectId: s.ref("projects"),
  }),
};
```

Rules:

- `.encrypted(refColumn)` is valid only when `refColumn` is a **non-nullable** ref to
  an `.encryptionSpace()` table; anything else is a schema-build error.
- TS types are unchanged for reads/writes: `todo.title` is `string` (modulo the
  `Locked` state, §6). Under the hood `encrypted()` composes with the existing
  `ColumnTransform` mechanism in `dsl.ts`: serialize per declared type → encrypt →
  store as BYTEA envelope.
- Encrypted columns are excluded from the query/where DSL, indexes, and policy
  expressions — enforced both at the type level and at schema-build time. Filtering
  on encrypted values happens client-side; this trade-off is accepted.
- Mixed scopes in one table are allowed (different encrypted columns may name
  different space refs).

## 3. Persistence model

Marking a table with `.encryptionSpace()` generates a framework-managed companion table
(conceptually `projects$keys`):

| Column                 | Meaning                                                                  |
| ---------------------- | ------------------------------------------------------------------------ |
| `space_id`             | ref → the space row                                                      |
| `key_id`               | UUID identifying the key (v1: one per space; future epochs slot in here) |
| `recipient_user_id`    | who this copy is for (policies, member listing)                          |
| `recipient_public_key` | the X25519 key the copy was sealed to (what recipients match on)         |
| `sealed_key`           | the 32-byte space key, sealed to the recipient (HPKE blob)               |

Properties:

- **Space creation is atomic with key creation.** When a client inserts a row into a
  `.encryptionSpace()` table, the runtime generates the space key and writes the creator's
  sealed copy in the same batch. A space never persists keyless.
- **Sharing = inserting one row.** Concurrent invites are concurrent inserts; no LWW
  conflict is possible (this is why a JSON map on the space row was rejected).
- **Policies (v1):** rows are world-readable (sealed copies are useless without the
  recipient's private key, and this keeps sync trivial). Insert is allowed for any
  authenticated user; delete is likewise open to any authenticated user in v1
  ("own rows plus the space creator" needs the `created_by` permissions work — a
  malicious delete is a recoverable annoyance, since re-sharing heals it).
  Open insert permits junk key rows — an annoyance, not a confidentiality loss, since
  clients only trust copies they can unseal (§7). Tightening insert to "members only"
  depends on the `created_by` permissions work
  (`specs/todo/a_mvp/commit_author_principal_created_by_permissions.md`) and can land
  later without a format change.
- **Updates are disallowed entirely:** key rows are immutable. Share = insert,
  revoke = delete. There is no legitimate mutation of `sealed_key` or the recipient
  columns, and allowing one would let an attacker repoint an existing row at a
  different key (e.g. swap `recipient_public_key`). If key epochs land later,
  re-sealing is a new insert under a new `key_id`, never an update.
- **Ciphertext envelope** per value, stored as BYTEA:
  `[alg_id: 1][key_id: 16][nonce: 24][ciphertext+tag]`.
  Every value records which key (and algorithm) encrypted it, so adding key epochs or
  new ciphers later is additive, not a migration.

## 4. Runtime API & data flow

Space tables get typed key methods; reads and writes stay transparent.

```ts
// publish my key (app-managed directory)
await db.users.update(me, { e2eeKey: db.e2ee.publicKey() });

// invite: app fetches the recipient's key from its own table, then
await db.projects.shareKey(projectId, recipient.e2eeKey);

// revoke (v1 = policy-only): delete their sealed copy; app updates its own ACL rows
await db.projects.unshareKey(projectId, recipient.e2eeKey);

// member listing for UI: [{ recipient_user_id, recipient_public_key }]
await db.projects.keyHolders(projectId);

// writes & reads: transparent
await db.todos.insert({ title: "secret", projectId }); // encrypts under projectId's key
const todo = await db.todos.get(id); // decrypts; todo.title is string
```

`shareKey` / `unshareKey` / `keyHolders` exist only on tables marked
`.encryptionSpace()` (typed-app layer shaping).

**Who can share.** Anyone who already holds the space key — sealing requires the
plaintext key, so only current key holders can produce a valid share, and a
non-member's `shareKey` fails locally with `E2EEKeyUnavailable`. This gate is
cryptographic and cannot be bypassed via the server (which never has the key). In
v1 there is no admin tier: possession = full re-share rights, and "only owners can
invite" is an app-level convention enforced by the app's own policies on row
delivery, not by the key layer. Damage from a rogue share is bounded because the
key alone reads nothing — the recipient still needs the rows, which the app's
ordinary policies control.

**Key cache.** The companion table syncs like any other table. On first touch of a
space, the runtime unseals the space key with the derived private key and holds it in
an in-memory cache — never persisted unsealed; re-established after restart from
synced rows + the auth secret. Mutations and decryption hit the cache synchronously.

## 5. Rust API

The crypto and the key cache live in the Rust core (`runtime_core`), beneath both
the TS bindings and the Rust `JazzClient` — Rust is not a port of the TS feature;
the TS API is sugar over the same core operations.

**Schema** (mirrors §2, same build-time rules):

```rust
let schema = SchemaBuilder::new()
    .table(
        TableSchemaBuilder::new("projects")
            .column("name", ColumnType::Text)
            .encryption_space(),
    )
    .table(
        TableSchemaBuilder::new("todos")
            .encrypted_column("title", ColumnType::Text, "projectId")
            .column("done", ColumnType::Boolean)
            .fk_column("projectId", "projects"),
    )
    .build();
```

**Client** (same semantics as the TS methods in §4):

```rust
// encryption identity, derived from the LoFi seed the client connected with
let my_key: E2eePublicKey = client.e2ee_public_key()?;

// key management
client.share_key("projects", project_id, &recipient_key)?;    // insert into projects$keys
client.unshare_key("projects", project_id, &recipient_key)?;  // delete their sealed copy
let holders = client.key_holders("projects", project_id).await?;

// reads & writes stay transparent: insert/update take plaintext Values,
// queries return plaintext Values when the key is available
client.insert("todos", values)?;  // encrypts "title" under projectId's key
```

- `share_key` / `unshare_key` / `key_holders` are string-table-addressed like the
  rest of `JazzClient` (there is no typed-app layer in Rust); calling them on a
  table not marked as an encryption space is a runtime error.
- **Locked state:** the core `Value` enum gains a `Locked` variant. This is the
  source of truth the TS `Locked` sentinel (§6) maps to; bindings translate it.
- **Errors:** `JazzError::E2EEKeyUnavailable` mirrors the TS error (§7).
- A Rust client is just another key holder: a trusted backend worker that has been
  `share_key`-ed into a space can decrypt. Access follows possession of a sealed key
  copy — not the language, binding, or tier. (Conversely: sharing into a
  server-resident worker re-introduces a party that can read plaintext; that is an
  app-level trust decision, not a protocol property.)

## 6. Locked values

If a client holds a row but not its key (key row not yet synced, or never shared),
the plaintext cannot exist. Encrypted columns are typed `T | Locked`, where `Locked`
is an exported sentinel with an `isLocked(v)` guard. This forces app code to handle
the state honestly instead of receiving an ambiguous `null`. In practice the state is
rare — row policies usually prevent non-members from receiving rows at all — but
"key hasn't synced yet" is a genuine local-first state and the types should say so.

## 7. Error handling

- **Write without key** (`insert`/`update` touching an encrypted column when the
  space key is unavailable): typed `E2EEKeyUnavailable` error thrown before the
  mutation applies. Never silently writes plaintext or garbage.
- **`shareKey` with a malformed public key:** validation error at call time.
- **Nullable/missing space ref:** impossible by construction (§2).
- **Sign-out:** the in-memory key cache is wiped with the auth secret.
- **Bogus key rows** (open-insert hole, §3): unseal failure ⇒ row ignored, warning
  surfaced in dev tools. Trust comes only from successful unseal.
- **Space row deletion:** companion rows follow existing ref/cascade semantics.

## 8. Encryption algorithms

All crypto executes in the **Rust core** (WASM in the browser, native via NAPI/RN):
one implementation across platforms, synchronous inside the engine where mutations
and queries already run, and plaintext never crosses the JS boundary at rest. The
browser engine already ships `ed25519-dalek` inside a multi-MB WASM bundle (9.3 MB
raw / ~3.0 MB gzipped as currently built — see §9 for provenance and caveats), so
the estimated E2EE additions of tens of KB are ≲2% of even the compressed size —
changing the LoFi auth key algorithm to keep the bundle size in check is
unnecessary.

**Value encryption: XChaCha20-Poly1305** (`chacha20poly1305` crate, pure Rust).

- Fast in WASM (no AES hardware there); constant-time by construction, unlike
  software AES.
- The 24-byte random nonce is load-bearing: many offline clients encrypt under the
  same space key concurrently, so any counter- or coordination-based nonce scheme is
  wrong for this system. XChaCha makes independent random nonces safe.
- **AAD binds context:** each value is authenticated against
  `(table, column, row_id, key_id)`, so ciphertext cannot be grafted between rows or
  columns by anyone with database access.
- Space keys: 32 random bytes from the OS CSPRNG (`getrandom`, already a dependency).

**Key sealing: HPKE (RFC 9180)** with `DHKEM(X25519) + HKDF-SHA256 +
ChaCha20-Poly1305` (`hpke` crate). Standards-track, ~80-byte sealed blobs, and its
KEM slot is exactly where the post-quantum upgrade lands: a future hybrid
(X-Wing / X25519+ML-KEM) is a new KEM id in the sealed blob, with old shares still
unsealing. Fallback if the `hpke` crate disappoints on audit or size: `crypto_box`
sealed boxes (libsodium-compatible), with hand-rolled agility.

Sealing happens only at share/open time (human-scale frequency); only correctness and
agility matter there, not throughput.

**Post-quantum stance:** not in v1. The symmetric layer (256-bit keys) is already
PQ-resistant; only the sealing step is exposed to harvest-now-decrypt-later. The
`alg_id` byte in every envelope and sealed blob makes the hybrid upgrade additive.

## 9. Cost summary

| Concern                          | Number                                                                                     | Provenance                                                                                                                                                                                                                                                                                      |
| -------------------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current WASM engine size         | 9.3 MB raw, ~3.0 MB gzip −9                                                                | Measured 2026-06-12 on `crates/jazz-wasm/pkg/jazz_wasm_bg.wasm`, built by `wasm-pack build --release`. Caveat: the release profile passes `wasm-opt -O -g`, which keeps the name section for profilers — a size-trimmed production build would be smaller, so the baseline itself is not final. |
| E2EE bundle delta                | Estimate: tens of KB (≲2% even against the 3.0 MB compressed size)                         | Typical compiled sizes of `chacha20poly1305` + `hpke`; the X25519 curve backend is already present via `ed25519-dalek`. **Must be measured during implementation**: build the engine with and without the E2EE feature, compare stripped + gzipped sizes.                                       |
| Value encrypt/decrypt throughput | Estimate: order of 100 MB/s+ in WASM; per-value cost µs-scale for byte-to-KB column values | Published ChaCha20-Poly1305 software benchmarks, not measured in this engine. Validate with an in-repo benchmark before relying on transparent decryption in hot query paths.                                                                                                                   |
| Per-recipient key row            | ~80-byte sealed blob (v1); ~1.1 KB if the PQ hybrid lands                                  | RFC 9180 sizes for DHKEM(X25519) (enc 32 B + ct ≈ 48 B); ML-KEM-768 ciphertext ≈ 1088 B.                                                                                                                                                                                                        |
| LoFi auth key algorithm          | Unchanged                                                                                  | ed25519 is already in the engine; no bundle pressure to switch.                                                                                                                                                                                                                                 |
| Server-side cost                 | Encrypted columns: no indexing, filtering, sorting, or compression                         | By design (§2).                                                                                                                                                                                                                                                                                 |

## 10. Testing

Black-box integration tests through the public API only (project convention: build
schemas/permissions via the public API, no JSON-like definitions):

1. **Round-trip:** client A creates a space, inserts encrypted rows, reads them back
   decrypted.
2. **Server blindness:** inspect persisted state via a backend/storage context —
   encrypted columns contain only ciphertext envelopes; plaintext appears nowhere.
3. **Sharing:** A shares with B's published key; B syncs and reads plaintext.
4. **Locked state:** C receives rows (policy allows) but no key — sees `Locked`,
   `isLocked` returns true.
5. **Revocation (v1 semantics):** A unshares B; policies stop new rows reaching B.
6. **Write-without-key:** C's insert into the space fails with `E2EEKeyUnavailable`.
7. **Concurrent invites:** two clients invite different users concurrently; both
   sealed rows survive sync.
8. **Restart persistence:** client restarts; key is re-established from the auth
   secret + synced rows; no re-share needed.
9. **Context binding:** ciphertext copied between rows/columns fails authentication.
10. **Rust client parity:** the same scenarios exercised through `JazzClient`
    (shared scenario definitions where practical), since the Rust API is a
    first-class consumer of the feature.
11. **Key-row immutability:** an `UPDATE` against a `$keys` row is rejected by
    policy, from both bindings.

## 11. Known v1 limitations (accepted)

- **Policy-only revocation:** a removed member who colludes with (or compromises) the
  server can decrypt ciphertext written after removal, since the key is not rotated.
  Key epochs are the designed-for upgrade path (`key_id` in every envelope).
- **TOFU directory:** a malicious app/server that swaps a published public key can
  intercept future shares. Key verification/transparency is out of scope for v1.
- **Open insert and delete on key tables** until `created_by` permissions land.
- **No share hierarchy:** every key holder can re-share (§4); restricting invites to
  owners/admins is app-level convention in v1, upgradeable alongside `created_by`.
- **Encrypted columns are server-opaque:** no indexes, filters, sorts, compression,
  or lens transforms; clients filter locally.
- **LocalFirst Auth users only.** Documenting community-run LoFi auth setups is a
  follow-up documentation task.
