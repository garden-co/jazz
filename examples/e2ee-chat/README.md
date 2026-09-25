# Encrypted chat with image uploads

A local-first React chat using Jazz's schema-owned end-to-end encryption. This is a separate example: `examples/chat-react` is unchanged. The layout, theme, fonts, stateless Button/Item widgets, message metadata/bubbles and bottom composer are reused or trimmed from that example. The composer uses plain text, not HTML. There are no public rooms, automatic joining, avatars, reactions, canvases or application debug globals.

## Run locally

From the repository root, with the maintained Jazz native/WASM and package builds available:

```sh
pnpm install
pnpm --filter e2ee-chat dev --host 127.0.0.1 --port 5183 --strictPort
```

Open `http://127.0.0.1:5183`. The standard Jazz Vite plugin starts the local server and deploys `schema.ts` and `permissions.ts`. Use a secure browser context (localhost or HTTPS) with IndexedDB and Web Locks enabled. For a separately deployed server, configure `VITE_JAZZ_APP_ID`, `VITE_JAZZ_SERVER_URL` and optionally `VITE_JAZZ_ENV` (default `dev`) before building. To use that server during development, also set `JAZZ_ADMIN_SECRET` so the Vite plugin can deploy the schema and permissions instead of starting a local server. Keep this secret on the development host; never put it in a `VITE_` variable.

1. Open two independent browser profiles or normal/private windows, not merely two tabs sharing an account. Each profile persists its own local-first account and encryption keys.
2. Wait for **Your account** in both windows. Copy the recipient's account ID.
3. In the owner window, choose **New encrypted chat**, enter **Recipient account ID**, then **Share chat**. Wait for **Chat shared**.
4. Send that page's URL to the recipient. The `?chat=<UUID>` parameter selects a room; it is not an invite token and grants no access.
5. Write a message and/or select **Image attachment**, then **Send**. The recipient can view the authenticated image and choose **Download image**. Either member can send. Reload preserves the account and keys.

PNG, JPEG, WebP and GIF are accepted, up to **10 MiB per image**, excluding empty files. SVG and other active/document formats are refused. These MIME/size limits are browser UX checks, not server-enforced security or cryptographic claims. The browser's `File.stream()` feeds `Db.insertStreaming`; the app never converts uploads to DataURLs or buffers the entire upload before sending. SDK reads currently materialise the decrypted file. Object URLs are revoked when replaced or unmounted.

## Work offline

Account and encryption setup runs automatically when the app opens. There is no
initialisation button or offline-mode switch. The first visit needs a connection
to obtain the authenticated application catalogue. Keep the same browser profile:
the cached catalogue, Jazz database and separate private key store are all needed
for persistent offline use.

After that first visit, loss of the Jazz server connection does not prevent
creating your own chat or saving text and images locally. The app itself must
still be available: this example does not install a service worker or promise
that an uncached page can load without its web server.

Local durability is not server acceptance. Pending work is kept across browser
restart and reconciled when the server returns, using the original transaction,
epoch and ciphertext. It does not rerun an upload stream. Sharing with another
account still requires accepted recipient keys and server-authorised membership;
an unknown recipient is not silently enrolled or granted access.

If a server-backed query fails during disconnection, choose **Reload chat** after
the server returns. Those queries do not automatically retry a terminal error.
Reload reconnects and opens new subscriptions; it does not initialise another
account, recreate the room or resubmit the image. This example does not promise
automatic confirmation-status recovery without a reload.

## Ownership, encryption and failure handling

- `chats.ownerId` is immutable. An immutable `chatOwners` row binds that chat to its verified creator through declared chat and account references. The binding can only be inserted by that creator and cannot be updated or deleted. Root authorization requires the matching account/chat pair, preventing another account from forging an ownership binding or inserting a foreign duplicate encryption root for an existing room. Only the owner can add memberships, grant room keys or author an epoch successor.
- Members can read/send messages and reload retained keys; `senderId` must equal the verified account. Only the owner can publish device-key deliveries under this example's application policy, so later device-key redistribution requires the owner to be available. A non-owner recipient can message but cannot publish device deliveries. Package-owned device policies are untouched; this example does not provide multi-device administration UI.
- `text`, `filename`, `mimeType` and `payload` are encrypted under the chat scope. Account IDs, room IDs, sender IDs, membership, timestamps, ciphertext sizes and access patterns remain observable. Authenticated nonmembers can read encryption control metadata. This is not metadata privacy.
- Creating a room inserts the chat, ownership binding and owner's membership in one exclusive transaction. Because `chats` is the registered encryption scope, Jazz prepares its root and creator grant in that transaction, even before the first message. The binding is plaintext control metadata, readable by authenticated accounts; it adds no private information beyond the already-visible ownership identifiers.
- Sharing has **two distinct accepted steps**: upsert the deterministic UUIDv5 membership row, then grant the current epoch. It is not atomic. If the recipient has not opened the app yet, the grant can fail after membership acceptance. Ask them to open it and explicitly retry **Share chat**. The same chat/account pair uses the same membership ID; repeated grants are idempotent for the current epoch. UUIDv5 is row identity, not custom cryptography.
- The composer releases the draft after `wait({ tier: "local" })` and shows **Saved on this device**, then observes the original Global completion separately. **Accepted by server** requires that exact write handle's Global success. After restart, **Available from server** means an ID-only `ReadTier.Remote` query observed the row from Core at the Global read tier, without pending local writes; it neither downloads the message payload nor recovers that write handle's Global receipt. Other local rows show **Local · acceptance unconfirmed**. None of these labels promises the recipient has read the message. A failure does **not** prove rollback: data may already be accepted while key delivery needs maintenance. Use **Check encryption**, inspect the history, and **Reload chat** if a previous query failed. Failure before Local completion retains the draft; the app never automatically replays a source. Resending an ambiguously accepted message can duplicate it.
- Files and metadata are published together through the SDK. Uploads support the SDK's root Bytea write path, not nested streaming or partial-byte updates. Stale-epoch uploads can be rejected and need explicit retry after maintenance.

Account selection uses the public `createAccountManager` browser persistence. Encryption state uses a separate IndexedDB `AccountStore`, keyed by registry URL, environment and account ID. Each update reads, synchronously transforms and writes within one strict-durability readwrite transaction, so concurrent connections/tabs cannot overwrite one another's key updates. The store does no cryptography and resolves only after transaction completion.

These are browser-profile secrets. XSS or someone with access to the profile can access them. Clearing browser storage can lose the only keys. This example does not implement multi-device approval, recovery, member removal or revocation UI. The SDK exposes those operations; revocation does not erase previously decrypted data, retained history or ciphertext already observed by a server. This example makes no GA, security-audit or React Native parity claim.

## Checks and selectors

```sh
pnpm --filter e2ee-chat typecheck
pnpm --filter e2ee-chat validate
pnpm --filter e2ee-chat build
pnpm --filter e2ee-chat test:unit
pnpm --filter e2ee-chat test:browser
pnpm --filter e2ee-chat test:e2e
```

The test scripts use the maintained correctness-consumer gate to select receipt-verified native and WASM artifacts for the whole fixture process tree. Prepare those artifacts through the repository's normal build workflow before running the scripts; do not bypass the gate or substitute a standalone server binary.

`test:unit` runs real-server owner/membership/encryption restrictions, atomic candidate-bundle denial, sharing retry and ciphertext-observer coverage. `test:browser` runs concurrent/reopened/aborted key-store operations against real Chromium IndexedDB.

`test:e2e` starts the actual Vite app at port 5183 and places the Jazz server behind a TCP gate. It closes existing connections and rejects new ones, including worker connections, while leaving Vite reachable. Through the UI, it creates a chat and selects a real PNG File while partitioned, verifies exact downloaded bytes, terminates Chromium and reopens the same persistent profile while still offline. It verifies the same account, message text, exact PNG bytes and **Local · acceptance unconfirmed** status after restart. It then unblocks the server, uses **Reload chat** to reconnect, observes **Available from server**, shares with a second automatically initialised account, verifies the recipient's exact image download and reply, reloads the recipient and checks outsider denial. Screenshots cover offline creation, process restart and recipient delivery; a trace is retained on failure. The test never injects an SDK client or mocks the server.

It also sends a non-decodable image with an allowed MIME type and verifies that the UI shows an unavailable-image message without a broken image or download link.

Automation uses `data-testid="account-id"`, `data-testid="chat-id"`, `data-testid="send-status"`, and the accessible names **New encrypted chat**, **Recipient account ID**, **Share chat**, **Message**, **Image attachment**, **Send**, **Download image**, **Check encryption** and **Reload chat**.
