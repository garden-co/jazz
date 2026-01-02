# Legacy Jazz (jazz.tools)

## What is Jazz?

Jazz is a distributed database that syncs across frontend, backend, and cloud infrastructure. It provides "local state that's instantly synced and stored in the cloud" - enabling developers to build multiplayer, offline-capable applications without years of custom infrastructure work.

## Core Problem It Solves

Building apps with shared state across devices and users typically requires reinventing complex infrastructure (as Figma, Notion, Linear did over years). Jazz provides this as a ready-made solution.

## Key Features

- **Offline-first**: Works offline, syncs when reconnected
- **Real-time multiplayer**: Collaborative editing with conflict resolution
- **End-to-end encryption**: Data encrypted on devices
- **Built-in auth & permissions**: User identity and access control included
- **File handling**: Upload, sync, and progressive loading of files/images

## Architecture Overview

### Tech Stack
- **TypeScript** for the main framework (jazz-tools)
- **Rust** for cryptographic primitives (compiled to WASM, NAPI, React Native bindings)
- Monorepo managed with pnpm and Turbo

### Main Packages

1. **cojson** - The core: cryptographic JSON data structures supporting transactions, permissions, and sync
2. **cojson-storage-*** - Storage adapters (SQLite, IndexedDB, Cloudflare Durable Objects)
3. **cojson-transport-ws** - WebSocket sync protocol
4. **jazz-tools** - High-level framework with React/Svelte/Node bindings

## Core Concepts

### CoValues (Collaborative Values)

The fundamental building block. Five types:

1. **CoMap** - Ordered key-value map with conflict-free updates
2. **CoList** - Ordered list with CRDT semantics
3. **CoStream** - Append-only event stream
4. **BinaryCoStream** - Binary data streaming (files)
5. **CoPlainText** - Collaborative text with character-level CRDT

### Identity Model

- **Account** - Tied to a user, has a profile, can have multiple agents
- **Agent** - Cryptographic identity (Ed25519 signer + X25519 sealer keypair)
- **Session** - Chain of transactions from one agent

### Groups (Permissions)

`Group` is a special CoMap that manages access control. This is a central and complex abstraction:

**Roles**: reader, writer, admin, manager, writeOnly (plus invite variants like readerInvite, writerInvite, etc.)

**Key characteristics**:
- **Early-bound**: Permissions are determined at creation time by the group that owns the CoValue
- **Rigid hierarchy**: Role inheritance is fixed (admin > manager > writer > reader > writeOnly)
- **Coupled to encryption**: The group manages encryption keys, and adding/removing members requires key rotation
- **Parent/child group extension**: Groups can extend other groups to inherit members, but this creates complex key revelation chains

**How it works internally**:
- Group is a CoMap storing `{accountID: role}` entries
- `readKey` points to the current encryption key
- Key revelations stored as `{keyID}_for_{accountOrAgentID}` (sealed secrets)
- Parent group references stored as `parent_{groupID}` with inheritance role
- When members are removed, key rotation propagates through child groups

**Pain points**:
- Adding/removing members requires key rotation (expensive, complex)
- Permission checks require traversing group hierarchy
- Key revelation chains are complex and error-prone
- Permissions are tightly coupled to encryption - you can't have one without the other
- All this complexity exists even when e2ee isn't needed

### Transactions

All changes are cryptographically signed transactions:
- Can be "private" (encrypted) or "trusting" (plaintext)
- Include timestamp, signing key, and changes
- Organized into sessions (chains of transactions from one author)

## Synchronization

### LocalNode

Represents a local view of loaded CoValues from one account's perspective:
- Manages CoValueCore instances
- Runs SyncManager for peer communication
- Connects to storage and network peers

### Sync Protocol (5 message types)

1. **load** - Request to load a CoValue with known state
2. **known** - Announce what transactions we have
3. **content** - Send new transactions
4. **done** - Signal completion
5. Disconnected error state

### Known State Compression

Each side tracks which transactions they've seen per session, avoiding retransmission.

### Priority-Based Queuing

- HIGH: Accounts, Groups
- MEDIUM: Most data
- LOW: Binary streams/files

## Cryptography

- **Signing**: Ed25519
- **Session Encryption**: XSalsa20 (stream cipher without authentication - hashing and signing already provide authentication)
- **Sealing** (key revelation): XSalsa20-Poly1305 (authenticated encryption)
- **Key Exchange**: X25519
- **Hashing**: BLAKE3
- **Encoding**: Base58, Base64URL

Multiple implementations: Pure JS (Noble.js), WASM, Node native (NAPI), React Native native.

## Developer Experience

Developers define schemas using CoValues:

```typescript
const Message = co.map({
  text: co.plainText(),
  image: co.optional(co.image()),
});

const Chat = co.list(Message);
```

These behave like reactive local JSON but sync automatically across all connected peers.
