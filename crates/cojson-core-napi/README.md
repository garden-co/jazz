# cojson-core-napi

A high-performance Node.js native addon for cryptographic operations, built with Rust and NAPI. This package provides essential cryptographic primitives for the Jazz framework, including hashing, encryption, digital signatures, and key management.

## Features

- **BLAKE3 Hashing**: Fast, secure cryptographic hashing with incremental support
- **Ed25519 Digital Signatures**: High-performance elliptic curve digital signatures
- **X25519 Key Exchange**: Secure key exchange for encrypted communications
- **XSalsa20 Encryption**: Stream cipher encryption with optional authentication
- **Session Management**: Transaction logging and cryptographic session handling
- **Cross-Platform**: Pre-built binaries for multiple platforms and architectures

## Jazz Framework Integration

This package is a core component of the Jazz framework, providing the cryptographic foundation for:

- **CoJSON**: Collaborative JSON documents with cryptographic integrity
- **Real-time Collaboration**: Secure peer-to-peer synchronization
- **Authentication**: Cryptographic identity and session management
- **End-to-End Encryption**: Secure data transmission and storage

The package is used throughout the Jazz ecosystem by other packages like `cojson`, `jazz-tools`, and various examples in the monorepo.

## Installation

### In the Jazz Monorepo

If you're working within the Jazz monorepo, the package is already available as a workspace dependency:

```bash
# From the monorepo root
pnpm install

# Build the NAPI package specifically
pnpm run build:napi

# Run tests for the NAPI package
pnpm test:napi
```

### As a Standalone Package

```bash
pnpm install cojson-core-napi
```

The package includes pre-built binaries for the following platforms:
- Windows (x64, x86, ARM64)
- macOS (x64, ARM64)
- Linux (x64, ARM64, ARM, musl)
- FreeBSD (x64)
- Android (ARM64, ARM)

## API Reference

### Hashing Functions

#### `blake3HashOnce(data: Uint8Array): Uint8Array`

Hash data once using BLAKE3. Returns 32 bytes of hash output.

```typescript
import { blake3HashOnce } from 'cojson-core-napi';

const data = new TextEncoder().encode("Hello, World!");
const hash = blake3HashOnce(data);
console.log(hash.length); // 32
```

#### `blake3HashOnceWithContext(data: Uint8Array, context: Uint8Array): Uint8Array`

Hash data with a context prefix for domain separation.

```typescript
import { blake3HashOnceWithContext } from 'cojson-core-napi';

const data = new TextEncoder().encode("message");
const context = new TextEncoder().encode("domain");
const hash = blake3HashOnceWithContext(data, context);
```

#### `Blake3Hasher`

Incremental hashing for large datasets.

```typescript
import { Blake3Hasher } from 'cojson-core-napi';

const hasher = new Blake3Hasher();
hasher.update(new TextEncoder().encode("part1"));
hasher.update(new TextEncoder().encode("part2"));
const hash = hasher.finalize();
```

### Digital Signatures (Ed25519)

#### `newEd25519SigningKey(): Uint8Array`

Generate a new Ed25519 signing key.

```typescript
import { newEd25519SigningKey, ed25519VerifyingKey, ed25519Sign, ed25519Verify } from 'cojson-core-napi';

const signingKey = newEd25519SigningKey();
const verifyingKey = ed25519VerifyingKey(signingKey);

const message = new TextEncoder().encode("Hello, World!");
const signature = ed25519Sign(signingKey, message);
const isValid = ed25519Verify(verifyingKey, message, signature);
```

#### Key Management Functions

- `ed25519SigningKeyFromBytes(bytes: Uint8Array): Uint8Array` - Validate and copy signing key bytes
- `ed25519VerifyingKeyFromBytes(bytes: Uint8Array): Uint8Array` - Validate and copy verifying key bytes
- `ed25519SignatureFromBytes(bytes: Uint8Array): Uint8Array` - Validate and copy signature bytes
- `ed25519SigningKeyToPublic(signingKey: Uint8Array): Uint8Array` - Derive public key from signing key
- `ed25519SigningKeySign(signingKey: Uint8Array, message: Uint8Array): Uint8Array` - Sign with signing key

### Key Exchange (X25519)

#### `newX25519PrivateKey(): Uint8Array`

Generate a new X25519 private key.

```typescript
import { newX25519PrivateKey, x25519PublicKey, x25519DiffieHellman } from 'cojson-core-napi';

const alicePrivate = newX25519PrivateKey();
const alicePublic = x25519PublicKey(alicePrivate);

const bobPrivate = newX25519PrivateKey();
const bobPublic = x25519PublicKey(bobPrivate);

// Perform key exchange
const aliceShared = x25519DiffieHellman(alicePrivate, bobPublic);
const bobShared = x25519DiffieHellman(bobPrivate, alicePublic);
// aliceShared and bobShared are identical
```

### Encryption Functions

#### `encrypt(value: Uint8Array, keySecret: string, nonceMaterial: Uint8Array): Uint8Array`
#### `decrypt(ciphertext: Uint8Array, keySecret: string, nonceMaterial: Uint8Array): Uint8Array`

Encrypt/decrypt data with a key secret and nonce material.

```typescript
import { encrypt, decrypt } from 'cojson-core-napi';

const keySecret = "keySecret_z11111111111111111111111111111111"; // Base58-encoded key
const nonceMaterial = new TextEncoder().encode("nonce");
const plaintext = new TextEncoder().encode("Secret message");

const ciphertext = encrypt(plaintext, keySecret, nonceMaterial);
const decrypted = decrypt(ciphertext, keySecret, nonceMaterial);
```

#### `encryptXsalsa20(key: Uint8Array, nonceMaterial: Uint8Array, plaintext: Uint8Array): Uint8Array`
#### `decryptXsalsa20(key: Uint8Array, nonceMaterial: Uint8Array, ciphertext: Uint8Array): Uint8Array`

XSalsa20 stream cipher encryption (without authentication).

```typescript
import { encryptXsalsa20, decryptXsalsa20 } from 'cojson-core-napi';

const key = new Uint8Array(32); // 32-byte key
const nonceMaterial = new TextEncoder().encode("nonce");
const plaintext = new TextEncoder().encode("Message");

const ciphertext = encryptXsalsa20(key, nonceMaterial, plaintext);
const decrypted = decryptXsalsa20(key, nonceMaterial, ciphertext);
```

### Sealing/Unsealing (Authenticated Encryption)

#### `seal(message: Uint8Array, senderSecret: string, recipientId: string, nonceMaterial: Uint8Array): Uint8Array`
#### `unseal(sealedMessage: Uint8Array, recipientSecret: string, senderId: string, nonceMaterial: Uint8Array): Uint8Array`

Authenticated encryption with perfect forward secrecy using X25519 + XSalsa20-Poly1305.

```typescript
import { seal, unseal, getSealerId } from 'cojson-core-napi';

const senderSecret = "sealerSecret_z..."; // Base58-encoded private key
const recipientId = "sealer_z..."; // Base58-encoded public key
const nonceMaterial = new TextEncoder().encode("nonce");
const message = new TextEncoder().encode("Secret message");

const sealed = seal(message, senderSecret, recipientId, nonceMaterial);
const unsealed = unseal(sealed, senderSecret, recipientId, nonceMaterial);
```

### Signing/Verification (Base58-wrapped)

#### `sign(message: Uint8Array, secret: Uint8Array): string`
#### `verify(signature: Uint8Array, message: Uint8Array, id: Uint8Array): boolean`

Sign and verify messages with base58-encoded signatures and IDs.

```typescript
import { sign, verify, getSignerId } from 'cojson-core-napi';

const secret = new TextEncoder().encode("signerSecret_z...");
const message = new TextEncoder().encode("Hello, World!");

const signature = sign(message, secret);
const isValid = verify(new TextEncoder().encode(signature), message, new TextEncoder().encode("signer_z..."));
```

### Utility Functions

#### `generateNonce(nonceMaterial: Uint8Array): Uint8Array`

Generate a 24-byte nonce from input material using BLAKE3.

```typescript
import { generateNonce } from 'cojson-core-napi';

const nonceMaterial = new TextEncoder().encode("input");
const nonce = generateNonce(nonceMaterial);
console.log(nonce.length); // 24
```

#### `getSealerId(secret: Uint8Array): string`
#### `getSignerId(secret: Uint8Array): string`

Derive sealer/signer IDs from secrets.

```typescript
import { getSealerId, getSignerId } from 'cojson-core-napi';

const sealerSecret = new TextEncoder().encode("sealerSecret_z...");
const sealerId = getSealerId(sealerSecret);
console.log(sealerId); // "sealer_z..."

const signerSecret = new TextEncoder().encode("signerSecret_z...");
const signerId = getSignerId(signerSecret);
console.log(signerId); // "signer_z..."
```

### Session Management

#### `SessionLog`

Manage cryptographic sessions and transactions.

```typescript
import { SessionLog } from 'cojson-core-napi';

const session = new SessionLog("coId", "sessionId", "signerId");

// Add a new private transaction
const result = session.addNewPrivateTransaction(
  '{"changes": "..."}',
  "signerSecret_z...",
  "keySecret_z...",
  "keyId",
  Date.now(),
  "metadata"
);

// Add a new trusting transaction
const signature = session.addNewTrustingTransaction(
  '{"changes": "..."}',
  "signerSecret_z...",
  Date.now(),
  "metadata"
);

// Decrypt transaction data
const changes = session.decryptNextTransactionChangesJson(0, "keySecret_z...");
const meta = session.decryptNextTransactionMetaJson(0, "keySecret_z...");
```

## Error Handling

All functions throw `Error` objects with descriptive messages for invalid inputs or cryptographic failures.

```typescript
try {
  const hash = blake3HashOnce(invalidData);
} catch (error) {
  console.error('Hashing failed:', error.message);
}
```

## Performance

This package is built with Rust for maximum performance:
- BLAKE3 hashing is significantly faster than SHA-256
- Ed25519 signatures are faster than RSA
- X25519 key exchange is faster than RSA key exchange
- Native code execution avoids JavaScript overhead

## Platform Support

The package includes pre-built binaries for:
- **Windows**: x64, x86, ARM64
- **macOS**: x64, ARM64 (Apple Silicon)
- **Linux**: x64, ARM64, ARM, musl
- **FreeBSD**: x64
- **Android**: ARM64, ARM

## Development

### Within the Jazz Monorepo

```bash
# From the monorepo root - install all dependencies
pnpm install

# Build only the NAPI package
pnpm run build:napi

# Build all packages including NAPI
pnpm run build:packages

# Run tests for the NAPI package
pnpm test:napi

# Run all tests (excluding NAPI tests)
pnpm test

```

### Using in Other Jazz Packages

```typescript
// In any Jazz package, import directly
import { 
  blake3HashOnce, 
  newEd25519SigningKey, 
  encrypt, 
  decrypt 
} from 'cojson-core-napi';

// The package is available as a workspace dependency
// No additional installation needed
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please see the main Jazz repository for contribution guidelines.
