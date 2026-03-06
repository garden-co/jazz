import { beforeAll, describe, expect, test } from "vitest";
import { base58 } from "@scure/base";

import {
  blake3HashOnce, 
  generateNonce, 
  Blake3Hasher, 
  newEd25519SigningKey,
  ed25519VerifyingKey,
  ed25519Sign,
  ed25519Verify,
  ed25519SigningKeyFromBytes,
  ed25519VerifyingKeyFromBytes,
  ed25519SignatureFromBytes,
  ed25519SigningKeyToPublic,
  ed25519SigningKeySign,
  encrypt,
  decrypt,
  seal, 
  unseal,
  sign,
  verify,
  getSealerId,
  getSignerId,
  newX25519PrivateKey,
  x25519PublicKey,
  x25519DiffieHellman,
  decryptXsalsa20,
  encryptXsalsa20,
  initialize,
  SessionMap,
} from '../index';

beforeAll(async () => {
  await initialize()
})

describe("blake3", () => {

  test("hashOnce", () => {
    const inputString = "test input";
    const hash = blake3HashOnce(new TextEncoder().encode(inputString));

    // BLAKE3 produces 32-byte hashes
    expect(hash.length).toBe(32);

    // Same input should produce same hash
    const hash2 = blake3HashOnce(new TextEncoder().encode(inputString));
    expect(Array.from(hash)).toEqual(Array.from(hash2));

    // Different input should produce different hash
    const hash3 = blake3HashOnce(new TextEncoder().encode("different input"));
    expect(Array.from(hash)).not.toEqual(Array.from(hash3));
  })

  test("generateNonce", () => {
    const inputString = "test input";
    const nonce = generateNonce(new TextEncoder().encode(inputString));
    expect(nonce.length).toBe(24);

    // Same input should produce same nonce
    const nonce2 = generateNonce(new TextEncoder().encode(inputString));
    expect(Array.from(nonce)).toEqual(Array.from(nonce2));

    // Different input should produce different nonce
    const nonce3 = generateNonce(new TextEncoder().encode("different input"));
    expect(Array.from(nonce)).not.toEqual(Array.from(nonce3));
  });

  test("blake3 Hasher State", () => {
    const state = new Blake3Hasher();
    expect(state).toBeDefined();
    expect(state instanceof Object).toBe(true);
  });

  test("incremental hashing", () => {
    const state = new Blake3Hasher();

    const data = new Uint8Array([1, 2, 3, 4, 5]);
    state.update(data);

    // Check that this matches a direct hash
    const hash1 = blake3HashOnce(data);
    const stateHash1 = state.finalize();
    expect(Array.from(hash1), 'First update should match direct hash').toEqual(Array.from(stateHash1));

    // Verify the exact expected hash 
    const exptected_firtst_hash = Uint8Array.from([
      2, 79, 103, 192, 66, 90, 61, 192, 47, 186, 245, 140, 185, 61, 229, 19, 46, 61, 117,
      197, 25, 250, 160, 186, 218, 33, 73, 29, 136, 201, 112, 87,
    ]);
    expect(Array.from(stateHash1), 'First update should match expected hash').toEqual(Array.from(exptected_firtst_hash));

    const state2 = new Blake3Hasher();

    // Compare with a single hash of all data
    const data2 = new Uint8Array([6, 7, 8, 9, 10]);
    state2.update(data);
    state2.update(data2);

    const combinedData = new Uint8Array([...data, ...data2]);

    // Check that this matches a direct hash
    const hash2 = blake3HashOnce(combinedData);
    const stateHash2 = state2.finalize();
    expect(Array.from(hash2), 'Final state should match direct hash of all data').toEqual(Array.from(stateHash2));

    // Test final hash matches expected value

    const expected_final_hash = Uint8Array.from([
      165, 131, 141, 69, 2, 69, 39, 236, 196, 244, 180, 213, 147, 124, 222, 39, 68, 223, 54,
      176, 242, 97, 200, 101, 204, 79, 21, 233, 56, 51, 1, 199,
    ])
    expect(Array.from(stateHash2), "Final state should match expected hash").toEqual(Array.from(expected_final_hash));
  });

});

describe("ed25519", () => {
  test("key generation and signing", () => {
    // Generate signing key
    const signingKey = newEd25519SigningKey();
    expect(signingKey.length).toBe(32);

    // Derive verifying key
    const verifyingKey = ed25519VerifyingKey(signingKey);
    expect(verifyingKey.length).toBe(32);

    // Different signing keys produce different verifying keys
    const signingKey2 = newEd25519SigningKey();
    const verifyingKey2 = ed25519VerifyingKey(signingKey2);
    expect(Array.from(verifyingKey)).not.toEqual(Array.from(verifyingKey2));

    // Sign and verify
    const message = new TextEncoder().encode("Test message");
    const signature = ed25519Sign(signingKey, message);
    expect(signature.length).toBe(64);

    // Successful verification
    expect(ed25519Verify(verifyingKey, message, signature)).toBe(true);

    // Wrong message
    const wrongMessage = new TextEncoder().encode("Wrong message");
    expect(ed25519Verify(verifyingKey, wrongMessage, signature)).toBe(false);

    // Wrong key
    expect(ed25519Verify(verifyingKey2, message, signature)).toBe(false);

    // Tampered signature
    const tamperedSignature = new Uint8Array(signature);
    tamperedSignature[0] ^= 1;
    expect(ed25519Verify(verifyingKey, message, tamperedSignature)).toBe(false);
  });

  test("error cases", () => {
    // Invalid signing key length
    const invalidSigningKey = new Uint8Array(31);
    expect(() => ed25519VerifyingKey(invalidSigningKey)).toThrow();
    expect(() => ed25519Sign(invalidSigningKey, new Uint8Array([1, 2, 3]))).toThrow();

    // Invalid verifying key length
    const invalidVerifyingKey = new Uint8Array(31);
    const validSigningKey = newEd25519SigningKey();
    const validSignature = ed25519Sign(validSigningKey, new Uint8Array([1, 2, 3]));
    expect(() => ed25519Verify(invalidVerifyingKey, new Uint8Array([1, 2, 3]), validSignature)).toThrow();

    // Invalid signature length
    const validVerifyingKey = ed25519VerifyingKey(validSigningKey);
    const invalidSignature = new Uint8Array(63);
    expect(() => ed25519Verify(validVerifyingKey, new Uint8Array([1, 2, 3]), invalidSignature)).toThrow();

    // Too long keys
    const tooLongKey = new Uint8Array(33);
    expect(() => ed25519VerifyingKey(tooLongKey)).toThrow();
    expect(() => ed25519Sign(tooLongKey, new Uint8Array([1, 2, 3]))).toThrow();

    // Too long signature
    const tooLongSignature = new Uint8Array(65);
    expect(() => ed25519Verify(validVerifyingKey, new Uint8Array([1, 2, 3]), tooLongSignature)).toThrow();
  });

  test("signing key from bytes", () => {
    const key = newEd25519SigningKey();
    const keyCopy = ed25519SigningKeyFromBytes(key);
    expect(Array.from(keyCopy)).toEqual(Array.from(key));
    expect(() => ed25519SigningKeyFromBytes(new Uint8Array(31))).toThrow();
  });

  test("verifying key from bytes", () => {
    const key = newEd25519SigningKey();
    const verifyingKey = ed25519VerifyingKey(key);
    const keyCopy = ed25519VerifyingKeyFromBytes(verifyingKey);
    expect(Array.from(keyCopy)).toEqual(Array.from(verifyingKey));
    expect(() => ed25519VerifyingKeyFromBytes(new Uint8Array(31))).toThrow();
  });

  test("signature from bytes", () => {
    const key = newEd25519SigningKey();
    const message = new TextEncoder().encode("msg");
    const signature = ed25519Sign(key, message);
    const sigCopy = ed25519SignatureFromBytes(signature);
    expect(Array.from(sigCopy)).toEqual(Array.from(signature));
    expect(() => ed25519SignatureFromBytes(new Uint8Array(63))).toThrow();
  });

  test("signing key to public", () => {
    const key = newEd25519SigningKey();
    const pub1 = ed25519SigningKeyToPublic(key);
    const pub2 = ed25519VerifyingKey(key);
    expect(Array.from(pub1)).toEqual(Array.from(pub2));
  });

  test("signing key sign", () => {
    const key = newEd25519SigningKey();
    const message = new TextEncoder().encode("msg");
    const sig1 = ed25519SigningKeySign(key, message);
    const sig2 = ed25519Sign(key, message);
    expect(Array.from(sig1)).toEqual(Array.from(sig2));
  });
});

describe("encrypt/decrypt", () => {
  // Example base58-encoded key with "keySecret_z" prefix (32 bytes of zeros)
  const keySecret = "keySecret_z11111111111111111111111111111111";
  const nonceMaterial = new TextEncoder().encode("test_nonce_material");

  test("encrypt and decrypt roundtrip", () => {
    const plaintext = new TextEncoder().encode("Hello, World!");
    const ciphertext = encrypt(plaintext, keySecret, nonceMaterial);
    expect(ciphertext.length).toBeGreaterThan(0);

    const decrypted = decrypt(ciphertext, keySecret, nonceMaterial);
    expect(Array.from(decrypted)).toEqual(Array.from(plaintext));
  });

  test("invalid key secret format", () => {
    const plaintext = new TextEncoder().encode("test");
    const invalidKey = "invalid_key";
    expect(() => encrypt(plaintext, invalidKey, nonceMaterial)).toThrow();
    expect(() => decrypt(plaintext, invalidKey, nonceMaterial)).toThrow();
  });

  test("invalid base58 encoding", () => {
    const plaintext = new TextEncoder().encode("test");
    const badKey = "keySecret_z!!!!";
    expect(() => encrypt(plaintext, badKey, nonceMaterial)).toThrow();
    expect(() => decrypt(plaintext, badKey, nonceMaterial)).toThrow();
  });
});


describe("encrypt/decrypt", () => {
  // Example base58-encoded key with "keySecret_z" prefix (32 bytes of zeros)
  const keySecret = "keySecret_z11111111111111111111111111111111";
  const nonceMaterial = new TextEncoder().encode("test_nonce_material");

  test("encrypt and decrypt roundtrip", () => {
    const plaintext = new TextEncoder().encode("Hello, World!");
    const ciphertext = encrypt(plaintext, keySecret, nonceMaterial);
    expect(ciphertext.length).toBeGreaterThan(0);

    const decrypted = decrypt(ciphertext, keySecret, nonceMaterial);
    expect(Array.from(decrypted)).toEqual(Array.from(plaintext));
  });

  test("invalid key secret format", () => {
    const plaintext = new TextEncoder().encode("test");
    const invalidKey = "invalid_key";
    expect(() => encrypt(plaintext, invalidKey, nonceMaterial)).toThrow();
    expect(() => decrypt(plaintext, invalidKey, nonceMaterial)).toThrow();
  });

  test("invalid base58 encoding", () => {
    const plaintext = new TextEncoder().encode("test");
    const badKey = "keySecret_z!!!!";
    expect(() => encrypt(plaintext, badKey, nonceMaterial)).toThrow();
    expect(() => decrypt(plaintext, badKey, nonceMaterial)).toThrow();
  });
});

describe("seal/unseal", () => {
  // Helper to generate a valid sealerSecret_z and sealer_z keypair using the Rust NAPI API
  // For the test, we use 32 bytes of zeros for simplicity (not secure, but matches test vectors)
  const zeroKey = new Uint8Array(32);
  const senderSecret = "sealerSecret_z" + base58.encode(zeroKey);
  const recipientId = "sealer_z" + base58.encode(zeroKey);
  const nonceMaterial = new TextEncoder().encode("test_nonce_material");

  test("seal and unseal roundtrip", () => {
    // Import seal/unseal from the module
    const message = new TextEncoder().encode("Secret message");

    const sealed = seal(message, senderSecret, recipientId, nonceMaterial);
    expect(sealed.length).toBeGreaterThan(0);

    const unsealed = unseal(sealed, senderSecret, recipientId, nonceMaterial);
    expect(Array.from(unsealed)).toEqual(Array.from(message));
  });

  test("invalid sender secret format", () => {
    const message = new TextEncoder().encode("test");
    const invalidSenderSecret = "invalid_key";
    expect(() => seal(message, invalidSenderSecret, recipientId, nonceMaterial)).toThrow();
  });

  test("invalid recipient id format", () => {
    const message = new TextEncoder().encode("test");
    const invalidRecipientId = "invalid_key";
    expect(() => seal(message, senderSecret, invalidRecipientId, nonceMaterial)).toThrow();
  });

  test("invalid base58 encoding", () => {
    const message = new TextEncoder().encode("test");
    const badSenderSecret = "sealerSecret_z!!!!";
    expect(() => seal(message, badSenderSecret, recipientId, nonceMaterial)).toThrow();
  });
});

describe("sign/verify (Ed25519, base58-wrapped)", () => {
  const encoder = new TextEncoder()
  const decoder = new TextDecoder();
  // Helper to generate a valid signerSecret_z and signer_z keypair
  function makeKeyPair() {
    const signingKey = newEd25519SigningKey();
    const secret = encoder.encode("signerSecret_z" + base58.encode(signingKey));
    const verifyingKey = ed25519VerifyingKey(signingKey);
    const signerId = encoder.encode("signer_z" + base58.encode(verifyingKey));
    return { signingKey, secret, verifyingKey, signerId };
  }

  test("sign and verify roundtrip", () => {
    const { secret, signerId } = makeKeyPair();
    const message = encoder.encode("hello world");

    // sign_internal: sign message with secret
    const signature = sign(message, secret);
    expect(typeof signature).toBe("string");
    expect(signature.startsWith("signature_z")).toBe(true);

    // verify_internal: verify signature with signerId
    const valid = verify( encoder.encode(signature), message, signerId);
    expect(valid).toBe(true);
  });

  test("invalid inputs", () => {
    const message = new TextEncoder().encode("hello world");

    // Invalid base58 in secret
    expect(() => sign(message, encoder.encode("signerSecret_z!!!invalid!!!"))).toThrow();

    // Invalid signature format
    expect(() => verify( encoder.encode("not_a_signature"), message,  encoder.encode("signer_z123"))).toThrow();

    // Invalid signer ID format
    expect(() => verify( encoder.encode("signature_z123"), message,  encoder.encode("not_a_signer"))).toThrow();
  });

  test("get_signer_id", () => {
    const { secret, signerId } = makeKeyPair();

    // get_signer_id_internal: derive signer ID from secret
    const derivedId = getSignerId(secret);
    expect(typeof derivedId).toBe("string");
    expect(derivedId.startsWith("signer_z")).toBe(true);
    expect(derivedId).toBe(decoder.decode(signerId));

    // Same secret produces same ID
    const derivedId2 = getSignerId(secret);
    expect(derivedId2).toBe(derivedId);

    // Invalid secret format
    expect(() => getSignerId(encoder.encode("invalid_secret"))).toThrow();

    // Invalid base58
    expect(() => getSignerId(encoder.encode("signerSecret_z!!!invalid!!!"))).toThrow();
  });
});

describe("x25519", () => {
  const encoder = new TextEncoder();

  test("key generation and public key derivation", () => {
    const privateKey = newX25519PrivateKey();
    expect(privateKey.length).toBe(32);

    const publicKey = x25519PublicKey(privateKey);
    expect(publicKey.length).toBe(32);

    const privateKey2 = newX25519PrivateKey();
    const publicKey2 = x25519PublicKey(privateKey2);
    expect(Array.from(publicKey)).not.toEqual(Array.from(publicKey2));
  });

  test("diffie-hellman key exchange", () => {
    const senderPrivate = newX25519PrivateKey();
    const senderPublic = x25519PublicKey(senderPrivate);

    const recipientPrivate = newX25519PrivateKey();
    const recipientPublic = x25519PublicKey(recipientPrivate);

    const sharedSecret1 = x25519DiffieHellman(senderPrivate, recipientPublic);
    const sharedSecret2 = x25519DiffieHellman(recipientPrivate, senderPublic);

    expect(Array.from(sharedSecret1)).toEqual(Array.from(sharedSecret2));
    expect(sharedSecret1.length).toBe(32);

    const otherRecipientPrivate = newX25519PrivateKey();
    const otherRecipientPublic = x25519PublicKey(otherRecipientPrivate);
    const differentSharedSecret = x25519DiffieHellman(senderPrivate, otherRecipientPublic);
    expect(Array.from(sharedSecret1)).not.toEqual(Array.from(differentSharedSecret));
  });

  test("getSealerId", () => {
    const privateKey = newX25519PrivateKey();
    const secret = encoder.encode("sealerSecret_z" + base58.encode(privateKey));
    const sealerId = getSealerId(secret);
    expect(typeof sealerId).toBe("string");
    expect(sealerId.startsWith("sealer_z")).toBe(true);

    const sealerId2 = getSealerId(secret);
    expect(sealerId2).toBe(sealerId);

    expect(() => getSealerId(encoder.encode("invalid_secret"))).toThrow();
    expect(() => getSealerId(encoder.encode("sealerSecret_z!!!invalid!!!"))).toThrow();
  });
});

describe("xsalsa20", () => {
  const encoder = new TextEncoder();

  test("encryptXsalsa20 and decryptXsalsa20 roundtrip", () => {
    const key = new Uint8Array(32); // all zeros
    const nonceMaterial = encoder.encode("test_nonce_material");
    const plaintext = encoder.encode("Hello, World!");

    const ciphertext = encryptXsalsa20(key, nonceMaterial, plaintext);
    expect(ciphertext.length).toBe(plaintext.length);

    const decrypted = decryptXsalsa20(key, nonceMaterial, ciphertext);
    expect(Array.from(decrypted)).toEqual(Array.from(plaintext));
  });

  test("different nonce produces different ciphertext", () => {
    const key = new Uint8Array(32);
    const nonceMaterial1 = encoder.encode("nonce1");
    const nonceMaterial2 = encoder.encode("nonce2");
    const plaintext = encoder.encode("Hello, World!");

    const ciphertext1 = encryptXsalsa20(key, nonceMaterial1, plaintext);
    const ciphertext2 = encryptXsalsa20(key, nonceMaterial2, plaintext);

    expect(Array.from(ciphertext1)).not.toEqual(Array.from(ciphertext2));
  });

  test("different key produces different ciphertext", () => {
    const key1 = new Uint8Array(32);
    const key2 = new Uint8Array(32);
    key2[0] = 1;
    const nonceMaterial = encoder.encode("test_nonce_material");
    const plaintext = encoder.encode("Hello, World!");

    const ciphertext1 = encryptXsalsa20(key1, nonceMaterial, plaintext);
    const ciphertext2 = encryptXsalsa20(key2, nonceMaterial, plaintext);

    expect(Array.from(ciphertext1)).not.toEqual(Array.from(ciphertext2));
  });

  test("invalid key length throws", () => {
    const key = new Uint8Array(31);
    const nonceMaterial = encoder.encode("test_nonce_material");
    const plaintext = encoder.encode("Hello, World!");
    expect(() => encryptXsalsa20(key, nonceMaterial, plaintext)).toThrow();
    expect(() => decryptXsalsa20(key, nonceMaterial, plaintext)).toThrow();
  });

  test("tampered ciphertext does not match original", () => {
    const key = new Uint8Array(32);
    const nonceMaterial = encoder.encode("test_nonce_material");
    const plaintext = encoder.encode("Hello, World!");

    const ciphertext = encryptXsalsa20(key, nonceMaterial, plaintext);
    const tampered = new Uint8Array(ciphertext);
    tampered[0] ^= 1;

    const decrypted = decryptXsalsa20(key, nonceMaterial, tampered);
    expect(Array.from(decrypted)).not.toEqual(Array.from(plaintext));
  });
});

describe("SessionMap", () => {
  // Helper to create a valid CoValueHeader JSON
  // Note: createdAt should be an ISO string or omitted
  // RulesetGroup requires: { initialAdmin: "co_...", type: "group" }
  function createGroupHeader(createdAt?: string) {
    return JSON.stringify({
      createdAt: createdAt ?? new Date().toISOString(),
      meta: null,
      ruleset: { initialAdmin: "co_zAdmin123", type: "group" },
      type: "comap",
      uniqueness: "test-uniqueness-" + Math.random().toString(36).slice(2),
    });
  }

  test("create SessionMap with valid header", () => {
    const coId = "co_zTestCoValue123";
    const header = createGroupHeader();

    const sessionMap = new SessionMap(coId, header, undefined, true);
    expect(sessionMap).toBeDefined();

    // Get header should return valid JSON
    const returnedHeader = sessionMap.getHeader();
    expect(returnedHeader).toBeDefined();
    const parsedHeader = JSON.parse(returnedHeader);
    expect(parsedHeader.type).toBe("comap");
    expect(parsedHeader.ruleset.type).toBe("group");
  });

  test("invalid header throws error", () => {
    const coId = "co_zTestCoValue123";
    expect(() => new SessionMap(coId, "invalid json", undefined, true)).toThrow();
    expect(() => new SessionMap(coId, "{}", undefined, true)).toThrow(); // Missing required fields
  });

  test("get session IDs - empty initially", () => {
    const coId = "co_zTestCoValue123";
    const header = createGroupHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    const sessionIds = sessionMap.getSessionIds();
    expect(Array.isArray(sessionIds)).toBe(true);
    expect(sessionIds.length).toBe(0);
  });

  test("get transaction count - returns -1 for non-existent session", () => {
    const coId = "co_zTestCoValue123";
    const header = createGroupHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    const count = sessionMap.getTransactionCount("non_existent_session");
    expect(count).toBe(-1);
  });

  test("get known state - empty initially", () => {
    const coId = "co_zTestCoValue123";
    const header = createGroupHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    const knownState = sessionMap.getKnownState();
    expect(knownState).toBeDefined();
    expect(knownState.id).toBe(coId);
    expect(knownState.header).toBe(true);
    expect(knownState.sessions).toEqual({});
  });

  test("mark as deleted", () => {
    const coId = "co_zTestCoValue123";
    const header = createGroupHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    expect(sessionMap.isDeleted()).toBe(false);
    sessionMap.markAsDeleted();
    expect(sessionMap.isDeleted()).toBe(true);
  });

  test("set and get streaming known state", () => {
    const coId = "co_zTestCoValue123";
    const header = createGroupHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    // Initially no streaming known state (WASM returns undefined for Option::None)
    expect(sessionMap.getKnownStateWithStreaming()).toBeUndefined();

    // Set streaming known state
    const streamingSessions = JSON.stringify({ "session1": 5 });
    sessionMap.setStreamingKnownState(streamingSessions);

    // Now should have streaming known state
    const knownStateWithStreaming = sessionMap.getKnownStateWithStreaming();
    expect(knownStateWithStreaming).toBeDefined();
    expect(knownStateWithStreaming!.id).toBe(coId);
  });

  test("header serialization is stable (alphabetically sorted keys)", () => {
    const coId = "co_zTestCoValue123";
    const createdAt = "2023-11-14T22:13:20.000Z";
    const header = createGroupHeader(createdAt);
    const sessionMap = new SessionMap(coId, header, undefined, true);

    const returnedHeader = sessionMap.getHeader();
    const parsedHeader = JSON.parse(returnedHeader);

    // Keys should be in alphabetical order when stringified
    const keys = Object.keys(parsedHeader);
    const sortedKeys = [...keys].sort();
    expect(keys).toEqual(sortedKeys);
  });

  test("different rulesets", () => {
    const coId = "co_zTestCoValue123";
    const now = new Date().toISOString();
    
    // Group ruleset - requires initialAdmin field
    const groupHeader = JSON.stringify({
      createdAt: now,
      meta: null,
      ruleset: { initialAdmin: "co_zAdmin123", type: "group" },
      type: "comap",
      uniqueness: "test-group",
    });
    const groupSessionMap = new SessionMap(coId, groupHeader, undefined, true);
    expect(JSON.parse(groupSessionMap.getHeader()).ruleset.type).toBe("group");

    // OwnedByGroup ruleset
    const ownedByGroupHeader = JSON.stringify({
      createdAt: now,
      meta: null,
      ruleset: { group: "co_zGroupId123", type: "ownedByGroup" },
      type: "comap",
      uniqueness: "test-owned",
    });
    const ownedSessionMap = new SessionMap(coId, ownedByGroupHeader, undefined, true);
    const ownedParsed = JSON.parse(ownedSessionMap.getHeader());
    expect(ownedParsed.ruleset.type).toBe("ownedByGroup");
    expect(ownedParsed.ruleset.group).toBe("co_zGroupId123");

    // UnsafeAllowAll ruleset
    const unsafeHeader = JSON.stringify({
      createdAt: now,
      meta: null,
      ruleset: { type: "unsafeAllowAll" },
      type: "comap",
      uniqueness: "test-unsafe",
    });
    const unsafeSessionMap = new SessionMap(coId, unsafeHeader, undefined, true);
    expect(JSON.parse(unsafeSessionMap.getHeader()).ruleset.type).toBe("unsafeAllowAll");
  });

  test("header with meta object", () => {
    const coId = "co_zTestCoValue123";
    const headerWithMeta = JSON.stringify({
      createdAt: new Date().toISOString(),
      meta: { key: "value", nested: { a: 1 } },
      ruleset: { initialAdmin: "co_zAdmin123", type: "group" },
      type: "comap",
      uniqueness: "test-with-meta",
    });

    const sessionMap = new SessionMap(coId, headerWithMeta, undefined, true);
    const returnedHeader = JSON.parse(sessionMap.getHeader());
    expect(returnedHeader.meta).toEqual({ key: "value", nested: { a: 1 } });
  });

  test("header uniqueness can be null", () => {
    const coId = "co_zTestCoValue123";
    const headerWithNullUniqueness = JSON.stringify({
      createdAt: new Date().toISOString(),
      meta: null,
      ruleset: { initialAdmin: "co_zAdmin123", type: "group" },
      type: "comap",
      uniqueness: null,
    });

    const sessionMap = new SessionMap(coId, headerWithNullUniqueness, undefined, true);
    const returnedHeader = JSON.parse(sessionMap.getHeader());
    expect(returnedHeader.uniqueness).toBeNull();
  });
});

describe("SessionMap - Transaction Flow", () => {
  // Helper to create a valid CoValueHeader JSON
  function createUnsafeHeader() {
    return JSON.stringify({
      createdAt: new Date().toISOString(),
      meta: null,
      ruleset: { type: "unsafeAllowAll" },
      type: "comap",
      uniqueness: "test-uniqueness-" + Math.random().toString(36).slice(2),
    });
  }

  // Helper to create a valid signer secret and ID
  function createSignerKeyPair() {
    const signingKey = newEd25519SigningKey();
    const signerSecret = "signerSecret_z" + base58.encode(signingKey);
    const verifyingKey = ed25519VerifyingKey(signingKey);
    const signerId = "signer_z" + base58.encode(verifyingKey);
    return { signerSecret, signerId };
  }

  // Helper to create a valid key for encryption
  function createKeyPair() {
    const key = new Uint8Array(32);
    crypto.getRandomValues(key);
    const keyId = "key_z" + base58.encode(key).slice(0, 20);
    const keySecret = "keySecret_z" + base58.encode(key);
    return { keyId, keySecret };
  }

  test("create and retrieve trusting transaction", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret, signerId } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create a trusting transaction
    const changes = JSON.stringify({ key: "value", number: 42 });
    const madeAt = Date.now();

    const result = sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      changes,
      undefined, // no meta
      madeAt
    );

    // Result should be JSON with signature and transaction
    const parsed = JSON.parse(result);
    expect(parsed.signature).toBeDefined();
    expect(parsed.signature.startsWith("signature_z")).toBe(true);
    expect(parsed.transaction).toBeDefined();

    // Session should now exist
    const sessionIds = sessionMap.getSessionIds();
    expect(sessionIds).toContain(sessionId);

    // Transaction count should be 1
    expect(sessionMap.getTransactionCount(sessionId)).toBe(1);

    // Get the transaction
    const tx = sessionMap.getTransaction(sessionId, 0);
    expect(tx).toBeDefined();
    const txParsed = JSON.parse(tx!);
    expect(txParsed.changes).toBe(changes);
    expect(txParsed.madeAt).toBe(madeAt);

    // Known state should reflect the transaction
    const knownState = sessionMap.getKnownState();
    expect(knownState.sessions[sessionId]).toBe(1);
  });

  test("create and retrieve private (encrypted) transaction", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const { keyId, keySecret } = createKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create a private (encrypted) transaction
    const changes = JSON.stringify({ secret: "data", count: 100 });
    const madeAt = Date.now();

    const result = sessionMap.makeNewPrivateTransaction(
      sessionId,
      signerSecret,
      changes,
      keyId,
      keySecret,
      undefined, // no meta
      madeAt
    );

    // Result should be JSON with signature and transaction
    const parsed = JSON.parse(result);
    expect(parsed.signature).toBeDefined();
    expect(parsed.transaction).toBeDefined();
    expect(parsed.transaction.encryptedChanges).toBeDefined();
    expect(parsed.transaction.keyUsed).toBe(keyId);

    // Session should now exist
    expect(sessionMap.getTransactionCount(sessionId)).toBe(1);

    // Decrypt the transaction
    const decrypted = sessionMap.decryptTransaction(sessionId, 0, keySecret);
    expect(decrypted).toBeDefined();
    expect(decrypted).toBe(changes);
  });

  test("multiple transactions in same session", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create multiple transactions
    for (let i = 0; i < 5; i++) {
      const changes = JSON.stringify({ index: i });
      sessionMap.makeNewTrustingTransaction(
        sessionId,
        signerSecret,
        changes,
        undefined,
        Date.now() + i
      );
    }

    // Should have 5 transactions
    expect(sessionMap.getTransactionCount(sessionId)).toBe(5);

    // Verify each transaction
    for (let i = 0; i < 5; i++) {
      const tx = sessionMap.getTransaction(sessionId, i);
      expect(tx).toBeDefined();
      const txParsed = JSON.parse(tx!);
      expect(JSON.parse(txParsed.changes).index).toBe(i);
    }

    // Get all transactions at once
    const allTx = sessionMap.getSessionTransactions(sessionId, 0);
    expect(allTx).toBeDefined();
    expect(allTx!.length).toBe(5);

    // Get transactions from index 2
    const partialTx = sessionMap.getSessionTransactions(sessionId, 2);
    expect(partialTx).toBeDefined();
    expect(partialTx!.length).toBe(3);

    // Known state should show 5 transactions
    const knownState = sessionMap.getKnownState();
    expect(knownState.sessions[sessionId]).toBe(5);
  });

  test("multiple sessions in same SessionMap", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();

    const session1 = `${coId}_session_z1`;
    const session2 = `${coId}_session_z2`;
    const session3 = `${coId}_session_z3`;

    // Create transactions in different sessions
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1, tx: 2 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session2, signerSecret, JSON.stringify({ s: 2 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session3, signerSecret, JSON.stringify({ s: 3 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session3, signerSecret, JSON.stringify({ s: 3, tx: 2 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session3, signerSecret, JSON.stringify({ s: 3, tx: 3 }), undefined, Date.now());

    // Verify session IDs
    const sessionIds = sessionMap.getSessionIds();
    expect(sessionIds).toContain(session1);
    expect(sessionIds).toContain(session2);
    expect(sessionIds).toContain(session3);
    expect(sessionIds.length).toBe(3);

    // Verify transaction counts
    expect(sessionMap.getTransactionCount(session1)).toBe(2);
    expect(sessionMap.getTransactionCount(session2)).toBe(1);
    expect(sessionMap.getTransactionCount(session3)).toBe(3);

    // Known state should reflect all sessions
    const knownState = sessionMap.getKnownState();
    expect(knownState.sessions[session1]).toBe(2);
    expect(knownState.sessions[session2]).toBe(1);
    expect(knownState.sessions[session3]).toBe(3);
  });

  test("get last signature", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // No signature initially
    expect(sessionMap.getLastSignature(sessionId)).toBeUndefined();

    // Create a transaction
    const result = sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      JSON.stringify({ test: 1 }),
      undefined,
      Date.now()
    );
    const { signature } = JSON.parse(result);

    // Last signature should be set
    const lastSig = sessionMap.getLastSignature(sessionId);
    expect(lastSig).toBe(signature);

    // Add another transaction
    const result2 = sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      JSON.stringify({ test: 2 }),
      undefined,
      Date.now()
    );
    const { signature: signature2 } = JSON.parse(result2);

    // Last signature should update
    const lastSig2 = sessionMap.getLastSignature(sessionId);
    expect(lastSig2).toBe(signature2);
    expect(lastSig2).not.toBe(signature);
  });

  test("transaction with meta", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create a transaction with meta
    const changes = JSON.stringify({ key: "value" });
    const meta = JSON.stringify({ author: "test", priority: 1 });
    const madeAt = Date.now();

    sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      changes,
      meta,
      madeAt
    );

    // Get the transaction and verify meta
    const tx = sessionMap.getTransaction(sessionId, 0);
    const txParsed = JSON.parse(tx!);
    expect(txParsed.meta).toBe(meta);
  });

  test("private transaction with encrypted meta", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const { keyId, keySecret } = createKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create a private transaction with meta
    const changes = JSON.stringify({ secret: "data" });
    const meta = JSON.stringify({ timestamp: Date.now() });

    sessionMap.makeNewPrivateTransaction(
      sessionId,
      signerSecret,
      changes,
      keyId,
      keySecret,
      meta,
      Date.now()
    );

    // Decrypt the transaction
    const decryptedChanges = sessionMap.decryptTransaction(sessionId, 0, keySecret);
    expect(decryptedChanges).toBe(changes);

    // Decrypt the meta
    const decryptedMeta = sessionMap.decryptTransactionMeta(sessionId, 0, keySecret);
    expect(decryptedMeta).toBe(meta);
  });

  test("add transactions from external source (simulating sync)", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    
    // Simulate two peers with their own SessionMaps
    const sessionMap1 = new SessionMap(coId, header, undefined, true);
    const sessionMap2 = new SessionMap(coId, header, undefined, true);
    
    const { signerSecret, signerId } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Peer 1 creates transactions
    sessionMap1.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      JSON.stringify({ from: "peer1", tx: 1 }),
      undefined,
      Date.now()
    );

    const result2 = sessionMap1.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      JSON.stringify({ from: "peer1", tx: 2 }),
      undefined,
      Date.now()
    );
    const { signature: sig2 } = JSON.parse(result2);

    // getSessionTransactions returns a JSON array of JSON strings (double-encoded)
    // For addTransactions, we need a JSON array of Transaction objects
    const allTxJsonStrings = sessionMap1.getSessionTransactions(sessionId, 0);
    expect(allTxJsonStrings).toBeDefined();

    // Parse the array of JSON strings, then parse each string to get objects
    const txObjects = allTxJsonStrings!.map((s) => JSON.parse(s));
    const txArrayJson = JSON.stringify(txObjects);

    // Add to peer 2 (skip verification for simplicity in test)
    sessionMap2.addTransactions(
      sessionId,
      signerId,
      txArrayJson,
      sig2,
      true
    );

    // Peer 2 should now have the same transactions
    expect(sessionMap2.getTransactionCount(sessionId)).toBe(2);

    // Verify transaction content
    const tx = sessionMap2.getTransaction(sessionId, 0);
    expect(tx).toBeDefined();
    const txParsed = JSON.parse(tx!);
    expect(JSON.parse(txParsed.changes).from).toBe("peer1");
  });

  test("streaming known state management", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();

    const session1 = `${coId}_session_z1`;
    const session2 = `${coId}_session_z2`;

    // Create some transactions
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1, tx: 2 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session2, signerSecret, JSON.stringify({ s: 2 }), undefined, Date.now());

    // Base known state
    const baseKnownState = sessionMap.getKnownState();
    expect(baseKnownState.sessions[session1]).toBe(2);
    expect(baseKnownState.sessions[session2]).toBe(1);

    // Set streaming known state
    const streamingSessions = JSON.stringify({ [session1]: 5, [session2]: 3 });
    sessionMap.setStreamingKnownState(streamingSessions);

    // Known state with streaming should combine both
    const combined = sessionMap.getKnownStateWithStreaming();
    expect(combined).toBeDefined();
    expect(combined!.sessions[session1]).toBe(5);
    expect(combined!.sessions[session2]).toBe(3);
  });

  test("getKnownState returns native JS object", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();

    const session1 = `${coId}_session_z1`;
    const session2 = `${coId}_session_z2`;

    // Create some transactions
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1, tx: 2 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session2, signerSecret, JSON.stringify({ s: 2 }), undefined, Date.now());

    // Get known state (returns native object, no JSON parsing needed)
    const knownState = sessionMap.getKnownState();
    
    // Verify it's a native object with correct structure
    expect(typeof knownState).toBe("object");
    expect(knownState.id).toBe(coId);
    expect(knownState.header).toBe(true);
    expect(typeof knownState.sessions).toBe("object");
    expect(knownState.sessions[session1]).toBe(2);
    expect(knownState.sessions[session2]).toBe(1);
  });

  test("getKnownStateWithStreaming returns native JS object", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();

    const session1 = `${coId}_session_z1`;
    const session2 = `${coId}_session_z2`;

    // Create some transactions
    sessionMap.makeNewTrustingTransaction(session1, signerSecret, JSON.stringify({ s: 1 }), undefined, Date.now());
    sessionMap.makeNewTrustingTransaction(session2, signerSecret, JSON.stringify({ s: 2 }), undefined, Date.now());

    // Initially no streaming state
    const noStreaming = sessionMap.getKnownStateWithStreaming();
    expect(noStreaming).toBeUndefined();

    // Set streaming known state
    const streamingSessions = JSON.stringify({ [session1]: 5, [session2]: 3 });
    sessionMap.setStreamingKnownState(streamingSessions);

    // Get known state with streaming (returns native object)
    const knownStateWithStreaming = sessionMap.getKnownStateWithStreaming();
    
    // Verify it's a native object with correct structure
    expect(knownStateWithStreaming).toBeDefined();
    expect(typeof knownStateWithStreaming).toBe("object");
    expect(knownStateWithStreaming!.id).toBe(coId);
    expect(knownStateWithStreaming!.header).toBe(true);
    expect(knownStateWithStreaming!.sessions[session1]).toBe(5);
    expect(knownStateWithStreaming!.sessions[session2]).toBe(3);
  });

  test("deletion flow", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create a transaction
    sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      JSON.stringify({ data: "test" }),
      undefined,
      Date.now()
    );

    expect(sessionMap.isDeleted()).toBe(false);
    expect(sessionMap.getTransactionCount(sessionId)).toBe(1);

    // Mark as deleted
    sessionMap.markAsDeleted();
    expect(sessionMap.isDeleted()).toBe(true);

    // Transactions should still be accessible
    expect(sessionMap.getTransactionCount(sessionId)).toBe(1);
  });

  test("different CoValue types", () => {
    const coId = "co_zTestCoValue123";

    // comap
    const comapHeader = JSON.stringify({
      createdAt: new Date().toISOString(),
      meta: null,
      ruleset: { type: "unsafeAllowAll" },
      type: "comap",
      uniqueness: "test-comap",
    });
    const comapSession = new SessionMap(coId, comapHeader, undefined, true);
    expect(JSON.parse(comapSession.getHeader()).type).toBe("comap");

    // colist
    const colistHeader = JSON.stringify({
      createdAt: new Date().toISOString(),
      meta: null,
      ruleset: { type: "unsafeAllowAll" },
      type: "colist",
      uniqueness: "test-colist",
    });
    const colistSession = new SessionMap(coId, colistHeader, undefined, true);
    expect(JSON.parse(colistSession.getHeader()).type).toBe("colist");

    // costream
    const costreamHeader = JSON.stringify({
      createdAt: new Date().toISOString(),
      meta: null,
      ruleset: { type: "unsafeAllowAll" },
      type: "costream",
      uniqueness: "test-costream",
    });
    const costreamSession = new SessionMap(coId, costreamHeader, undefined, true);
    expect(JSON.parse(costreamSession.getHeader()).type).toBe("costream");
  });

  test("error handling - invalid session for get operations", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    // Non-existent session
    expect(sessionMap.getTransactionCount("nonexistent")).toBe(-1);
    expect(sessionMap.getTransaction("nonexistent", 0)).toBeUndefined();
    expect(sessionMap.getSessionTransactions("nonexistent", 0)).toBeUndefined();
    expect(sessionMap.getLastSignature("nonexistent")).toBeUndefined();
  });

  test("error handling - invalid transaction index", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);
    const { signerSecret } = createSignerKeyPair();
    const sessionId = `${coId}_session_z${Math.random().toString(36).slice(2)}`;

    // Create one transaction
    sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      JSON.stringify({ test: 1 }),
      undefined,
      Date.now()
    );

    // Valid index
    expect(sessionMap.getTransaction(sessionId, 0)).toBeDefined();

    // Invalid index (out of bounds)
    expect(sessionMap.getTransaction(sessionId, 1)).toBeUndefined();
    expect(sessionMap.getTransaction(sessionId, 100)).toBeUndefined();
  });

  test("decryptTransaction returns undefined for nonexistent session", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    // Decrypting from a non-existent session should return undefined, not throw
    const result = sessionMap.decryptTransaction(
      "nonexistent_session",
      0,
      "keySecret_z11111111111111111111111111111111"
    );

    expect(result).toBeUndefined();
  });

  test("decryptTransactionMeta returns undefined for nonexistent session", () => {
    const coId = "co_zTestCoValue123";
    const header = createUnsafeHeader();
    const sessionMap = new SessionMap(coId, header, undefined, true);

    // Decrypting meta from a non-existent session should return undefined, not throw
    const result = sessionMap.decryptTransactionMeta(
      "nonexistent_session",
      0,
      "keySecret_z11111111111111111111111111111111"
    );

    expect(result).toBeUndefined();
  });
});
