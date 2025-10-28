import { describe, expect, test } from "vitest";
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
} from '../index';

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
