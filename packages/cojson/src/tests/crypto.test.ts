import { xsalsa20poly1305 } from "@noble/ciphers/salsa";
import { x25519 } from "@noble/curves/ed25519";
import { blake3 } from "@noble/hashes/blake3";
import { base58, base64url } from "@scure/base";
import { expect, test, vi } from "vitest";
import { shortHashLength } from "../crypto/crypto.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { SessionID } from "../ids.js";
import { stableStringify } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";

const crypto = await WasmCrypto.create();

const name = crypto.constructor.name;

test(`Signatures round-trip and use stable stringify [${name}]`, () => {
  const data = { b: "world", a: "hello" };
  const signer = crypto.newRandomSigner();
  const signature = crypto.sign(signer, data);

  expect(signature).toMatch(/^signature_z/);
  expect(
    crypto.verify(
      signature,
      { a: "hello", b: "world" },
      crypto.getSignerID(signer),
    ),
  ).toBe(true);
});

test(`Invalid signatures don't verify [${name}]`, () => {
  const data = { b: "world", a: "hello" };
  const signer = crypto.newRandomSigner();
  const signer2 = crypto.newRandomSigner();
  const wrongSignature = crypto.sign(signer2, data);

  expect(crypto.verify(wrongSignature, data, crypto.getSignerID(signer))).toBe(
    false,
  );
});

test(`encrypting round-trips, but invalid receiver can't unseal [${name}]`, () => {
  const data = { b: "world", a: "hello" };
  const sender = crypto.newRandomSealer();
  const sealer = crypto.newRandomSealer();
  const wrongSealer = crypto.newRandomSealer();

  const nOnceMaterial = {
    in: "co_zTEST",
    tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
  } as const;

  const sealed = crypto.seal({
    message: data,
    from: sender,
    to: crypto.getSealerID(sealer),
    nOnceMaterial,
  });

  expect(
    crypto.unseal(sealed, sealer, crypto.getSealerID(sender), nOnceMaterial),
  ).toEqual(data);
  expect(() =>
    crypto.unseal(
      sealed,
      wrongSealer,
      crypto.getSealerID(sender),
      nOnceMaterial,
    ),
  ).toThrow("Wrong tag");

  // trying with wrong sealer secret, by hand
  const nOnce = blake3(
    new TextEncoder().encode(stableStringify(nOnceMaterial)),
  ).slice(0, 24);
  const sealer3priv = base58.decode(
    wrongSealer.substring("sealerSecret_z".length),
  );
  const senderPub = base58.decode(
    crypto.getSealerID(sender).substring("sealer_z".length),
  );
  const sealedBytes = base64url.decode(sealed.substring("sealed_U".length));
  const sharedSecret = x25519.getSharedSecret(sealer3priv, senderPub);

  expect(() => {
    const _ = xsalsa20poly1305(sharedSecret, nOnce).decrypt(sealedBytes);
  }).toThrow("invalid tag");
});

test(`Hashing is deterministic [${name}]`, () => {
  expect(crypto.secureHash({ b: "world", a: "hello" })).toEqual(
    crypto.secureHash({ a: "hello", b: "world" }),
  );

  expect(crypto.shortHash({ b: "world", a: "hello" })).toEqual(
    crypto.shortHash({ a: "hello", b: "world" }),
  );
});

test(`Encryption of keySecrets round-trips [${name}]`, () => {
  const toEncrypt = crypto.newRandomKeySecret();
  const encrypting = crypto.newRandomKeySecret();

  const keys = {
    toEncrypt,
    encrypting,
  };

  const encrypted = crypto.encryptKeySecret(keys);

  const decrypted = crypto.decryptKeySecret(encrypted, encrypting.secret);

  expect(decrypted).toEqual(toEncrypt.secret);
});

test(`Encryption of keySecrets doesn't decrypt with a wrong key [${name}]`, () => {
  const toEncrypt = crypto.newRandomKeySecret();
  const encrypting = crypto.newRandomKeySecret();
  const encryptingWrong = crypto.newRandomKeySecret();

  const keys = {
    toEncrypt,
    encrypting,
  };

  const encrypted = crypto.encryptKeySecret(keys);

  const decrypted = crypto.decryptKeySecret(encrypted, encryptingWrong.secret);

  expect(decrypted).toBeUndefined();
});

test(`Unsealing malformed JSON logs error [${name}]`, () => {
  const data = "not valid json";
  const sender = crypto.newRandomSealer();
  const sealer = crypto.newRandomSealer();

  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

  const nOnceMaterial = {
    in: "co_zTEST",
    tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
  } as const;

  // Create a sealed message with invalid JSON
  const nOnce = blake3(
    new TextEncoder().encode(stableStringify(nOnceMaterial)),
  ).slice(0, 24);

  const senderPriv = base58.decode(sender.substring("sealerSecret_z".length));
  const sealerPub = base58.decode(
    crypto.getSealerID(sealer).substring("sealer_z".length),
  );

  const plaintext = new TextEncoder().encode(data);
  const sharedSecret = x25519.getSharedSecret(senderPriv, sealerPub);
  const sealedBytes = xsalsa20poly1305(sharedSecret, nOnce).encrypt(plaintext);
  const sealed = `sealed_U${base64url.encode(sealedBytes)}`;

  const result = crypto.unseal(
    sealed as any,
    sealer,
    crypto.getSealerID(sender),
    nOnceMaterial,
  );

  expect(result).toBeUndefined();
  expect(consoleSpy.mock.lastCall?.[0]).toContain(
    "Failed to decrypt/parse sealed message",
  );
});

// ============================================================================
// shortHash implementation consistency tests
// ============================================================================

/**
 * Compute shortHash using the base TypeScript implementation (reference).
 * This mirrors the CryptoProvider.shortHash method exactly.
 */
function referenceShortHash(value: JsonValue): string {
  const textEncoder = new TextEncoder();
  return `shortHash_z${base58.encode(
    blake3(textEncoder.encode(stableStringify(value))).slice(
      0,
      shortHashLength,
    ),
  )}`;
}

test(`shortHash WASM implementation matches TypeScript reference [${name}]`, () => {
  // Test with simple object
  const simpleObj = { hello: "world" };
  expect(crypto.shortHash(simpleObj)).toBe(referenceShortHash(simpleObj));

  // Test with object where key order differs (should produce same hash due to stableStringify)
  const objA = { b: "world", a: "hello" };
  const objB = { a: "hello", b: "world" };
  expect(crypto.shortHash(objA)).toBe(referenceShortHash(objA));
  expect(crypto.shortHash(objB)).toBe(referenceShortHash(objB));
  expect(crypto.shortHash(objA)).toBe(crypto.shortHash(objB));

  // Test with nested objects
  const nestedObj = { outer: { inner: { deep: "value" } }, top: 123 };
  expect(crypto.shortHash(nestedObj)).toBe(referenceShortHash(nestedObj));

  // Test with arrays
  const arrayValue = [1, 2, 3, "four", { five: 5 }];
  expect(crypto.shortHash(arrayValue)).toBe(referenceShortHash(arrayValue));

  // Test with primitives
  expect(crypto.shortHash("string")).toBe(referenceShortHash("string"));
  expect(crypto.shortHash(42)).toBe(referenceShortHash(42));
  expect(crypto.shortHash(true)).toBe(referenceShortHash(true));
  expect(crypto.shortHash(null)).toBe(referenceShortHash(null));

  // Test with empty structures
  expect(crypto.shortHash({})).toBe(referenceShortHash({}));
  expect(crypto.shortHash([])).toBe(referenceShortHash([]));
});

test(`shortHash produces correct format [${name}]`, () => {
  const value = { test: "data" };
  const hash = crypto.shortHash(value);

  // Should start with "shortHash_z" prefix
  expect(hash).toMatch(/^shortHash_z/);

  // The base58 encoded part should be reasonable length (19 bytes base58 encoded)
  const base58Part = hash.substring("shortHash_z".length);
  expect(base58Part.length).toBeGreaterThan(0);

  // Should only contain valid base58 characters
  expect(base58Part).toMatch(
    /^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]+$/,
  );
});

test(`shortHash with complex nested structure matches reference [${name}]`, () => {
  const complexValue = {
    users: [
      { id: 1, name: "Alice", active: true },
      { id: 2, name: "Bob", active: false },
    ],
    metadata: {
      created: 1234567890,
      tags: ["important", "test"],
      config: {
        nested: {
          deeply: {
            value: "found",
          },
        },
      },
    },
    nullField: null,
    emptyArray: [],
    emptyObject: {},
  };

  expect(crypto.shortHash(complexValue)).toBe(referenceShortHash(complexValue));
});

test(`shortHash with special characters matches reference [${name}]`, () => {
  // Test with unicode
  const unicodeValue = { emoji: "🎉", chinese: "你好", arabic: "مرحبا" };
  expect(crypto.shortHash(unicodeValue)).toBe(referenceShortHash(unicodeValue));

  // Test with escape sequences
  const escapeValue = {
    quote: 'has "quotes"',
    newline: "has\nnewline",
    tab: "has\ttab",
  };
  expect(crypto.shortHash(escapeValue)).toBe(referenceShortHash(escapeValue));
});
