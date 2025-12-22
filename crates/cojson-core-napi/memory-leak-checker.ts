import { base58 } from "@scure/base";

import {
  Blake3Hasher,
  SessionLog,
  blake3HashOnce,
  blake3HashOnceWithContext,
  decrypt,
  decryptXsalsa20,
  ed25519Sign,
  ed25519SignatureFromBytes,
  ed25519SigningKeyFromBytes,
  ed25519SigningKeySign,
  ed25519SigningKeyToPublic,
  ed25519VerifyingKey,
  ed25519VerifyingKeyFromBytes,
  ed25519Verify,
  encrypt,
  encryptXsalsa20,
  generateNonce,
  getSealerId,
  getSignerId,
  newEd25519SigningKey,
  newX25519PrivateKey,
  seal,
  sign,
  unseal,
  verify,
  x25519DiffieHellman,
  x25519PublicKey,
} from "./index.js";
import { pathToFileURL } from "node:url";

type LeakScenario = {
  name: string;
  iterations?: number;
  action: (iteration: number) => void;
};

const encoder = new TextEncoder();

const DEFAULT_ITERATIONS = Number(process.env.LEAK_ITERATIONS ?? "100000");
const COMPLEX_ITERATIONS = Number(process.env.LEAK_COMPLEX_ITERATIONS ?? "100000");
const ALLOWED_GROWTH_MB = Number(process.env.LEAK_TOLERANCE_MB ?? "8");

function forceGc() {
  if (typeof global.gc === "function") {
    // A couple of passes stabilise the heap before measuring
    global.gc();
    global.gc();
  }
}

function rssUsedMb() {
  return process.memoryUsage().rss / 1024 / 1024;
}

function runScenario(scenario: LeakScenario) {
  const iterations = scenario.iterations ?? DEFAULT_ITERATIONS;

  // Warmup to avoid counting JIT allocations as leaks
  for (let i = 0; i < Math.min(20, iterations); i += 1) {
    scenario.action(i);
  }

  forceGc();
  const start = rssUsedMb();

  for (let i = 0; i < iterations; i += 1) {
    scenario.action(i);
  }

  forceGc();
  const end = rssUsedMb();
  const growth = end - start;

  const status = growth > ALLOWED_GROWTH_MB ? "LEAK?" : "ok";
  console.log(
    `[${status}] ${scenario.name.padEnd(32)} | iterations=${iterations} | Δrss=${growth.toFixed(2)} MB`,
  );

  return { name: scenario.name, growth, iterations };
}

function makeKeySecretBytes(label: string, size = 32) {
  const buf = new Uint8Array(size);
  buf[0] = label.length % 255;
  buf[buf.length - 1] = 1;
  return buf;
}

function sessionMaterials() {
  const signingKey = newEd25519SigningKey();
  const verifyingKey = ed25519VerifyingKey(signingKey);
  const signerSecret = `signerSecret_z${base58.encode(signingKey)}`;
  const signerId = `signer_z${base58.encode(verifyingKey)}`;

  const keySecretBytes = makeKeySecretBytes("encryption");
  const keySecret = `keySecret_z${base58.encode(keySecretBytes)}`;
  const keyId = `key_z${base58.encode(keySecretBytes)}`;

  const coId = `co_z${base58.encode(makeKeySecretBytes("coid"))}`;
  const sessionId = `session_z${base58.encode(makeKeySecretBytes("session"))}`;

  return { signerSecret, signerId, keySecret, keyId, coId, sessionId };
}

const nonceMaterial = encoder.encode("memory-leak-check");
const plaintext = encoder.encode("Hello from the memory leak checker!");
const xsalsaKey = makeKeySecretBytes("xsalsa");
const xsalsaNonce = encoder.encode("nonce-xsalsa");
const blakeData = encoder.encode("blake-data");
const blakeContext = encoder.encode("blake-context");

const scenarios: LeakScenario[] = [
  {
    name: "blake3HashOnce",
    action: () => {
      blake3HashOnce(blakeData);
    },
  },
  {
    name: "blake3HashOnceWithContext",
    action: () => {
      blake3HashOnceWithContext(blakeData, blakeContext);
    },
  },
  {
    name: "Blake3Hasher lifecycle",
    action: () => {
      const hasher = new Blake3Hasher();
      hasher.update(blakeData);
      const cloned = hasher.clone();
      cloned.update(blakeContext);
      hasher.finalize();
      cloned.finalize();
    },
  },
  {
    name: "ed25519 signing/verify",
    action: (iteration) => {
      const signingKey = newEd25519SigningKey();
      const verifyingKey = ed25519VerifyingKey(signingKey);
      const message = encoder.encode(`ed25519-${iteration}`);
      const signature = ed25519Sign(signingKey, message);

      ed25519Verify(verifyingKey, message, signature);
      ed25519SigningKeyFromBytes(signingKey);
      ed25519VerifyingKeyFromBytes(verifyingKey);
      ed25519SignatureFromBytes(signature);
      ed25519SigningKeyToPublic(signingKey);
      ed25519SigningKeySign(signingKey, message);
    },
  },
  {
    name: "encrypt/decrypt (keySecret)",
    action: () => {
      const keyBytes = makeKeySecretBytes("keySecret");
      const keySecret = `keySecret_z${base58.encode(keyBytes)}`;
      const cipher = encrypt(plaintext, keySecret, nonceMaterial);
      decrypt(cipher, keySecret, nonceMaterial);
    },
  },
  {
    name: "xsalsa20 encrypt/decrypt",
    action: () => {
      const ciphertext = encryptXsalsa20(xsalsaKey, xsalsaNonce, plaintext);
      decryptXsalsa20(xsalsaKey, xsalsaNonce, ciphertext);
    },
  },
  {
    name: "generateNonce",
    action: (iteration) => {
      generateNonce(encoder.encode(`nonce-${iteration}`));
    },
  },
  {
    name: "getSignerId/getSealerId",
    action: (iteration) => {
      const signingKey = newEd25519SigningKey();
      const signerSecret = encoder.encode(`signerSecret_z${base58.encode(signingKey)}`);
      getSignerId(signerSecret);

      const sealerKey = newX25519PrivateKey();
      const sealerSecret = encoder.encode(`sealerSecret_z${base58.encode(sealerKey)}`);
      getSealerId(sealerSecret);
    },
  },
  {
    name: "x25519 keys + diffie-hellman",
    action: () => {
      const privateA = newX25519PrivateKey();
      const privateB = newX25519PrivateKey();
      const publicA = x25519PublicKey(privateA);
      const publicB = x25519PublicKey(privateB);
      x25519DiffieHellman(privateA, publicB);
      x25519DiffieHellman(privateB, publicA);
    },
  },
  {
    name: "seal/unseal",
    action: (iteration) => {
      const senderSecretBytes = newX25519PrivateKey();
      const recipientSecretBytes = newX25519PrivateKey();
      const senderSecret = `sealerSecret_z${base58.encode(senderSecretBytes)}`;
      const recipientSecret = `sealerSecret_z${base58.encode(recipientSecretBytes)}`;
      const senderId = `sealer_z${base58.encode(x25519PublicKey(senderSecretBytes))}`;
      const recipientId = `sealer_z${base58.encode(x25519PublicKey(recipientSecretBytes))}`;
      const message = encoder.encode(`seal-message-${iteration}`);
      const nonce = encoder.encode(`seal-nonce-${iteration % 16}`);

      const sealed = seal(message, senderSecret, recipientId, nonce);
      unseal(sealed, recipientSecret, senderId, nonce);
    },
  },
  {
    name: "sign/verify wrappers",
    action: (iteration) => {
      const signingKey = newEd25519SigningKey();
      const signerSecret = encoder.encode(`signerSecret_z${base58.encode(signingKey)}`);
      const signerId = encoder.encode(`signer_z${base58.encode(ed25519VerifyingKey(signingKey))}`);
      const message = encoder.encode(`wrapped-message-${iteration}`);

      const signatureStr = sign(message, signerSecret);
      verify(encoder.encode(signatureStr), message, signerId);
    },
  },
  {
    name: "SessionLog end-to-end",
    iterations: COMPLEX_ITERATIONS,
    action: (iteration) => {
      const { signerSecret, signerId, keySecret, keyId, coId, sessionId } = sessionMaterials();
      const session = new SessionLog(coId, `${sessionId}-${iteration}`, signerId);

      const privateMeta = JSON.stringify({ m: iteration });
      session.addNewPrivateTransaction(
        JSON.stringify({ value: iteration, note: "private" }),
        signerSecret,
        keySecret,
        keyId,
        Date.now(),
        privateMeta,
      );

      session.decryptNextTransactionChangesJson(0, keySecret);
      session.decryptNextTransactionMetaJson(0, keySecret);

      const trustingSignature = session.addNewTrustingTransaction(
        JSON.stringify({ value: iteration, note: "trusting" }),
        signerSecret,
        Date.now(),
        JSON.stringify({ meta: "trust" }),
      );

      const trustingTxJson = JSON.stringify({
        changes: `{"value":${iteration},"note":"external"}`,
        madeAt: Date.now(),
        meta: null,
        privacy: "trusting",
      });

      session.tryAdd([trustingTxJson], trustingSignature, true);
      session.clone();
    },
  },
];

export function checkMemoryLeak() {
  if (typeof global.gc !== "function") {
    console.warn("Run with `node --expose-gc` to get reliable heap readings.");
  }

  console.log(`Running ${scenarios.length} leak scenarios (tolerance ${ALLOWED_GROWTH_MB} MB)...`);
  const results = scenarios.map(runScenario);
  const regressions = results.filter((r) => r.growth > ALLOWED_GROWTH_MB);

  if (regressions.length > 0) {
    console.warn("\nPotential leaks detected:");
    regressions.forEach((r) =>
      console.warn(` - ${r.name}: Δrss=${r.growth.toFixed(2)} MB after ${r.iterations} iterations`),
    );
  } else {
    console.log("\nAll scenarios stayed within tolerance.");
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  checkMemoryLeak();
}
