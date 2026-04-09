import { beforeAll, describe, expect, it } from "vitest";
import {
  initSelfSignedAuth,
  loadOrCreateIdentitySeed,
  deriveSelfSignedUserId,
  mintSelfSignedToken,
  type SeedStorageLike,
} from "./self-signed-auth.js";

function createMemoryStorage(): SeedStorageLike {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => store.set(key, value),
  };
}

// Deterministic mock bindings that simulate WASM/NAPI behavior.
let seedCounter = 0;
beforeAll(() => {
  seedCounter = 0;
  initSelfSignedAuth({
    generateIdentitySeed: () => {
      seedCounter++;
      // Return a deterministic base64url-encoded "seed" for testing.
      return `mock-seed-b64url-${seedCounter}${"A".repeat(20)}`;
    },
    deriveSelfSignedUserId: (seedB64: string) => {
      // Deterministic UUIDv5-shaped string derived from seed.
      const hash = simpleHash(seedB64);
      return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-5${hash.slice(13, 16)}-a${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
    },
    mintSelfSignedToken: (seedB64: string, audience: string, _ttlSecs?: number) => {
      const header = btoa(JSON.stringify({ alg: "EdDSA", typ: "JWT" }));
      const userId = `user-from-${seedB64.slice(0, 10)}`;
      const payload = btoa(
        JSON.stringify({
          iss: "urn:jazz:self-signed",
          sub: userId,
          aud: audience,
          jazz_pub_key: "mock-pub-key",
          exp: Math.floor(Date.now() / 1000) + 3600,
          iat: Math.floor(Date.now() / 1000),
        }),
      );
      return `${header}.${payload}.mock-signature`;
    },
  });
});

function simpleHash(input: string): string {
  let h = 0;
  for (let i = 0; i < input.length; i++) {
    h = (Math.imul(31, h) + input.charCodeAt(i)) | 0;
  }
  return Math.abs(h).toString(16).padStart(32, "0");
}

describe("self-signed-auth", () => {
  describe("loadOrCreateIdentitySeed", () => {
    it("creates a new seed when none exists", () => {
      const storage = createMemoryStorage();
      const seed = loadOrCreateIdentitySeed("app-1", { storage });
      expect(seed.version).toBe(1);
      expect(seed.seed).toBeTruthy();
      expect(seed.seed.length).toBeGreaterThan(10);
    });

    it("returns the same seed on subsequent calls", () => {
      const storage = createMemoryStorage();
      const seed1 = loadOrCreateIdentitySeed("app-1", { storage });
      const seed2 = loadOrCreateIdentitySeed("app-1", { storage });
      expect(seed1.seed).toBe(seed2.seed);
    });

    it("creates different seeds for different appIds", () => {
      const storage = createMemoryStorage();
      const seed1 = loadOrCreateIdentitySeed("app-1", { storage });
      const seed2 = loadOrCreateIdentitySeed("app-2", { storage });
      expect(seed1.seed).not.toBe(seed2.seed);
    });

    it("works without storage", () => {
      const seed = loadOrCreateIdentitySeed("app-1");
      expect(seed.version).toBe(1);
      expect(seed.seed).toBeTruthy();
    });
  });

  describe("deriveSelfSignedUserId", () => {
    it("returns a UUIDv5-shaped string", () => {
      const storage = createMemoryStorage();
      const seed = loadOrCreateIdentitySeed("app-1", { storage });
      const userId = deriveSelfSignedUserId(seed.seed);
      expect(userId).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[a-f][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
    });

    it("is deterministic for the same seed", () => {
      const storage = createMemoryStorage();
      const seed = loadOrCreateIdentitySeed("app-1", { storage });
      expect(deriveSelfSignedUserId(seed.seed)).toBe(deriveSelfSignedUserId(seed.seed));
    });
  });

  describe("mintSelfSignedToken", () => {
    it("mints a JWT with correct claims", () => {
      const storage = createMemoryStorage();
      const seed = loadOrCreateIdentitySeed("app-1", { storage });
      const token = mintSelfSignedToken(seed.seed, "app-1");

      // Decode payload without verification
      const payloadB64 = token.split(".")[1]!;
      const payload = JSON.parse(atob(payloadB64));

      expect(payload.iss).toBe("urn:jazz:self-signed");
      expect(payload.aud).toBe("app-1");
      expect(payload.jazz_pub_key).toBeTruthy();
      expect(payload.exp).toBeGreaterThan(Math.floor(Date.now() / 1000));
    });
  });
});
