import { describe, expect, it, vi } from "vitest";
import { inspectorAuthorityHeaders } from "./inspector-auth.js";
import { fetchSchemaHashes } from "./schema-fetch.js";
import {
  peerIdentityForWebSocketAuth,
  policyClaimsForAdmittedWebSocket,
} from "./native-runtime/websocket.js";
import { canonicalAuthorSubject } from "./author-id.js";

describe("scoped Inspector authority", () => {
  it("uses its dedicated HTTP header and suppresses upstream error bodies", async () => {
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("credential echo", { status: 403 }));
    try {
      await expect(
        fetchSchemaHashes("https://sync.example", { appId: "sample", inspectorToken: "scoped" }),
      ).rejects.toThrow("403");
      expect(fetcher.mock.calls[0]?.[1]?.headers).toEqual({ "X-Jazz-Inspector-Token": "scoped" });
      await expect(
        fetchSchemaHashes("https://sync.example", { appId: "sample", inspectorToken: "scoped" }),
      ).rejects.not.toThrow("credential echo");
      expect(() =>
        inspectorAuthorityHeaders({ inspectorToken: "scoped", adminSecret: "root" }),
      ).toThrow();
      expect(() => inspectorAuthorityHeaders({})).toThrow();
    } finally {
      fetcher.mockRestore();
    }
  });
  it("uses SYSTEM transport identity without adopting caller policy claims", () => {
    const auth = JSON.stringify({ inspector_token: "scoped" });
    expect(new TextDecoder().decode(peerIdentityForWebSocketAuth(auth, new Uint8Array()))).toBe(
      canonicalAuthorSubject("urn:jazz:system", "system"),
    );
    expect(policyClaimsForAdmittedWebSocket(auth)).toEqual({});
  });
});
