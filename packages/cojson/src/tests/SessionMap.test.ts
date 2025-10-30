import { describe, expect, it, beforeEach } from "vitest";
import { NapiCrypto } from "../crypto/NapiCrypto.js";
import { setCurrentTestCryptoProvider, setupTestNode } from "./testUtils.js";
import { stableStringify } from "../jsonStringify.js";

const napiCrypto = await NapiCrypto.create();
setCurrentTestCryptoProvider(napiCrypto);

let syncServer: ReturnType<typeof setupTestNode>;

beforeEach(() => {
  syncServer = setupTestNode({ isSyncServer: true });
});

describe("SessionMap - Error Handling", () => {
  it("should return error when adding transaction with invalid signature", () => {
    const client = setupTestNode({ connected: true });
    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("test", "value", "trusting");

    const result = map.core.tryAddTransactions(
      client.node.currentSessionID,
      [
        {
          privacy: "trusting",
          changes: stableStringify([{ op: "set", key: "test", value: "bad" }]),
          madeAt: Date.now(),
        },
      ],
      "signature_z12345678901234567890123456789012345678901234567890" as any,
      false, // don't skip verify
    );

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error.type).toBe("InvalidSignature");
      if (result.error.type === "InvalidSignature") {
        expect(result.error.id).toBe(map.id);
        expect(result.error.sessionID).toBe(client.node.currentSessionID);
        expect(result.error.error).toBeDefined();
      }
    }
  });

  it("should throw error when SessionLogImpl.tryAdd fails with invalid signature", () => {
    const client = setupTestNode({ connected: true });
    const group = client.node.createGroup();
    const map = group.createMap();

    const sessionMap = map.core.verified.sessions;

    const result = sessionMap.addTransaction(
      client.node.currentSessionID,
      undefined,
      [
        {
          privacy: "trusting",
          changes: stableStringify([{ op: "set", key: "key", value: "value" }]),
          madeAt: Date.now(),
        },
      ],
      "signature_zinvalidsignature1234567890123456789012345678901234" as any,
      false,
    );

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error.type).toBe("InvalidSignature");
      if (result.error.type === "InvalidSignature") {
        expect(result.error.error).toBeDefined();
      }
    }
  });

  it("should fail to verify signatures without proper signer ID", () => {
    const agentSecret = napiCrypto.newRandomAgentSecret();
    const sessionID = napiCrypto.newRandomSessionID(
      napiCrypto.getAgentID(agentSecret),
    );

    const sessionLog = napiCrypto.createSessionLog(
      "co_z12345678901234567890123456789012345678901234567890",
      sessionID,
    );

    expect(() =>
      sessionLog.tryAdd(
        [
          {
            privacy: "trusting",
            changes: stableStringify([{ op: "set", key: "count", value: 1 }]),
            madeAt: Date.now(),
          },
        ],
        "signature_z12345678901234567890123456789012345678901234567890" as any,
        false,
      ),
    ).toThrow(
      expect.objectContaining({
        message: expect.stringContaining("Signature verification failed"),
      }),
    );
  });

  it("should fail verification with completely invalid signature format", () => {
    const client = setupTestNode({ connected: true });
    const group = client.node.createGroup();
    const map = group.createMap();

    const result = map.core.tryAddTransactions(
      client.node.currentSessionID,
      [
        {
          privacy: "trusting",
          changes: stableStringify([
            { op: "set", key: "test", value: "value" },
          ]),
          madeAt: Date.now(),
        },
      ],
      "signature_zthisisnotavalidsignatureatallxxxxxxxxxxxxxxxxx" as any,
      false,
    );

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error.type).toBe("InvalidSignature");
    }
  });
});
