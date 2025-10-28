import { assert, beforeEach, describe, expect, it } from "vitest";
import {
  loadCoValueOrFail,
  setCurrentTestCryptoProvider,
  setupTestNode,
  setupTestAccount,
} from "./testUtils";
import { PureJSCrypto } from "../crypto/PureJSCrypto";
import { stableStringify } from "../jsonStringify";

const jsCrypto = await PureJSCrypto.create();
setCurrentTestCryptoProvider(jsCrypto);

let syncServer: ReturnType<typeof setupTestNode>;

beforeEach(() => {
  syncServer = setupTestNode({ isSyncServer: true });
});

// A suite of tests focused on high-level tests that verify:
// - Keys creation and unsealing
// - Signature creation and verification
// - Encryption and decryption of values
describe("PureJSCrypto", () => {
  it("successfully creates a private CoValue and reads it in another session", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("count", 0, "private");
    map.set("count", 1, "private");
    map.set("count", 2, "private");

    const client2 = client.spawnNewSession();

    const mapInTheOtherSession = await loadCoValueOrFail(client2.node, map.id);

    expect(mapInTheOtherSession.get("count")).toEqual(2);
  });

  it("successfully updates a private CoValue and reads it in another session", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("count", 0, "private");
    map.set("count", 1, "private");
    map.set("count", 2, "private");

    const client2 = client.spawnNewSession();

    const mapInTheOtherSession = await loadCoValueOrFail(client2.node, map.id);
    mapInTheOtherSession.set("count", 3, "private");

    await mapInTheOtherSession.core.waitForSync();

    expect(mapInTheOtherSession.get("count")).toEqual(3);
  });

  it("can invite another account to a group and share a private CoValue", async () => {
    const client = setupTestNode({
      connected: true,
    });
    const account = await setupTestAccount({
      connected: true,
    });

    const group = client.node.createGroup();
    const invite = group.createInvite("admin");

    await account.node.acceptInvite(group.id, invite);

    const map = group.createMap();
    map.set("secret", "private-data", "private");

    // The other account should be able to read the private value
    const mapInOtherSession = await loadCoValueOrFail(account.node, map.id);
    expect(mapInOtherSession.get("secret")).toEqual("private-data");

    mapInOtherSession.set("secret", "modified", "private");

    await mapInOtherSession.core.waitForSync();

    expect(map.get("secret")).toEqual("modified");
  });

  it("rejects sessions with invalid signatures", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("count", 0, "trusting");

    // Create a new session with the same agent
    const client2 = client.spawnNewSession();

    // This should work normally
    const mapInOtherSession = await loadCoValueOrFail(client2.node, map.id);
    expect(mapInOtherSession.get("count")).toEqual(0);

    mapInOtherSession.core.tryAddTransactions(
      client2.node.currentSessionID,
      [
        {
          privacy: "trusting",
          changes: stableStringify([{ op: "set", key: "count", value: 1 }]),
          madeAt: Date.now(),
        },
      ],
      "signature_z12345678",
      true,
    );

    const content =
      mapInOtherSession.core.verified.newContentSince(undefined)?.[0];
    assert(content);

    client.node.syncManager.handleNewContent(content, "storage");

    expect(map.get("count")).toEqual(0);
  });

  it("can add a meta to a private transaction", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();

    map.core.makeTransaction([], "private", {
      meta: {
        count: 1,
      },
    });

    await map.core.waitForSync();

    const session2 = client.spawnNewSession();

    const mapInOtherSession = await loadCoValueOrFail(session2.node, map.id);

    const decryptedMeta =
      mapInOtherSession.core.verified.decryptTransactionMeta(
        client.node.currentSessionID,
        0,
        map.core.getCurrentReadKey().secret!,
      );

    expect(decryptedMeta).toEqual({
      meta: {
        count: 1,
      },
    });
  });

  it("can add a meta to a trusting transaction", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();

    map.core.makeTransaction([], "trusting", {
      meta: {
        count: 1,
      },
    });

    await map.core.waitForSync();

    const session2 = client.spawnNewSession();

    const mapInOtherSession = await loadCoValueOrFail(session2.node, map.id);

    const transferredMeta = JSON.parse(
      mapInOtherSession.core.verified.sessions.get(client.node.currentSessionID)
        ?.transactions[0]?.meta!,
    );

    expect(transferredMeta).toEqual({
      meta: {
        count: 1,
      },
    });
  });
});

describe("PureJSSessionLog", () => {
  it("fails to verify signatures without a signer ID", async () => {
    const agentSecret = jsCrypto.newRandomAgentSecret();
    const sessionID = jsCrypto.newRandomSessionID(
      jsCrypto.getAgentID(agentSecret),
    );

    const sessionLog = jsCrypto.createSessionLog("co_z12345678", sessionID);
    expect(() =>
      sessionLog.tryAdd(
        [
          {
            privacy: "trusting",
            changes: stableStringify([{ op: "set", key: "count", value: 1 }]),
            madeAt: Date.now(),
          },
        ],
        "signature_z12345678",
        false,
      ),
    ).toThrow("Tried to add transactions without signer ID");
  });
});
