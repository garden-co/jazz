import {
  assert,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";
import { CoValueCore } from "../coValueCore/coValueCore.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { stableStringify } from "../jsonStringify.js";
import { LocalNode } from "../localNode.js";
import {
  agentAndSessionIDFromSecret,
  createTestMetricReader,
  createTestNode,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  nodeWithRandomAgentAndSessionID,
  randomAgentAndSessionID,
  setupTestNode,
  tearDownTestMetricReader,
} from "./testUtils.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import { setMaxTxSizeBytes } from "../config.js";

const Crypto = await WasmCrypto.create();

let metricReader: ReturnType<typeof createTestMetricReader>;
const agentSecret =
  "sealerSecret_zE3Nr7YFr1KkVbJSx4JDCzYn4ApYdm8kJ5ghNBxREHQya/signerSecret_z9fEu4eNG1eXHMak3YSzY7uLdoG8HESSJ8YW4xWdNNDSP";

beforeEach(() => {
  metricReader = createTestMetricReader();
  setupTestNode({ isSyncServer: true });
});

afterEach(() => {
  tearDownTestMetricReader();
});

test("transactions with wrong signature are rejected", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const { transaction, signature } =
    coValue.verified.makeNewTrustingTransaction(
      node.currentSessionID,
      node.getCurrentAgent(),
      [{ hello: "world" }],
      undefined,
      Date.now(),
    );

  transaction.madeAt = Date.now() + 1000;

  // Delete the transaction from the coValue
  node.internalDeleteCoValue(coValue.id);
  node.syncManager.handleNewContent(
    {
      action: "content",
      id: coValue.id,
      header: coValue.verified.header,
      priority: CO_VALUE_PRIORITY.LOW,
      new: {},
    },
    "import",
  );

  const newEntry = node.getCoValue(coValue.id);

  // eslint-disable-next-line neverthrow/must-use-result
  const result = newEntry.tryAddTransactions(
    node.currentSessionID,
    [transaction],
    signature,
  );

  expect(result.isErr()).toBe(true);
  expect(newEntry.getValidSortedTransactions().length).toBe(0);
});

describe("transactions that exceed the byte size limit are rejected", () => {
  beforeEach(() => {
    setMaxTxSizeBytes(1 * 1024);
  });

  afterEach(() => {
    setMaxTxSizeBytes(1 * 1024 * 1024);
  });

  test("makeTransaction should throw error when transaction exceeds byte size limit", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const largeBinaryData = "x".repeat(1024 + 100);

    expect(() => {
      coValue.makeTransaction(
        [
          {
            data: largeBinaryData,
          },
        ],
        "trusting",
      );
    }).toThrow(/Transaction is too large to be synced/);
  });

  test("makeTransaction should work for transactions under byte size limit", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const smallData = "Hello, world!";

    const success = coValue.makeTransaction(
      [
        {
          data: smallData,
        },
      ],
      "trusting",
    );

    expect(success).toBe(true);
  });
});

test("New transactions in a group correctly update owned values, including subscriptions", async () => {
  const [agent, sessionID] = randomAgentAndSessionID();
  const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

  const timeBeforeEdit = Date.now() - 1000;
  const dateNowMock = vi
    .spyOn(Date, "now")
    .mockImplementation(() => timeBeforeEdit);

  const group = node.createGroup();

  await new Promise((resolve) => setTimeout(resolve, 10));

  const map = group.createMap();

  map.set("hello", "world");

  const listener = vi.fn();

  map.subscribe((map) => {
    listener(map.get("hello"));
  });

  expect(listener).toHaveBeenLastCalledWith("world");

  expect(map.core.getValidSortedTransactions().length).toBe(1);
  expect(group.get(agent.id)).toBe("admin");

  group.core.makeTransaction(
    [
      {
        op: "set",
        key: agent.id,
        value: "revoked",
      },
    ],
    "trusting",
  );

  expect(group.get(agent.id)).toBe("revoked");
  dateNowMock.mockReset();

  expect(listener).toHaveBeenCalledTimes(2);
  expect(listener).toHaveBeenLastCalledWith(undefined);
  expect(map.core.getValidSortedTransactions().length).toBe(0);
});

test("correctly records transactions", async () => {
  const node = nodeWithRandomAgentAndSessionID();

  const changes1 = stableStringify([{ hello: "world" }]);
  node.syncManager.recordTransactionsSize(
    [
      {
        privacy: "trusting",
        changes: changes1,
        madeAt: Date.now(),
      },
    ],
    "server",
  );

  let value = await metricReader.getMetricValue("jazz.transactions.size", {
    source: "server",
  });
  assert(typeof value !== "number" && !!value?.count);
  expect(value.count).toBe(1);
  expect(value.sum).toBe(changes1.length);

  const changes2 = stableStringify([{ foo: "bar" }]);
  node.syncManager.recordTransactionsSize(
    [
      {
        privacy: "trusting",
        changes: changes2,
        madeAt: Date.now(),
      },
    ],
    "server",
  );

  value = await metricReader.getMetricValue("jazz.transactions.size", {
    source: "server",
  });
  assert(typeof value !== "number" && !!value?.count);
  expect(value.count).toBe(2);
  expect(value.sum).toBe(changes1.length + changes2.length);
});

test("(smoke test) records transactions from local node", async () => {
  const node = nodeWithRandomAgentAndSessionID();

  node.createGroup();

  let value = await metricReader.getMetricValue("jazz.transactions.size", {
    source: "local",
  });

  assert(typeof value !== "number" && !!value?.count);
  expect(value.count).toBe(3);
});

test("creating a coValue with a group should't trigger automatically a content creation (performance)", () => {
  const node = createTestNode();

  const group = node.createGroup();

  const getCurrentContentSpy = vi.spyOn(
    CoValueCore.prototype,
    "getCurrentContent",
  );
  const groupSpy = vi.spyOn(group.core, "getCurrentContent");

  getCurrentContentSpy.mockClear();

  node.createCoValue({
    type: "comap",
    ruleset: { type: "ownedByGroup", group: group.id },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  // It's called once for the group and never for the coValue
  expect(getCurrentContentSpy).toHaveBeenCalledTimes(0);
  expect(groupSpy).toHaveBeenCalledTimes(0);

  getCurrentContentSpy.mockRestore();
});

test("loading a coValue core without having the owner group available doesn't crash", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const otherNode = createTestNode();

  const group = otherNode.createGroup();

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "ownedByGroup", group: group.id },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  expect(coValue.id).toBeDefined();
});

test("listeners are notified even if the previous listener threw an error", async () => {
  const { node1, node2 } = await createTwoConnectedNodes("server", "server");

  const group = node1.node.createGroup();
  group.addMember("everyone", "writer");

  const coMap = group.createMap();

  const spy1 = vi.fn();
  const spy2 = vi.fn();

  coMap.subscribe(spy1);
  coMap.subscribe(spy2);

  spy1.mockImplementation(() => {
    throw new Error("test");
  });

  const errorLog = vi.spyOn(console, "error").mockImplementation(() => {});

  coMap.set("hello", "world");

  expect(spy1).toHaveBeenCalledTimes(2);
  expect(spy2).toHaveBeenCalledTimes(2);
  expect(errorLog).toHaveBeenCalledTimes(1);

  await coMap.core.waitForSync();

  const mapOnNode2 = await loadCoValueOrFail(node2.node, coMap.id);

  expect(mapOnNode2.get("hello")).toBe("world");

  errorLog.mockRestore();
});

test("creates a transaction with trusting meta information", async () => {
  const client = setupTestNode();

  const group = client.node.createGroup();
  const map = group.createMap();
  map.core.makeTransaction([], "trusting", {
    meta: true,
  });

  expect(map.core.verifiedTransactions[0]?.tx.meta).toBe(`{"meta":true}`);
  expect(map.core.verifiedTransactions[0]?.meta).toEqual({ meta: true });
});

test("creates a transaction with private meta information", async () => {
  const client = setupTestNode({ connected: true });

  const group = client.node.createGroup();
  const map = group.createMap();
  map.core.makeTransaction([], "private", {
    meta: true,
  });

  const localTransactionMeta = map.core.verified.decryptTransactionMeta(
    client.node.currentSessionID,
    0,
    map.core.getCurrentReadKey().secret!,
  );

  expect(localTransactionMeta).toEqual({ meta: true });

  const newSession = client.spawnNewSession();

  const mapOnNewSession = await loadCoValueOrFail(newSession.node, map.id);

  const syncedTransactionMeta =
    mapOnNewSession.core.verified.decryptTransactionMeta(
      client.node.currentSessionID,
      0,
      mapOnNewSession.core.getCurrentReadKey().secret!,
    );

  expect(syncedTransactionMeta).toEqual({ meta: true });
});

test("getValidTransactions should skip private transactions with invalid JSON", () => {
  const [agent, sessionID] = agentAndSessionIDFromSecret(agentSecret);
  const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

  const fixtures = {
    id: "co_zWwrEiushQLvbkWd6Z3L8WxTU1r",
    signature:
      "signature_z3ktW7wxMnW7VYExCGZv4Ug2UJSW3ag6zLDiP8GpZThzif6veJt7JipYpUgshhuGbgHtLcWywWSWysV7hChxFypDt",
    decrypted:
      '[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]',
    key: {
      secret: "keySecret_z3dU66SsyQkkGKpNCJW6NX74MnfVGHUyY7r85b4M8X88L",
      id: "key_z5XUAHyoqUV9zXWvMK",
    },
    transaction: {
      privacy: "private",
      madeAt: 0,
      encryptedChanges:
        "encrypted_UNAxqdUSGRZ2rzuLU99AFPKCe2C0HwsTzMWQreXZqLr6RpWrSMa-5lwgwIev7xPHTgZFq5UyUgMFrO9zlHJHJGgjJcDzFihY=" as any,
      keyUsed: "key_z5XUAHyoqUV9zXWvMK",
    },
    session:
      "sealer_z5yhsCCe2XwLTZC4254mUoMASshm3Diq49JrefPpjTktp/signer_z7gVGDpNz9qUtsRxAkHMuu4DYdtVVCG4XELTKPYdoYLPr_session_z9mDP8FoonSA",
  } as const;

  const group = node.createGroup();
  const map = group.createMap();

  map.set("hello", "world");

  // This should fail silently, because the encryptedChanges will be outputted as gibberish
  map.core
    .tryAddTransactions(
      fixtures.session,
      [fixtures.transaction],
      fixtures.signature,
    )
    ._unsafeUnwrap();

  // Get valid transactions - should only include the valid one
  const validTransactions = map.core.getValidTransactions();

  expect(validTransactions).toHaveLength(1);
});

describe("markErrored and isErroredInPeer", () => {
  test("markErrored should mark a peer as errored with the provided error", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peerId = "test-peer-1";
    const testError = {
      type: "InvalidSignature" as const,
      id: coValue.id,
      newSignature: "invalid-signature" as any,
      sessionID: sessionID,
      signerID: "test-signer" as any,
    };

    // Initially, the peer should not be errored
    expect(coValue.isErroredInPeer(peerId)).toBe(false);

    // Mark the peer as errored
    coValue.markErrored(peerId, testError);

    // Verify the peer is now marked as errored
    expect(coValue.isErroredInPeer(peerId)).toBe(true);

    // Verify the peer state contains the error
    const peerState = coValue.getStateForPeer(peerId);
    expect(peerState).toBeDefined();
    expect(peerState?.type).toBe("errored");
  });

  test("markErrored should update loading state and notify listeners", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peerId = "test-peer-2";
    const testError = {
      type: "InvalidHash" as const,
      id: coValue.id,
      expectedNewHash: "expected-hash" as any,
      givenExpectedNewHash: "given-hash" as any,
    };

    const listener = vi.fn();
    coValue.subscribe(listener);

    // Mark the peer as errored
    coValue.markErrored(peerId, testError);

    // Verify the listener was called
    expect(listener).toHaveBeenCalled();
  });

  test("isErroredInPeer should return false for non-existent peers", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const nonExistentPeerId = "non-existent-peer";

    // Verify non-existent peer is not errored
    expect(coValue.isErroredInPeer(nonExistentPeerId)).toBe(false);
  });

  test("isErroredInPeer should return false for peers with other states", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peerId = "test-peer-3";

    // Mark peer as pending
    coValue.markPending(peerId);
    expect(coValue.isErroredInPeer(peerId)).toBe(false);

    // Mark peer as unavailable
    coValue.markNotFoundInPeer(peerId);
    expect(coValue.isErroredInPeer(peerId)).toBe(false);

    // Mark peer as available
    coValue.provideHeader({} as any, peerId);
    expect(coValue.isErroredInPeer(peerId)).toBe(false);
  });

  test("markErrored should work with multiple peers", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peer1Id = "peer-1";
    const peer2Id = "peer-2";
    const peer3Id = "peer-3";

    const error1 = {
      type: "InvalidSignature" as const,
      id: coValue.id,
      newSignature: "invalid-signature-1" as any,
      sessionID: sessionID,
      signerID: "test-signer-1" as any,
    };

    const error2 = {
      type: "InvalidHash" as const,
      id: coValue.id,
      expectedNewHash: "expected-hash-2" as any,
      givenExpectedNewHash: "given-hash-2" as any,
    };

    // Mark different peers as errored
    coValue.markErrored(peer1Id, error1);
    coValue.markErrored(peer2Id, error2);

    // Verify each peer is correctly marked as errored
    expect(coValue.isErroredInPeer(peer1Id)).toBe(true);
    expect(coValue.isErroredInPeer(peer2Id)).toBe(true);
    expect(coValue.isErroredInPeer(peer3Id)).toBe(false);
  });

  test("markErrored should override previous peer states", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peerId = "test-peer-4";

    // Initially mark as pending
    coValue.markPending(peerId);
    expect(coValue.isErroredInPeer(peerId)).toBe(false);

    // Then mark as errored
    const testError = {
      type: "TriedToAddTransactionsWithoutVerifiedState" as const,
      id: coValue.id,
    };

    coValue.markErrored(peerId, testError);

    // Verify the peer is now errored
    expect(coValue.isErroredInPeer(peerId)).toBe(true);

    const peerState = coValue.getStateForPeer(peerId);
    expect(peerState?.type).toBe("errored");
  });

  test("markErrored should work with different error types", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peerId = "test-peer-5";

    // Test with InvalidSignature error
    const invalidSignatureError = {
      type: "InvalidSignature" as const,
      id: coValue.id,
      newSignature: "invalid-sig" as any,
      sessionID: sessionID,
      signerID: "test-signer" as any,
    };

    coValue.markErrored(peerId, invalidSignatureError);
    expect(coValue.isErroredInPeer(peerId)).toBe(true);

    // Test with InvalidHash error
    const invalidHashError = {
      type: "InvalidHash" as const,
      id: coValue.id,
      expectedNewHash: "expected" as any,
      givenExpectedNewHash: "given" as any,
    };

    coValue.markErrored(peerId, invalidHashError);
    expect(coValue.isErroredInPeer(peerId)).toBe(true);

    // Test with TriedToAddTransactionsWithoutVerifiedState error
    const noVerifiedStateError = {
      type: "TriedToAddTransactionsWithoutVerifiedState" as const,
      id: coValue.id,
    };

    coValue.markErrored(peerId, noVerifiedStateError);
    expect(coValue.isErroredInPeer(peerId)).toBe(true);
  });

  test("markErrored should trigger immediate notification", () => {
    const [agent, sessionID] = randomAgentAndSessionID();
    const node = new LocalNode(agent.agentSecret, sessionID, Crypto);

    const coValue = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const peerId = "test-peer-6";
    const testError = {
      type: "InvalidSignature" as const,
      id: coValue.id,
      newSignature: "test-sig" as any,
      sessionID: sessionID,
      signerID: "test-signer" as any,
    };

    let notificationCount = 0;
    const listener = () => {
      notificationCount++;
    };

    coValue.subscribe(listener);

    // Mark as errored
    coValue.markErrored(peerId, testError);

    // Verify immediate notification
    expect(notificationCount).toBeGreaterThan(0);
  });
});
