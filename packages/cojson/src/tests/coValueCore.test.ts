import { assert, afterEach, beforeEach, expect, test, vi } from "vitest";
import { CoValueCore, Transaction } from "../coValueCore.js";
import { MapOpPayload } from "../coValues/coMap.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { stableStringify } from "../jsonStringify.js";
import { LocalNode } from "../localNode.js";
import { Role } from "../permissions.js";
import {
  createTestMetricReader,
  createTestNode,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  randomAnonymousAccountAndSessionID,
  tearDownTestMetricReader,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

let metricReader: ReturnType<typeof createTestMetricReader>;

beforeEach(() => {
  metricReader = createTestMetricReader();
});

afterEach(() => {
  tearDownTestMetricReader();
});

test("Can create coValue with new agent credentials and add transaction to it", () => {
  const [account, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(account, sessionID, Crypto);

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const transaction: Transaction = {
    privacy: "trusting",
    madeAt: Date.now(),
    changes: stableStringify([
      {
        hello: "world",
      },
    ]),
  };

  const { expectedNewHash } = coValue.expectedNewHashAfter(
    node.currentSessionID,
    [transaction],
  );

  expect(
    coValue
      .tryAddTransactions(
        node.currentSessionID,
        [transaction],
        expectedNewHash,
        Crypto.sign(account.currentSignerSecret(), expectedNewHash),
      )
      ._unsafeUnwrap(),
  ).toBe(true);
});

test("transactions with wrong signature are rejected", () => {
  const wrongAgent = Crypto.newRandomAgentSecret();
  const [agentSecret, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(agentSecret, sessionID, Crypto);

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const transaction: Transaction = {
    privacy: "trusting",
    madeAt: Date.now(),
    changes: stableStringify([
      {
        hello: "world",
      },
    ]),
  };

  const { expectedNewHash } = coValue.expectedNewHashAfter(
    node.currentSessionID,
    [transaction],
  );

  // eslint-disable-next-line neverthrow/must-use-result
  coValue
    .tryAddTransactions(
      node.currentSessionID,
      [transaction],
      expectedNewHash,
      Crypto.sign(Crypto.getAgentSignerSecret(wrongAgent), expectedNewHash),
    )
    ._unsafeUnwrapErr({ withStackTrace: true });
});

test("transactions with correctly signed, but wrong hash are rejected", () => {
  const [account, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(account, sessionID, Crypto);

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const transaction: Transaction = {
    privacy: "trusting",
    madeAt: Date.now(),
    changes: stableStringify([
      {
        hello: "world",
      },
    ]),
  };

  const { expectedNewHash } = coValue.expectedNewHashAfter(
    node.currentSessionID,
    [
      {
        privacy: "trusting",
        madeAt: Date.now(),
        changes: stableStringify([
          {
            hello: "wrong",
          },
        ]),
      },
    ],
  );

  // eslint-disable-next-line neverthrow/must-use-result
  coValue
    .tryAddTransactions(
      node.currentSessionID,
      [transaction],
      expectedNewHash,
      Crypto.sign(account.currentSignerSecret(), expectedNewHash),
    )
    ._unsafeUnwrapErr({ withStackTrace: true });
});

test("New transactions in a group correctly update owned values, including subscriptions", async () => {
  const [account, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(account, sessionID, Crypto);

  const group = node.createGroup();

  const timeBeforeEdit = Date.now();

  await new Promise((resolve) => setTimeout(resolve, 10));

  const map = group.createMap();

  map.set("hello", "world");

  const listener = vi.fn();

  map.subscribe(listener);

  expect(listener.mock.calls[0]?.[0].get("hello")).toBe("world");

  const resignationThatWeJustLearnedAbout = {
    privacy: "trusting",
    madeAt: timeBeforeEdit,
    changes: stableStringify([
      {
        op: "set",
        key: account.id,
        value: "revoked",
      } satisfies MapOpPayload<typeof account.id, Role>,
    ]),
  } satisfies Transaction;

  const { expectedNewHash } = group.core.expectedNewHashAfter(sessionID, [
    resignationThatWeJustLearnedAbout,
  ]);

  const signature = Crypto.sign(
    node.account.currentSignerSecret(),
    expectedNewHash,
  );

  expect(map.core.getValidSortedTransactions().length).toBe(1);

  const manuallyAdddedTxSuccess = group.core
    .tryAddTransactions(
      node.currentSessionID,
      [resignationThatWeJustLearnedAbout],
      expectedNewHash,
      signature,
    )
    ._unsafeUnwrap({ withStackTrace: true });

  expect(manuallyAdddedTxSuccess).toBe(true);

  expect(listener.mock.calls.length).toBe(2);
  expect(listener.mock.calls[1]?.[0].get("hello")).toBe(undefined);

  expect(map.core.getValidSortedTransactions().length).toBe(0);
});

test("correctly records transactions", async () => {
  const [account, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(account, sessionID, Crypto);

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
  const [account, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(account, sessionID, Crypto);

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
  const [account, sessionID] = randomAnonymousAccountAndSessionID();
  const node = new LocalNode(account, sessionID, Crypto);

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
