import { beforeEach, expect, test } from "vitest";
import { expectList } from "../coValue.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { LocalNode } from "../localNode.js";
import {
  loadCoValueOrFail,
  randomAnonymousAccountAndSessionID,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

beforeEach(async () => {
  setupTestNode({ isSyncServer: true });
});

test("Empty CoList works", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");
  expect(content.toJSON()).toEqual([]);
});

test("Can append, prepend, delete and replace items in CoList", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.append("world", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world"]);
  content.prepend("beautiful", 1, "trusting");
  expect(content.toJSON()).toEqual(["hello", "beautiful", "world"]);
  content.prepend("hooray", 3, "trusting");
  expect(content.toJSON()).toEqual(["hello", "beautiful", "world", "hooray"]);
  content.replace(2, "universe", "trusting");
  expect(content.toJSON()).toEqual([
    "hello",
    "beautiful",
    "universe",
    "hooray",
  ]);
  content.delete(2, "trusting");
  expect(content.toJSON()).toEqual(["hello", "beautiful", "hooray"]);
});

test("Push is equivalent to append after last item", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.append("world", undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world"]);
  content.append("hooray", undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray"]);
});

test("appendItems add an array of items at the end of the list", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.appendItems(["world", "hooray", "universe"], undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray", "universe"]);
});

test("appendItems at index", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");
  content.append("second", 0, "trusting");
  expect(content.toJSON()).toEqual(["first", "second"]);

  content.appendItems(["third", "fourth"], 1, "trusting");
  expect(content.toJSON()).toEqual(["first", "second", "third", "fourth"]);

  content.appendItems(["hello", "world"], 0, "trusting");
  expect(content.toJSON()).toEqual([
    "first",
    "hello",
    "world",
    "second",
    "third",
    "fourth",
  ]);
});

test("appendItems at index", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");
  expect(content.toJSON()).toEqual(["first"]);

  content.appendItems(["second"], 0, "trusting");
  expect(content.toJSON()).toEqual(["first", "second"]);

  content.appendItems(["third"], 1, "trusting");
  expect(content.toJSON()).toEqual(["first", "second", "third"]);
});

test("appendItems with negative index", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.appendItems(["world", "hooray", "universe"], -1, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray", "universe"]);
});

test("Can push into empty list", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");

  content.append("hello", undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
});

test("init the list correctly", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const content = group.createList(["hello", "world", "hooray", "universe"]);

  expect(content.type).toEqual("colist");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray", "universe"]);

  content.append("hello", content.toJSON().length - 1, "trusting");
  expect(content.toJSON()).toEqual([
    "hello",
    "world",
    "hooray",
    "universe",
    "hello",
  ]);
});

test("Items prepended to start appear with latest first", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.prepend("first", 0, "trusting");
  content.prepend("second", 0, "trusting");
  content.prepend("third", 0, "trusting");

  expect(content.toJSON()).toEqual(["third", "second", "first"]);
});

test("mixing prepend and append", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const list = expectList(coValue.getCurrentContent());

  list.append(2, undefined, "trusting");
  list.prepend(1, undefined, "trusting");
  list.append(3, undefined, "trusting");

  expect(list.toJSON()).toEqual([1, 2, 3]);
});

test("Items appended to start", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");
  content.append("second", 0, "trusting");
  content.append("third", 0, "trusting");

  // This result is correct because "third" is appended after "first"
  // Using the Array methods this would be the same as doing content.splice(1, 0, "third")
  expect(content.toJSON()).toEqual(["first", "third", "second"]);
});

test("syncing appends with an older timestamp", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const otherClient = setupTestNode({});

  const otherClientConnection = otherClient.connectToSyncServer({
    ourName: "otherClient",
  });

  const coValue = client.node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const list = expectList(coValue.getCurrentContent());

  list.append(1, undefined, "trusting");
  list.append(2, undefined, "trusting");

  const listOnOtherClient = await loadCoValueOrFail(otherClient.node, list.id);

  otherClientConnection.peerState.gracefulShutdown();

  listOnOtherClient.append(3, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.append(4, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  listOnOtherClient.append(5, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.append(6, undefined, "trusting");

  otherClient.connectToSyncServer({
    ourName: "otherClient",
  });

  await waitFor(() => {
    expect(list.toJSON()).toEqual([1, 2, 4, 6, 3, 5]);
  });

  expect(listOnOtherClient.toJSON()).toEqual(list.toJSON());
});

test("syncing prepends with an older timestamp", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const otherClient = setupTestNode({});

  const otherClientConnection = otherClient.connectToSyncServer();

  const coValue = client.node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const list = expectList(coValue.getCurrentContent());

  list.prepend(1, undefined, "trusting");
  list.prepend(2, undefined, "trusting");

  const listOnOtherClient = await loadCoValueOrFail(otherClient.node, list.id);

  otherClientConnection.peerState.gracefulShutdown();

  listOnOtherClient.prepend(3, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.prepend(4, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  listOnOtherClient.prepend(5, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.prepend(6, undefined, "trusting");

  otherClient.connectToSyncServer();

  await waitFor(() => {
    expect(list.toJSON()).toEqual([6, 4, 5, 3, 2, 1]);
  });

  expect(listOnOtherClient.toJSON()).toEqual(list.toJSON());
});

test("totalValidTransactions should return the number of valid transactions processed", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const otherClient = setupTestNode({});

  const otherClientConnection = otherClient.connectToSyncServer();

  const group = client.node.createGroup();
  group.addMember("everyone", "reader");

  const list = group.createList([1, 2]);

  const listOnOtherClient = await loadCoValueOrFail(otherClient.node, list.id);

  otherClientConnection.peerState.gracefulShutdown();

  group.addMember("everyone", "writer");

  await new Promise((resolve) => setTimeout(resolve, 50));

  listOnOtherClient.append(3, undefined, "trusting");

  expect(listOnOtherClient.toJSON()).toEqual([1, 2]);
  expect(listOnOtherClient.totalValidTransactions).toEqual(1);

  otherClient.connectToSyncServer();

  await waitFor(() => {
    expect(listOnOtherClient.core.getCurrentContent().toJSON()).toEqual([
      1, 2, 3,
    ]);
  });

  expect(
    listOnOtherClient.core.getCurrentContent().totalValidTransactions,
  ).toEqual(2);
});
