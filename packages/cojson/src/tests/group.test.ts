import { expect, test } from "vitest";
import { RawCoList } from "../coValues/coList.js";
import { RawCoMap } from "../coValues/coMap.js";
import { RawCoStream } from "../coValues/coStream.js";
import { RawBinaryCoStream } from "../coValues/coStream.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { LocalNode } from "../localNode.js";
import { randomAnonymousAccountAndSessionID } from "./testUtils.js";

const Crypto = await WasmCrypto.create();

test("Can create a RawCoMap in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const map = group.createMap();

  expect(map.core.getCurrentContent().type).toEqual("comap");
  expect(map instanceof RawCoMap).toEqual(true);
});

test("Can create a CoList in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const list = group.createList();

  expect(list.core.getCurrentContent().type).toEqual("colist");
  expect(list instanceof RawCoList).toEqual(true);
});

test("Can create a CoStream in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const stream = group.createStream();

  expect(stream.core.getCurrentContent().type).toEqual("costream");
  expect(stream instanceof RawCoStream).toEqual(true);
});

test("Can create a FileStream in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const stream = group.createBinaryStream();

  expect(stream.core.getCurrentContent().type).toEqual("costream");
  expect(stream.headerMeta.type).toEqual("binary");
  expect(stream instanceof RawBinaryCoStream).toEqual(true);
});
