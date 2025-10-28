import { describe, expect, test } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { CO_VALUE_PRIORITY, getPriorityFromHeader } from "../priority.js";
import {
  createAccountInNode,
  nodeWithRandomAgentAndSessionID,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

describe("getPriorityFromHeader", () => {
  test("returns MEDIUM priority for boolean or undefined headers", () => {
    expect(getPriorityFromHeader(true)).toEqual(CO_VALUE_PRIORITY.MEDIUM);
    expect(getPriorityFromHeader(false)).toEqual(CO_VALUE_PRIORITY.MEDIUM);
    expect(getPriorityFromHeader(undefined)).toEqual(CO_VALUE_PRIORITY.MEDIUM);
  });

  test("returns MEDIUM priority for costream type", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const costream = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    expect(getPriorityFromHeader(costream.verified.header)).toEqual(
      CO_VALUE_PRIORITY.MEDIUM,
    );
  });

  test("returns LOW priority for binary costream type", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const costream = node.createCoValue({
      type: "costream",
      ruleset: { type: "unsafeAllowAll" },
      meta: { type: "binary" },
      ...Crypto.createdNowUnique(),
    });

    expect(getPriorityFromHeader(costream.verified.header)).toEqual(
      CO_VALUE_PRIORITY.LOW,
    );
  });

  test("returns HIGH priority for account type", async () => {
    const node = nodeWithRandomAgentAndSessionID();

    const account = createAccountInNode(node);

    expect(getPriorityFromHeader(account.account.core.verified.header)).toEqual(
      CO_VALUE_PRIORITY.HIGH,
    );
  });

  test("returns HIGH priority for group type", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();

    expect(getPriorityFromHeader(group.core.verified.header)).toEqual(
      CO_VALUE_PRIORITY.HIGH,
    );
  });

  test("returns MEDIUM priority for other types", () => {
    const node = nodeWithRandomAgentAndSessionID();

    const comap = node.createCoValue({
      type: "comap",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const colist = node.createCoValue({
      type: "colist",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    expect(getPriorityFromHeader(comap.verified.header)).toEqual(
      CO_VALUE_PRIORITY.MEDIUM,
    );
    expect(getPriorityFromHeader(colist.verified.header)).toEqual(
      CO_VALUE_PRIORITY.MEDIUM,
    );
  });
});
