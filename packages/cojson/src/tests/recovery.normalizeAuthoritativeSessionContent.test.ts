import { assert, beforeEach, describe, expect, test } from "vitest";
import { fillCoMapWithLargeData, setupTestNode } from "./testUtils.js";
import { normalizeAuthoritativeSessionContent } from "../recovery/normalizeAuthoritativeSessionContent.js";

beforeEach(() => {
  setupTestNode({ isSyncServer: true });
});

describe("normalizeAuthoritativeSessionContent", () => {
  test("normalizes single chunk authoritative content", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("k", "v", "trusting");

    const full =
      map.core.verified?.getFullSessionContent(client.node.currentSessionID) ??
      [];

    const normalizedResult = normalizeAuthoritativeSessionContent(full);
    expect(normalizedResult.ok).toBe(true);
    if (!normalizedResult.ok) {
      return;
    }
    const normalized = normalizedResult.value;

    expect(normalized.content).toEqual(full);
    expect(normalized.transactions.length).toBe(1);
    expect(normalized.lastSignature).toBe(full[0]?.lastSignature);
  });

  test("sorts chunks canonically and preserves continuity", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    fillCoMapWithLargeData(map);

    const full =
      map.core.verified?.getFullSessionContent(client.node.currentSessionID) ??
      [];
    expect(full.length).toBeGreaterThan(1);

    const reversed = [...full].reverse();
    const normalizedResult = normalizeAuthoritativeSessionContent(reversed);
    expect(normalizedResult.ok).toBe(true);
    if (!normalizedResult.ok) {
      return;
    }
    const normalized = normalizedResult.value;

    expect(normalized.content).toEqual(full);
    expect(normalized.transactions.length).toBe(
      full.reduce((total, piece) => total + piece.newTransactions.length, 0),
    );
    expect(normalized.lastSignature).toBe(full[full.length - 1]?.lastSignature);
  });

  test("returns error on non-contiguous authoritative chunks", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    fillCoMapWithLargeData(map);

    const full =
      map.core.verified?.getFullSessionContent(client.node.currentSessionID) ??
      [];

    const first = full[0];
    const second = full[1];

    assert(first);
    assert(second);

    const malformed = [
      first,
      {
        ...second,
        after: second.after + 1,
      },
    ];

    const normalizedResult = normalizeAuthoritativeSessionContent(malformed);
    expect(normalizedResult.ok).toBe(false);
    if (normalizedResult.ok) {
      return;
    }
    expect(normalizedResult.error.name).toBe(
      "AuthoritativeSessionNormalizationError",
    );
  });
});
