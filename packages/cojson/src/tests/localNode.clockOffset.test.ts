import { describe, expect, test } from "vitest";
import { ClockOffset } from "../ClockOffset.js";
import { LocalNode } from "../localNode.js";
import { randomAgentAndSessionID } from "./testUtils.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";

const Crypto = await WasmCrypto.create();

function makeNode(opts: { experimental_clockSyncFromServerPings?: boolean }) {
  const [admin, session] = randomAgentAndSessionID();
  return new LocalNode(
    admin.agentSecret,
    session,
    Crypto,
    undefined,
    undefined,
    opts,
  );
}

describe("LocalNode clock offset wiring", () => {
  test("clockOffset is always present on the node, regardless of flag state", () => {
    const withFlag = makeNode({ experimental_clockSyncFromServerPings: true });
    const withoutFlag = makeNode({
      experimental_clockSyncFromServerPings: false,
    });

    expect(withFlag.clockOffset).toBeInstanceOf(ClockOffset);
    expect(withoutFlag.clockOffset).toBeInstanceOf(ClockOffset);
  });

  test("with the flag enabled, seeding clockOffset with a +10_000 ms sample pulls stampNow() forward by ~10_000 ms", () => {
    const flagged = makeNode({ experimental_clockSyncFromServerPings: true });
    const control = makeNode({
      experimental_clockSyncFromServerPings: false,
    });

    const localReceiveTime = Date.now();
    flagged.clockOffset.addSample({
      serverTime: localReceiveTime + 10_000,
      localReceiveTime,
    });

    const controlStamp = control.stampNow();
    const flaggedStamp = flagged.stampNow();

    const delta = flaggedStamp - controlStamp;
    expect(delta).toBeGreaterThanOrEqual(9_900);
    expect(delta).toBeLessThanOrEqual(10_100);
  });

  test("getClockOffsetDiagnostics() reflects current offset and sample count", () => {
    const node = makeNode({ experimental_clockSyncFromServerPings: true });

    expect(node.getClockOffsetDiagnostics()).toEqual({
      currentOffset: 0,
      sampleCount: 0,
    });

    const base = Date.now();
    node.clockOffset.addSample({
      serverTime: base + 400,
      localReceiveTime: base,
    });
    node.clockOffset.addSample({
      serverTime: base + 600,
      localReceiveTime: base,
    });

    expect(node.getClockOffsetDiagnostics()).toEqual({
      currentOffset: 500,
      sampleCount: 2,
    });
  });

  test("without the flag, seeding clockOffset with a +10_000 ms sample does NOT affect stampNow()", () => {
    const node = makeNode({ experimental_clockSyncFromServerPings: false });

    const localReceiveTime = Date.now();
    node.clockOffset.addSample({
      serverTime: localReceiveTime + 10_000,
      localReceiveTime,
    });

    const before = Date.now();
    const stamp = node.stampNow();
    const after = Date.now();

    expect(stamp).toBeGreaterThanOrEqual(before);
    expect(stamp).toBeLessThanOrEqual(after + 100);
  });
});
