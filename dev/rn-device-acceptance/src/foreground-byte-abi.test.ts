import assert from "node:assert/strict";
import test from "node:test";
import {
  proveForegroundByteAbi,
  proveForegroundRevoked,
  proveForegroundScopeIsolation,
  proveSameJsiRuntimeWriteSubscription,
  type ForegroundByteCodec,
} from "./foreground-byte-abi.ts";
import { NATIVE_RELAY_ABI_VERSION } from "jazz-rn/native-relay-abi";

const capability = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
const codec = {
  encode(command: "probe" | "tick" | "close") {
    return Uint8Array.of(command === "probe" ? 0 : command === "tick" ? 1 : 2);
  },
  decode(bytes: Uint8Array) {
    if (bytes[0] === 0 && bytes[1] === NATIVE_RELAY_ABI_VERSION)
      return { type: "probe" as const, abiVersion: NATIVE_RELAY_ABI_VERSION };
    if (bytes[0] === 1) return { type: "ticked" as const };
    if (bytes[0] === 2) return { type: "closed" as const, closed: bytes[1] === 1 };
    throw new Error("malformed response");
  },
};

test("foreground receipt sends the v1 Probe, Tick, and Close byte commands", () => {
  let closed = false;
  const commands: number[] = [];
  const foreground = {
    execute(command: Uint8Array) {
      commands.push(command[0]!);
      if (closed && command[0] !== 2) throw new Error("foreground closed");
      if (command[0] === 0) return Uint8Array.of(0, NATIVE_RELAY_ABI_VERSION);
      if (command[0] === 1) return Uint8Array.of(1);
      if (command[0] === 2) {
        const first = !closed;
        closed = true;
        return Uint8Array.of(2, first ? 1 : 0);
      }
      throw new Error("unexpected command");
    },
    tick() {},
    close: () => false,
  };
  proveForegroundByteAbi(
    {
      abiVersion: NATIVE_RELAY_ABI_VERSION,
      openAttached: (received) => {
        assert.deepEqual(received, capability);
        return foreground;
      },
    },
    capability,
    codec,
  );
  assert.deepEqual(commands, [0, 1, 2, 0]);
});

test("foreground receipt treats a native revocation as an execution error", () => {
  const revokedForeground = {
    execute() {
      throw new Error("native relay rejected revoked foreground");
    },
    tick() {},
    close: () => false,
  };
  proveForegroundRevoked(revokedForeground, codec.encode);
});

test("scope-isolation receipt keeps both native-selected scope stores disjoint", () => {
  const scopeA = Uint8Array.from({ length: 32 }, () => 1);
  const scopeB = Uint8Array.from({ length: 32 }, () => 2);
  const scopeCodec: ForegroundByteCodec = {
    encode(command) {
      if (command === "probe") return Uint8Array.of(0);
      if (command === "tick") return Uint8Array.of(1);
      if (command === "close") return Uint8Array.of(7);
      const tags = {
        prepareQuery: 2,
        all: 3,
        subscribe: 4,
        drainSubscription: 5,
        unsubscribe: 6,
        poll: 8,
        cancel: 9,
        beginTransaction: 10,
        insert: 11,
        update: 12,
        upsert: 13,
        delete: 14,
        commitTransaction: 15,
        rollbackTransaction: 16,
      } as const;
      return Uint8Array.of(tags[command.type]);
    },
    decode(bytes) {
      switch (bytes[0]) {
        case 2:
          return { type: "preparedQuery", query: bytes[1]! };
        case 3:
          return { type: "rows", rows: bytes.subarray(2) };
        case 7:
          return { type: "closed", closed: bytes[1] === 1 };
        case 11:
          return { type: "transactionOpened", transaction: bytes[1]! };
        case 13:
          return { type: "mutationStaged" };
        case 14:
          return { type: "transactionCommitted", txId: bytes.subarray(1) };
        default:
          throw new Error(`unexpected mock response ${bytes[0]}`);
      }
    },
  };
  let aWasWritten = false;
  let bWasWritten = false;
  const rows = (containsA: boolean, containsB: boolean) =>
    Uint8Array.of(
      3,
      0,
      ...(containsA ? utf8("scope-a-private-row") : []),
      ...(containsB ? utf8("scope-b-private-row") : []),
    );
  const scopeFactory = (bLeaksA: boolean, aLeaksB: boolean) => ({
    abiVersion: NATIVE_RELAY_ABI_VERSION,
    openAttached(capability: Uint8Array) {
      const isA = capability[0] === 1;
      return {
        execute(command: Uint8Array) {
          switch (command[0]) {
            case 10:
              return Uint8Array.of(11, 1); // TransactionOpened { 1 }
            case 13:
              if (isA) aWasWritten = true;
              else bWasWritten = true;
              return Uint8Array.of(13); // MutationStaged
            case 15:
              return Uint8Array.of(14, ...new Uint8Array(16).fill(1));
            case 2:
              return Uint8Array.of(2, 1); // PreparedQuery { 1 }
            case 3:
              return rows(isA ? aWasWritten : bLeaksA, isA ? aLeaksB : bWasWritten);
            case 7:
              return Uint8Array.of(7, 1);
            default:
              throw new Error(`unexpected foreground command ${command[0]}`);
          }
        },
        tick() {},
        close: () => true,
      };
    },
  });

  const passing = scopeFactory(false, false);
  proveForegroundScopeIsolation(passing, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });
  proveForegroundScopeIsolation(passing, scopeB, scopeCodec, {
    write: "b",
    contains: ["b"],
    excludes: ["a"],
  });
  proveForegroundScopeIsolation(passing, scopeA, scopeCodec, {
    contains: ["a"],
    excludes: ["b"],
  });

  aWasWritten = false;
  bWasWritten = false;
  const leaking = scopeFactory(true, false);
  assert.throws(
    () =>
      proveForegroundScopeIsolation(leaking, scopeB, scopeCodec, {
        contains: [],
        excludes: ["a"],
      }),
    /observed scope A's persisted fixture row/,
  );

  aWasWritten = false;
  bWasWritten = false;
  const reverseLeaking = scopeFactory(false, true);
  assert.throws(
    () =>
      proveForegroundScopeIsolation(reverseLeaking, scopeA, scopeCodec, {
        contains: [],
        excludes: ["b"],
      }),
    /observed scope B's persisted fixture row/,
  );
});

test("two aliases in one installed JSI runtime require B to observe A's committed subscription delta", () => {
  const command = {
    encode(value: unknown) {
      return new TextEncoder().encode(JSON.stringify(value));
    },
    decode(bytes: Uint8Array) {
      const decoded = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
      if (decoded.type === "subscriptionEvents" && Array.isArray(decoded.events)) {
        for (const event of decoded.events) {
          if (event?.type === "delta" && Array.isArray(event.delta))
            event.delta = Uint8Array.from(event.delta);
        }
      }
      return decoded;
    },
  } as unknown as ForegroundByteCodec;
  let committed = false;
  let opened = 0;
  const factory = {
    abiVersion: NATIVE_RELAY_ABI_VERSION,
    openAttached(received: Uint8Array) {
      assert.deepEqual(received, capability);
      const peer = opened++;
      return {
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          const response =
            request.type === "prepareQuery"
              ? { type: "preparedQuery", query: 1 }
              : request.type === "subscribe"
                ? { type: "subscribed", subscription: 2 }
                : request.type === "beginTransaction"
                  ? { type: "transactionOpened", transaction: 3 }
                  : request.type === "upsert"
                    ? { type: "mutationStaged" }
                    : request.type === "commitTransaction"
                      ? ((committed = true),
                        {
                          type: "transactionCommitted",
                          txId: new Uint8Array(16).fill(1),
                        })
                      : request.type === "drainSubscription"
                        ? {
                            type: "subscriptionEvents",
                            events:
                              peer === 1 && committed
                                ? [
                                    {
                                      type: "delta",
                                      reset: false,
                                      settled: true,
                                      tier: "local",
                                      delta: Array.from(
                                        new TextEncoder().encode("foreground-a-subscription-row"),
                                      ),
                                    },
                                  ]
                                : [],
                          }
                        : request.type === "unsubscribe"
                          ? { type: "unsubscribed", closed: true }
                          : { type: "closed", closed: true };
          return command.encode(response);
        },
        tick() {},
        close: () => true,
      };
    },
  };
  proveSameJsiRuntimeWriteSubscription(factory, capability, command);

  committed = false;
  opened = 0;
  const noObservation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription")
            return command.encode({ type: "subscriptionEvents", events: [] });
          return foreground.execute(bytes);
        },
      };
    },
  };
  assert.throws(
    () => proveSameJsiRuntimeWriteSubscription(noObservation, capability, command),
    /did not observe foreground A's committed row/,
  );
});

function utf8(value: string): number[] {
  return Array.from(new TextEncoder().encode(value));
}
