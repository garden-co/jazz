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
const subscriptionRowId = Uint8Array.from({ length: 16 }, (_, index) => index + 17);
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
  const stages: string[] = [];
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
    (stage) => stages.push(stage),
  );
  assert.deepEqual(commands, [0, 1, 2, 0]);
  assert.deepEqual(stages, [
    "foreground-open-failed",
    "foreground-probe-failed",
    "foreground-tick-failed",
    "foreground-close-failed",
  ]);
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

test("scope-isolation receipt keeps both native-selected scope stores disjoint", async () => {
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
      if (command.type === "poll") return Uint8Array.of(tags.poll, command.operation);
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
        case 15:
          return { type: "pending", operation: bytes[1]! };
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
  const scopeFactory = (
    bLeaksA: boolean,
    aLeaksB: boolean,
    delayedAReads = 0,
    writerMustProgress = false,
    writerMustYield = false,
  ) => ({
    abiVersion: NATIVE_RELAY_ABI_VERSION,
    openAttached(capability: Uint8Array) {
      const isA = capability[0] === 1;
      let stagedWrite = false;
      let scheduler: ((urgency: string) => void) | undefined;
      return {
        execute(command: Uint8Array) {
          switch (command[0]) {
            case 10:
              return Uint8Array.of(11, 1); // TransactionOpened { 1 }
            case 13:
              if (writerMustProgress) stagedWrite = true;
              else if (isA) aWasWritten = true;
              else bWasWritten = true;
              return Uint8Array.of(13); // MutationStaged
            case 15:
              return Uint8Array.of(14, ...new Uint8Array(16).fill(1));
            case 2:
              return Uint8Array.of(2, 1); // PreparedQuery { 1 }
            case 3:
              if (isA && delayedAReads > 0) {
                delayedAReads -= 1;
                return rows(false, false);
              }
              return rows(isA ? aWasWritten : bLeaksA, isA ? aLeaksB : bWasWritten);
            case 7:
              return Uint8Array.of(7, 1);
            default:
              throw new Error(`unexpected foreground command ${command[0]}`);
          }
        },
        tick() {
          if (!stagedWrite) return;
          if (writerMustYield) {
            stagedWrite = false;
            setTimeout(() => {
              if (!scheduler) return;
              scheduler("immediate");
              if (isA) aWasWritten = true;
              else bWasWritten = true;
            }, 0);
            return;
          }
          if (isA) aWasWritten = true;
          else bWasWritten = true;
          stagedWrite = false;
        },
        setTickScheduler(callback: (urgency: string) => void) {
          scheduler = callback;
        },
        close: () => true,
      };
    },
  });

  const passing = scopeFactory(false, false);
  const stages: string[] = [];
  await proveForegroundScopeIsolation(
    passing,
    scopeA,
    scopeCodec,
    {
      write: "a",
      contains: ["a"],
      excludes: ["b"],
    },
    (stage) => stages.push(stage),
  );
  assert.deepEqual(stages, [
    "scope-isolation-open-failed",
    "scope-isolation-write-failed",
    "scope-isolation-writer-read-failed",
    "scope-isolation-open-failed",
    "scope-isolation-read-failed",
    "scope-isolation-assert-failed",
  ]);
  await proveForegroundScopeIsolation(passing, scopeB, scopeCodec, {
    write: "b",
    contains: ["b"],
    excludes: ["a"],
  });
  await proveForegroundScopeIsolation(passing, scopeA, scopeCodec, {
    contains: ["a"],
    excludes: ["b"],
  });

  const eventuallyVisible = scopeFactory(false, false, 2);
  await proveForegroundScopeIsolation(eventuallyVisible, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });

  aWasWritten = false;
  bWasWritten = false;
  const writerProgressRequired = scopeFactory(false, false, 0, true);
  await proveForegroundScopeIsolation(writerProgressRequired, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });

  aWasWritten = false;
  bWasWritten = false;
  const nativeWakeRequiresEventLoop = scopeFactory(false, false, 0, true, true);
  await proveForegroundScopeIsolation(nativeWakeRequiresEventLoop, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });

  const lifecycleReceipt = (failure?: "reader-open" | "reader-read" | "reader-close") => {
    let opens = 0;
    const closes: string[] = [];
    const factory = {
      abiVersion: NATIVE_RELAY_ABI_VERSION,
      openAttached() {
        opens += 1;
        if (opens === 2 && failure === "reader-open")
          throw new Error("planted reader open failure");
        const role = opens === 1 ? "writer" : "reader";
        return {
          execute(command: Uint8Array) {
            if (role === "writer") {
              if (command[0] === 10) return Uint8Array.of(11, 1);
              if (command[0] === 13) return Uint8Array.of(13);
              if (command[0] === 15) return Uint8Array.of(14, ...new Uint8Array(16).fill(1));
              if (command[0] === 2) return Uint8Array.of(2, 1);
              if (command[0] === 3) return rows(true, false);
            } else {
              if (command[0] === 2) return Uint8Array.of(2, 1);
              if (command[0] === 3) {
                if (failure === "reader-read") throw new Error("planted reader read failure");
                return rows(true, false);
              }
            }
            throw new Error(`unexpected ${role} lifecycle command ${command[0]}`);
          },
          tick() {},
          setTickScheduler() {},
          close() {
            closes.push(role);
            if (role === "reader" && failure === "reader-close")
              throw new Error("planted reader close failure");
            return true;
          },
        };
      },
    };
    return { factory, closes };
  };

  const cleanLifecycle = lifecycleReceipt();
  await proveForegroundScopeIsolation(cleanLifecycle.factory, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });
  assert.deepEqual(cleanLifecycle.closes, ["reader", "writer"]);

  for (const failure of ["reader-open", "reader-read", "reader-close"] as const) {
    const failedLifecycle = lifecycleReceipt(failure);
    await assert.rejects(
      async () =>
        proveForegroundScopeIsolation(failedLifecycle.factory, scopeA, scopeCodec, {
          write: "a",
          contains: ["a"],
          excludes: ["b"],
        }),
      new RegExp(`planted ${failure.replace("-", " ")} failure`),
    );
    assert.deepEqual(
      failedLifecycle.closes,
      failure === "reader-open" ? ["writer"] : ["reader", "writer"],
    );
  }

  const pendingReceipt = (settles: boolean, emitsWake = true) => {
    const metrics = { all: 0, poll: 0, tick: 0, close: 0 };
    let scheduler: ((urgency: string) => void) | undefined;
    const scheduleWake = () => {
      if (!emitsWake) return;
      setTimeout(() => {
        const ticksBeforeCallback = metrics.tick;
        scheduler?.("immediate");
        assert.equal(
          metrics.tick,
          ticksBeforeCallback,
          "native wake callback must not re-enter foreground.tick",
        );
      }, 0);
    };
    const factory = {
      abiVersion: NATIVE_RELAY_ABI_VERSION,
      openAttached() {
        return {
          execute(command: Uint8Array) {
            switch (command[0]) {
              case 2:
                return Uint8Array.of(2, 1);
              case 3:
                metrics.all += 1;
                if (metrics.all > 1)
                  throw new Error("scope receipt reissued all instead of polling");
                scheduleWake();
                return Uint8Array.of(15, 42);
              case 8:
                metrics.poll += 1;
                assert.equal(command[1], 42);
                if (settles && metrics.poll === 2) return rows(true, false);
                scheduleWake();
                return Uint8Array.of(15, 42);
              case 7:
                return Uint8Array.of(7, 1);
              default:
                throw new Error(`unexpected pending foreground command ${command[0]}`);
            }
          },
          tick() {
            metrics.tick += 1;
          },
          setTickScheduler(callback: (urgency: string) => void) {
            scheduler = callback;
          },
          close() {
            metrics.close += 1;
            return true;
          },
        };
      },
    };
    return { factory, metrics };
  };

  const pendingThenVisible = pendingReceipt(true);
  await proveForegroundScopeIsolation(pendingThenVisible.factory, scopeA, scopeCodec, {
    contains: ["a"],
    excludes: ["b"],
  });
  assert.deepEqual(pendingThenVisible.metrics, { all: 1, poll: 2, tick: 3, close: 1 });

  const pendingForever = pendingReceipt(false);
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(pendingForever.factory, scopeA, scopeCodec, {
        contains: ["a"],
        excludes: ["b"],
      }),
    /did not settle after bounded ticks/,
  );
  assert.deepEqual(pendingForever.metrics, { all: 1, poll: 95, tick: 96, close: 1 });

  const missingNativeWake = pendingReceipt(true, false);
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(missingNativeWake.factory, scopeA, scopeCodec, {
        contains: ["a"],
        excludes: ["b"],
      }),
    /did not settle after bounded ticks/,
  );
  assert.deepEqual(missingNativeWake.metrics, { all: 1, poll: 0, tick: 96, close: 1 });

  aWasWritten = false;
  bWasWritten = false;
  const leaking = scopeFactory(true, false);
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(leaking, scopeB, scopeCodec, {
        contains: [],
        excludes: ["a"],
      }),
    /observed scope A's persisted fixture row/,
  );

  aWasWritten = false;
  bWasWritten = false;
  const reverseLeaking = scopeFactory(false, true);
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(reverseLeaking, scopeA, scopeCodec, {
        contains: [],
        excludes: ["b"],
      }),
    /observed scope B's persisted fixture row/,
  );
});

test("two aliases in one installed JSI runtime require B to observe A's committed subscription delta", async () => {
  let insertedRowId: Uint8Array | undefined;
  let insertedCells: Uint8Array | undefined;
  const command = {
    encode(value: unknown) {
      if (
        typeof value === "object" &&
        value !== null &&
        "type" in value &&
        value.type === "insert" &&
        "rowId" in value &&
        value.rowId instanceof Uint8Array
      ) {
        insertedRowId = value.rowId;
        if ("cells" in value && value.cells instanceof Uint8Array) insertedCells = value.cells;
      }
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
      if (decoded.type === "inserted" && Array.isArray(decoded.rowId)) {
        decoded.rowId = Uint8Array.from(decoded.rowId);
      }
      return decoded;
    },
  } as unknown as ForegroundByteCodec;
  let committed = false;
  let opened = 0;
  let emitCommitWake = true;
  const schedulers: Array<((urgency: string) => void) | undefined> = [];
  const ticks = [0, 0];
  const factory = {
    abiVersion: NATIVE_RELAY_ABI_VERSION,
    openAttached(received: Uint8Array) {
      assert.deepEqual(received, capability);
      const peer = opened++;
      let initialResetDrained = false;
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
                  : request.type === "insert"
                    ? { type: "inserted", rowId: Array.from(subscriptionRowId) }
                    : request.type === "commitTransaction"
                      ? (setTimeout(() => {
                          committed = true;
                          const bTicksBeforeWake = ticks[1];
                          if (emitCommitWake) schedulers[1]?.("immediate");
                          assert.equal(
                            ticks[1],
                            bTicksBeforeWake,
                            "subscription wake must not re-enter foreground.tick",
                          );
                        }, 0),
                        {
                          type: "transactionCommitted",
                          txId: new Uint8Array(16).fill(1),
                        })
                      : request.type === "drainSubscription"
                        ? {
                            type: "subscriptionEvents",
                            events:
                              peer === 1 && !initialResetDrained
                                ? ((initialResetDrained = true),
                                  [
                                    {
                                      type: "delta",
                                      reset: true,
                                      settled: true,
                                      tier: "local",
                                      delta: [],
                                    },
                                  ])
                                : peer === 1 && committed
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
        tick() {
          ticks[peer] += 1;
        },
        setTickScheduler(callback: (urgency: string) => void) {
          schedulers[peer] = callback;
        },
        close: () => true,
      };
    },
  };
  const subscriptionStages: string[] = [];
  await proveSameJsiRuntimeWriteSubscription(
    factory,
    capability,
    command,
    subscriptionRowId,
    (stage) => subscriptionStages.push(stage),
  );
  assert.deepEqual(
    insertedRowId,
    subscriptionRowId,
    "the receipt inserts the host-run row instead of upserting a retained fixed id",
  );
  assert.equal(
    insertedCells?.[9],
    insertedCells ? insertedCells.byteLength - 10 : undefined,
    "the fixed Rust record envelope includes its primitive-string variant byte",
  );
  assert.deepEqual(subscriptionStages, [
    "same-runtime-open-failed",
    "same-runtime-subscribe-failed",
    "same-runtime-initial-reset-failed",
    "same-runtime-write-failed",
    "same-runtime-transaction-open-failed",
    "same-runtime-mutation-stage-failed",
    "same-runtime-commit-failed",
    "same-runtime-delta-failed",
    "same-runtime-postcommit-wake-failed",
    "same-runtime-delta-drain-failed",
    "same-runtime-delta-content-failed",
    "same-runtime-unsubscribe-failed",
  ]);

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let splitOpened = 0;
  let unsettledResetDrains = 0;
  const unsettledInitialReset = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = splitOpened++;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (peer === 1 && request.type === "drainSubscription") {
            unsettledResetDrains += 1;
            if (unsettledResetDrains === 1) {
              // Consume the underlying reset but preserve only its reset
              // semantics; durability settlement is orthogonal to proving
              // that the later content delta was not the opening snapshot.
              foreground.execute(bytes);
              return command.encode({
                type: "subscriptionEvents",
                events: [{ type: "delta", reset: true, settled: false, tier: "local", delta: [] }],
              });
            }
          }
          return foreground.execute(bytes);
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    unsettledInitialReset,
    capability,
    command,
    subscriptionRowId,
  );
  assert.equal(unsettledResetDrains, 2, "unsettled reset and committed delta drain separately");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let prematureOpened = 0;
  const settlementBeforeReset = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = prematureOpened++;
      let drained = false;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (peer === 1 && request.type === "drainSubscription") {
            if (!drained) {
              drained = true;
              foreground.execute(bytes);
              return command.encode({
                type: "subscriptionEvents",
                events: [{ type: "delta", reset: false, settled: true, tier: "local", delta: [] }],
              });
            }
            return command.encode({ type: "subscriptionEvents", events: [] });
          }
          return foreground.execute(bytes);
        },
      };
    },
  };
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        settlementBeforeReset,
        capability,
        command,
        subscriptionRowId,
      ),
    /initial subscription reset did not materialize/,
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let delayedOpened = 0;
  let delayedEmptyDrains = 0;
  let delayedTicks = 0;
  let delayedTurns = 0;
  let allowDelayedProgress = true;
  const delayedInitialReset = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = delayedOpened++;
      let resetReady = false;
      let progressionCycles = 0;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (peer === 1 && request.type === "drainSubscription" && !resetReady) {
            delayedEmptyDrains += 1;
            return command.encode({ type: "subscriptionEvents", events: [] });
          }
          return foreground.execute(bytes);
        },
        tick() {
          foreground.tick();
          if (peer !== 1) return;
          delayedTicks += 1;
          if (allowDelayedProgress)
            setTimeout(() => {
              progressionCycles += 1;
              resetReady = progressionCycles >= 2;
              delayedTurns += 1;
            }, 0);
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    delayedInitialReset,
    capability,
    command,
    subscriptionRowId,
  );
  assert.equal(delayedEmptyDrains, 2, "two successive reset drains are ready but empty");
  assert.ok(delayedTicks >= 2, "each empty drain receives an ordinary B tick");
  assert.ok(delayedTurns >= 2, "two yielded turns are required before the reset is ready");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  delayedOpened = 0;
  delayedEmptyDrains = 0;
  delayedTicks = 0;
  delayedTurns = 0;
  allowDelayedProgress = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        delayedInitialReset,
        capability,
        command,
        subscriptionRowId,
      ),
    /initial subscription reset did not materialize/,
  );
  assert.equal(delayedEmptyDrains, 96);
  assert.equal(delayedTurns, 0);
  allowDelayedProgress = true;

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let pendingPolls = 0;
  let pendingDrains = 0;
  let emitPendingWake = true;
  const pendingHydration = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      let scheduler: ((urgency: string) => void) | undefined;
      let pendingResponse: Uint8Array | undefined;
      let pollsForDrain = 0;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string; operation?: number };
          if (request.type === "drainSubscription") {
            assert.equal(pendingResponse, undefined);
            pendingDrains += 1;
            pendingResponse = foreground.execute(bytes);
            pollsForDrain = 0;
            if (emitPendingWake) setTimeout(() => scheduler?.("immediate"), 0);
            return command.encode({ type: "pending", operation: 57 });
          }
          if (request.type === "poll") {
            assert.equal(request.operation, 57);
            assert.ok(pendingResponse);
            pendingPolls += 1;
            pollsForDrain += 1;
            if (pollsForDrain === 1) {
              if (emitPendingWake) setTimeout(() => scheduler?.("immediate"), 0);
              return command.encode({ type: "pending", operation: 57 });
            }
            const response = pendingResponse;
            pendingResponse = undefined;
            return response;
          }
          return foreground.execute(bytes);
        },
        setTickScheduler(callback: (urgency: string) => void) {
          scheduler = callback;
          foreground.setTickScheduler?.(callback);
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    pendingHydration,
    capability,
    command,
    subscriptionRowId,
  );
  assert.equal(pendingDrains, 2, "a retained operation must not reissue DrainSubscription");
  assert.equal(pendingPolls, 4, "both drains preserve one operation across repeated Polls");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  pendingDrains = 0;
  pendingPolls = 0;
  emitPendingWake = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        pendingHydration,
        capability,
        command,
        subscriptionRowId,
      ),
    /subscription drain did not settle after bounded ticks/,
  );
  assert.equal(pendingDrains, 1);
  assert.equal(pendingPolls, 0, "a retained operation cannot Poll without a fresh native wake");
  emitPendingWake = true;

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  const terminalCloses: number[] = [];
  let terminalOpened = 0;
  const terminalOperation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = terminalOpened++;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription")
            return command.encode({ type: "operationError", reason: "cancelled" });
          return foreground.execute(bytes);
        },
        close() {
          terminalCloses.push(peer);
          return foreground.close();
        },
      };
    },
  };
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        terminalOperation,
        capability,
        command,
        subscriptionRowId,
      ),
    /unexpected response/,
  );
  assert.deepEqual(terminalCloses, [0, 1], "both foregrounds close after terminal operation error");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  emitCommitWake = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(factory, capability, command, subscriptionRowId),
    /did not observe foreground A's committed row/,
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  emitCommitWake = true;
  const missingNativeWake = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      return { ...foreground, setTickScheduler() {} };
    },
  };
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        missingNativeWake,
        capability,
        command,
        subscriptionRowId,
      ),
    /did not observe foreground A's committed row/,
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  const noObservation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      let initialResetDrained = false;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription" && initialResetDrained)
            return command.encode({
              type: "subscriptionEvents",
              events: [
                {
                  type: "delta",
                  reset: false,
                  settled: true,
                  tier: "local",
                  delta: [],
                },
              ],
            });
          if (request.type === "drainSubscription") initialResetDrained = true;
          return foreground.execute(bytes);
        },
      };
    },
  };
  const noObservationStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        noObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => noObservationStages.push(stage),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(noObservationStages.includes("same-runtime-delta-drain-failed"));
  assert.ok(
    !noObservationStages.includes("same-runtime-delta-content-failed"),
    "an empty settlement event is not evidence of row-bearing delta content",
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  const wrongObservation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      let initialResetDrained = false;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription" && initialResetDrained)
            return command.encode({
              type: "subscriptionEvents",
              events: [
                {
                  type: "delta",
                  reset: false,
                  settled: true,
                  tier: "local",
                  delta: Array.from(new TextEncoder().encode("wrong-row")),
                },
              ],
            });
          if (request.type === "drainSubscription") initialResetDrained = true;
          return foreground.execute(bytes);
        },
      };
    },
  };
  const wrongObservationStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        wrongObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => wrongObservationStages.push(stage),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(wrongObservationStages.includes("same-runtime-delta-drain-failed"));
  assert.ok(
    wrongObservationStages.includes("same-runtime-delta-content-failed"),
    "a non-empty wrong-row payload reaches content diagnostics without satisfying observation",
  );
});

function utf8(value: string): number[] {
  return Array.from(new TextEncoder().encode(value));
}
