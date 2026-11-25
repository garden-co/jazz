import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { Encryption, type EncryptionNative, type EncryptionReport } from "./index.js";

// Report delivery to listeners: a throwing listener must not cost the others
// their reports, and a replay must not reorder what established listeners see.

const report = (reason: string) =>
  ({
    type: "rotation-check-failed",
    space: null,
    reason,
  }) as unknown as EncryptionReport;
const reasons = (seen: EncryptionReport[]) =>
  seen.map((seen) => (seen as unknown as { reason: string }).reason);

/** A native side with a report queue; the snapshot can add reports, as a drive would. */
function fakeNative(onSnapshot: (queue: string[]) => void = () => {}) {
  const queue: string[] = [];
  const native = {
    encryptionCall: (op: string) => {
      if (op !== "reportsForListener") return Promise.reject(new Error(`unexpected op ${op}`));
      // Like the runtime: the queue is taken when the call is made.
      const queued = queue.splice(0);
      onSnapshot(queue);
      return Promise.resolve(JSON.stringify({ active: [], queued }));
    },
    takeEncryptionReports: () => queue.splice(0),
    encryptionState: () => JSON.stringify({ status: "active" }),
  } as unknown as EncryptionNative;
  const push = (...reports: EncryptionReport[]) =>
    queue.push(...reports.map((one) => JSON.stringify(one)));
  return { native, push };
}

let logged: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  vi.useFakeTimers();
  logged = vi.spyOn(console, "error").mockImplementation(() => {});
});
afterEach(() => {
  logged.mockRestore();
  vi.useRealTimers();
});

const tick = () => vi.advanceTimersByTimeAsync(10);

it("keeps delivering a batch to every listener when one throws", async () => {
  const { native, push } = fakeNative();
  const encryption = new Encryption(native, { reportIntervalMs: 10 });
  const thrower: EncryptionReport[] = [];
  const other: EncryptionReport[] = [];
  encryption.onReport((seen) => {
    thrower.push(seen);
    throw new Error("listener failed");
  });
  encryption.onReport((seen) => other.push(seen));
  await tick(); // the replay that establishes both

  push(report("a"), report("b"), report("c"));
  await tick();

  expect(reasons(thrower)).toEqual(["a", "b", "c"]);
  expect(reasons(other)).toEqual(["a", "b", "c"]);
  expect(logged).toHaveBeenCalledTimes(3);
  encryption.stop();
});

it("replays the whole snapshot to every waiting listener when one throws", async () => {
  const { native, push } = fakeNative();
  const encryption = new Encryption(native, { reportIntervalMs: 10 });
  push(report("a"), report("b"));
  const thrower: EncryptionReport[] = [];
  const other: EncryptionReport[] = [];
  encryption.onReport((seen) => {
    thrower.push(seen);
    throw new Error("listener failed");
  });
  encryption.onReport((seen) => other.push(seen));
  await tick();

  expect(reasons(thrower)).toEqual(["a", "b"]);
  expect(reasons(other)).toEqual(["a", "b"]);
  expect(logged).toHaveBeenCalledTimes(2);

  // Both are established: the next reports reach them on the next tick.
  push(report("c"));
  await tick();
  expect(reasons(other)).toEqual(["a", "b", "c"]);
  encryption.stop();
});

it("keeps report order for established listeners in the tick that starts a replay", async () => {
  // The snapshot drives the runtime, which produces a newer report after
  // the queue was taken.
  let produce = false;
  const { native, push } = fakeNative((queue) => {
    if (produce) queue.push(JSON.stringify(report("newer")));
  });
  const encryption = new Encryption(native, { reportIntervalMs: 10 });
  const established: EncryptionReport[] = [];
  encryption.onReport((seen) => established.push(seen));
  await tick();

  push(report("older"));
  produce = true;
  const late: EncryptionReport[] = [];
  encryption.onReport((seen) => late.push(seen));
  await tick(); // starts the replay
  produce = false;
  await tick();

  expect(reasons(established)).toEqual(["older", "newer"]);
  expect(reasons(late)).toEqual(["older", "newer"]);
  encryption.stop();
});

it("skips a report that is not valid JSON, in polling and in a replay", async () => {
  let snapshotQueue: string[] = [];
  const { native, push } = fakeNative();
  const call = native.encryptionCall.bind(native);
  native.encryptionCall = (op, args) =>
    snapshotQueue.length > 0
      ? Promise.resolve(JSON.stringify({ active: [], queued: snapshotQueue.splice(0) }))
      : call(op, args);
  const take = native.takeEncryptionReports.bind(native);
  let polled: string[] = [];
  native.takeEncryptionReports = () => [...polled.splice(0), ...take()];
  const encryption = new Encryption(native, { reportIntervalMs: 10 });

  // A replay whose snapshot holds an invalid report.
  snapshotQueue = [JSON.stringify(report("a")), "{not json", JSON.stringify(report("b"))];
  const first: EncryptionReport[] = [];
  encryption.onReport((seen) => first.push(seen));
  await tick();
  expect(reasons(first)).toEqual(["a", "b"]);

  // Polling, with an invalid report in the middle of a batch.
  polled = [JSON.stringify(report("c")), "{not json"];
  push(report("d"));
  await tick();
  expect(reasons(first)).toEqual(["a", "b", "c", "d"]);
  expect(logged).toHaveBeenCalledTimes(2);
  encryption.stop();
});

it("gives a late listener whose snapshot fails the reports delivered meanwhile", async () => {
  const queue: string[] = [];
  let calls = 0;
  let reject!: (error: Error) => void;
  const native = {
    encryptionCall: () => {
      calls += 1;
      if (calls === 1) return Promise.resolve(JSON.stringify({ active: [], queued: [] }));
      return new Promise<string>((_, fail) => {
        reject = fail;
      });
    },
    takeEncryptionReports: () => queue.splice(0),
    encryptionState: () => JSON.stringify({ status: "active" }),
  } as unknown as EncryptionNative;
  const push = (reason: string) => queue.push(JSON.stringify(report(reason)));
  const encryption = new Encryption(native, { reportIntervalMs: 10 });
  const established: EncryptionReport[] = [];
  encryption.onReport((seen) => established.push(seen));
  await tick();

  const late: EncryptionReport[] = [];
  encryption.onReport((seen) => late.push(seen));
  push("before");
  await tick(); // starts the replay, whose snapshot stays pending
  push("during");
  await tick();
  expect(reasons(established)).toEqual(["before", "during"]);
  expect(late).toEqual([]);

  reject(new Error("database: snapshot unavailable"));
  await tick();
  expect(late[0]).toMatchObject({ type: "report-replay-failed", code: "database" });
  expect(reasons(late.slice(1))).toEqual(["before", "during"]);

  push("after");
  await tick();
  expect(reasons(established)).toEqual(["before", "during", "after"]);
  expect(reasons(late.slice(1))).toEqual(["before", "during", "after"]);
  encryption.stop();
});
