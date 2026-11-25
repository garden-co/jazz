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
