import { expect, it, vi } from "vitest";
import { Encryption, type EncryptionNative } from "./index.js";
import { formatSecret } from "../local-first-auth/secret.js";

// A device account awaiting approval recovers by itself with its account
// secret. The runtime answers `null` when there is nothing to recover from
// yet; every other failure must reach the caller.

const secret = new Uint8Array(32).fill(7);

function awaitingDevice(restore: () => Promise<string>) {
  const calls: { op: string; args: Record<string, unknown> }[] = [];
  const native = {
    encryptionCall: async (op: string, args: Record<string, unknown>) => {
      calls.push({ op, args });
      if (op === "ready") return JSON.stringify("awaiting-approval");
      if (op === "recoveryRestore") return restore();
      throw new Error(`unexpected op ${op}`);
    },
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "awaiting-approval" }),
  } as unknown as EncryptionNative;
  const encryption = new Encryption(native, { accountSecret: () => formatSecret(secret) });
  return { encryption, calls };
}

it("asks for the automatic recovery and stays awaiting approval when there is nothing to recover from", async () => {
  const { encryption, calls } = awaitingDevice(async () => "null");

  await expect(encryption.ready()).resolves.toBe("awaiting-approval");
  expect(calls[1]).toEqual({
    op: "recoveryRestore",
    args: { secret: formatSecret(secret), automatic: true, verifiedStatus: "awaiting-approval" },
  });
});

it("returns the status the automatic recovery reached", async () => {
  const { encryption } = awaitingDevice(async () => JSON.stringify("active"));

  await expect(encryption.ready()).resolves.toBe("active");
});

it("registers the account secret of an active device as the automatic start-up registration", async () => {
  // `automatic`: a retired root of the account secret does not stop the start
  // (an explicit registration of it is refused).
  const calls: { op: string; args: Record<string, unknown> }[] = [];
  const native = {
    encryptionCall: async (op: string, args: Record<string, unknown>) => {
      calls.push({ op, args });
      if (op === "ready") return JSON.stringify("active");
      if (op === "recoveryRestore") return JSON.stringify("active");
      throw new Error(`unexpected op ${op}`);
    },
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "active" }),
  } as unknown as EncryptionNative;
  const encryption = new Encryption(native, { accountSecret: () => formatSecret(secret) });

  await expect(encryption.ready()).resolves.toBe("active");
  expect(calls[1]).toEqual({
    op: "recoveryRestore",
    args: { secret: formatSecret(secret), automatic: true, verifiedStatus: "active" },
  });
});

it("returns a fresh reenrollment without waiting for a recovery call", async () => {
  const call = vi.fn(async (op: string) => {
    if (op === "ready") return JSON.stringify("reenrolled");
    throw new Error(`unexpected ${op}`);
  });
  const encryption = new Encryption(
    {
      encryptionCall: call,
      encryptionState: () => JSON.stringify({ status: "active" }),
      takeEncryptionReports: () => [],
    },
    { accountSecret: () => formatSecret(secret) },
  );

  await expect(encryption.ready()).resolves.toBe("reenrolled");
  expect(call.mock.calls).toEqual([["ready", {}]]);
});

it.each([
  ["store", "store: keychain unavailable"],
  ["database", "database: the transaction failed"],
  ["protocol", "protocol: E2EE recovery failed"],
])("surfaces a %s failure of the automatic recovery", async (code, message) => {
  const { encryption } = awaitingDevice(async () => {
    throw new Error(message);
  });

  await expect(encryption.ready()).rejects.toMatchObject({
    name: "EncryptionError",
    code,
    message: message.slice(code.length + 2),
  });
});

it("keeps the runtime's reason in the message of a refused operation", async () => {
  const { encryption } = awaitingDevice(async () => {
    throw new Error("device-not-active: E2EE operation refused");
  });

  await expect(encryption.ready()).rejects.toMatchObject({
    name: "EncryptionError",
    code: "device-not-active",
    message: expect.stringContaining("device-not-active"),
  });
});

it("keeps a startup failure for a listener that subscribes later", async () => {
  vi.useFakeTimers();
  const restore = vi.fn(async () => {
    throw new Error("device-not-active: E2EE operation refused");
  });
  const { encryption } = awaitingDevice(restore);
  try {
    encryption.start();
    await vi.advanceTimersByTimeAsync(100);
    expect(restore).toHaveBeenCalledOnce();

    const listener = vi.fn();
    encryption.onReport(listener);
    await vi.advanceTimersByTimeAsync(100);
    expect(listener.mock.calls).toEqual([
      [
        {
          type: "ready-failed",
          code: "device-not-active",
          message: "device-not-active: E2EE operation refused",
        },
      ],
    ]);
  } finally {
    encryption.stop();
    vi.useRealTimers();
  }
});

it("replays active unreadable reports to a new listener until withdrawn", async () => {
  vi.useFakeTimers();
  const report = {
    type: "unreadable",
    table: "tasks",
    id: null,
    column: null,
    code: "key-pending",
    space: { scopeId: "projects", id: "project" },
  };
  const pending = [JSON.stringify(report)];
  const encryption = new Encryption({
    encryptionCall: vi.fn(),
    encryptionState: vi.fn(),
    takeEncryptionReports: () => pending.splice(0),
  });
  try {
    const first = vi.fn();
    encryption.onReport(first);
    await vi.advanceTimersByTimeAsync(25);
    expect(first).toHaveBeenCalledExactlyOnceWith(report);

    const second = vi.fn();
    encryption.onReport(second);
    await vi.advanceTimersByTimeAsync(25);
    expect(second).toHaveBeenCalledExactlyOnceWith(report);
    expect(first).toHaveBeenCalledTimes(1);

    pending.push(JSON.stringify({ ...report, type: "withdrawn" }));
    await vi.advanceTimersByTimeAsync(25);
    const third = vi.fn();
    encryption.onReport(third);
    await vi.advanceTimersByTimeAsync(25);
    expect(third).not.toHaveBeenCalled();
  } finally {
    encryption.stop();
    vi.useRealTimers();
  }
});

it.each([true, false])(
  "honours each caller's deadline during shared verification (startup first: %s)",
  async (startupFirst) => {
    vi.useFakeTimers();
    try {
      let status: string | null = null;
      const call = vi.fn(async (op: string) => {
        if (op === "recoveryRestore") return JSON.stringify("active");
        throw new Error(`unexpected ${op}`);
      });
      const encryption = new Encryption(
        {
          encryptionCall: call,
          encryptionState: () => JSON.stringify({ status, stage: "initial sync" }),
          takeEncryptionReports: () => [],
        },
        { accountSecret: () => formatSecret(secret) },
      );
      if (startupFirst) encryption.start();
      const short = vi.fn();
      void encryption.ready({ timeoutMs: 10 }).catch(short);
      const long = encryption.ready({ timeoutMs: startupFirst ? 500 : 2 ** 31 });
      await vi.advanceTimersByTimeAsync(10);
      expect(short).toHaveBeenCalledWith(
        expect.objectContaining({
          code: "database",
          message: expect.stringContaining("initial sync"),
        }),
      );
      await vi.advanceTimersByTimeAsync(100);
      status = "awaiting-approval";
      await vi.advanceTimersByTimeAsync(100);
      await expect(long).resolves.toBe("active");
      expect(call.mock.calls).toEqual([["recoveryRestore", expect.anything()]]);
      encryption.stop();
    } finally {
      vi.useRealTimers();
    }
  },
);

it("lets a caller time out during recovery while another waits for the same result", async () => {
  vi.useFakeTimers();
  try {
    let finish!: (result: string) => void;
    const restoring = new Promise<string>((resolve) => {
      finish = resolve;
    });
    const { encryption, calls } = awaitingDevice(() => restoring);
    const long = encryption.ready();
    const short = vi.fn();
    void encryption.ready({ timeoutMs: 10 }).catch(short);
    await vi.advanceTimersByTimeAsync(10);
    expect(short).toHaveBeenCalledWith(expect.objectContaining({ code: "database" }));
    finish(JSON.stringify("active"));
    await expect(long).resolves.toBe("active");
    expect(calls.map(({ op }) => op)).toEqual(["ready", "recoveryRestore"]);
  } finally {
    vi.useRealTimers();
  }
});
