// A stand-in for `db.encryption` in component tests: it keeps this browser's
// device ID, status and the account's devices, records every call, and lets
// a test change what the next check sees.
import { vi } from "vitest";
import type { AccountStatus, EncryptionHandle } from "../src/lib/device-state";

/** An encryption failure as Jazz gives it: a stable `code`. */
export function encryptionError(code: string, message = code): Error {
  return Object.assign(new Error(message), { name: "EncryptionError", code });
}

export interface FakeDevice {
  id: string;
  status: "active" | "waiting";
  removalRequested?: boolean;
}

export function fakeEncryption(
  initial: {
    deviceId?: string;
    status?: AccountStatus;
    devices?: FakeDevice[];
  } = {},
) {
  const state = {
    deviceId: initial.deviceId ?? "device-this",
    status: initial.status ?? ("active" as AccountStatus),
    devices: initial.devices ?? [{ id: initial.deviceId ?? "device-this", status: "active" }],
    /** Set to make every call reject with this failure (a closed runtime). */
    failure: null as Error | null,
    recoveryRoots: [] as { id: string; device: string }[],
  };
  const calls: string[] = [];
  const challengeListeners = new Set<() => void>();
  const reportListeners = new Set<(report: { type: string; [key: string]: unknown }) => void>();
  const guard = async () => {
    if (state.failure) throw state.failure;
  };

  const handle = {
    deviceId: vi.fn(async () => {
      await guard();
      return state.deviceId;
    }),
    ready: vi.fn(async () => {
      await guard();
      return state.status;
    }),
    devices: {
      list: vi.fn(async () => {
        await guard();
        return state.devices.map((device) => ({
          ...device,
          isThisDevice: device.id === state.deviceId,
          removalRequested: device.removalRequested ?? false,
        }));
      }),
      challenge: vi.fn(async (deviceId: string) => {
        calls.push(`challenge:${deviceId}`);
        return {
          deviceId,
          code: vi.fn(async () => "123456" as string | null),
          approve: vi.fn(async (code: string) => {
            calls.push(`approve:${code}`);
            if (code !== "123456") throw encryptionError("approval-code-mismatch");
            state.devices = state.devices.map((device) =>
              device.id === deviceId ? { ...device, status: "active" } : device,
            );
          }),
        };
      }),
      answer: vi.fn(async () => "654321"),
      remove: vi.fn(async (deviceId: string) => {
        calls.push(`remove:${deviceId}`);
        state.devices = state.devices.filter((device) => device.id !== deviceId);
      }),
      requestRemoval: vi.fn(async () => {
        calls.push("requestRemoval");
        state.devices = state.devices.map((device) =>
          device.id === state.deviceId ? { ...device, removalRequested: true } : device,
        );
      }),
    },
    recovery: {
      list: vi.fn(async () => state.recoveryRoots),
      enable: vi.fn(
        async (): Promise<{ method: "passkey" } | { method: "phrase"; phrase: string }> => ({
          method: "passkey",
        }),
      ),
      restore: vi.fn(async (_phrase: string) => {
        state.status = "active";
        return state.status;
      }),
      restoreFromPasskey: vi.fn(async (_options: { appName: string }) => {
        state.status = "active";
        return state.status;
      }),
    },
    onChallenge: vi.fn((listener: () => void) => {
      challengeListeners.add(listener);
      return () => challengeListeners.delete(listener);
    }),
    onReport: vi.fn((listener: (report: { type: string; [key: string]: unknown }) => void) => {
      reportListeners.add(listener);
      return () => reportListeners.delete(listener);
    }),
  };

  return {
    state,
    calls,
    handle,
    encryption: handle as unknown as EncryptionHandle,
    challenge: () => challengeListeners.forEach((listener) => listener()),
    report: (report: { type: string; [key: string]: unknown }) =>
      reportListeners.forEach((listener) => listener(report)),
  };
}
