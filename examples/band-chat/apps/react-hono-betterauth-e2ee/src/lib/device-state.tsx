import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { useDb } from "jazz-tools/react";
import { leaveState, readLeaveRecord, type LeaveRecord, type LeaveState } from "./leave-record";

/** The app's encryption handle: device approval, removal and recovery. */
export type EncryptionHandle = ReturnType<typeof useDb>["encryption"];
export type AccountStatus = Awaited<ReturnType<EncryptionHandle["ready"]>>;
export type DeviceInfo = Awaited<ReturnType<EncryptionHandle["devices"]["list"]>>[number];

export interface Failure {
  code: string;
  message: string;
}

/** An encryption call's failure, with the stable `code` Jazz gives it. */
export function failureOf(error: unknown): Failure {
  const code = (error as { code?: unknown } | null)?.code;
  return {
    code: typeof code === "string" ? code : "error",
    message: error instanceof Error ? error.message : String(error),
  };
}

/** How often the device list is read again. */
const POLL_MS = 3000;

export interface DeviceState {
  encryption: EncryptionHandle;
  accountId: string;
  /** `null` until the first check has answered. */
  status: AccountStatus | null;
  thisDeviceId: string | null;
  /** The account's devices; `null` until read, and while this browser is not active. */
  devices: DeviceInfo[] | null;
  /** The last check's failure, if it failed. */
  failure: Failure | null;
  /** The account check that runs at start-up gave up (a `ready-failed` report). */
  readyFailed: Failure | null;
  /** On a browser waiting for approval: the code it showed after answering a challenge. */
  approval: { code: string } | { failure: Failure } | null;
  /** Where leaving this browser stands (see `leaveState`). */
  leaving: LeaveState;
  /** Checks again now, after an action. */
  refresh(): Promise<void>;
  /** Checks the account again after a `ready-failed` report. */
  retryReady(): Promise<void>;
  /** Answers the challenge another device started; shows the code. */
  answerChallenge(): Promise<void>;
  /** Re-reads the leave record (after it was written or cleared). */
  reloadLeaveRecord(): void;
}

const DeviceStateContext = createContext<DeviceState | null>(null);

/** The device state, or `null` outside a `DeviceStateProvider`. */
export function useDeviceStateIfAny(): DeviceState | null {
  return useContext(DeviceStateContext);
}

export function useDeviceState(): DeviceState {
  const state = useContext(DeviceStateContext);
  if (!state) throw new Error("useDeviceState needs a DeviceStateProvider");
  return state;
}

/**
 * Watches this browser's place in the account: its status, its device ID and
 * the account's device list, read at start-up and again every few seconds.
 * Jazz itself notices when the account changes (an approval, a completed
 * removal) and acts on it; this reads the result.
 */
export function DeviceStateProvider({
  encryption,
  accountId,
  pollMs = POLL_MS,
  children,
}: {
  encryption: EncryptionHandle;
  accountId: string;
  /** How often to check again, in milliseconds. */
  pollMs?: number;
  children: ReactNode;
}) {
  const [status, setStatus] = useState<AccountStatus | null>(null);
  const [thisDeviceId, setThisDeviceId] = useState<string | null>(null);
  const [devices, setDevices] = useState<DeviceInfo[] | null>(null);
  const [failure, setFailure] = useState<Failure | null>(null);
  const [readyFailed, setReadyFailed] = useState<Failure | null>(null);
  const [approval, setApproval] = useState<DeviceState["approval"]>(null);
  const [record, setRecord] = useState<LeaveRecord | null>(readLeaveRecord);
  const statusRef = useRef<AccountStatus | null>(null);
  const inFlight = useRef<Promise<void> | null>(null);
  const mounted = useRef(true);

  const check = useCallback(async () => {
    try {
      // Rejects with code `closed` once Jazz has closed its encryption
      // runtime, as it does when erasing the keys after a removal fails.
      const id = await encryption.deviceId();
      if (!mounted.current) return;
      // A replacement ID completes a leave even when the new device cannot list devices yet.
      setThisDeviceId(id);
      let next = statusRef.current;
      // A waiting browser asks again each time, so it sees its approval; an
      // active one reads the list and asks again only if it left the list.
      if (next !== "active") next = await encryption.ready();
      let listed: DeviceInfo[] | null = null;
      if (next === "active") {
        listed = await encryption.devices.list();
        const mine = listed.find((device) => device.isThisDevice);
        if (mine?.status !== "active") {
          next = await encryption.ready();
          if (next !== "active") listed = null;
        }
      }
      if (!mounted.current) return;
      statusRef.current = next;
      setStatus(next);
      setDevices((previous) => (sameDevices(previous, listed) ? previous : listed));
      setFailure(null);
      if (next === "active") setApproval(null);
    } catch (error) {
      if (!mounted.current) return;
      const failed = failureOf(error);
      // Before the local-first client has loaded the handle waits, then
      // gives up; the next check tries again.
      if ((error as Error | null)?.name === "EncryptionNeedsLocalFirstError") return;
      setFailure(failed);
    }
  }, [encryption]);

  const refresh = useCallback(() => {
    inFlight.current ??= check().finally(() => {
      inFlight.current = null;
    });
    return inFlight.current;
  }, [check]);

  useEffect(() => {
    mounted.current = true;
    void refresh();
    const timer = setInterval(() => void refresh(), pollMs);
    return () => {
      mounted.current = false;
      clearInterval(timer);
    };
  }, [refresh, pollMs]);

  const answerChallenge = useCallback(async () => {
    try {
      const code = await encryption.devices.answer();
      setApproval({ code });
    } catch (error) {
      setApproval({ failure: failureOf(error) });
    }
  }, [encryption]);

  useEffect(() => {
    const stopChallenge = encryption.onChallenge(() => void answerChallenge());
    const stopReports = encryption.onReport((report) => {
      if (report.type === "ready-failed") {
        setReadyFailed({ code: String(report.code), message: String(report.message) });
      }
    });
    return () => {
      stopChallenge();
      stopReports();
    };
  }, [encryption, answerChallenge]);

  const retryReady = useCallback(async () => {
    try {
      const next = await encryption.ready();
      statusRef.current = next;
      setStatus(next);
      setReadyFailed(null);
    } catch (error) {
      setReadyFailed(failureOf(error));
    }
    await refresh();
  }, [encryption, refresh]);

  const leaving = leaveState(record, accountId, thisDeviceId, failure?.code === "closed");

  const value = useMemo<DeviceState>(
    () => ({
      encryption,
      accountId,
      status,
      thisDeviceId,
      devices,
      failure,
      readyFailed,
      approval,
      leaving,
      refresh,
      retryReady,
      answerChallenge,
      reloadLeaveRecord: () => setRecord(readLeaveRecord()),
    }),
    [
      encryption,
      accountId,
      status,
      thisDeviceId,
      devices,
      failure,
      readyFailed,
      approval,
      leaving,
      refresh,
      retryReady,
      answerChallenge,
    ],
  );
  return <DeviceStateContext.Provider value={value}>{children}</DeviceStateContext.Provider>;
}

function sameDevices(a: DeviceInfo[] | null, b: DeviceInfo[] | null): boolean {
  if (a === null || b === null) return a === b;
  return (
    a.length === b.length &&
    a.every(
      (device, i) =>
        device.id === b[i]!.id &&
        device.status === b[i]!.status &&
        device.isThisDevice === b[i]!.isThisDevice,
    )
  );
}
