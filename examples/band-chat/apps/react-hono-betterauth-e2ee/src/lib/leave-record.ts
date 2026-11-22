/**
 * The wish to leave this browser, kept across reloads.
 *
 * "Leave this browser" asks another of the user's devices to remove this one.
 * When that device has done so and this browser next checks its account, Jazz
 * erases the account's keys here and enrols the browser again under a new
 * device ID. The page notices by comparing this record with the device ID it
 * sees now; only a different ID proves the old keys are gone. A device ID is
 * not a secret.
 */
const KEY = "band-chat:leaving";

export interface LeaveRecord {
  /** The Jazz account that asked to leave. */
  accountId: string;
  /** This browser's device ID when it asked. */
  deviceId: string;
}

export function readLeaveRecord(): LeaveRecord | null {
  try {
    const value = JSON.parse(localStorage.getItem(KEY) ?? "null") as Partial<LeaveRecord> | null;
    return typeof value?.accountId === "string" && typeof value.deviceId === "string"
      ? { accountId: value.accountId, deviceId: value.deviceId }
      : null;
  } catch {
    return null;
  }
}

export function writeLeaveRecord(record: LeaveRecord): void {
  localStorage.setItem(KEY, JSON.stringify(record));
}

export function clearLeaveRecord(): void {
  localStorage.removeItem(KEY);
}

/**
 * Where leaving stands for `accountId`, given the device ID this browser has
 * now (`null` while unknown) and whether its encryption runtime has closed.
 * - `none`: no wish to leave for this account.
 * - `pending`: waiting for another device to remove this one.
 * - `complete`: a new device ID, so the old keys are gone; sign out.
 * - `unfinished`: the runtime closed (erasing the keys or storing the new
 *   device failed), so the keys may still be here. Never signs out.
 */
export type LeaveState = "none" | "pending" | "complete" | "unfinished";

export function leaveState(
  record: LeaveRecord | null,
  accountId: string,
  thisDeviceId: string | null,
  runtimeClosed: boolean,
): LeaveState {
  if (!record || record.accountId !== accountId) return "none";
  if (runtimeClosed) return "unfinished";
  if (thisDeviceId === null || thisDeviceId === record.deviceId) return "pending";
  return "complete";
}
