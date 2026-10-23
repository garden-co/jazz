import type { AccountStore } from "../accounts/persistence.js";
import {
  withLocalDeviceStore,
  type DeviceKeyLifetime,
  type StoredDevices,
} from "./local-device.js";

type Entry = { scope: string; spaceId: string; epochId: string; payload: number[] };
type SpaceStore = StoredDevices & { recoveredSpaceKeysV1?: Entry[] };

function recoveredStore(state: SpaceStore) {
  const entries = state.recoveredSpaceKeysV1 ?? [];
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  if (!Array.isArray(entries)) throw new Error("Invalid recovered E2EE space keys");
  const seen = new Set<string>();
  for (const entry of entries) {
    if (
      !entry ||
      typeof entry.scope !== "string" ||
      !entry.scope ||
      typeof entry.spaceId !== "string" ||
      !uuid.test(entry.spaceId) ||
      typeof entry.epochId !== "string" ||
      !uuid.test(entry.epochId) ||
      !Array.isArray(entry.payload) ||
      entry.payload.length !== 32 ||
      entry.payload.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)
    )
      throw new Error("Invalid recovered E2EE space keys");
    const id = JSON.stringify([entry.scope, entry.spaceId, entry.epochId]);
    if (seen.has(id)) throw new Error("Duplicate recovered E2EE space key");
    seen.add(id);
  }
  return { state, entries };
}

/** Local candidate only: callers must revalidate membership and the full key history. */
export async function loadRecoveredSpaceKey(
  store: AccountStore,
  scope: string,
  spaceId: string,
  epochId: string,
  assertOpen: () => void,
  keyLifetime: DeviceKeyLifetime,
): Promise<Uint8Array | undefined> {
  const current = await store.read();
  assertOpen();
  return withLocalDeviceStore(current, keyLifetime, (parsed) => {
    const { entries } = recoveredStore(parsed);
    const entry = entries.find(
      (entry) => entry.scope === scope && entry.spaceId === spaceId && entry.epochId === epochId,
    );
    return entry ? Uint8Array.from(entry.payload) : undefined;
  });
}

export async function retainRecoveredSpaceKey(
  store: AccountStore,
  scope: string,
  spaceId: string,
  epochId: string,
  secret: Uint8Array,
  assertOpen: () => void,
  keyLifetime: DeviceKeyLifetime,
): Promise<void> {
  if (secret.length !== 32) throw new Error("Invalid recovered E2EE space key");
  const payload = Array.from(secret);
  let updated = false;
  try {
    await store.update((value) => {
      assertOpen();
      return withLocalDeviceStore(value, keyLifetime, (parsed) => {
        const { state, entries } = recoveredStore(parsed);
        const existing = entries.find(
          (entry) =>
            entry.scope === scope && entry.spaceId === spaceId && entry.epochId === epochId,
        );
        if (existing && existing.payload.some((byte, i) => byte !== payload[i]))
          throw new Error("Conflicting recovered E2EE space key");
        state.recoveredSpaceKeysV1 = existing
          ? entries
          : [...entries, { scope, spaceId, epochId, payload }];
        recoveredStore(state);
        const result = JSON.stringify(state);
        updated = true;
        return result;
      });
    });
    if (!updated) throw new Error("E2EE key store did not perform the update");
    assertOpen();
  } finally {
    payload.fill(0);
  }
}
