import type { AccountStore } from "../accounts/persistence.js";
import { decodeLocalDeviceStore } from "./local-device.js";

type StagedKey = { scope: string; groupId: string; epochId: string; payload: number[] };

function decode(value: string | null) {
  const state = decodeLocalDeviceStore(value) as ReturnType<typeof decodeLocalDeviceStore> & {
    stagedGroupKeysV1?: StagedKey[];
  };
  const entries = state.stagedGroupKeysV1 ?? [];
  if (!Array.isArray(entries)) throw new Error("Invalid staged E2EE group keys");
  const ids = new Set<string>();
  for (const entry of entries) {
    if (
      !entry ||
      typeof entry.scope !== "string" ||
      typeof entry.groupId !== "string" ||
      typeof entry.epochId !== "string" ||
      !Array.isArray(entry.payload) ||
      entry.payload.length !== 32 ||
      entry.payload.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255) ||
      ids.has(JSON.stringify([entry.scope, entry.groupId]))
    )
      throw new Error("Invalid staged E2EE group keys");
    ids.add(JSON.stringify([entry.scope, entry.groupId]));
  }
  return { state, entries };
}

/** Returns an owned candidate key; the caller must authenticate it and clear it. */
export async function loadStagedGroupKey(
  store: AccountStore,
  scope: string,
  groupId: string,
  assertOpen: () => void,
): Promise<{ epochId: string; secret: Uint8Array } | undefined> {
  const { entries } = decode(await store.read());
  assertOpen();
  const entry = entries.find((entry) => entry.scope === scope && entry.groupId === groupId);
  return entry ? { epochId: entry.epochId, secret: Uint8Array.from(entry.payload) } : undefined;
}

/** Private write-ahead staging only; no entry establishes an accepted group. */
export async function stageGroupKey(
  store: AccountStore,
  scope: string,
  groupId: string,
  epochId: string,
  payload: Uint8Array | null,
  assertOpen: () => void,
): Promise<void> {
  if (payload && payload.length !== 32) throw new Error("Invalid staged E2EE group key");
  const encoded = payload ? Array.from(payload) : undefined;
  let updated = false;
  try {
    await store.update((current) => {
      assertOpen();
      const { state, entries } = decode(current);
      const existing = entries.find((entry) => entry.scope === scope && entry.groupId === groupId);
      if (encoded && existing) throw new Error("E2EE group key is already staged");
      if (existing && existing.epochId !== epochId)
        throw new Error("Staged E2EE group epoch does not match");
      state.stagedGroupKeysV1 = entries.filter((entry) => entry !== existing);
      if (encoded) state.stagedGroupKeysV1.push({ scope, groupId, epochId, payload: encoded });
      const result = JSON.stringify(state);
      updated = true;
      return result;
    });
    if (!updated) throw new Error("E2EE key store did not perform the update");
    assertOpen();
  } finally {
    encoded?.fill(0);
  }
}
