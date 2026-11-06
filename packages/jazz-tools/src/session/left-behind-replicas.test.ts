import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AccountHandle } from "../accounts/state.js";
import {
  leftBehindReplicas,
  whileNoTabHolds,
  holdReplica,
  wipeLeftBehindReplicas,
  type ReplicaOwner,
} from "./left-behind-replicas.js";

const REGISTRY = "https://app.example/jazz";
const owner = (subject: string): ReplicaOwner => ({
  account: `account-${subject}`,
  identity: { issuer: "jazz:backend", subject },
});

/** A record holding `owners`, as an earlier page load left it. */
function recordOf(...owners: ReplicaOwner[]) {
  const record = leftBehindReplicas(REGISTRY);
  for (const o of owners)
    record.remember({ id: o.account, identity: o.identity } as unknown as AccountHandle);
  return record;
}

function wipeWith(
  record: ReturnType<typeof leftBehindReplicas>,
  overrides: Partial<Parameters<typeof wipeLeftBehindReplicas>[0]> = {},
) {
  const wipe = vi.fn(async (o: ReplicaOwner) => {
    record.forget(o.account);
    return true;
  });
  const done = wipeLeftBehindReplicas({
    record,
    current: () => undefined,
    signedIn: () => false,
    confirmSignedOut: async () => true,
    stillLeftBehind: () => true,
    wipe,
    ...overrides,
  });
  return { wipe: (overrides.wipe as typeof wipe | undefined) ?? wipe, done };
}

beforeEach(() => {
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
    removeItem: (key: string) => void storage.delete(key),
  });
});
afterEach(() => vi.unstubAllGlobals());

describe("wiping replicas a sign-out left behind", () => {
  it("records only the account and identity, never claims", () => {
    const record = leftBehindReplicas(REGISTRY);
    record.remember({
      id: "account-alice",
      identity: { issuer: "jazz:backend", subject: "alice" },
      claims: { email: "alice@example.com" },
    } as unknown as AccountHandle);
    expect(JSON.stringify(localStorage)).not.toContain("alice@example.com");
    expect(record.read()).toEqual([owner("alice")]);
  });

  it("wipes a user the session doesn't hold, and keeps the one it does", async () => {
    const record = recordOf(owner("alice"), owner("bob"));
    const { wipe, done } = wipeWith(record, {
      current: () => "account-bob",
      signedIn: () => true,
    });
    await done;
    expect(wipe.mock.calls.map(([o]) => o.identity.subject)).toEqual(["alice"]);
    expect(record.read()).toEqual([owner("bob")]);
  });

  it("wipes once a second answer confirms no one is signed in", async () => {
    const record = recordOf(owner("alice"));
    const { wipe, done } = wipeWith(record);
    await done;
    expect(wipe).toHaveBeenCalledTimes(1);
    expect(record.read()).toEqual([]);
  });

  it("wipes nothing on one signed-out answer the next one contradicts (a failed refresh)", async () => {
    const record = recordOf(owner("alice"));
    const { wipe, done } = wipeWith(record, { confirmSignedOut: async () => false });
    await done;
    expect(wipe).not.toHaveBeenCalled();
    expect(record.read()).toHaveLength(1);
  });

  it("wipes nothing when the backend can't be asked", async () => {
    const record = recordOf(owner("alice"));
    const { wipe, done } = wipeWith(record, {
      confirmSignedOut: async () => {
        throw new Error("offline");
      },
    });
    await done;
    expect(wipe).not.toHaveBeenCalled();
  });

  it("keeps a replica whose user signed back in, or that another tab holds", async () => {
    const record = recordOf(owner("alice"), owner("bob"));
    const { wipe, done } = wipeWith(record, {
      stillLeftBehind: (account) => account !== "account-alice",
      wipe: vi.fn(async () => false),
    });
    await done;
    expect(wipe).toHaveBeenCalledTimes(1);
    expect(record.read()).toHaveLength(2);
  });

  it("keeps a replica it failed to wipe for a later load", async () => {
    const record = recordOf(owner("alice"));
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { done } = wipeWith(record, {
      wipe: async () => {
        throw new Error("storage busy");
      },
    });
    await done;
    expect(record.read()).toHaveLength(1);
    expect(warned).toHaveBeenCalledWith(expect.stringMatching(/later load/), expect.any(Error));
    warned.mockRestore();
  });
});

describe("holding a replica open across tabs", () => {
  /** Web Locks as a browser has them, for shared and exclusive `ifAvailable` requests. */
  function fakeLocks() {
    const held = new Map<string, number>();
    return {
      async request(
        name: string,
        options: { mode: "shared" | "exclusive"; ifAvailable?: boolean },
        callback: (lock: unknown) => Promise<unknown>,
      ) {
        if (options.mode === "exclusive") {
          if (held.get(name)) return callback(null);
          held.set(name, -1);
        } else held.set(name, (held.get(name) ?? 0) + 1);
        try {
          return await callback({ name });
        } finally {
          if (options.mode === "exclusive") held.delete(name);
          else held.set(name, held.get(name)! - 1);
        }
      },
    };
  }

  it("doesn't wipe a replica another tab holds open, and does once it lets go", async () => {
    vi.stubGlobal("navigator", { locks: fakeLocks() });
    const release = await holdReplica(REGISTRY, "account-alice");
    const wipe = vi.fn(async () => {});
    expect(await whileNoTabHolds(REGISTRY, "account-alice", wipe)).toBe(false);
    expect(wipe).not.toHaveBeenCalled();
    release();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(await whileNoTabHolds(REGISTRY, "account-alice", wipe)).toBe(true);
    expect(wipe).toHaveBeenCalledTimes(1);
  });
});
