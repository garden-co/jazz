import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RememberedBackendUser } from "../accounts/enrollment.js";
import { leftBehindReplicas, wipeLeftBehindReplicas } from "./left-behind-replicas.js";

const user = (subject: string): RememberedBackendUser => ({
  account: `account-${subject}`,
  identity: { issuer: "jazz:backend", subject },
  claims: {},
});

/** A record holding `users`, as an earlier page load left it. */
function recordOf(...users: RememberedBackendUser[]) {
  localStorage.setItem(
    `jazz-clear-on-sign-out-v1:${encodeURIComponent("https://app.example/jazz")}`,
    JSON.stringify(users),
  );
  return leftBehindReplicas("https://app.example/jazz");
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
  it("wipes a user the backend no longer names, and keeps the one it does", async () => {
    const record = recordOf(user("alice"), user("bob"));
    const wipe = vi.fn(async (_user: RememberedBackendUser) => {});
    await wipeLeftBehindReplicas({
      record,
      signedIn: async () => ({ issuer: "jazz:backend", subject: "bob" }),
      wipe,
    });
    expect(wipe.mock.calls.map(([u]) => u.identity.subject)).toEqual(["alice"]);
    expect(record.read().map((u) => u.identity.subject)).toEqual(["bob"]);
  });

  it("wipes every replica once no one is signed in", async () => {
    const record = recordOf(user("alice"));
    const wipe = vi.fn(async (_user: RememberedBackendUser) => {});
    await wipeLeftBehindReplicas({ record, signedIn: async () => null, wipe });
    expect(wipe).toHaveBeenCalledTimes(1);
    expect(record.read()).toEqual([]);
  });

  it("wipes nothing when the backend can't be asked", async () => {
    const record = recordOf(user("alice"));
    const wipe = vi.fn(async (_user: RememberedBackendUser) => {});
    await wipeLeftBehindReplicas({
      record,
      signedIn: async () => {
        throw new Error("offline");
      },
      wipe,
    });
    expect(wipe).not.toHaveBeenCalled();
    expect(record.read()).toHaveLength(1);
  });

  it("keeps a replica it failed to wipe for the next load", async () => {
    const record = recordOf(user("alice"));
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    await wipeLeftBehindReplicas({
      record,
      signedIn: async () => null,
      wipe: async () => {
        throw new Error("storage busy");
      },
    });
    expect(record.read()).toHaveLength(1);
    expect(warned).toHaveBeenCalledWith(expect.stringMatching(/next load/), expect.any(Error));
    warned.mockRestore();
  });
});
