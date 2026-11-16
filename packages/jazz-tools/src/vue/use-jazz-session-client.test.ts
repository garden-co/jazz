import { beforeEach, describe, expect, it, vi } from "vitest";
import { shallowRef, triggerRef } from "vue";
import type { PublicSession } from "../runtime/context.js";

// Only the client provider's key (Symbol("jazz")) resolves, to this ref: no
// <JazzProvider> session above, as under <JazzClientProvider client>.
const mockClientRef = shallowRef<any>(null);
let clientProvided = true;

vi.mock("vue", async (importOriginal) => {
  const actual = await importOriginal<typeof import("vue")>();
  return {
    ...actual,
    inject: (key: unknown, defaultValue: unknown) =>
      String(key) === "Symbol(jazz)" && clientProvided ? mockClientRef : defaultValue,
  };
});

import { useJazzSession } from "./session.js";

function makeSession(subject: string): PublicSession {
  return {
    user: "00000000-0000-4000-8000-000000000002",
    identity: { issuer: "https://issuer.example", subject },
    claims: { role: "admin" },
    authMode: "external",
  };
}

describe("vue/useJazzSession under <JazzClientProvider client>", () => {
  beforeEach(() => {
    clientProvided = true;
    mockClientRef.value = null;
  });

  it("is starting until the client resolves, then ready with the client's user", () => {
    const { snapshot } = useJazzSession();
    expect(snapshot.value.status).toBe("starting");
    let current: PublicSession | null = makeSession("alice");
    const client = {
      get session() {
        return current;
      },
      db: {},
    };
    mockClientRef.value = client;
    expect(snapshot.value.status).toBe("ready");
    expect(snapshot.value.client).toBe(client);
    expect(snapshot.value.user).toEqual({
      accountId: "00000000-0000-4000-8000-000000000002",
      kind: "backend",
      identity: { issuer: "https://issuer.example", subject: "alice" },
      claims: { role: "admin" },
    });
    // The provider triggers the ref when the client's auth changes.
    current = null;
    triggerRef(mockClientRef);
    expect(snapshot.value.user).toBeNull();
  });

  it("refuses session operations, naming why", async () => {
    mockClientRef.value = { session: makeSession("alice"), db: {} };
    const session = useJazzSession();
    await expect(session.signOut()).rejects.toThrow(/outside a Jazz session/);
    expect(() => session.exportDeviceKey()).toThrow(/outside a Jazz session/);
  });

  it("still throws outside any provider", () => {
    clientProvided = false;
    expect(() => useJazzSession()).toThrow(/<JazzProvider>, <JazzClientProvider>/);
  });
});
