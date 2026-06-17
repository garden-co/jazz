import { beforeEach, describe, expect, it, vi } from "vitest";
import { isRef, shallowRef, triggerRef } from "vue";
import type { Session } from "../runtime/context.js";

// Override inject so tests can control what the provider key resolves to without
// mounting a real Vue component tree.  computed/shallowRef/triggerRef are kept real.
const mockClientRef = shallowRef<any>(null);

vi.mock("vue", async (importOriginal) => {
  const actual = await importOriginal<typeof import("vue")>();
  return {
    ...actual,
    inject: (_key: unknown, defaultValue: unknown) =>
      mockClientRef.value !== null ? mockClientRef : (defaultValue ?? null),
  };
});

import { useSession } from "./provider.js";

function makeSession(userId: string): Session {
  return { user_id: userId, claims: {}, authMode: "local-first" };
}

describe("vue/useSession", () => {
  beforeEach(() => {
    mockClientRef.value = null;
  });

  it("throws when no JazzProvider context is present", () => {
    expect(() => useSession()).toThrow("Jazz Vue composables must be used within <JazzProvider>");
  });

  it("returns a reactive ComputedRef", () => {
    mockClientRef.value = { session: null };

    const result = useSession();

    expect(isRef(result)).toBe(true);
  });

  it(".value reflects the current session", () => {
    const session = makeSession("alice");
    mockClientRef.value = { session };

    const sessionRef = useSession();

    expect(sessionRef.value).toEqual(session);
  });

  it(".value is null when the client has no session", () => {
    mockClientRef.value = { session: null };

    const sessionRef = useSession();

    expect(sessionRef.value).toBeNull();
  });

  it(".value updates when triggerRef is called after the session getter changes", () => {
    // Model the real JazzClient: session is exposed via a getter over a mutable local variable.
    // onAuthChanged updates the variable and calls triggerRef(clientRef).
    let currentSession: Session | null = makeSession("alice");
    const client = {
      get session() {
        return currentSession;
      },
    };
    mockClientRef.value = client;

    const sessionRef = useSession();
    expect(sessionRef.value).toEqual(makeSession("alice"));

    currentSession = makeSession("bob");
    triggerRef(mockClientRef);

    expect(sessionRef.value).toEqual(makeSession("bob"));
  });

  it(".value goes null when session is cleared and triggerRef fires", () => {
    let currentSession: Session | null = makeSession("alice");
    const client = {
      get session() {
        return currentSession;
      },
    };
    mockClientRef.value = client;

    const sessionRef = useSession();
    expect(sessionRef.value).toEqual(makeSession("alice"));

    currentSession = null;
    triggerRef(mockClientRef);

    expect(sessionRef.value).toBeNull();
  });
});
