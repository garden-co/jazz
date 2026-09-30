import { describe, expect, it, vi } from "vitest";
import {
  beginSignupIntent,
  claimsSignupIntent,
  enrollAndBootstrap,
  type IntentStorage,
} from "./enrollment.js";
import { JazzLifecycle } from "./jazz-lifecycle.js";

const email = "new@big-label.test";
const identityId = "identity-1";

function memoryStorage(): IntentStorage {
  const values = new Map<string, string>();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => void values.set(key, value),
    removeItem: (key) => void values.delete(key),
  };
}

/** An account registry that, like the real one, assigns an identity only once. */
function accounts(afterRegister: () => void = () => {}) {
  let assigned = false;
  const selected = { id: "A", identity: { issuer: "better-auth", subject: identityId } };
  const manager = {
    getLoggedIn: () => (assigned ? selected : undefined),
    registerJWT: vi.fn(async () => {
      if (assigned) throw new Error("identity_already_assigned");
      assigned = true;
      afterRegister();
      return selected;
    }),
    loginJWT: vi.fn(async () => {
      if (!assigned) throw new Error("identity_not_found");
      return selected;
    }),
  };
  const lifecycle = new JazzLifecycle(
    manager as never,
    async () => ({ shutdown: async () => {} }) as never,
    () => {},
  );
  return { manager, lifecycle };
}

function attempt(
  lifecycle: JazzLifecycle,
  storage: IntentStorage,
  bootstrap: (token: string) => Promise<void>,
  isCurrent: () => boolean,
) {
  return enrollAndBootstrap({
    lifecycle,
    storage,
    email,
    identityId,
    getToken: async () => "token",
    bootstrap,
    isCurrent,
  });
}

describe("BigLabel enrollment", () => {
  it("registers a new sign-up once when strict mode discards the first attempt", async () => {
    const { manager, lifecycle } = accounts();
    const storage = memoryStorage();
    const bootstrap = vi.fn(async () => {});
    beginSignupIntent(storage, email);

    // What React strict mode does to the effect: run, clean up, run again,
    // all before any of the first run's async work gets going.
    let firstIsCurrent = true;
    const first = attempt(lifecycle, storage, bootstrap, () => firstIsCurrent);
    firstIsCurrent = false;
    void lifecycle.close();
    const second = attempt(lifecycle, storage, bootstrap, () => true);

    await expect(first).resolves.toBe(false);
    await expect(second).resolves.toBe(true);
    expect(manager.registerJWT).toHaveBeenCalledTimes(1);
    expect(manager.loginJWT).not.toHaveBeenCalled();
    expect(bootstrap).toHaveBeenCalledTimes(1);
    expect(claimsSignupIntent(storage, email, identityId)).toBe(false);
  });

  it("logs in on the next attempt after an abandoned attempt already registered", async () => {
    // The session changes while the registration request is in flight.
    let firstIsCurrent = true;
    const { manager, lifecycle } = accounts(() => (firstIsCurrent = false));
    const storage = memoryStorage();
    const bootstrap = vi.fn(async () => {});
    beginSignupIntent(storage, email);

    await expect(attempt(lifecycle, storage, bootstrap, () => firstIsCurrent)).resolves.toBe(false);
    await expect(attempt(lifecycle, storage, bootstrap, () => true)).resolves.toBe(true);

    expect(manager.loginJWT).toHaveBeenCalledTimes(1);
    expect(bootstrap).toHaveBeenCalledTimes(1);
  });
});
