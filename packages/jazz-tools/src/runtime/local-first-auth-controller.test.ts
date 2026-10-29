import { describe, expect, it, vi } from "vitest";
import type { AuthSecretStore } from "./auth-secret-store.js";
import {
  getLocalFirstAuthController,
  initialLocalFirstAuthState,
} from "./local-first-auth-controller.js";

function memoryStore(initial = "secret-a") {
  let secret: string | null = initial;
  const store = {
    loadSecret: vi.fn(async () => secret),
    saveSecret: vi.fn(async (next: string) => {
      secret = next;
    }),
    clearSecret: vi.fn(async () => {
      secret = null;
    }),
    getOrCreateSecret: vi.fn(async () => {
      secret ??= "generated";
      return secret;
    }),
  } satisfies AuthSecretStore;
  return store;
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("local-first auth controller", () => {
  it("shares one controller and one load across consumers of a store", async () => {
    const store = memoryStore();
    const controller = getLocalFirstAuthController(store);
    expect(getLocalFirstAuthController(store)).toBe(controller);

    const first = vi.fn();
    const second = vi.fn();
    controller.subscribe(first);
    controller.subscribe(second);
    await flush();

    expect(store.getOrCreateSecret).toHaveBeenCalledTimes(1);
    expect(controller.getState()).toEqual({ secret: "secret-a", isLoading: false, error: null });
    expect(first).toHaveBeenCalled();
    expect(second).toHaveBeenCalled();
  });

  it("resets when the last subscriber leaves and loads again on the next one", async () => {
    const store = memoryStore();
    const controller = getLocalFirstAuthController(store);

    const unsubscribeA = controller.subscribe(() => {});
    const unsubscribeB = controller.subscribe(() => {});
    await flush();
    unsubscribeA();
    expect(controller.getState().secret).toBe("secret-a");

    unsubscribeB();
    expect(controller.getState()).toBe(initialLocalFirstAuthState);

    controller.subscribe(() => {});
    await flush();
    expect(store.getOrCreateSecret).toHaveBeenCalledTimes(2);
    expect(controller.getState().secret).toBe("secret-a");
  });

  it("ignores a load that resolves after the last subscriber left", async () => {
    const store = memoryStore();
    const controller = getLocalFirstAuthController(store);

    const unsubscribe = controller.subscribe(() => {});
    unsubscribe();
    await flush();

    expect(controller.getState()).toBe(initialLocalFirstAuthState);
  });

  it("reloads every subscriber after login and sign-out", async () => {
    const store = memoryStore();
    const controller = getLocalFirstAuthController(store);
    controller.subscribe(() => {});
    await flush();

    await controller.login("secret-b");
    await flush();
    expect(controller.getState().secret).toBe("secret-b");

    await controller.signOut();
    await flush();
    expect(store.clearSecret).toHaveBeenCalledTimes(1);
    expect(controller.getState().secret).toBe("generated");
  });

  it("still reloads subscribers when saving the secret rejects", async () => {
    const store = memoryStore();
    store.saveSecret.mockRejectedValueOnce(new Error("quota"));
    const controller = getLocalFirstAuthController(store);
    controller.subscribe(() => {});
    await flush();

    await expect(controller.login("secret-b")).rejects.toThrow("quota");
    await flush();

    expect(store.getOrCreateSecret).toHaveBeenCalledTimes(2);
    expect(controller.getState()).toEqual({ secret: "secret-a", isLoading: false, error: null });
  });

  it("still reloads subscribers when clearing the secret rejects", async () => {
    const store = memoryStore();
    store.clearSecret.mockRejectedValueOnce(new Error("locked"));
    const controller = getLocalFirstAuthController(store);
    controller.subscribe(() => {});
    await flush();

    await expect(controller.signOut()).rejects.toThrow("locked");
    await flush();

    expect(store.getOrCreateSecret).toHaveBeenCalledTimes(2);
    expect(controller.getState()).toEqual({ secret: "secret-a", isLoading: false, error: null });
  });

  it("captures load failures, including synchronous throws, as error state", async () => {
    const store = memoryStore();
    store.getOrCreateSecret.mockImplementationOnce(() => {
      throw new Error("malformed storage");
    });
    const controller = getLocalFirstAuthController(store);
    controller.subscribe(() => {});
    await flush();

    expect(controller.getState()).toMatchObject({ secret: null, isLoading: false });
    expect(controller.getState().error?.message).toBe("malformed storage");
  });

  it("does not warn again when a reload fails with the same error", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const store = memoryStore();
    const failure = new Error("boom");
    store.getOrCreateSecret.mockRejectedValue(failure);
    const controller = getLocalFirstAuthController(store);

    controller.watch(() => {}, "test");
    await flush();
    await controller.login("secret-b");
    await flush();
    expect(store.getOrCreateSecret).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenCalledTimes(1);

    store.getOrCreateSecret.mockRejectedValue(new Error("boom again"));
    await controller.login("secret-c");
    await flush();
    expect(warn).toHaveBeenCalledTimes(2);
    warn.mockRestore();
  });

  it("warns once per distinct load failure for each watcher", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const store = memoryStore();
    store.getOrCreateSecret.mockRejectedValueOnce(new Error("boom"));
    const controller = getLocalFirstAuthController(store);
    const apply = vi.fn();

    controller.watch(apply, "test");
    await flush();
    controller.subscribe(() => {});
    await flush();

    expect(warn).toHaveBeenCalledTimes(1);
    expect(apply).toHaveBeenLastCalledWith(
      expect.objectContaining({ isLoading: false, error: expect.any(Error) }),
    );
    warn.mockRestore();
  });
});
