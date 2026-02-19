import { describe, expect, it } from "vitest";
import {
  localAuthTokenStorageKey,
  resolveLocalAuthDefaults,
  type LocalAuthStorageLike,
} from "./local-auth.js";

class MemoryStorage implements LocalAuthStorageLike {
  private readonly data = new Map<string, string>();

  getItem(key: string): string | null {
    return this.data.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
}

describe("local-auth defaults", () => {
  it("defaults to anonymous mode with persisted token when no auth is configured", () => {
    const storage = new MemoryStorage();

    const first = resolveLocalAuthDefaults({ appId: "app-defaults" }, { storage });
    const second = resolveLocalAuthDefaults({ appId: "app-defaults" }, { storage });

    expect(first.localAuthMode).toBe("anonymous");
    expect(first.localAuthToken).toBeTruthy();
    expect(second.localAuthMode).toBe("anonymous");
    expect(second.localAuthToken).toBe(first.localAuthToken);
  });

  it("uses explicit token override and defaults mode to anonymous", () => {
    const resolved = resolveLocalAuthDefaults({
      appId: "app-explicit-token",
      localAuthToken: "my-device-token",
    });

    expect(resolved.localAuthMode).toBe("anonymous");
    expect(resolved.localAuthToken).toBe("my-device-token");
  });

  it("keeps local auth unset when JWT auth is configured", () => {
    const storage = new MemoryStorage();
    const resolved = resolveLocalAuthDefaults(
      {
        appId: "app-jwt",
        jwtToken: "external-jwt",
      },
      { storage },
    );

    expect(resolved.localAuthMode).toBeUndefined();
    expect(resolved.localAuthToken).toBeUndefined();
  });

  it("generates and persists token when local mode is explicit but token is omitted", () => {
    const storage = new MemoryStorage();
    const resolved = resolveLocalAuthDefaults(
      {
        appId: "app-explicit-mode",
        localAuthMode: "anonymous",
      },
      { storage },
    );

    expect(resolved.localAuthMode).toBe("anonymous");
    expect(resolved.localAuthToken).toBeTruthy();
    expect(storage.getItem(localAuthTokenStorageKey("app-explicit-mode", "anonymous"))).toBe(
      resolved.localAuthToken,
    );
  });
});
