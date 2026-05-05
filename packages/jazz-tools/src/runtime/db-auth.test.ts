import { afterEach, describe, expect, it, vi } from "vitest";
import { createDb, type DbConfig } from "./db.js";
import { LocalFirstAuthManager, resolveDbAuthConfig } from "./db-auth.js";
import type { BackendTokenOptions, DbBackendModule } from "./db-backend.js";

function createBackendModuleStub() {
  return {
    mintLocalFirstToken: vi.fn(
      (options: BackendTokenOptions) =>
        `local:${options.secret}:${options.audience}:${options.ttlSeconds}`,
    ),
    mintAnonymousToken: vi.fn(
      (options: BackendTokenOptions) =>
        `anonymous:${options.secret}:${options.audience}:${options.ttlSeconds}`,
    ),
  } as Pick<DbBackendModule<DbConfig>, "mintLocalFirstToken" | "mintAnonymousToken"> & {
    mintLocalFirstToken: ReturnType<typeof vi.fn<(options: BackendTokenOptions) => string>>;
    mintAnonymousToken: ReturnType<typeof vi.fn<(options: BackendTokenOptions) => string>>;
  };
}

class LoadingBackendModuleStub implements Pick<
  DbBackendModule<DbConfig>,
  "load" | "createClient" | "mintLocalFirstToken" | "mintAnonymousToken"
> {
  readonly load = vi.fn(async (_config: DbConfig) => undefined);
  readonly createClient = vi.fn(() => {
    throw new Error("createClient should not be called");
  });
  readonly mintLocalFirstToken = vi.fn((_options: BackendTokenOptions) => "local-jwt");
  readonly mintAnonymousToken = vi.fn((_options: BackendTokenOptions) => "anonymous-jwt");
}

describe("resolveDbAuthConfig", () => {
  it("rejects secret and jwtToken before backend setup", async () => {
    const backend = createBackendModuleStub();
    const loadingBackendModule = new LoadingBackendModuleStub();

    expect(() =>
      resolveDbAuthConfig(
        {
          appId: "test-app",
          secret: "alice-secret",
          jwtToken: "existing-jwt",
        },
        backend,
      ),
    ).toThrow("mutually exclusive");

    expect(backend.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(backend.mintAnonymousToken).not.toHaveBeenCalled();

    await expect(
      createDb({
        appId: "test-app",
        secret: "alice-secret",
        jwtToken: "existing-jwt",
        runtime: loadingBackendModule as unknown as DbBackendModule<DbConfig>,
      }),
    ).rejects.toThrow("mutually exclusive");
    expect(loadingBackendModule.load).not.toHaveBeenCalled();
  });

  it("rejects jwtToken and cookieSession before minting auth tokens", () => {
    const backend = createBackendModuleStub();

    expect(() =>
      resolveDbAuthConfig(
        {
          appId: "test-app",
          jwtToken: "existing-jwt",
          cookieSession: {
            user_id: "alice",
            claims: { role: "reader" },
            authMode: "external",
          },
        },
        backend,
      ),
    ).toThrow("mutually exclusive");

    expect(backend.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(backend.mintAnonymousToken).not.toHaveBeenCalled();
  });

  it("mints a local-first startup token when secret is present", () => {
    const backend = createBackendModuleStub();

    const resolved = resolveDbAuthConfig(
      {
        appId: "auth-app",
        secret: "alice-secret",
        serverUrl: "https://example.test",
      },
      backend,
    );

    expect(resolved.localFirstSecret).toBe("alice-secret");
    expect(resolved.config.secret).toBe("alice-secret");
    expect(resolved.config.jwtToken).toBe("local:alice-secret:auth-app:3600");
    expect(backend.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "auth-app",
        ttlSeconds: 3600,
      }),
    );
    const tokenOptions = backend.mintLocalFirstToken.mock.calls[0]?.[0];
    expect(tokenOptions?.nowSeconds).toBeTypeOf("bigint");
    expect(backend.mintAnonymousToken).not.toHaveBeenCalled();
  });

  it("mints an anonymous startup token when user auth is absent", () => {
    const backend = createBackendModuleStub();

    const resolved = resolveDbAuthConfig({ appId: "anonymous-app" }, backend);

    expect(resolved.localFirstSecret).toBeNull();
    expect(resolved.config.jwtToken).toMatch(/^anonymous:[A-Za-z0-9_-]{43}:anonymous-app:3600$/);
    expect(backend.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(backend.mintAnonymousToken).toHaveBeenCalledWith(
      expect.objectContaining({
        audience: "anonymous-app",
        ttlSeconds: 3600,
      }),
    );
  });

  it("does not mint anonymous auth for admin-secret clients", () => {
    const backend = createBackendModuleStub();

    const resolved = resolveDbAuthConfig(
      {
        appId: "admin-app",
        adminSecret: "admin-secret",
      },
      backend,
    );

    expect(resolved.localFirstSecret).toBeNull();
    expect(resolved.config.jwtToken).toBeUndefined();
    expect(backend.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(backend.mintAnonymousToken).not.toHaveBeenCalled();
  });
});

describe("LocalFirstAuthManager", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("mints identity proofs with default ttl 60", () => {
    const backend = createBackendModuleStub();
    const manager = new LocalFirstAuthManager({
      appId: "auth-app",
      secret: "alice-secret",
      backend,
      applyToken: vi.fn(),
      isShuttingDown: () => false,
    });

    const proof = manager.getIdentityProof({ audience: "proof-audience" });

    expect(proof).toBe("local:alice-secret:proof-audience:60");
    expect(backend.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "proof-audience",
        ttlSeconds: 60,
      }),
    );
  });

  it("refreshes app auth at 80 percent of the 3600 second ttl", async () => {
    vi.useFakeTimers();
    const backend = createBackendModuleStub();
    const applyToken = vi.fn();
    const manager = new LocalFirstAuthManager({
      appId: "auth-app",
      secret: "alice-secret",
      backend,
      applyToken,
      isShuttingDown: () => false,
    });

    manager.start();

    await vi.advanceTimersByTimeAsync(2_879_999);
    expect(applyToken).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);

    expect(applyToken).toHaveBeenCalledWith("local:alice-secret:auth-app:3600");
    expect(backend.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "auth-app",
        ttlSeconds: 3600,
      }),
    );
    expect(vi.getTimerCount()).toBe(1);

    manager.stop();
  });

  it("clears the refresh timer on stop", async () => {
    vi.useFakeTimers();
    const backend = createBackendModuleStub();
    const applyToken = vi.fn();
    const manager = new LocalFirstAuthManager({
      appId: "auth-app",
      secret: "alice-secret",
      backend,
      applyToken,
      isShuttingDown: () => false,
    });

    manager.start();
    expect(vi.getTimerCount()).toBe(1);

    manager.stop();

    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(2_880_000);
    expect(applyToken).not.toHaveBeenCalled();
  });
});
