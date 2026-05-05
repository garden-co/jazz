import { afterEach, describe, expect, it, vi } from "vitest";
import { createDbWithRuntimeModule, type DbConfig } from "./db.js";
import { LocalFirstAuthManager, resolveDbAuthConfig } from "./db-auth.js";
import type { DbRuntimeModule, RuntimeTokenOptions } from "./db-runtime-module.js";

function createRuntimeModuleStub() {
  return {
    mintLocalFirstToken: vi.fn(
      (options: RuntimeTokenOptions) =>
        `local:${options.secret}:${options.audience}:${options.ttlSeconds}`,
    ),
    mintAnonymousToken: vi.fn(
      (options: RuntimeTokenOptions) =>
        `anonymous:${options.secret}:${options.audience}:${options.ttlSeconds}`,
    ),
  } as Pick<DbRuntimeModule<DbConfig>, "mintLocalFirstToken" | "mintAnonymousToken"> & {
    mintLocalFirstToken: ReturnType<typeof vi.fn<(options: RuntimeTokenOptions) => string>>;
    mintAnonymousToken: ReturnType<typeof vi.fn<(options: RuntimeTokenOptions) => string>>;
  };
}

class LoadingRuntimeModuleStub implements Pick<
  DbRuntimeModule<DbConfig>,
  "load" | "createClient" | "mintLocalFirstToken" | "mintAnonymousToken"
> {
  readonly load = vi.fn(async (_config: DbConfig) => undefined);
  readonly createClient = vi.fn(() => {
    throw new Error("createClient should not be called");
  });
  readonly mintLocalFirstToken = vi.fn((_options: RuntimeTokenOptions) => "local-jwt");
  readonly mintAnonymousToken = vi.fn((_options: RuntimeTokenOptions) => "anonymous-jwt");
}

describe("resolveDbAuthConfig", () => {
  it("rejects secret and jwtToken before runtime setup", async () => {
    const runtimeModule = createRuntimeModuleStub();
    const loadingRuntimeModule = new LoadingRuntimeModuleStub();

    expect(() =>
      resolveDbAuthConfig(
        {
          appId: "test-app",
          secret: "alice-secret",
          jwtToken: "existing-jwt",
        },
        runtimeModule,
      ),
    ).toThrow("mutually exclusive");

    expect(runtimeModule.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(runtimeModule.mintAnonymousToken).not.toHaveBeenCalled();

    await expect(
      createDbWithRuntimeModule(
        {
          appId: "test-app",
          secret: "alice-secret",
          jwtToken: "existing-jwt",
        },
        loadingRuntimeModule as unknown as DbRuntimeModule<DbConfig>,
      ),
    ).rejects.toThrow("mutually exclusive");
    expect(loadingRuntimeModule.load).not.toHaveBeenCalled();
  });

  it("rejects jwtToken and cookieSession before minting auth tokens", () => {
    const runtimeModule = createRuntimeModuleStub();

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
        runtimeModule,
      ),
    ).toThrow("mutually exclusive");

    expect(runtimeModule.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(runtimeModule.mintAnonymousToken).not.toHaveBeenCalled();
  });

  it("mints a local-first startup token when secret is present", () => {
    const runtimeModule = createRuntimeModuleStub();

    const resolved = resolveDbAuthConfig(
      {
        appId: "auth-app",
        secret: "alice-secret",
        serverUrl: "https://example.test",
      },
      runtimeModule,
    );

    expect(resolved.localFirstSecret).toBe("alice-secret");
    expect(resolved.config.secret).toBe("alice-secret");
    expect(resolved.config.jwtToken).toBe("local:alice-secret:auth-app:3600");
    expect(runtimeModule.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "auth-app",
        ttlSeconds: 3600,
      }),
    );
    const tokenOptions = runtimeModule.mintLocalFirstToken.mock.calls[0]?.[0];
    expect(tokenOptions?.nowSeconds).toBeTypeOf("bigint");
    expect(runtimeModule.mintAnonymousToken).not.toHaveBeenCalled();
  });

  it("mints an anonymous startup token when user auth is absent", () => {
    const runtimeModule = createRuntimeModuleStub();

    const resolved = resolveDbAuthConfig({ appId: "anonymous-app" }, runtimeModule);

    expect(resolved.localFirstSecret).toBeNull();
    expect(resolved.config.jwtToken).toMatch(/^anonymous:[A-Za-z0-9_-]{43}:anonymous-app:3600$/);
    expect(runtimeModule.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(runtimeModule.mintAnonymousToken).toHaveBeenCalledWith(
      expect.objectContaining({
        audience: "anonymous-app",
        ttlSeconds: 3600,
      }),
    );
  });

  it("does not mint anonymous auth for admin-secret clients", () => {
    const runtimeModule = createRuntimeModuleStub();

    const resolved = resolveDbAuthConfig(
      {
        appId: "admin-app",
        adminSecret: "admin-secret",
      },
      runtimeModule,
    );

    expect(resolved.localFirstSecret).toBeNull();
    expect(resolved.config.jwtToken).toBeUndefined();
    expect(runtimeModule.mintLocalFirstToken).not.toHaveBeenCalled();
    expect(runtimeModule.mintAnonymousToken).not.toHaveBeenCalled();
  });
});

describe("LocalFirstAuthManager", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("mints identity proofs with default ttl 60", () => {
    const runtimeModule = createRuntimeModuleStub();
    const manager = new LocalFirstAuthManager({
      appId: "auth-app",
      secret: "alice-secret",
      runtimeModule,
      applyToken: vi.fn(),
      isShuttingDown: () => false,
    });

    const proof = manager.getIdentityProof({ audience: "proof-audience" });

    expect(proof).toBe("local:alice-secret:proof-audience:60");
    expect(runtimeModule.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "proof-audience",
        ttlSeconds: 60,
      }),
    );
  });

  it("refreshes app auth at 80 percent of the 3600 second ttl", async () => {
    vi.useFakeTimers();
    const runtimeModule = createRuntimeModuleStub();
    const applyToken = vi.fn();
    const manager = new LocalFirstAuthManager({
      appId: "auth-app",
      secret: "alice-secret",
      runtimeModule,
      applyToken,
      isShuttingDown: () => false,
    });

    manager.start();

    await vi.advanceTimersByTimeAsync(2_879_999);
    expect(applyToken).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);

    expect(applyToken).toHaveBeenCalledWith("local:alice-secret:auth-app:3600");
    expect(runtimeModule.mintLocalFirstToken).toHaveBeenCalledWith(
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
    const runtimeModule = createRuntimeModuleStub();
    const applyToken = vi.fn();
    const manager = new LocalFirstAuthManager({
      appId: "auth-app",
      secret: "alice-secret",
      runtimeModule,
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
