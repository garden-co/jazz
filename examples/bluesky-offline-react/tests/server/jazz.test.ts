import { afterEach, describe, expect, it, vi } from "vitest";

const contexts = vi.hoisted(() => {
  const authenticationDb = { role: "authentication" };
  const projectionDb = { role: "projection" };
  const createJazzContext = vi.fn()
    .mockReturnValueOnce({ asBackend: () => authenticationDb })
    .mockReturnValueOnce({ asBackend: () => projectionDb });
  return { authenticationDb, projectionDb, createJazzContext };
});

vi.mock("jazz-tools/backend", () => ({
  createJazzContext: contexts.createJazzContext,
}));

describe("server Jazz contexts", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.resetModules();
    contexts.createJazzContext.mockClear();
  });

  it("refuses to start without Jazz sync credentials", async () => {
    vi.stubEnv("JAZZ_SERVER_URL", "");
    vi.stubEnv("BACKEND_SECRET", "secret");
    await expect(import("../../server/jazz.js")).rejects.toThrow("JAZZ_SERVER_URL is required");

    vi.resetModules();
    vi.stubEnv("JAZZ_SERVER_URL", "http://127.0.0.1:4200");
    vi.stubEnv("BACKEND_SECRET", "");
    await expect(import("../../server/jazz.js")).rejects.toThrow("BACKEND_SECRET is required");
  });

  it("keeps credentials and projections in separate synced replicas", async () => {
    vi.stubEnv("JAZZ_SERVER_URL", "http://127.0.0.1:4200");
    vi.stubEnv("BACKEND_SECRET", "backend-secret");
    vi.stubEnv("JAZZ_ADMIN_SECRET", "admin-secret");

    const jazz = await import("../../server/jazz.js");

    expect(contexts.createJazzContext).toHaveBeenCalledTimes(2);
    const authenticationConfig = contexts.createJazzContext.mock.calls[0]?.[0];
    expect(contexts.createJazzContext).toHaveBeenNthCalledWith(1, expect.objectContaining({
      backendSecret: "backend-secret",
      driver: { type: "persistent", dataPath: "./data/auth.db" },
      serverUrl: "http://127.0.0.1:4200",
    }));
    expect(authenticationConfig).toHaveProperty("adminSecret", "admin-secret");
    expect(contexts.createJazzContext).toHaveBeenNthCalledWith(2, expect.objectContaining({
      driver: { type: "persistent", dataPath: "./data/projection.db" },
      serverUrl: "http://127.0.0.1:4200",
      backendSecret: "backend-secret",
      adminSecret: "admin-secret",
    }));
    expect(jazz.authenticationDb).toBe(contexts.authenticationDb);
    expect(jazz.projectionDb).toBe(contexts.projectionDb);
  });
});
