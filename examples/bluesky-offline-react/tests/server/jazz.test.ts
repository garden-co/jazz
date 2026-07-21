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

  it("keeps credentials local while projecting through a separate synced replica", async () => {
    vi.stubEnv("JAZZ_SERVER_URL", "http://127.0.0.1:4200");
    vi.stubEnv("BACKEND_SECRET", "backend-secret");
    vi.stubEnv("JAZZ_ADMIN_SECRET", "admin-secret");

    const jazz = await import("../../server/jazz.js");

    expect(contexts.createJazzContext).toHaveBeenCalledTimes(2);
    expect(contexts.createJazzContext).toHaveBeenNthCalledWith(1, expect.objectContaining({
      driver: { type: "persistent", dataPath: "./data/jazz.db" },
      serverUrl: undefined,
    }));
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
