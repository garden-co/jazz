import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("jazz-tools/backend", () => ({
  createJazzContext: () => ({ asBackend: () => ({}) }),
}));

describe("Jazz backend configuration", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.resetModules();
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
});
