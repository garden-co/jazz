import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDb = {
  shutdown: vi.fn().mockResolvedValue(undefined),
  subscribeAll: vi.fn(),
};

const mockSession = { user_id: "alice", claims: { role: "admin" } };

vi.mock("../runtime/db.js", () => ({
  createDb: vi.fn().mockResolvedValue(mockDb),
}));

vi.mock("../runtime/client-session.js", () => ({
  resolveClientSession: vi.fn().mockResolvedValue(mockSession),
}));

vi.mock("../runtime/local-auth.js", () => ({
  resolveLocalAuthDefaults: vi.fn((config) => config),
}));

const { createJazzClient } = await import("./create-jazz-client.js");
const { createDb } = await import("../runtime/db.js");
const { resolveClientSession } = await import("../runtime/client-session.js");
const { resolveLocalAuthDefaults } = await import("../runtime/local-auth.js");

describe("createJazzClient", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("resolves local auth defaults before creating db", async () => {
    const config = { appId: "test-app", env: "dev" } as any;
    await createJazzClient(config);

    expect(resolveLocalAuthDefaults).toHaveBeenCalledWith(config);
  });

  it("creates db and resolves session in parallel", async () => {
    const config = { appId: "test-app" } as any;
    const client = await createJazzClient(config);

    expect(createDb).toHaveBeenCalledWith(config);
    expect(resolveClientSession).toHaveBeenCalledWith(config);
    expect(client.db).toBe(mockDb);
    expect(client.session).toBe(mockSession);
  });

  it("shutdown delegates to db.shutdown", async () => {
    const config = { appId: "test-app" } as any;
    const client = await createJazzClient(config);

    await client.shutdown();
    expect(mockDb.shutdown).toHaveBeenCalledOnce();
  });

  it("returns null session when resolveClientSession returns null", async () => {
    const { resolveClientSession: mockResolve } = await import("../runtime/client-session.js");
    (mockResolve as ReturnType<typeof vi.fn>).mockResolvedValueOnce(null);

    const config = { appId: "test-app" } as any;
    const client = await createJazzClient(config);

    expect(client.session).toBeNull();
  });
});
