import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { JazzClient } from "../runtime/client.js";
import { Db, type DbConfig, createDb } from "./db.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

vi.mock("./create-jazz-rn-runtime.js", () => ({
  createJazzRnRuntime: vi.fn(),
}));

vi.mock("jazz-rn", () => ({
  default: {
    jazz_rn: {
      mintLocalFirstToken: vi.fn(),
    },
  },
}));

class TestDb extends Db {
  public exposeGetClient(schema: WasmSchema): JazzClient {
    return this.getClient(schema);
  }
}

function makeSchema(tableName: string): WasmSchema {
  return {
    [tableName]: {
      columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
    },
  };
}

function makeClientStub() {
  const shutdown = vi.fn(async () => undefined);
  const updateAuthToken = vi.fn();
  return {
    client: { shutdown, updateAuthToken } as unknown as JazzClient,
    shutdown,
    updateAuthToken,
  };
}

describe("createDb", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("RNDB-U06 mints a JWT from localFirstSecret and passes it to Db when auth is set", async () => {
    const jazzRn = (await import("jazz-rn")).default;
    const mintMock = vi.mocked(jazzRn.jazz_rn.mintLocalFirstToken);
    mintMock.mockReturnValue("minted-jwt");

    const config: DbConfig = {
      appId: "test-app",
      auth: { localFirstSecret: "base64url-seed-32bytes" },
    };
    const db = await createDb(config);

    expect(mintMock).toHaveBeenCalledWith("base64url-seed-32bytes", "test-app", BigInt(3600));
    expect(db).toBeInstanceOf(Db);
  });

  it("RNDB-U07 skips JWT minting and returns a plain Db when auth is absent", async () => {
    const jazzRn = (await import("jazz-rn")).default;
    const mintMock = vi.mocked(jazzRn.jazz_rn.mintLocalFirstToken);

    const config: DbConfig = { appId: "test-app" };
    const db = await createDb(config);

    expect(mintMock).not.toHaveBeenCalled();
    expect(db).toBeInstanceOf(Db);
  });
});

describe("react-native Db", () => {
  const createJazzRnRuntimeMock = vi.mocked(createJazzRnRuntime);

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("RNDB-U01 forwards config to runtime and client wiring on first schema access", () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const { client } = makeClientStub();
    const runtime = { id: "runtime-a" };
    const schema = makeSchema("todos");
    const config: DbConfig = {
      appId: "rn-app",
      serverUrl: "https://example.test",
      serverPathPrefix: "/apps/rn-app",
      env: "prod",
      userBranch: "user-branch",
      jwtToken: "jwt-token",
      adminSecret: "admin-secret",
      tier: "worker",
      dataPath: "/tmp/rn-data",
    };

    createJazzRnRuntimeMock.mockReturnValue(runtime as never);
    connectWithRuntimeSpy.mockReturnValue(client);

    const db = new TestDb(config);
    const resolved = db.exposeGetClient(schema);

    expect(resolved).toBe(client);
    expect(createJazzRnRuntimeMock).toHaveBeenCalledWith({
      schema,
      appId: config.appId,
      env: config.env,
      userBranch: config.userBranch,
      tier: config.tier,
      dataPath: config.dataPath,
    });
    expect(connectWithRuntimeSpy).toHaveBeenCalledWith(
      runtime,
      {
        appId: config.appId,
        schema,
        serverUrl: config.serverUrl,
        serverPathPrefix: config.serverPathPrefix,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        adminSecret: config.adminSecret,
        tier: config.tier,
        defaultDurabilityTier: config.tier,
      },
      {
        onAuthFailure: expect.any(Function),
      },
    );
  });

  it("RNDB-U02 reuses cached clients for same schema key and creates new clients for distinct schemas", () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const schemaA = makeSchema("todos");
    const schemaAClone = JSON.parse(JSON.stringify(schemaA)) as WasmSchema;
    const schemaB = makeSchema("projects");

    const { client: clientA } = makeClientStub();
    const { client: clientB } = makeClientStub();

    createJazzRnRuntimeMock
      .mockReturnValueOnce({ id: "runtime-a" } as never)
      .mockReturnValueOnce({ id: "runtime-b" } as never);
    connectWithRuntimeSpy.mockReturnValueOnce(clientA).mockReturnValueOnce(clientB);

    const db = new TestDb({ appId: "rn-app" });
    const first = db.exposeGetClient(schemaA);
    const second = db.exposeGetClient(schemaAClone);
    const third = db.exposeGetClient(schemaB);

    expect(first).toBe(clientA);
    expect(second).toBe(clientA);
    expect(third).toBe(clientB);
    expect(first).not.toBe(third);
    expect(createJazzRnRuntimeMock).toHaveBeenCalledTimes(2);
    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(2);
  });

  it("RNDB-U03 shutdown closes all memoized clients and clears cache", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const schemaA = makeSchema("todos");
    const schemaB = makeSchema("projects");

    const clientA = makeClientStub();
    const clientB = makeClientStub();
    const clientAfterShutdown = makeClientStub();

    createJazzRnRuntimeMock
      .mockReturnValueOnce({ id: "runtime-a" } as never)
      .mockReturnValueOnce({ id: "runtime-b" } as never)
      .mockReturnValueOnce({ id: "runtime-c" } as never);
    connectWithRuntimeSpy
      .mockReturnValueOnce(clientA.client)
      .mockReturnValueOnce(clientB.client)
      .mockReturnValueOnce(clientAfterShutdown.client);

    const db = new TestDb({ appId: "rn-app" });
    const firstA = db.exposeGetClient(schemaA);
    const firstB = db.exposeGetClient(schemaB);

    expect(firstA).toBe(clientA.client);
    expect(firstB).toBe(clientB.client);

    await db.shutdown();

    expect(clientA.shutdown).toHaveBeenCalledTimes(1);
    expect(clientB.shutdown).toHaveBeenCalledTimes(1);

    const secondA = db.exposeGetClient(schemaA);
    expect(secondA).toBe(clientAfterShutdown.client);
    expect(secondA).not.toBe(firstA);
    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(3);
  });

  it("RNDB-U04 surfaces runtime and client wiring failures", () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const schema = makeSchema("todos");
    const db = new TestDb({ appId: "rn-app" });

    const runtimeError = new Error("runtime wiring failed");
    createJazzRnRuntimeMock.mockImplementationOnce(() => {
      throw runtimeError;
    });

    expect(() => db.exposeGetClient(schema)).toThrow(runtimeError);
    expect(connectWithRuntimeSpy).not.toHaveBeenCalled();

    const clientError = new Error("client wiring failed");
    createJazzRnRuntimeMock.mockReturnValueOnce({ id: "runtime" } as never);
    connectWithRuntimeSpy.mockImplementationOnce(() => {
      throw clientError;
    });

    expect(() => db.exposeGetClient(schema)).toThrow(clientError);
  });

  it("RNDB-U05 forwards updateAuthToken to cached native clients", () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const schemaA = makeSchema("todos");
    const schemaB = makeSchema("projects");
    const clientA = makeClientStub();
    const clientB = makeClientStub();

    createJazzRnRuntimeMock
      .mockReturnValueOnce({ id: "runtime-a" } as never)
      .mockReturnValueOnce({ id: "runtime-b" } as never);
    connectWithRuntimeSpy.mockReturnValueOnce(clientA.client).mockReturnValueOnce(clientB.client);

    const db = new TestDb({ appId: "rn-app", jwtToken: "stale-jwt" });
    db.exposeGetClient(schemaA);
    db.exposeGetClient(schemaB);

    db.updateAuthToken("fresh-jwt");

    expect(clientA.updateAuthToken).toHaveBeenCalledWith("fresh-jwt");
    expect(clientB.updateAuthToken).toHaveBeenCalledWith("fresh-jwt");
  });
});
