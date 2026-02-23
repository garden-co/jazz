import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { JazzClient } from "../runtime/client.js";
import { Db, type DbConfig } from "./db.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

vi.mock("./create-jazz-rn-runtime.js", () => ({
  createJazzRnRuntime: vi.fn(),
}));

class TestDb extends Db {
  public exposeGetClient(schema: WasmSchema): JazzClient {
    return this.getClient(schema);
  }
}

function makeSchema(tableName: string): WasmSchema {
  return {
    tables: {
      [tableName]: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    },
  };
}

function makeClientStub() {
  const shutdown = vi.fn(async () => undefined);
  return {
    client: { shutdown } as unknown as JazzClient,
    shutdown,
  };
}

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
      localAuthMode: "demo",
      localAuthToken: "local-token",
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
    expect(connectWithRuntimeSpy).toHaveBeenCalledWith(runtime, {
      appId: config.appId,
      schema,
      serverUrl: config.serverUrl,
      serverPathPrefix: config.serverPathPrefix,
      env: config.env,
      userBranch: config.userBranch,
      jwtToken: config.jwtToken,
      localAuthMode: config.localAuthMode,
      localAuthToken: config.localAuthToken,
      adminSecret: config.adminSecret,
      tier: config.tier,
    });
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
});
