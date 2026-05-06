import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { JazzClient, WriteResult, type DirectInsertResult } from "../runtime/client.js";
import { Db, type DbConfig, createDb } from "./db.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

vi.mock("./create-jazz-rn-runtime.js", () => ({
  createJazzRnRuntime: vi.fn(),
}));

vi.mock("jazz-rn", () => ({
  default: {
    jazz_rn: {
      mintLocalFirstToken: vi.fn(),
      mintAnonymousToken: vi.fn(),
    },
  },
}));

function makeTodosApp() {
  return s.defineApp({
    todos: s.table({
      title: s.string(),
    }),
  });
}

function makeProjectsApp() {
  return s.defineApp({
    projects: s.table({
      title: s.string(),
    }),
  });
}

function makeClientStub() {
  const shutdown = vi.fn(async () => undefined);
  const updateAuthToken = vi.fn();
  const updateCookieSession = vi.fn();
  const connectTransport = vi.fn();
  let client: JazzClient & {
    create: ReturnType<typeof vi.fn>;
    shutdown: ReturnType<typeof vi.fn>;
    updateAuthToken: ReturnType<typeof vi.fn>;
    updateCookieSession: ReturnType<typeof vi.fn>;
    connectTransport: ReturnType<typeof vi.fn>;
  };
  const create = vi.fn(() => {
    const row: DirectInsertResult = {
      id: "todo-1",
      values: [{ type: "Text", value: "Buy milk" }],
      batchId: "batch-1",
    };
    return new WriteResult(row, row.batchId, client);
  });
  client = {
    create,
    shutdown,
    updateAuthToken,
    updateCookieSession,
    connectTransport,
    getRuntime: vi.fn(() => ({}) as never),
  } as unknown as typeof client;
  return {
    client,
    create,
    shutdown,
    updateAuthToken,
    updateCookieSession,
    connectTransport,
  };
}

describe("createDb", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("RNDB-U06 mints a JWT from secret and passes it to Db when secret is set", async () => {
    const jazzRn = (await import("jazz-rn")).default;
    const mintMock = vi.mocked(jazzRn.jazz_rn.mintLocalFirstToken);
    mintMock.mockReturnValue("minted-jwt");

    const config: DbConfig = {
      appId: "test-app",
      secret: "base64url-seed-32bytes",
    };
    const db = await createDb(config);

    expect(mintMock).toHaveBeenCalledWith("base64url-seed-32bytes", "test-app", BigInt(3600));
    expect(db).toBeInstanceOf(Db);
    await db.shutdown();
  });

  it("RNDB-U07 mints an anonymous JWT when auth is absent", async () => {
    const jazzRn = (await import("jazz-rn")).default;
    const localFirstMintMock = vi.mocked(jazzRn.jazz_rn.mintLocalFirstToken);
    const anonymousMintMock = vi.mocked(jazzRn.jazz_rn.mintAnonymousToken);
    anonymousMintMock.mockReturnValue("anonymous-jwt");

    const config: DbConfig = { appId: "test-app" };
    const db = await createDb(config);

    expect(localFirstMintMock).not.toHaveBeenCalled();
    expect(anonymousMintMock).toHaveBeenCalledWith(expect.any(String), "test-app", BigInt(3600));
    expect(db).toBeInstanceOf(Db);
    await db.shutdown();
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

  it("RNDB-U01 forwards config to runtime and client wiring on first write", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const { client } = makeClientStub();
    const runtime = { id: "runtime-a" };
    const app = makeTodosApp();
    const schema = app.todos._schema;
    const config: DbConfig = {
      appId: "rn-app",
      serverUrl: "https://example.test",
      env: "prod",
      userBranch: "user-branch",
      jwtToken: "jwt-token",
      adminSecret: "admin-secret",
      tier: "edge",
      dataPath: "/tmp/rn-data",
    };

    createJazzRnRuntimeMock.mockReturnValue(runtime as never);
    connectWithRuntimeSpy.mockReturnValue(client);

    const db = await createDb(config);
    const inserted = db.insert(app.todos, { title: "Buy milk" });

    expect(inserted.value).toEqual({ id: "todo-1", title: "Buy milk" });
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
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        adminSecret: config.adminSecret,
        tier: config.tier,
        defaultDurabilityTier: "local",
      },
      {
        onAuthFailure: expect.any(Function),
      },
    );
  });

  it("RNDB-U02 reuses cached clients for same schema key and creates new clients for distinct schemas", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const todosApp = makeTodosApp();
    const todosAppClone = makeTodosApp();
    const projectsApp = makeProjectsApp();

    const { client: clientA } = makeClientStub();
    const { client: clientB } = makeClientStub();

    createJazzRnRuntimeMock
      .mockReturnValueOnce({ id: "runtime-a" } as never)
      .mockReturnValueOnce({ id: "runtime-b" } as never);
    connectWithRuntimeSpy.mockReturnValueOnce(clientA).mockReturnValueOnce(clientB);

    const db = await createDb({ appId: "rn-app" });
    db.insert(todosApp.todos, { title: "Buy milk" });
    db.insert(todosAppClone.todos, { title: "Buy milk" });
    db.insert(projectsApp.projects, { title: "Buy milk" });

    expect(clientA.create).toHaveBeenCalledTimes(2);
    expect(clientB.create).toHaveBeenCalledTimes(1);
    expect(createJazzRnRuntimeMock).toHaveBeenCalledTimes(2);
    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(2);
  });

  it("RNDB-U03 shutdown closes all memoized clients and clears cache", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const todosApp = makeTodosApp();
    const projectsApp = makeProjectsApp();

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

    const db = await createDb({ appId: "rn-app" });
    const firstA = db.insert(todosApp.todos, { title: "Buy milk" });
    const firstB = db.insert(projectsApp.projects, { title: "Buy milk" });

    expect(firstA.value.id).toBe("todo-1");
    expect(firstB.value.id).toBe("todo-1");

    await db.shutdown();

    expect(clientA.shutdown).toHaveBeenCalledTimes(1);
    expect(clientB.shutdown).toHaveBeenCalledTimes(1);

    db.insert(todosApp.todos, { title: "Buy milk" });
    expect(clientAfterShutdown.create).toHaveBeenCalledTimes(1);
    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(3);
  });

  it("RNDB-U04 surfaces runtime and client wiring failures", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const app = makeTodosApp();
    const db = await createDb({ appId: "rn-app" });

    const runtimeError = new Error("runtime wiring failed");
    createJazzRnRuntimeMock.mockImplementationOnce(() => {
      throw runtimeError;
    });

    expect(() => db.insert(app.todos, { title: "Buy milk" })).toThrow(runtimeError);
    expect(connectWithRuntimeSpy).not.toHaveBeenCalled();

    const clientError = new Error("client wiring failed");
    createJazzRnRuntimeMock.mockReturnValueOnce({ id: "runtime" } as never);
    connectWithRuntimeSpy.mockImplementationOnce(() => {
      throw clientError;
    });

    expect(() => db.insert(app.todos, { title: "Buy milk" })).toThrow(clientError);
  });

  it("RNDB-U05 forwards updateAuthToken to cached native clients", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const todosApp = makeTodosApp();
    const projectsApp = makeProjectsApp();
    const clientA = makeClientStub();
    const clientB = makeClientStub();

    createJazzRnRuntimeMock
      .mockReturnValueOnce({ id: "runtime-a" } as never)
      .mockReturnValueOnce({ id: "runtime-b" } as never);
    connectWithRuntimeSpy.mockReturnValueOnce(clientA.client).mockReturnValueOnce(clientB.client);

    const db = await createDb({ appId: "rn-app", jwtToken: "stale-jwt" });
    db.insert(todosApp.todos, { title: "Buy milk" });
    db.insert(projectsApp.projects, { title: "Buy milk" });

    db.updateAuthToken("fresh-jwt");

    expect(clientA.updateAuthToken).toHaveBeenCalledWith("fresh-jwt");
    expect(clientB.updateAuthToken).toHaveBeenCalledWith("fresh-jwt");
  });

  it("RNDB-U10 forwards updateCookieSession to cached native clients", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const app = makeTodosApp();
    const stub = makeClientStub();

    createJazzRnRuntimeMock.mockReturnValue({ id: "runtime" } as never);
    connectWithRuntimeSpy.mockReturnValue(stub.client);

    const db = await createDb({
      appId: "rn-app",
      cookieSession: {
        user_id: "alice",
        claims: { role: "reader" },
        authMode: "external",
      },
    });
    db.insert(app.todos, { title: "Buy milk" });

    const cookieSession = {
      user_id: "alice",
      claims: { role: "editor" },
      authMode: "external" as const,
    };
    db.updateCookieSession(cookieSession);

    expect(stub.updateCookieSession).toHaveBeenCalledWith(cookieSession);
  });

  it("RNDB-U08 calls connectTransport when serverUrl is configured", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const stub = makeClientStub();
    createJazzRnRuntimeMock.mockReturnValue({ id: "runtime" } as never);
    connectWithRuntimeSpy.mockReturnValue(stub.client);

    const db = await createDb({
      appId: "rn-app",
      serverUrl: "https://example.test",
      jwtToken: "jwt-x",
      adminSecret: "admin-y",
    });
    db.insert(makeTodosApp().todos, { title: "Buy milk" });

    expect(stub.connectTransport).toHaveBeenCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: "admin-y",
    });
  });

  it("RNDB-U09 does not call connectTransport when serverUrl is absent", async () => {
    const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
    const stub = makeClientStub();
    createJazzRnRuntimeMock.mockReturnValue({ id: "runtime" } as never);
    connectWithRuntimeSpy.mockReturnValue(stub.client);

    const db = await createDb({ appId: "rn-app" });
    db.insert(makeTodosApp().todos, { title: "Buy milk" });

    expect(stub.connectTransport).not.toHaveBeenCalled();
  });
});
