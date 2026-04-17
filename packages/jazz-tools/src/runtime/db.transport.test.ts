import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { JazzClient } from "./client.js";
import { Db, type DbConfig } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";

class TestDb extends Db {
  constructor(config: DbConfig) {
    super(config, { WasmRuntime: class {} } as never);
  }
  public exposeGetClient(schema: WasmSchema): JazzClient {
    return this.getClient(schema);
  }
}

function makeSchema(): WasmSchema {
  return {
    todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
  };
}

function makeClientStub() {
  return {
    shutdown: vi.fn(async () => undefined),
    updateAuthToken: vi.fn(),
    connectTransport: vi.fn(),
    getRuntime: vi.fn(() => ({}) as never),
  } as unknown as JazzClient & {
    connectTransport: ReturnType<typeof vi.fn>;
  };
}

describe("runtime/Db direct path upstream wiring", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("DBRT-U01 calls connectTransport when serverUrl is configured and no worker", () => {
    const client = makeClientStub();
    const connectSyncSpy = vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      serverPathPrefix: "/apps/app",
      jwtToken: "jwt-x",
      adminSecret: "admin-y",
    });
    db.exposeGetClient(makeSchema());

    expect(connectSyncSpy).toHaveBeenCalledTimes(1);
    expect((client as any).connectTransport).toHaveBeenCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: "admin-y",
    });
  });

  it("DBRT-U02 does not call connectTransport when serverUrl is absent", () => {
    const client = makeClientStub();
    vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);

    const db = new TestDb({ appId: "app" });
    db.exposeGetClient(makeSchema());

    expect((client as any).connectTransport).not.toHaveBeenCalled();
  });
});
