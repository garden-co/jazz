import { describe, expect, it } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext } from "./context.js";

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

function makeClient() {
  const queryCalls: Array<[string, string | undefined, string | undefined]> = [];

  const runtime: Runtime = {
    insert: () => "00000000-0000-0000-0000-000000000001",
    update: () => {},
    delete: () => {},
    query: async (queryJson: string, sessionJson?: string | null, settledTier?: string | null) => {
      queryCalls.push([queryJson, sessionJson ?? undefined, settledTier ?? undefined]);
      return [];
    },
    subscribe: () => 1,
    unsubscribe: () => {},
    insertPersisted: async () => "00000000-0000-0000-0000-000000000001",
    updatePersisted: async () => {},
    deletePersisted: async () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({ tables: {} }),
    getSchemaHash: () => "schema-hash",
  };

  const context: AppContext = {
    appId: "test-app",
    schema: { tables: {} },
    serverUrl: "http://localhost:1625",
    backendSecret: "test-backend-secret",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (runtime: Runtime, context: AppContext): JazzClient;
  };
  return {
    client: new JazzClientCtor(runtime, context),
    queryCalls,
  };
}

describe("JazzClient.forRequest", () => {
  it("extracts sub + claims from a bearer JWT", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({
      sub: "user-123",
      claims: { role: "admin" },
    });

    const scopedClient = client.forRequest({
      header(name: string) {
        return name.toLowerCase() === "authorization" ? `Bearer ${token}` : undefined;
      },
    });

    await scopedClient.query('{"table":"todos"}');

    expect(queryCalls.length).toBe(1);
    expect(queryCalls[0][1]).toBe(
      JSON.stringify({
        user_id: "user-123",
        claims: { role: "admin" },
      }),
    );
  });

  it("supports Node-style headers object", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-456" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: [`Bearer ${token}`],
      },
    });

    await scopedClient.query('{"table":"todos"}');

    expect(queryCalls[0][1]).toBe(
      JSON.stringify({
        user_id: "user-456",
        claims: {},
      }),
    );
  });

  it("throws when Authorization header is missing", () => {
    const { client } = makeClient();

    expect(() => client.forRequest({ headers: {} })).toThrow(
      "Missing or invalid Authorization header",
    );
  });

  it("throws when JWT sub is missing", () => {
    const { client } = makeClient();
    const token = makeJwt({ claims: { role: "admin" } });

    expect(() =>
      client.forRequest({
        headers: {
          authorization: `Bearer ${token}`,
        },
      }),
    ).toThrow("JWT payload missing sub");
  });
});
