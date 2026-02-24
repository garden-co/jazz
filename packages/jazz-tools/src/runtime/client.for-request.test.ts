import { describe, expect, it, vi } from "vitest";
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
  const subscribeCalls: Array<[string, string | undefined, string | undefined]> = [];
  const subscribeCallbacks: Array<Function> = [];

  const runtime: Runtime = {
    insert: () => "00000000-0000-0000-0000-000000000001",
    update: () => {},
    delete: () => {},
    query: async (queryJson: string, sessionJson?: string | null, settledTier?: string | null) => {
      queryCalls.push([queryJson, sessionJson ?? undefined, settledTier ?? undefined]);
      return [];
    },
    subscribe: (
      queryJson: string,
      onUpdate: Function,
      sessionJson?: string | null,
      settledTier?: string | null,
    ) => {
      subscribeCalls.push([queryJson, sessionJson ?? undefined, settledTier ?? undefined]);
      subscribeCallbacks.push(onUpdate);
      return 1;
    },
    unsubscribe: () => {},
    insertWithAck: async () => "00000000-0000-0000-0000-000000000001",
    updateWithAck: async () => {},
    deleteWithAck: async () => {},
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
    subscribeCalls,
    subscribeCallbacks,
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

  it("accepts query builders for session-scoped query calls", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-789" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });

    const builder = {
      _build() {
        return '{"table":"todos","conditions":[{"column":"done","op":"eq","value":true}]}';
      },
    };

    await scopedClient.query(builder);

    expect(queryCalls[0][0]).toBe(builder._build());
  });

  it("accepts query builders for subscribe calls", () => {
    const { client, subscribeCalls } = makeClient();

    const builder = {
      _build() {
        return '{"table":"todos"}';
      },
    };

    const subId = client.subscribe(builder, () => {});

    expect(subId).toBe(1);
    expect(subscribeCalls[0][0]).toBe(builder._build());
  });

  it("normalizes RN indexed delta payloads for subscription callbacks", () => {
    const { client, subscribeCallbacks } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);

    subscribeCallbacks[0](
      JSON.stringify({
        added: [{ row: { id: "row-a", values: [] }, index: 0 }],
        removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
        updated: [
          {
            old_row: { id: "row-u", values: [] },
            new_row: { id: "row-u", values: [] },
            old_index: 0,
            new_index: 0,
          },
        ],
        pending: false,
      }),
    );

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith({
      added: [{ id: "row-a", values: [] }],
      removed: [{ id: "row-r", values: [] }],
      updated: [[{ id: "row-u", values: [] }, { id: "row-u", values: [] }]],
      pending: false,
    });
  });

  it("handles malformed subscription deltas by normalizing to empty arrays", () => {
    const { client, subscribeCallbacks } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);

    expect(() =>
      subscribeCallbacks[0](
        JSON.stringify({
          pending: true,
        }),
      ),
    ).not.toThrow();

    expect(callback).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [],
      pending: true,
    });
  });
});
