import {
  JazzClient,
  type TxId,
  type InsertResult,
  type MutationResult,
  type OpenTransactionId,
  type Runtime,
} from "../client.js";
import type { AppContext } from "../context.js";

export { JazzClient, type Runtime };
export type { AppContext };

export const schemaWithTodos = {
  todos: {
    columns: [
      {
        name: "done",
        column_type: { type: "Boolean" as const },
        nullable: false,
      },
    ],
  },
} as AppContext["schema"];

export function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

export function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

export async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
}

export const testTxId = (value: string): TxId => value as TxId;
export const testOpenTransactionId = (value: string): OpenTransactionId =>
  value as OpenTransactionId;

const committed = (value: string): MutationResult => ({
  kind: "committed",
  txId: testTxId(value),
});

export function mockRow(id = "todo-1"): InsertResult {
  return { id, values: [], ...committed(`transaction-${id}`) };
}

export function mockMutation(txId = "00000000000070008000000000000001"): MutationResult {
  return committed(txId);
}

export const runtimeTransactionRecordStubs = {
  beginTransaction: (_kind: "mergeable" | "exclusive", id: OpenTransactionId) => id,
  upsert: () => mockMutation("upsert-transaction-id"),
  commitTransaction: () => testTxId("committed-batch"),
  waitForTransaction: async () => {},
  rollbackTransaction: async () => false,
  connect: () => {},
  disconnect: async () => {},
  updateAuth: () => {},
  onAuthFailure: () => {},
  onMutationError: () => {},
};

export function makeClient() {
  const queryCalls: Array<[string, string | undefined, string | undefined, string | undefined]> =
    [];
  const createSubscriptionCalls: Array<
    [string, string | undefined, string | undefined, string | undefined]
  > = [];
  const executeSubscriptionCalls: Array<[number, Function]> = [];
  const unsubscribeCalls: number[] = [];
  let nextHandle = 0;

  const runtime: Runtime = {
    ...runtimeTransactionRecordStubs,
    insert: () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
      ...committed("plain-insert-transaction"),
    }),
    restore: () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
      ...committed("plain-restore-transaction"),
    }),
    update: () => committed("transaction-id"),
    delete: () => committed("transaction-id"),
    query: async (
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ) => {
      queryCalls.push([
        queryJson,
        sessionJson ?? undefined,
        tier ?? undefined,
        optionsJson ?? undefined,
      ]);
      return [];
    },
    createSubscription: (
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ) => {
      createSubscriptionCalls.push([
        queryJson,
        sessionJson ?? undefined,
        tier ?? undefined,
        optionsJson ?? undefined,
      ]);
      return nextHandle++;
    },
    executeSubscription: (handle: number, onUpdate: Function) => {
      executeSubscriptionCalls.push([handle, onUpdate]);
    },
    unsubscribe: (handle: number) => {
      unsubscribeCalls.push(handle);
    },
  };

  const context: AppContext = {
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
    backendSecret: "test-backend-secret",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };
  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    queryCalls,
    createSubscriptionCalls,
    executeSubscriptionCalls,
    unsubscribeCalls,
  };
}

export function makeClientWithContext(context: AppContext): JazzClient {
  let nextHandle = 0;
  const runtime: Runtime = {
    ...runtimeTransactionRecordStubs,
    insert: () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
      ...committed("plain-insert-transaction"),
    }),
    restore: () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
      ...committed("plain-restore-transaction"),
    }),
    update: () => committed("transaction-id"),
    delete: () => committed("transaction-id"),
    query: async () => [],
    createSubscription: () => nextHandle++,
    executeSubscription: () => {},
    unsubscribe: () => {},
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };
  return new JazzClientCtor(runtime, context, "edge");
}
