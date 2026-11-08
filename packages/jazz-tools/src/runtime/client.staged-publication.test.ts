import { expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { Session } from "./context.js";
import { mockMutation, mockRow, runtimeTransactionRecordStubs } from "./client-tests/support.js";

const ownerSession: Session = {
  issuer: "https://issuer.example",
  user_id: "owner",
  claims: { role: "writer" },
  authMode: "external",
};
const explicitSession: Session = { ...ownerSession, user_id: "explicit-reader" };

function fixture() {
  const stage = vi.fn<NonNullable<Runtime["stageStreamingMutation"]>>(async () => ({
    id: "00000000-0000-0000-0000-000000000001",
    attach: () => {},
    abort: () => true,
  }));
  const begin = vi.fn<Runtime["beginTransaction"]>((_kind, id) => id);
  const insert = vi.fn<Runtime["insert"]>(() => mockRow());
  const runtime: Runtime = {
    ...runtimeTransactionRecordStubs,
    beginTransaction: begin,
    stageStreamingMutation: stage,
    insert,
    restore: () => mockRow(),
    update: () => mockMutation(),
    delete: () => mockMutation(),
    query: async () => [],
    queryAccepted: async () => {
      throw new Error("This context-forwarding unit does not read authority evidence");
    },
    createSubscription: () => 1,
    executeSubscription: () => {},
    unsubscribe: () => {},
  };
  const client = JazzClient.connectWithRuntime(runtime, {
    appId: "staged-owner-test",
    schema: {},
    cookieSession: ownerSession,
  });
  return { client, stage, begin, insert };
}

async function* source() {
  yield new Uint8Array([1]);
}

it.each(["accepted-evidence", "client-accepted-evidence"] as const)(
  "stages with the same implicit owner context as %s opening",
  async (capability) => {
    const { client, stage, begin } = fixture();
    await client.stageStreamingMutation(
      "insert",
      "files",
      {},
      "payload",
      source(),
      undefined,
      undefined,
      undefined,
      undefined,
      capability,
    );
    client.openTransaction("serializable", undefined, undefined, undefined, undefined, capability);
    expect(stage.mock.calls[0]?.[5]).toBeUndefined();
    expect(begin.mock.calls[0]?.[2]).toBeUndefined();
    expect(stage.mock.calls[0]?.[7]).toBe(capability);
  },
);

it("preserves an explicit publication session and ordinary writing-session resolution", async () => {
  const { client, stage, begin, insert } = fixture();
  await client.stageStreamingMutation(
    "insert",
    "files",
    {},
    "payload",
    source(),
    undefined,
    explicitSession,
    undefined,
    undefined,
    "accepted-evidence",
  );
  client.openTransaction(
    "serializable",
    explicitSession,
    undefined,
    undefined,
    undefined,
    "accepted-evidence",
  );
  expect(JSON.parse(stage.mock.calls[0]![5]!)).toEqual(explicitSession);
  expect(stage.mock.calls[0]?.[5]).toBe(begin.mock.calls[0]?.[2]);
  await client.stageStreamingMutation("insert", "files", {}, "payload", source());
  expect(JSON.parse(stage.mock.calls[1]![5]!)).toEqual(ownerSession);
  client.insert("files", {});
  expect(JSON.parse(insert.mock.calls[0]![2]!)).toEqual(ownerSession);
});

it("forwards an explicit own-client session so the runtime can refuse it", async () => {
  const { client, stage } = fixture();
  await client.stageStreamingMutation(
    "insert",
    "files",
    {},
    "payload",
    source(),
    undefined,
    explicitSession,
    undefined,
    undefined,
    "client-accepted-evidence",
  );
  expect(JSON.parse(stage.mock.calls[0]![5]!)).toEqual(explicitSession);
  expect(() =>
    client.openTransaction(
      "serializable",
      explicitSession,
      undefined,
      undefined,
      undefined,
      "client-accepted-evidence",
    ),
  ).toThrow("Own-client evidence cannot select a session");
});
