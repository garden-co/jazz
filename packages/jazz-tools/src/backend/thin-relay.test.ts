import { describe, expect, it } from "vitest";
import { NapiThinRouter } from "jazz-napi";
import { decodeThinResponse, encodeThinRequest, type ThinBody, type ThinOp } from "../thin/wire.js";
import {
  decodeWebSocketFrameBatch,
  encodeWebSocketFrameBatch,
} from "../runtime/native-runtime/websocket.js";
import { COMMIT_OUTCOME_UNKNOWN, ThinRelay, type ThinTransactionClient } from "./thin-relay.js";

const TX = "0190f3e4000070008000000000000007";
const SCHEMA = new TextEncoder().encode('{"tables":[]}');

const message = (...requests: [number, ThinOp][]) =>
  encodeWebSocketFrameBatch(requests.map(([id, op]) => encodeThinRequest(id, op)));

/** The id a request frame (as the relay hands it to the session's client) is for. */
const requestId = (frame: Uint8Array) => {
  // Frame tag, then the id: one varint byte each in these tests.
  return frame[1]!;
};

/** The core's answer to request `id`, as the client receives it. */
const coreAnswer = (id: number) => new NapiThinRouter().refuse(id, "Ok", "answered")[0]!;

/** An answer the session's client gives: a response frame. */
const clientAnswer = (id: number) =>
  decodeWebSocketFrameBatch(new NapiThinRouter().refuse(id, "Ok", "served")[0]!)[0]!;

function decodeAll(messages: Uint8Array[]): { id: number; body: ThinBody }[] {
  return messages.flatMap((each) => decodeWebSocketFrameBatch(each).map(decodeThinResponse));
}

interface Harness {
  relay: ThinRelay;
  toCore: Uint8Array[];
  toClient: Uint8Array[];
  served: number[];
  closed: { code: number; reason: string }[];
  clientClosed: () => boolean;
}

function harness(
  options: {
    serve?: (request: Uint8Array) => Promise<Uint8Array>;
    open?: "refuse-cap";
    requestTimeoutMs?: number;
    answerDeadlineMs?: number;
  } = {},
): Harness {
  const toCore: Uint8Array[] = [];
  const toClient: Uint8Array[] = [];
  const served: number[] = [];
  const closed: { code: number; reason: string }[] = [];
  let clientClosed = false;
  const client: ThinTransactionClient = {
    serve: async (request) => {
      served.push(requestId(request));
      return options.serve ? options.serve(request) : clientAnswer(requestId(request));
    },
    close: async () => {
      clientClosed = true;
    },
  };
  const relay = new ThinRelay({
    openClient: () => (options.open === "refuse-cap" ? null : client),
    toCore: (each) => {
      toCore.push(each);
      return true;
    },
    toClient: (each) => toClient.push(each),
    close: (code, reason) => closed.push({ code, reason }),
    requestTimeoutMs: options.requestTimeoutMs,
    answerDeadlineMs: options.answerDeadlineMs,
  });
  return { relay, toCore, toClient, served, closed, clientClosed: () => clientClosed };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

describe("thin sessions' transactions in the backend", () => {
  it("serves a transaction request only once the core answered the plain write before it", async () => {
    const h = harness();
    await h.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    h.relay.fromCore(coreAnswer(1));
    await h.relay.fromClient(
      message(
        [2, { type: "insert", table: "notes", cells: new Uint8Array(), options: {} }],
        [3, { type: "begin", transaction: TX, kind: "mergeable" }],
      ),
    );
    await settle();
    expect(h.toCore).toHaveLength(2);
    expect(h.served).toEqual([]);
    h.relay.fromCore(coreAnswer(2));
    await settle();
    expect(h.served).toEqual([3]);
    expect(decodeAll(h.toClient).map((answer) => answer.id)).toEqual([3]);
  });

  it("sends a plain read after a commit only once the commit was answered", async () => {
    let finish: (() => void) | undefined;
    const h = harness({
      serve: (request) =>
        new Promise((resolve) => {
          finish = () => resolve(clientAnswer(requestId(request)));
        }),
    });
    await h.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    h.relay.fromCore(coreAnswer(1));
    await h.relay.fromClient(
      message(
        [2, { type: "commit", transaction: TX }],
        [3, { type: "query", query: new Uint8Array(), options: {} }],
      ),
    );
    await settle();
    expect(h.toCore).toHaveLength(1);
    finish!();
    await settle();
    expect(h.toCore).toHaveLength(2);
  });

  it("answers a commit the core doesn't confirm in time as of unknown outcome, never as failed", async () => {
    const h = harness({ serve: () => new Promise(() => {}), requestTimeoutMs: 20 });
    await h.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    h.relay.fromCore(coreAnswer(1));
    await h.relay.fromClient(message([2, { type: "commit", transaction: TX }]));
    await new Promise((resolve) => setTimeout(resolve, 60));
    const [answer] = decodeAll(h.toClient);
    expect(answer).toMatchObject({ id: 2, body: { type: "error", code: COMMIT_OUTCOME_UNKNOWN } });

    const read = harness({ serve: () => new Promise(() => {}), requestTimeoutMs: 20 });
    await read.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    read.relay.fromCore(coreAnswer(1));
    await read.relay.fromClient(
      message([2, { type: "query", query: new Uint8Array(), options: {}, transaction: TX }]),
    );
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(decodeAll(read.toClient)[0]).toMatchObject({
      id: 2,
      body: { type: "error", code: "not_observed" },
    });
  });

  it("refuses a session's transactions past the cap, and before a schema is bound", async () => {
    const capped = harness({ open: "refuse-cap" });
    await capped.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    capped.relay.fromCore(coreAnswer(1));
    await capped.relay.fromClient(
      message([2, { type: "begin", transaction: TX, kind: "mergeable" }]),
    );
    await settle();
    expect(decodeAll(capped.toClient)[0]).toMatchObject({
      body: { type: "error", code: "backpressure" },
    });

    const unbound = harness();
    await unbound.relay.fromClient(
      message([1, { type: "begin", transaction: TX, kind: "mergeable" }]),
    );
    await settle();
    expect(decodeAll(unbound.toClient)[0]).toMatchObject({
      body: { type: "error", code: "Argument" },
    });
  });

  it("closes a socket whose requests wait on an answer that never comes, but not one that keeps getting answers", async () => {
    const stuck = harness({ answerDeadlineMs: 30 });
    await stuck.relay.fromClient(
      message(
        [1, { type: "query", query: new Uint8Array(), options: {} }],
        [2, { type: "begin", transaction: TX, kind: "mergeable" }],
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 80));
    expect(stuck.closed).toEqual([{ code: 1011, reason: "an answer never came" }]);

    // Each answer comes within the deadline; together they take longer.
    const busy = harness({
      answerDeadlineMs: 40,
      serve: (request) =>
        new Promise((resolve) => setTimeout(() => resolve(clientAnswer(requestId(request))), 25)),
    });
    await busy.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    busy.relay.fromCore(coreAnswer(1));
    await busy.relay.fromClient(
      message(
        [2, { type: "begin", transaction: TX, kind: "mergeable" }],
        [3, { type: "query", query: new Uint8Array(), options: {}, transaction: TX }],
        [4, { type: "query", query: new Uint8Array(), options: {}, transaction: TX }],
        [5, { type: "query", query: new Uint8Array(), options: {} }],
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(busy.closed).toEqual([]);
    expect(busy.served).toEqual([2, 3, 4]);
    expect(busy.toCore).toHaveLength(2);
  });

  it("closes the session's client with the socket", async () => {
    const h = harness();
    await h.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    h.relay.fromCore(coreAnswer(1));
    await h.relay.fromClient(message([2, { type: "begin", transaction: TX, kind: "mergeable" }]));
    await settle();
    h.relay.end();
    await settle();
    expect(h.clientClosed()).toBe(true);
  });
});

describe("a request the core keeps waiting", () => {
  it("holds the session's next request until the first one ends", async () => {
    let finishFirst: (() => void) | undefined;
    let calls = 0;
    const h = harness({
      requestTimeoutMs: 20,
      serve: (request) => {
        calls += 1;
        if (calls === 1)
          return new Promise((resolve) => {
            finishFirst = () => resolve(clientAnswer(requestId(request)));
          });
        return Promise.resolve(clientAnswer(requestId(request)));
      },
    });
    await h.relay.fromClient(message([1, { type: "schema", schema: SCHEMA }]));
    h.relay.fromCore(coreAnswer(1));
    await h.relay.fromClient(
      message(
        [2, { type: "query", query: new Uint8Array(), options: {}, transaction: TX }],
        [3, { type: "query", query: new Uint8Array(), options: {}, transaction: TX }],
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 60));
    // The first is answered at its timeout; the second is not served yet.
    expect(decodeAll(h.toClient).map((answer) => answer.id)).toEqual([2]);
    expect(h.served).toEqual([2]);
    finishFirst!();
    await settle();
    expect(h.served).toEqual([2, 3]);
    expect(decodeAll(h.toClient).map((answer) => answer.id)).toEqual([2, 3]);
  });
});
