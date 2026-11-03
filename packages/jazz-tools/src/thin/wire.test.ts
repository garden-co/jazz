import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { decodeThinResponse, encodeThinRequest, type ThinBody, type ThinOp } from "./wire.js";

// The thin wire corpus pinned by `crates/jazz/layers/protocol/src/thin.rs`:
// one frame of every request op and response body, built from the same
// values there and here.

interface Corpus {
  format: string;
  requests: { name: string; frame_hex: string }[];
  responses: { name: string; frame_hex: string }[];
}

const corpus = JSON.parse(
  readFileSync(
    new URL("../../../../crates/jazz/fixtures/thin_wire_v1.json", import.meta.url),
    "utf8",
  ),
) as Corpus;

const ROW = new Uint8Array(16).fill(0x11);
const bytes = (...values: number[]) => new Uint8Array(values);
const text = (value: string) => new TextEncoder().encode(value);

const REQUESTS: Record<string, ThinOp> = {
  schema: { type: "schema", schema: text("{}") },
  subscribe: { type: "subscribe", query: bytes(1, 2, 3), options: { tier: "local-first" } },
  unsubscribe: { type: "unsubscribe", subscription: 300 },
  query: { type: "query", query: bytes(1, 2, 3), options: { tier: "remote" }, transaction: "tx-1" },
  insert: {
    type: "insert",
    table: "todos",
    cells: bytes(4, 5),
    options: { updatedAtMs: 1_700_000_000_000 },
  },
  update: {
    type: "update",
    table: "todos",
    row: ROW,
    patch: bytes(6),
    options: {},
    transaction: "tx-1",
  },
  upsert: { type: "upsert", table: "todos", row: ROW, cells: bytes(7), options: {} },
  delete: { type: "delete", table: "todos", row: ROW, options: {}, transaction: "tx-1" },
  restore: { type: "restore", table: "todos", row: ROW, cells: bytes(8), options: {} },
  updateLargeValues: {
    type: "updateLargeValues",
    table: "todos",
    row: ROW,
    patch: bytes(9),
    mutations: [],
    updatedAtMs: 1_700_000_000_000,
    transaction: "tx-1",
  },
  begin: { type: "begin", transaction: "tx-1", kind: "serializable" },
  commit: { type: "commit", transaction: "tx-1" },
  rollback: { type: "rollback", transaction: "tx-1" },
  captureSnapshot: { type: "captureSnapshot" },
  beginStream: {
    type: "beginStream",
    table: "todos",
    row: ROW,
    cells: bytes(10),
    column: "body",
    mutation: "insert",
    options: {},
    transaction: "tx-1",
  },
  streamChunk: { type: "streamChunk", stream: 7, chunk: text("chunk") },
  finishStream: { type: "finishStream", stream: 7 },
  abortStream: { type: "abortStream", stream: 7 },
};

const RESPONSES: Record<string, ThinBody> = {
  ok: { type: "ok" },
  rows: { type: "rows", rows: bytes(1, 2), relation: true },
  event: { type: "event", delta: bytes(3), reset: true, settled: false, tier: "global" },
  closed: { type: "closed" },
  staged: { type: "staged", row: ROW },
  snapshot: { type: "snapshot", snapshot: text("snap") },
  written: { type: "written", row: ROW, seq: 300 },
  error: { type: "error", code: "Argument", message: "no" },
};

function hex(frame: Uint8Array): string {
  return Array.from(frame, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function unhex(value: string): Uint8Array {
  return new Uint8Array(value.match(/../g)!.map((pair) => Number.parseInt(pair, 16)));
}

describe("thin wire corpus", () => {
  it("covers every op and body", () => {
    expect(corpus.format).toBe("jazz-thin-wire-corpus-v1");
    expect(corpus.requests.map((entry) => entry.name)).toEqual(Object.keys(REQUESTS));
    expect(corpus.responses.map((entry) => entry.name)).toEqual(Object.keys(RESPONSES));
  });

  it.each(corpus.requests.map((entry, index) => [entry.name, entry.frame_hex, index + 1] as const))(
    "encodes the %s request as the server reads it",
    (name, frameHex, id) => {
      expect(hex(encodeThinRequest(id, REQUESTS[name]!))).toBe(frameHex);
    },
  );

  it.each(
    corpus.responses.map((entry, index) => [entry.name, entry.frame_hex, index + 1] as const),
  )("decodes the %s response as the server writes it", (name, frameHex, id) => {
    expect(decodeThinResponse(unhex(frameHex))).toEqual({ id, body: RESPONSES[name] });
  });
});
