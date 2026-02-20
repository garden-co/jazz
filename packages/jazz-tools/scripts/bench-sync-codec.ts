import { performance } from "node:perf_hooks";
import { decode as decodeCbor, encode as encodeCbor } from "cbor-x";
import { Builder, ByteBuffer } from "flatbuffers";
import { encode as encodeFlex, toObject } from "flatbuffers/js/flexbuffers.js";
import { pack, unpack } from "msgpackr";
import { ObjectUpdatedPayload } from "./generated/jazz/bench/object-updated-payload.ts";
import { ServerEvent as FlatServerEvent } from "./generated/jazz/bench/server-event.ts";
import { SyncPayload as FlatSyncPayload } from "./generated/jazz/bench/sync-payload.ts";

type BenchResult = {
  label: string;
  ms: number;
  opsPerSec: number;
  bytes: number;
};

function asArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

function run(label: string, iterations: number, bytes: number, fn: () => void): BenchResult {
  for (let i = 0; i < 10_000; i += 1) fn();
  const start = performance.now();
  for (let i = 0; i < iterations; i += 1) fn();
  const elapsed = performance.now() - start;
  return {
    label,
    ms: elapsed,
    opsPerSec: (iterations / elapsed) * 1000,
    bytes,
  };
}

function print(result: BenchResult): void {
  const mbPerSec = (result.bytes * result.opsPerSec) / (1024 * 1024);
  console.log(
    `${result.label.padEnd(24)} ${result.opsPerSec.toFixed(0).padStart(10)} ops/s  ${mbPerSec
      .toFixed(2)
      .padStart(8)} MB/s`,
  );
}

const outbox = {
  destination: { Server: "server-1" },
  payload: {
    ObjectUpdated: {
      object_id: "0199f5c5-a93e-7e9d-9fd2-e4f8e4d00426",
      metadata: null,
      branch_name: "main",
      commits: [],
    },
  },
};

type SyncUpdateEvent = {
  type: "SyncUpdate";
  payload: {
    ObjectUpdated: {
      object_id: string;
      metadata: unknown;
      branch_name: string;
      commits: string[];
    };
  };
};

const serverEvent: SyncUpdateEvent = {
  type: "SyncUpdate",
  payload: outbox.payload as SyncUpdateEvent["payload"],
};

function encodeFlatEvent(value: SyncUpdateEvent): Uint8Array {
  const update = value.payload.ObjectUpdated;
  const builder = new Builder(256);
  const typeOffset = builder.createString(value.type);
  const objectIdOffset = builder.createString(update.object_id);
  const branchOffset = builder.createString(update.branch_name);
  const metadataOffset = builder.createString(
    update.metadata == null ? "" : JSON.stringify(update.metadata),
  );
  const commitOffsets = update.commits.map((commit) => builder.createString(commit));
  const commitsOffset = ObjectUpdatedPayload.createCommitsVector(builder, commitOffsets);
  const objectUpdatedOffset = ObjectUpdatedPayload.createObjectUpdatedPayload(
    builder,
    objectIdOffset,
    metadataOffset,
    branchOffset,
    commitsOffset,
  );
  const payloadOffset = FlatSyncPayload.createSyncPayload(builder, objectUpdatedOffset);
  FlatServerEvent.startServerEvent(builder);
  FlatServerEvent.addType(builder, typeOffset);
  FlatServerEvent.addPayload(builder, payloadOffset);
  const eventOffset = FlatServerEvent.endServerEvent(builder);
  FlatServerEvent.finishServerEventBuffer(builder, eventOffset);
  return builder.asUint8Array();
}

function decodeFlatEvent(bytes: Uint8Array): void {
  const event = FlatServerEvent.getRootAsServerEvent(new ByteBuffer(bytes));
  event.type();
  const payload = event.payload();
  const objectUpdated = payload?.objectUpdated();
  objectUpdated?.objectId();
  objectUpdated?.branchName();
  objectUpdated?.metadata();
  const commitsLength = objectUpdated?.commitsLength() ?? 0;
  for (let i = 0; i < commitsLength; i += 1) {
    objectUpdated?.commits(i);
  }
}

const jsonOutboxBytes = Buffer.from(JSON.stringify(outbox), "utf8");
const flexOutboxBytes = encodeFlex(outbox) as Uint8Array;
const msgpackOutboxBytes = pack(outbox);
const cborOutboxBytes = encodeCbor(outbox);
const jsonEventBytes = Buffer.from(JSON.stringify(serverEvent), "utf8");
const flexEventBytes = encodeFlex(serverEvent) as Uint8Array;
const msgpackEventBytes = pack(serverEvent);
const cborEventBytes = encodeCbor(serverEvent);
const flatEventBytes = encodeFlatEvent(serverEvent);

const iterations = 500_000;

console.log("\n=== sync-codec benchmark (JS) ===");
console.log(`iterations: ${iterations.toLocaleString()}`);
console.log(
  `outbox size: json=${jsonOutboxBytes.length}B flex=${flexOutboxBytes.length}B msgpack=${msgpackOutboxBytes.length}B cbor=${cborOutboxBytes.length}B`,
);
console.log(
  `event  size: json=${jsonEventBytes.length}B flex=${flexEventBytes.length}B msgpack=${msgpackEventBytes.length}B cbor=${cborEventBytes.length}B flat=${flatEventBytes.length}B\n`,
);

const results: BenchResult[] = [
  run("outbox/json encode", iterations, jsonOutboxBytes.length, () => {
    JSON.stringify(outbox);
  }),
  run("outbox/flex encode", iterations, flexOutboxBytes.length, () => {
    encodeFlex(outbox);
  }),
  run("outbox/msgpack encode", iterations, msgpackOutboxBytes.length, () => {
    pack(outbox);
  }),
  run("outbox/cbor encode", iterations, cborOutboxBytes.length, () => {
    encodeCbor(outbox);
  }),
  run("outbox/json decode", iterations, jsonOutboxBytes.length, () => {
    JSON.parse(jsonOutboxBytes.toString("utf8"));
  }),
  run("outbox/flex decode", iterations, flexOutboxBytes.length, () => {
    toObject(asArrayBuffer(flexOutboxBytes));
  }),
  run("outbox/msgpack decode", iterations, msgpackOutboxBytes.length, () => {
    unpack(msgpackOutboxBytes);
  }),
  run("outbox/cbor decode", iterations, cborOutboxBytes.length, () => {
    decodeCbor(cborOutboxBytes);
  }),
  run("event/json encode", iterations, jsonEventBytes.length, () => {
    JSON.stringify(serverEvent);
  }),
  run("event/flex encode", iterations, flexEventBytes.length, () => {
    encodeFlex(serverEvent);
  }),
  run("event/msgpack encode", iterations, msgpackEventBytes.length, () => {
    pack(serverEvent);
  }),
  run("event/cbor encode", iterations, cborEventBytes.length, () => {
    encodeCbor(serverEvent);
  }),
  run("event/flat encode", iterations, flatEventBytes.length, () => {
    encodeFlatEvent(serverEvent);
  }),
  run("event/json decode", iterations, jsonEventBytes.length, () => {
    JSON.parse(jsonEventBytes.toString("utf8"));
  }),
  run("event/flex decode", iterations, flexEventBytes.length, () => {
    toObject(asArrayBuffer(flexEventBytes));
  }),
  run("event/msgpack decode", iterations, msgpackEventBytes.length, () => {
    unpack(msgpackEventBytes);
  }),
  run("event/cbor decode", iterations, cborEventBytes.length, () => {
    decodeCbor(cborEventBytes);
  }),
  run("event/flat decode", iterations, flatEventBytes.length, () => {
    decodeFlatEvent(flatEventBytes);
  }),
];

for (const result of results) {
  print(result);
}
