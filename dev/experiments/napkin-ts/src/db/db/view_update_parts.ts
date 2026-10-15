// Bounded parts for a `ViewUpdate` larger than the routed payload limit.
import { encode } from "../../support/postcard.js";
import { equalBytes, toHex } from "../../support/bytes.js";
import {
  type SupportingRow,
  type SupportingRowsUpdate,
  type SyncMessage,
  type VersionBundle,
  type VersionCarrier,
  type ViewUpdatePayload,
  buildVersionCarriersFromSingletons,
  emptyPeerPayloadInventory,
  encodeSupportingRow,
  encodeVersionBundle,
  encodedSyncMessageLen,
  expandVersionCarriers,
  subscriptionKeyEquals,
} from "../../_extern/db-rest-wasm.js";

const PART_LENGTH_PREFIX_SLACK = 64;

export type OversizedViewUpdate =
  | { kind: "HeaderExceedsLimit"; headerBytes: number; limit: number }
  | { kind: "SupportingRowExceedsLimit"; rowBytes: number; capacity: number }
  | { kind: "VersionBundleExceedsLimit"; bundleBytes: number; capacity: number }
  | { kind: "MalformedCarrier" };

export class OversizedViewUpdateError extends Error {
  constructor(readonly detail: OversizedViewUpdate) {
    super(formatOversized(detail));
  }
}

function formatOversized(e: OversizedViewUpdate): string {
  switch (e.kind) {
    case "HeaderExceedsLimit":
      return `unsupported view update: its ${e.headerBytes}-byte header exceeds the ${e.limit}-byte routed payload limit`;
    case "SupportingRowExceedsLimit":
      return `unsupported view update: one ${e.rowBytes}-byte supporting row exceeds the ${e.capacity}-byte part capacity`;
    case "VersionBundleExceedsLimit":
      return `unsupported view update: one ${e.bundleBytes}-byte transaction bundle exceeds the ${e.capacity}-byte part capacity`;
    case "MalformedCarrier":
      return "unsupported view update: malformed version-carrier run";
  }
}

function takeLists(rows: SupportingRowsUpdate): [SupportingRow[], SupportingRow[]] {
  switch (rows.kind) {
    case "Snapshot":
      return [rows.rows, []];
    case "Delta":
      return [rows.adds, rows.removes];
    case "CatchUp":
      return [rows.changed, rows.left];
  }
  throw new Error("unknown supporting-rows transition");
}

function withLists(
  t: SupportingRowsUpdate,
  first: SupportingRow[],
  second: SupportingRow[],
): SupportingRowsUpdate {
  switch (t.kind) {
    case "Snapshot":
      return { kind: "Snapshot", revision: t.revision, rows: first };
    case "Delta":
      return {
        kind: "Delta",
        predecessor: t.predecessor,
        revision: t.revision,
        adds: first,
        removes: second,
      };
    default:
      return {
        kind: "CatchUp",
        predecessor: t.predecessor,
        revision: t.revision,
        changed: first,
        left: second,
      };
  }
}

function sameTransition(a: SupportingRowsUpdate, b: SupportingRowsUpdate): boolean {
  if (a.kind !== b.kind || !equalBytes(a.revision, b.revision)) return false;
  return a.kind === "Snapshot" || equalBytes(a.predecessor, b.predecessor);
}

function encodedLen(message: SyncMessage): number {
  try {
    return encodedSyncMessageLen(message);
  } catch {
    return Number.MAX_SAFE_INTEGER;
  }
}

interface PartContents {
  first: SupportingRow[];
  second: SupportingRow[];
  bundles: VersionBundle[];
  bytes: number;
}
const emptyPart = (): PartContents => ({ first: [], second: [], bundles: [], bytes: 0 });
const partIsEmpty = (p: PartContents) => !p.first.length && !p.second.length && !p.bundles.length;

/** Split `view` into parts whose encodings each fit `limit` bytes. */
export function splitViewUpdate(view: ViewUpdatePayload, limit: number): SyncMessage[] {
  const fail = (d: OversizedViewUpdate): never => {
    throw new OversizedViewUpdateError(d);
  };
  let bundles: VersionBundle[];
  try {
    bundles = expandVersionCarriers(view.versionCarriers);
  } catch {
    return fail({ kind: "MalformedCarrier" });
  }
  const template = view.supportingRows;
  const [first, second] = takeLists(template);
  const shell = (
    inventory: any,
    first: SupportingRow[],
    second: SupportingRow[],
    carriers: VersionCarrier[],
  ): ViewUpdatePayload => ({
    subscription: view.subscription,
    settledThrough: view.settledThrough,
    versionCarriers: carriers,
    peerPayloadInventory: inventory,
    supportingRows: withLists(template, first, second),
  });
  const headerBytes =
    encodedLen({ kind: "ViewUpdate", payload: shell(view.peerPayloadInventory, [], [], []) }) +
    PART_LENGTH_PREFIX_SLACK;
  if (headerBytes >= limit) fail({ kind: "HeaderExceedsLimit", headerBytes, limit });
  const capacity = limit - headerBytes;

  const parts: PartContents[] = [];
  let current = emptyPart();
  const admit = (bytes: number) => {
    if (!partIsEmpty(current) && current.bytes + bytes > capacity) {
      parts.push(current);
      current = emptyPart();
    }
    current.bytes += bytes;
  };
  for (const [list, into] of [
    [first, "first"],
    [second, "second"],
  ] as const) {
    for (const row of list) {
      const rowBytes = encode(row, encodeSupportingRow).length;
      if (rowBytes > capacity) fail({ kind: "SupportingRowExceedsLimit", rowBytes, capacity });
      admit(rowBytes);
      current[into].push(row);
    }
  }
  for (const bundle of bundles) {
    // A singleton carrier is the enum tag plus the bundle.
    const bundleBytes = encode(bundle, encodeVersionBundle).length + 1;
    if (bundleBytes > capacity) fail({ kind: "VersionBundleExceedsLimit", bundleBytes, capacity });
    admit(bundleBytes);
    current.bundles.push(bundle);
  }
  parts.push(current);

  return parts.map((part, index) => {
    const last = index + 1 === parts.length;
    const inventory = last ? view.peerPayloadInventory : emptyPeerPayloadInventory();
    const wrap = (payload: ViewUpdatePayload): SyncMessage => ({
      kind: last ? "ViewUpdate" : "ViewUpdatePart",
      payload,
    });
    // Packed runs are usually smaller, but per-body overrides can grow them.
    try {
      const packed = wrap(
        shell(inventory, part.first, part.second, buildVersionCarriersFromSingletons(part.bundles)),
      );
      if (encodedLen(packed) <= limit) return packed;
    } catch {}
    return wrap(
      shell(
        inventory,
        part.first,
        part.second,
        part.bundles.map((b) => ({ kind: "Bundle", bundle: b })),
      ),
    );
  });
}

/** Reassemble buffered `ViewUpdatePart`s and their final `ViewUpdate`. */
export function mergeViewUpdateParts(
  parts: ViewUpdatePayload[],
  last: ViewUpdatePayload,
): ViewUpdatePayload {
  const first: SupportingRow[] = [];
  const second: SupportingRow[] = [];
  const carriers: VersionCarrier[] = [];
  for (const part of [...parts, last]) {
    if (
      part !== last &&
      (!subscriptionKeyEquals(part.subscription, last.subscription) ||
        part.settledThrough !== last.settledThrough ||
        !sameTransition(part.supportingRows, last.supportingRows))
    )
      throw "view-update part does not match its final update";
    const [f, s] = takeLists(part.supportingRows);
    first.push(...f);
    second.push(...s);
    carriers.push(...part.versionCarriers);
  }
  const identities = new Set<string>();
  for (const row of [...first, ...second]) {
    const key = toHex(encode(row, encodeSupportingRow));
    if (identities.has(key)) throw "view-update parts repeat a supporting row";
    identities.add(key);
  }
  return {
    subscription: last.subscription,
    settledThrough: last.settledThrough,
    versionCarriers: carriers,
    peerPayloadInventory: last.peerPayloadInventory,
    supportingRows: withLists(last.supportingRows, first, second),
  };
}
