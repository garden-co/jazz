import type { NodeUuid } from "./ids.js";
import type { Reader, Writer } from "../support/postcard.js";

export const HLC_PHYSICAL_BITS = 46n;
export const HLC_COUNTER_BITS = 64n - HLC_PHYSICAL_BITS;
export const HLC_MAX_PHYSICAL_MS = (1n << HLC_PHYSICAL_BITS) - 1n;
export const HLC_MAX_LOGICAL_COUNTER = Number((1n << HLC_COUNTER_BITS) - 1n);

export class HlcOverflow extends Error {
  constructor(readonly physicalMs: bigint) {
    super(`packed HLC exhausted at physical millisecond ${physicalMs}`);
  }
}

export class HlcPhysicalMsOutOfRange extends Error {
  constructor(
    readonly physicalMs: bigint,
    readonly maxPhysicalMs: bigint,
  ) {
    super(`physical millisecond ${physicalMs} exceeds the packed HLC maximum ${maxPhysicalMs}`);
  }
}

/** Packed HLC values (TxTime / GlobalTime) are raw u64 bigints. */
export type TxTime = bigint;
export type GlobalTime = bigint;

const pack = (physicalMs: bigint, counter: number): bigint =>
  (physicalMs << HLC_COUNTER_BITS) | BigInt(counter);
export const hlcPhysicalMs = (t: bigint): bigint => t >> HLC_COUNTER_BITS;
export const hlcCounter = (t: bigint): number => Number(t & BigInt(HLC_MAX_LOGICAL_COUNTER));

export function globalTimeNew(physicalMs: bigint, counter: number): GlobalTime | undefined {
  return physicalMs <= HLC_MAX_PHYSICAL_MS && counter <= HLC_MAX_LOGICAL_COUNTER
    ? pack(physicalMs, counter)
    : undefined;
}

export function authorityNowMs(nowMs: bigint, fallbackMs: bigint): bigint {
  if (nowMs <= HLC_MAX_PHYSICAL_MS) return nowMs;
  return fallbackMs < HLC_MAX_PHYSICAL_MS ? fallbackMs : HLC_MAX_PHYSICAL_MS;
}

export function txTimeNew(physicalMs: bigint, counter: number): TxTime {
  if (physicalMs > HLC_MAX_PHYSICAL_MS)
    throw new Error("HLC physical component exceeds 46-bit packed range");
  if (counter > HLC_MAX_LOGICAL_COUNTER)
    throw new Error("HLC logical counter exceeds 18-bit packed range");
  return pack(physicalMs, counter);
}

export function txTimeFromPhysicalMs(physicalMs: bigint): TxTime {
  if (physicalMs > HLC_MAX_PHYSICAL_MS)
    throw new HlcPhysicalMsOutOfRange(physicalMs, HLC_MAX_PHYSICAL_MS);
  return physicalMs << HLC_COUNTER_BITS;
}

/** Shared by TxTime::tick and GlobalTime::tick. */
export function hlcTick(register: bigint, nowMs: bigint): bigint {
  const [p, c] = tickPacked(hlcPhysicalMs(register), hlcCounter(register), nowMs);
  return pack(p, c);
}

export const txTimeTickAfter = (t: TxTime): TxTime => hlcTick(t, hlcPhysicalMs(t));

function tickPacked(
  registeredPhysicalMs: bigint,
  registeredCounter: number,
  nowMs: bigint,
): [bigint, number] {
  if (nowMs > registeredPhysicalMs) {
    if (nowMs > HLC_MAX_PHYSICAL_MS) throw new HlcOverflow(HLC_MAX_PHYSICAL_MS);
    return [nowMs, 0];
  }
  if (registeredCounter < HLC_MAX_LOGICAL_COUNTER)
    return [registeredPhysicalMs, registeredCounter + 1];
  const physicalMs = registeredPhysicalMs + 1n;
  if (physicalMs > HLC_MAX_PHYSICAL_MS) throw new HlcOverflow(HLC_MAX_PHYSICAL_MS);
  return [physicalMs, 0];
}

export interface TxTimeSortKey {
  time: TxTime;
  node: NodeUuid;
}
export function compareTxTimeSortKey(a: TxTimeSortKey, b: TxTimeSortKey): number {
  if (a.time !== b.time) return a.time < b.time ? -1 : 1;
  return a.node < b.node ? -1 : a.node > b.node ? 1 : 0;
}

export const writeHlc = (w: Writer, t: bigint) => w.varint64(t);
export const readHlc = (r: Reader): bigint => r.varint64();
