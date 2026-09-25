import { describe, expect, it } from "vitest";
import { reactive, toRaw } from "vue";
import { createStore, produce } from "solid-js/store";
import { applyDelta, reconcileArray } from "./reconcile-array.js";
import { SubscriptionManager, type SubscriptionDelta } from "./runtime/subscription-manager.js";
import type { WasmRow } from "./drivers/types.js";

type Item = { id: string; name: string; kids: { id: string; v: number }[] };

type DecodedRowDelta = Array<
  | { kind: 0; id: string; index: number; row: WasmRow }
  | { kind: 1; id: string; index: number }
  | { kind: 2; id: string; index: number; row?: WasmRow | null }
>;

function wasmRow(id: string, name: string, count: number): WasmRow {
  return {
    id,
    values: [
      { type: "Text", value: name },
      { type: "Integer", value: count },
    ],
  } as WasmRow;
}

function transform(row: WasmRow): Item {
  const name = (row.values[0] as { value: string }).value;
  const count = (row.values[1] as { value: number }).value;
  return {
    id: row.id,
    name,
    kids: Array.from({ length: (count % 3) + 1 }, (_, i) => ({
      id: `${row.id}-k${i}`,
      v: count + i,
    })),
  };
}

function feed(manager: SubscriptionManager<Item>, delta: DecodedRowDelta): SubscriptionDelta<Item> {
  return (
    manager as unknown as {
      handleDecodedDelta(
        delta: DecodedRowDelta,
        transform: (row: WasmRow) => Item,
      ): SubscriptionDelta<Item>;
    }
  ).handleDecodedDelta(delta, transform);
}

function plain<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

/**
 * Drive a manager with random frames and apply each delta to a plain, a Vue
 * and a Solid target. Every target must equal the manager's result and keep
 * the identity of rows that stayed.
 */
function runRandomFrames(seed: number, bulk: boolean, wrap: "plain" | "vue" | "solid"): void {
  let state = seed;
  const rnd = (n: number) => {
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    return (state >>> 8) % n;
  };
  const manager = new SubscriptionManager<Item>();
  let ids: string[] = [];
  let nextId = 0;
  const plainTarget: Item[] = [];
  const vueTarget = reactive<Item[]>([]) as Item[];
  const [store, setStore] = createStore<{ data: Item[] }>({ data: [] });
  const reference: Item[] = [];

  for (let step = 0; step < 150; step++) {
    const frame: DecodedRowDelta = [];
    const used = new Set<string>();
    let length = ids.length;
    for (let n = bulk ? 32 + rnd(40) : rnd(6); n > 0; n--) {
      const kind = length === 0 ? 0 : rnd(3);
      if (kind === 0) {
        const id = `r${nextId++}`;
        used.add(id);
        frame.push({ kind: 0, id, index: rnd(length + 2), row: wasmRow(id, `a${step}`, rnd(9)) });
        length++;
        continue;
      }
      const id = ids[rnd(ids.length)]!;
      if (used.has(id)) continue;
      used.add(id);
      if (kind === 1) {
        frame.push({ kind: 1, id, index: rnd(length) });
        length--;
      } else {
        const row = rnd(3) ? wasmRow(id, `u${step}`, rnd(9)) : null;
        frame.push({ kind: 2, id, index: rnd(length), row });
      }
    }
    // Distinct increasing indexes send the manager down its bulk path.
    if (bulk) frame.forEach((change, i) => (change.index = i * 2));

    const delta = feed(manager, frame);
    ids = delta.all!.map((item) => item.id);
    const target = wrap === "plain" ? plainTarget : wrap === "vue" ? vueTarget : store.data;
    const raw = (item: Item) => (wrap === "vue" ? toRaw(item) : item);
    const before = new Map(target.map((item) => [item.id, raw(item)]));

    if (wrap === "solid")
      setStore(
        "data",
        produce((current) => applyDelta(current, delta)),
      );
    else applyDelta(target, delta);
    reconcileArray(reference, delta.all!);

    expect(plain(target)).toEqual(plain(delta.all));
    expect(plain(target)).toEqual(plain(reference));
    for (const item of target) {
      const previous = before.get(item.id);
      if (previous) expect(raw(item)).toBe(previous);
    }
  }
}

describe("applyDelta on framework targets", () => {
  for (const wrap of ["plain", "vue", "solid"] as const) {
    for (const bulk of [false, true]) {
      it(`matches the manager's result on ${wrap} targets${bulk ? " for bulk frames" : ""}`, () => {
        for (let seed = 1; seed < (bulk ? 4 : 40); seed++) runRandomFrames(seed, bulk, wrap);
      }, 60_000);
    }
  }

  it("does not rewrite a Vue array once per insert when a frame inserts many rows", () => {
    const manager = new SubscriptionManager<Item>();
    const initial = feed(
      manager,
      Array.from({ length: 1000 }, (_, i) => ({
        kind: 0 as const,
        id: `r${i}`,
        index: i,
        row: wasmRow(`r${i}`, "x", i),
      })),
    );
    // Count index writes under Vue's proxy: every one of them triggers
    // Vue's effects, so this is the cost a Vue list pays for the frame.
    let writes = 0;
    const counted = new Proxy([...initial.all!], {
      set(target, key, value, receiver) {
        if (typeof key === "string" && key !== "length") writes++;
        return Reflect.set(target, key, value, receiver);
      },
    });
    const target = reactive(counted) as Item[];

    const inserts = feed(
      manager,
      Array.from({ length: 50 }, (_, i) => ({
        kind: 0 as const,
        id: `n${i}`,
        index: i * 20,
        row: wasmRow(`n${i}`, "y", i),
      })),
    );
    applyDelta(target, inserts);

    expect(plain(target)).toEqual(plain(inserts.all));
    // One splice per insert would shift the rows after it every time
    // (about 25,000 writes here); one reconcile writes each index at most once.
    expect(writes).toBeLessThanOrEqual(inserts.all!.length);
  });
});
