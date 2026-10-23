// A minimal reader for the gzipped pprof heap profiles the Linux
// `jazz-tools server` serves at `GET /debug/pprof/heap`, so the harness can
// say where server memory goes without Go's pprof installed. It reads only
// what the summaries need: sample types, samples, locations, functions and
// the string table.
import { gunzipSync } from "node:zlib";

function reader(buf) {
  let pos = 0;
  const varint = () => {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      const byte = buf[pos++];
      result |= BigInt(byte & 0x7f) << shift;
      if (byte < 0x80) return result;
      shift += 7n;
    }
  };
  return {
    done: () => pos >= buf.length,
    varint,
    // Yields [field, wireType, value]; value is a bigint or a Buffer.
    next() {
      const key = Number(varint());
      const field = key >> 3;
      const wire = key & 7;
      if (wire === 0) return [field, wire, varint()];
      if (wire === 2) {
        const len = Number(varint());
        const value = buf.subarray(pos, pos + len);
        pos += len;
        return [field, wire, value];
      }
      if (wire === 1) {
        pos += 8;
        return [field, wire, 0n];
      }
      if (wire === 5) {
        pos += 4;
        return [field, wire, 0n];
      }
      throw new Error(`pprof: unsupported wire type ${wire}`);
    },
  };
}

function* messages(buf) {
  const r = reader(buf);
  while (!r.done()) yield r.next();
}

function packedOrSingle(wire, value) {
  if (wire === 0) return [value];
  const r = reader(value);
  const out = [];
  while (!r.done()) out.push(r.varint());
  return out;
}

/** Decode a (gzipped) pprof profile. */
export function decodeProfile(bytes) {
  const buf = bytes[0] === 0x1f && bytes[1] === 0x8b ? gunzipSync(bytes) : Buffer.from(bytes);
  const strings = [];
  const sampleTypes = [];
  const samples = [];
  const locations = new Map();
  const functions = new Map();
  for (const [field, , value] of messages(buf)) {
    if (field === 1) {
      const t = {};
      for (const [f, , v] of messages(value)) {
        if (f === 1) t.type = Number(v);
        if (f === 2) t.unit = Number(v);
      }
      sampleTypes.push(t);
    } else if (field === 2) {
      const s = { locations: [], values: [] };
      for (const [f, w, v] of messages(value)) {
        if (f === 1) s.locations.push(...packedOrSingle(w, v).map(Number));
        if (f === 2) s.values.push(...packedOrSingle(w, v).map((x) => BigInt.asIntN(64, x)));
      }
      samples.push(s);
    } else if (field === 4) {
      const loc = { id: 0, functions: [] };
      for (const [f, , v] of messages(value)) {
        if (f === 1) loc.id = Number(v);
        if (f === 4) {
          for (const [lf, , lv] of messages(v)) if (lf === 1) loc.functions.push(Number(lv));
        }
      }
      locations.set(loc.id, loc);
    } else if (field === 5) {
      const fn = { id: 0, name: 0 };
      for (const [f, , v] of messages(value)) {
        if (f === 1) fn.id = Number(v);
        if (f === 2) fn.name = Number(v);
      }
      functions.set(fn.id, fn);
    } else if (field === 6) {
      strings.push(value.toString("utf8"));
    }
  }
  const types = sampleTypes.map((t) => strings[t.type]);
  // Each sample's stack as function names, leaf first. A location with
  // inlined frames lists the innermost first, as pprof does.
  const stacks = samples.map((s) =>
    s.locations.flatMap((id) =>
      (locations.get(id)?.functions ?? []).map((f) => strings[functions.get(f)?.name ?? 0] ?? "?"),
    ),
  );
  return { types, samples, stacks };
}

// Frames that only say *how* memory was allocated, not *who* holds it.
const plumbing = [
  /^(alloc|core|std|hashbrown|indexmap|smallvec|bytes|serde|bincode|postcard)::/,
  /^<?(alloc|core|std|hashbrown|indexmap|smallvec|bytes)::/,
  /mimalloc|heap_profiling|SamplingAllocator|__rust_|__rdl_|__rg_|^mi_|^_mi_|malloc|realloc|calloc/,
  /^::(fold|from_iter|clone|extend|spec_extend|collect|into_iter|next|poll|call|call_once)$/,
  /^<[^>]*as (core|alloc)::/,
];
const isPlumbing = (name) => !name || name === "?" || plumbing.some((re) => re.test(name));

/** The closest frame that is application code: who asked for the memory. */
export function allocationSite(stack) {
  for (const frame of stack) if (!isPlumbing(frame)) return frame.replace(/::\{\{closure\}\}/g, "");
  return stack[0] ?? "?";
}

/**
 * Summarize in-use bytes: total, the top sites by flat bytes (attributed to
 * the nearest non-allocator frame) and the top Jazz subsystems by inclusive
 * bytes (the module path two levels deep, counted once per sample).
 */
export function summarizeHeap(bytes, { top = 12 } = {}) {
  const profile = decodeProfile(bytes);
  let index = profile.types.indexOf("inuse_space");
  if (index < 0) index = profile.types.length - 1;
  let total = 0n;
  const flat = new Map();
  const inclusive = new Map();
  for (let i = 0; i < profile.samples.length; i++) {
    const value = profile.samples[i].values[index] ?? 0n;
    if (value <= 0n) continue;
    total += value;
    const stack = profile.stacks[i];
    const site = allocationSite(stack);
    flat.set(site, (flat.get(site) ?? 0n) + value);
    const modules = new Set();
    for (const frame of stack) {
      const m = /^<?((?:jazz|groove)[a-z_]*::[a-z_]+(?:::[a-z_]+)?)/.exec(frame);
      if (m) modules.add(m[1]);
    }
    for (const m of modules) inclusive.set(m, (inclusive.get(m) ?? 0n) + value);
  }
  const rank = (map) =>
    [...map.entries()]
      .sort((a, b) => (b[1] > a[1] ? 1 : b[1] < a[1] ? -1 : 0))
      .slice(0, top)
      .map(([name, value]) => ({
        name,
        bytes: Number(value),
        share: total ? Number((value * 10000n) / total) / 10000 : 0,
      }));
  return {
    sampleType: profile.types[index],
    inUseBytes: Number(total),
    sites: rank(flat),
    subsystems: rank(inclusive),
  };
}
