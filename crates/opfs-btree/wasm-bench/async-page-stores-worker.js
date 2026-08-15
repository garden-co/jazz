import init, { WasmAsyncBTree } from "/pkg/opfs_btree.js";
import { IndexedDbPageStore, OpfsPageStore } from "/async-page-stores.js";

// IndexedDB is deliberately only an opaque page-addressed backing store here.
const PAGE_SIZE = 4096,
  CACHE_PAGES = 3,
  KEY_COUNT = 4096,
  CHECKPOINT_EVERY = 256;
const RANGE_ROWS = 32,
  MIN_PHASE_MS = 100;
const encoder = new TextEncoder();
const key = (i) => encoder.encode(`k${String(i).padStart(8, "0")}`);
const value = (i, size) => new Uint8Array(size).fill(i & 0xff);
function shuffled(count, seed) {
  const out = Array.from({ length: count }, (_, i) => i);
  let state = seed >>> 0;
  for (let i = count - 1; i > 0; i--) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    const j = state % (i + 1);
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}
async function measure(operation) {
  let operations = 0;
  const start = performance.now();
  do {
    operations += await operation();
  } while (performance.now() - start < MIN_PHASE_MS);
  const ms = performance.now() - start;
  return { ms, operations, ops_per_sec: (operations * 1000) / ms };
}
async function checkpointedPuts(tree, ids, size) {
  for (let at = 0; at < ids.length; at += CHECKPOINT_EVERY) {
    for (let index = at; index < Math.min(ids.length, at + CHECKPOINT_EVERY); index++) {
      const id = ids[index];
      await tree.put(key(id), value(id, size));
    }
    await tree.checkpoint();
  }
}
async function closeAndDestroy(Store, name, tree, store) {
  if (tree) tree.free();
  if (store) store.close();
  await Store.destroy(name);
}
async function preparedTree(Store, name, size) {
  const store = await Store.open(name, PAGE_SIZE);
  let tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
  await checkpointedPuts(
    tree,
    Array.from({ length: KEY_COUNT }, (_, i) => i),
    size,
  );
  tree.free();
  tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
  return { store, tree };
}
async function writePhase(Store, name, size, ids) {
  const store = await Store.open(name, PAGE_SIZE);
  let tree;
  try {
    tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
    await checkpointedPuts(
      tree,
      Array.from({ length: CHECKPOINT_EVERY }, (_, i) => i),
      size,
    );
    return await measure(async () => {
      await checkpointedPuts(tree, ids, size);
      return ids.length;
    });
  } finally {
    await closeAndDestroy(Store, name, tree, store);
  }
}
async function benchmark(Store, backend, size, repeat) {
  const name = `async-page-bench-${backend}-${size}-${repeat}-${Date.now()}-${Math.random()}`;
  const random = shuffled(KEY_COUNT, 0x51ed270b ^ (size << 8) ^ repeat),
    rows = {};
  rows.seq_put_checkpoint_256 = await writePhase(
    Store,
    `${name}-seq-put`,
    size,
    Array.from({ length: KEY_COUNT }, (_, i) => i + CHECKPOINT_EVERY),
  );
  rows.random_put_checkpoint_256 = await writePhase(
    Store,
    `${name}-random-put`,
    size,
    random.map((i) => i + CHECKPOINT_EVERY),
  );
  let store, tree;
  try {
    ({ store, tree } = await preparedTree(Store, `${name}-reads`, size));
    // One root-to-leaf path is at most the three-page cache for this data
    // shape. Preload exactly that path, then repeatedly read one key, so this
    // row is genuinely cache-hot rather than a shuffled multi-leaf workload.
    const hotKey = random[0];
    if (!(await tree.get(key(hotKey)))) throw new Error(`missing hot key ${hotKey}`);
    rows.warm_single_key_get = await measure(async () => {
      for (let i = 0; i < KEY_COUNT; i++) {
        if (!(await tree.get(key(hotKey)))) throw new Error(`missing hot key ${hotKey}`);
      }
      return KEY_COUNT;
    });
    tree.free();
    tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
    rows.cold_random_get = await measure(async () => {
      for (const id of random)
        if (!(await tree.get(key(id)))) throw new Error(`missing cold key ${id}`);
      return random.length;
    });
    tree.free();
    tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
    const starts = shuffled(KEY_COUNT - RANGE_ROWS, 0xa11ce ^ repeat);
    rows.cold_random_range_32 = await measure(async () => {
      for (const start of starts) {
        const found = await tree.range(key(start), key(start + RANGE_ROWS), RANGE_ROWS);
        if (found.length !== RANGE_ROWS)
          throw new Error(`range at ${start} returned ${found.length}`);
      }
      return starts.length;
    });
    rows.cold_random_range_32.rows_per_sec = rows.cold_random_range_32.ops_per_sec * RANGE_ROWS;
  } finally {
    await closeAndDestroy(Store, `${name}-reads`, tree, store);
  }
  try {
    ({ store, tree } = await preparedTree(Store, `${name}-mixed`, size));
    rows.mixed_90r_10w_checkpoint_256 = await measure(async () => {
      for (let i = 0; i < KEY_COUNT; i++) {
        const id = random[i];
        if (i % 10 === 0) await tree.put(key(id), value(id + 1, size));
        else if (!(await tree.get(key(id)))) throw new Error(`missing mixed key ${id}`);
        if ((i + 1) % CHECKPOINT_EVERY === 0) await tree.checkpoint();
      }
      if (KEY_COUNT % CHECKPOINT_EVERY !== 0) await tree.checkpoint();
      return KEY_COUNT;
    });
  } finally {
    await closeAndDestroy(Store, `${name}-mixed`, tree, store);
  }
  return rows;
}
async function pageStoreParity() {
  const names = {
    idb: `page-store-idb-${Date.now()}-${Math.random()}`,
    opfs: `page-store-opfs-${Date.now()}-${Math.random()}`,
  };
  const metadata = { pageSize: PAGE_SIZE, logicalLen: PAGE_SIZE * 3 },
    pages = [
      { pageId: 0, bytes: new Uint8Array(PAGE_SIZE).fill(1) },
      { pageId: 2, bytes: new Uint8Array(PAGE_SIZE).fill(2) },
    ],
    output = {};
  for (const [backend, Store] of [
    ["idb", IndexedDbPageStore],
    ["opfs", OpfsPageStore],
  ]) {
    const store = await Store.open(names[backend], PAGE_SIZE);
    await store.commit({ metadata, writes: pages, deletedPageIds: [] });
    const immediate = (await store.readPages([0, 2]))[1].bytes[0];
    store.close();
    const reopened = await Store.open(names[backend], PAGE_SIZE);
    output[backend] = [
      await reopened.metadata(),
      (await reopened.readPages([2]))[0].bytes[0],
      immediate,
    ];
    reopened.close();
    await Store.destroy(names[backend]);
  }
  return output;
}
async function treeVisibilityParity() {
  const names = {
    idb: `tree-visibility-idb-${Date.now()}-${Math.random()}`,
    opfs: `tree-visibility-opfs-${Date.now()}-${Math.random()}`,
  };
  const output = {};
  for (const [backend, Store] of [
    ["idb", IndexedDbPageStore],
    ["opfs", OpfsPageStore],
  ]) {
    const store = await Store.open(names[backend], PAGE_SIZE);
    let tree;
    try {
      tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
      const written = new Uint8Array([123, 45]);
      await tree.put(key(7), written);
      const immediate = await tree.get(key(7));
      if (!immediate || immediate[0] !== 123 || immediate[1] !== 45)
        throw new Error(`${backend} tree write was not immediately visible`);
      await tree.checkpoint();
      tree.free();
      tree = await WasmAsyncBTree.open(store, PAGE_SIZE, CACHE_PAGES);
      const reopened = await tree.get(key(7));
      if (!reopened || reopened[0] !== 123 || reopened[1] !== 45)
        throw new Error(`${backend} tree write did not survive reopen`);
      output[backend] = [immediate[0], immediate[1], reopened[0], reopened[1]];
    } finally {
      await closeAndDestroy(Store, names[backend], tree, store);
    }
  }
  return output;
}
self.onmessage = async () => {
  try {
    await init();
    const parity = await pageStoreParity(),
      tree_visibility = await treeVisibilityParity(),
      bench = {};
    for (const size of [32, 256]) {
      bench[size] = { idb: [], opfs: [] };
      for (let repeat = 0; repeat < 5; repeat++) {
        const order =
          repeat % 2
            ? [
                ["opfs", OpfsPageStore],
                ["idb", IndexedDbPageStore],
              ]
            : [
                ["idb", IndexedDbPageStore],
                ["opfs", OpfsPageStore],
              ];
        for (const [backend, Store] of order)
          bench[size][backend].push(await benchmark(Store, backend, size, repeat));
      }
    }
    self.postMessage({
      out: {
        parity,
        tree_visibility,
        bench,
        config: {
          page_size: PAGE_SIZE,
          cache_pages: CACHE_PAGES,
          key_count: KEY_COUNT,
          checkpoint_every: CHECKPOINT_EVERY,
          range_rows: RANGE_ROWS,
          min_phase_ms: MIN_PHASE_MS,
          user_agent: self.navigator.userAgent,
          hardware_concurrency: self.navigator.hardwareConcurrency,
        },
      },
    });
  } catch (error) {
    self.postMessage({ error: (error && (error.stack || error.message)) || String(error) });
  }
};
