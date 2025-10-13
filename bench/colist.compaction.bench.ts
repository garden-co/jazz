import { bench, describe } from "vitest";
import { WasmCrypto } from "cojson/src/crypto/WasmCrypto.js";
import { LocalNode } from "cojson/src/localNode.js";
import { RawCoList } from "cojson/src/coValues/coList.js";

const crypto = await WasmCrypto.create();

describe("CoList Graph Compaction Benchmarks", () => {
  describe("Sequential append operations (best case)", () => {
    bench(
      "100 sequential appends",
      () => {
        const account = LocalNode.internalCreateAccount({ crypto });
        const node = account.core.node;
        const group = node.createGroup();
        const list = group.createList() as RawCoList<number>;

        for (let i = 0; i < 100; i++) {
          list.append(i);
        }

        // Read the list (this is where optimization applies)
        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 100) throw new Error("Wrong length");
      },
      { iterations: 100 },
    );

    bench(
      "500 sequential appends",
      () => {
        const account = LocalNode.internalCreateAccount({ crypto });
        const node = account.core.node;
        const group = node.createGroup();
        const list = group.createList() as RawCoList<number>;

        for (let i = 0; i < 500; i++) {
          list.append(i);
        }

        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 500) throw new Error("Wrong length");
      },
      { iterations: 50 },
    );

    bench(
      "1000 sequential appends",
      () => {
        const account = LocalNode.internalCreateAccount({ crypto });
        const node = account.core.node;
        const group = node.createGroup();
        const list = group.createList() as RawCoList<number>;

        for (let i = 0; i < 1000; i++) {
          list.append(i);
        }

        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 1000) throw new Error("Wrong length");
      },
      { iterations: 25 },
    );
  });

  describe("Multiple reads on same list (cache benefit)", () => {
    const account = LocalNode.internalCreateAccount({ crypto });
    const node = account.core.node;
    const group = node.createGroup();
    const list = group.createList() as RawCoList<number>;

    for (let i = 0; i < 1000; i++) {
      list.append(i);
    }

    bench(
      "Read 1000 items (with cache)",
      () => {
        const items = list.asArray();
        if (items.length !== 1000) throw new Error("Wrong length");
      },
      { iterations: 10000 },
    );

    bench(
      "Read 1000 items (without cache)",
      () => {
        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 1000) throw new Error("Wrong length");
      },
      { iterations: 100 },
    );
  });

  describe("Mixed operations (realistic scenario)", () => {
    bench(
      "500 appends + 50 random inserts",
      () => {
        const account = LocalNode.internalCreateAccount({ crypto });
        const node = account.core.node;
        const group = node.createGroup();
        const list = group.createList() as RawCoList<number>;

        // Sequential appends
        for (let i = 0; i < 500; i++) {
          list.append(i);
        }

        // Random inserts (breaks chains)
        for (let i = 0; i < 50; i++) {
          const idx = Math.floor(Math.random() * list.asArray().length);
          list.append(1000 + i, idx);
        }

        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 550) throw new Error("Wrong length");
      },
      { iterations: 25 },
    );

    bench(
      "1000 appends + 100 deletes",
      () => {
        const account = LocalNode.internalCreateAccount({ crypto });
        const node = account.core.node;
        const group = node.createGroup();
        const list = group.createList() as RawCoList<number>;

        for (let i = 0; i < 1000; i++) {
          list.append(i);
        }

        // Delete some items
        for (let i = 0; i < 100; i++) {
          const items = list.asArray();
          if (items.length > 0) {
            const idx = Math.floor(Math.random() * items.length);
            list.delete(idx);
          }
        }

        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 900) throw new Error("Wrong length");
      },
      { iterations: 20 },
    );
  });

  describe("Prepend operations (less optimal)", () => {
    bench(
      "500 sequential prepends",
      () => {
        const account = LocalNode.internalCreateAccount({ crypto });
        const node = account.core.node;
        const group = node.createGroup();
        const list = group.createList() as RawCoList<number>;

        for (let i = 0; i < 500; i++) {
          list.prepend(i, 0);
        }

        list._cachedEntries = undefined;
        const items = list.asArray();
        if (items.length !== 500) throw new Error("Wrong length");
      },
      { iterations: 25 },
    );
  });

  describe("Compaction stats analysis", () => {
    bench("Analyze compaction potential (1000 sequential)", () => {
      const account = LocalNode.internalCreateAccount({ crypto });
      const node = account.core.node;
      const group = node.createGroup();
      const list = group.createList() as RawCoList<number>;

      for (let i = 0; i < 1000; i++) {
        list.append(i);
      }

      const stats = list.getCompactionStats();
      if (stats.totalNodes !== 1000) throw new Error("Wrong stats");
    });

    bench("Analyze compaction potential (mixed operations)", () => {
      const account = LocalNode.internalCreateAccount({ crypto });
      const node = account.core.node;
      const group = node.createGroup();
      const list = group.createList() as RawCoList<number>;

      for (let i = 0; i < 500; i++) {
        list.append(i);
      }

      for (let i = 0; i < 50; i++) {
        const idx = Math.floor(Math.random() * list.asArray().length);
        list.append(1000 + i, idx);
      }

      const stats = list.getCompactionStats();
      if (stats.totalNodes !== 550) throw new Error("Wrong stats");
    });
  });
});
