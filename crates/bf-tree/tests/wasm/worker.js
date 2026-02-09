// Web Worker for running bf-tree WASM tests
// This file loads the WASM module and runs tests/benchmarks

import init, {
  bench_sequential_insert,
  bench_random_read,
  bench_mixed,
  bench_opfs_sequential_insert,
  bench_opfs_random_read,
  bench_opfs_mixed,
  bench_opfs_cold_read,
  get_opfs_storage_info,
  clear_opfs_storage,
} from "./pkg/bf_tree_wasm_tests.js";

await init();

self.onmessage = async (e) => {
  const { command, params } = e.data;

  switch (command) {
    case "run-benchmarks": {
      const count = params?.count || 100000;
      const cacheSizeMb = params?.cacheSizeMb || 8;

      try {
        // Sequential Insert (Memory)
        const insertResult = bench_sequential_insert(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: insertResult.operation,
            count: insertResult.count,
            total_ms: insertResult.total_ms,
            ops_per_sec: insertResult.ops_per_sec,
          },
        });

        // Random Read (Memory)
        const readResult = bench_random_read(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: readResult.operation,
            count: readResult.count,
            total_ms: readResult.total_ms,
            ops_per_sec: readResult.ops_per_sec,
          },
        });

        // Mixed Read/Write (Memory)
        const mixedResult = bench_mixed(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: mixedResult.operation,
            count: mixedResult.count,
            total_ms: mixedResult.total_ms,
            ops_per_sec: mixedResult.ops_per_sec,
          },
        });

        self.postMessage({ type: "complete" });
      } catch (error) {
        self.postMessage({ type: "error", error: error.message });
      }
      break;
    }

    case "run-opfs-benchmarks": {
      const count = params?.count || 10000;
      const cacheSizeMb = params?.cacheSizeMb || 8;

      try {
        // Sequential Insert (OPFS)
        self.postMessage({ type: "status", message: "Running OPFS insert benchmark..." });
        const insertResult = await bench_opfs_sequential_insert(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: insertResult.operation,
            count: insertResult.count,
            total_ms: insertResult.total_ms,
            ops_per_sec: insertResult.ops_per_sec,
          },
        });

        // Random Read (OPFS)
        self.postMessage({ type: "status", message: "Running OPFS read benchmark..." });
        const readResult = await bench_opfs_random_read(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: readResult.operation,
            count: readResult.count,
            total_ms: readResult.total_ms,
            ops_per_sec: readResult.ops_per_sec,
          },
        });

        // Mixed Read/Write (OPFS)
        self.postMessage({ type: "status", message: "Running OPFS mixed benchmark..." });
        const mixedResult = await bench_opfs_mixed(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: mixedResult.operation,
            count: mixedResult.count,
            total_ms: mixedResult.total_ms,
            ops_per_sec: mixedResult.ops_per_sec,
          },
        });

        // Cold Read (OPFS) - reads from freshly opened tree
        self.postMessage({ type: "status", message: "Running OPFS cold read benchmark..." });
        const coldResult = await bench_opfs_cold_read(count, cacheSizeMb);
        self.postMessage({
          type: "benchmark-result",
          data: {
            operation: coldResult.operation,
            count: coldResult.count,
            total_ms: coldResult.total_ms,
            ops_per_sec: coldResult.ops_per_sec,
          },
        });

        self.postMessage({ type: "complete" });
      } catch (error) {
        self.postMessage({ type: "error", error: error.message || String(error) });
      }
      break;
    }

    case "get-storage-info": {
      try {
        const info = await get_opfs_storage_info();
        self.postMessage({ type: "storage-info", data: info });
      } catch (error) {
        self.postMessage({ type: "error", error: error.message || String(error) });
      }
      break;
    }

    case "clear-storage": {
      try {
        const deletedCount = await clear_opfs_storage();
        self.postMessage({ type: "storage-cleared", data: { deletedCount } });
      } catch (error) {
        self.postMessage({ type: "error", error: error.message || String(error) });
      }
      break;
    }

    default:
      self.postMessage({ type: "error", error: `Unknown command: ${command}` });
  }
};

// Signal that the worker is ready
self.postMessage({ type: "ready" });
