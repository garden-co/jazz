import {
  liveEdgeBackendOpen,
  liveEdgeBackendInsert,
  liveEdgeBackendClose,
} from "./tests/browser/live-edge-replay-node.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";
import { playwright } from "@vitest/browser-playwright";
import { readCorrectnessArtifactSnapshot } from "../../dev/artifacts/test-artifact-store.mjs";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  stopJazzServerByUrl,
  unblockJazzServerNetwork,
} from "./tests/browser/testing-server-node.js";
import {
  closeRemoteBrowserDb,
  createRemoteBrowserDb,
  deleteRemoteBrowserIndexedDbAndWaitForReload,
  insertRemoteBrowserDbRow,
  updateRemoteBrowserDbRow,
  queryRemoteBrowserDbRows,
  restartRemoteBrowserDb,
  waitForRemoteBrowserDbTitle,
} from "./tests/browser/remote-browser-db-node.js";
import {
  REALISTIC_BROWSER_BENCH_TEST,
  shouldExcludeRealisticBrowserBench,
} from "./src/browser-benchmark-mode.js";

const realisticBrowserScenarios = process.env.JAZZ_REALISTIC_BROWSER_SCENARIOS ?? "";
const realisticBrowserRunId = process.env.JAZZ_REALISTIC_BROWSER_RUN_ID ?? "";
const realisticBrowserLimitOverrides =
  process.env.JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON ?? "";
const abstractBench = process.env.JAZZ_ABSTRACT_BENCH ?? "";
const performanceArtifactRun =
  abstractBench !== "" || realisticBrowserScenarios !== "" || realisticBrowserRunId !== "";
const browserName = process.env.JAZZ_BROWSER ?? "chromium";
if (!(["chromium", "firefox", "webkit"] as const).includes(browserName as never)) {
  throw new Error(`Unsupported JAZZ_BROWSER=${browserName}`);
}
const excludeRealisticBrowserBench = shouldExcludeRealisticBrowserBench();
const realisticBrowserBenchReportDir = resolve(__dirname, ".vitest-browser-bench");
const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");
const correctnessSnapshot =
  sealedWasmPackage || performanceArtifactRun
    ? null
    : readCorrectnessArtifactSnapshot(resolve(__dirname, "../.."));
const jazzWasmTestEntry = sealedWasmPackage
  ? resolve(sealedWasmPackage, "jazz_wasm.js")
  : correctnessSnapshot
    ? resolve(correctnessSnapshot.wasmPackage, "jazz_wasm.js")
    : resolve(__dirname, "../../crates/jazz-wasm");

export default defineConfig({
  define: {
    __JAZZ_BROWSER_SOAK__: JSON.stringify(process.env.JAZZ_BROWSER_SOAK ?? ""),
    __JAZZ_ABSTRACT_BENCH__: JSON.stringify(abstractBench),
    __JAZZ_REALISTIC_BROWSER_SCENARIOS__: JSON.stringify(realisticBrowserScenarios),
    __JAZZ_REALISTIC_BROWSER_RUN_ID__: JSON.stringify(realisticBrowserRunId),
    __JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON__: JSON.stringify(realisticBrowserLimitOverrides),
  },
  plugins: [wasm(), topLevelAwait(), svelte()],
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
    fs: {
      allow: [resolve(__dirname, "../..")],
    },
  },
  optimizeDeps: {
    include: ["react/jsx-dev-runtime", "react/jsx-runtime"],
  },
  resolve: {
    alias: {
      // Needed because jazz-tools browser tests import from source (../../src/),
      // bypassing node_modules resolution. Consumers don't need this.
      // Point Vite at the workspace package, rather than its generated
      // directory. The package manifest selects pkg/jazz_wasm.js; wasm-pack's
      // web target intentionally does not emit a second manifest in pkg/.
      "jazz-wasm": jazzWasmTestEntry,
    },
  },
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    // Browser-backed files share Chromium CPU and main-thread transport work.
    // Keep concurrency below the host CPU count so worker round trips are not
    // starved when CI runs the Node/Turbo suite alongside this suite.
    // WebKit gives each browser file its own heavyweight WPE/WASM process.
    // Four concurrent files exceed practical memory pressure before exercising
    // product concurrency; the multi-tab suites below provide that coverage.
    maxWorkers: browserName === "webkit" ? 1 : 4,
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [
        {
          browser: browserName as "chromium" | "firefox" | "webkit",
          headless: true,
        },
      ],
      commands: {
        writeBrowserStorageCorpus: async (_context, records: Record<string, string>) => {
          const output = process.env.JAZZ_BROWSER_CORPUS_OUT;
          if (!output) return null;
          // A reviewed source run exports to a new external candidate, never
          // overwriting a checked-in or previously produced physical receipt.
          writeFileSync(output, `${JSON.stringify(records, null, 2)}\n`, { flag: "wx" });
          return output;
        },
        liveEdgeBackendOpen: async (_context, info) => liveEdgeBackendOpen(info),
        liveEdgeBackendInsert: async (_context, appId, seed, title) =>
          liveEdgeBackendInsert(appId, seed, title),
        liveEdgeBackendClose: async (_context, appId) => liveEdgeBackendClose(appId),
        jazzBrowserTopologyLog: async (_context, status, label, elapsedMs) => {
          console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
        },
        jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
        jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
        jazzServerBlockNetwork: async ({ context }, serverUrl) =>
          blockJazzServerNetwork(context, serverUrl),
        jazzServerUnblockNetwork: async ({ context }, serverUrl) =>
          unblockJazzServerNetwork(context, serverUrl),
        createRemoteBrowserDb: async ({ context, page }, input) =>
          createRemoteBrowserDb(context, page, input),
        waitForRemoteBrowserDbTitle: async (_commandContext, input) =>
          waitForRemoteBrowserDbTitle(input),
        closeRemoteBrowserDb: async (_commandContext, id) => closeRemoteBrowserDb(id),
        insertRemoteBrowserDbRow: async (_commandContext, id, tabIndex, row, table) =>
          insertRemoteBrowserDbRow(id, tabIndex, row, table),
        updateRemoteBrowserDbRow: async (_commandContext, id, tabIndex, rowId, patch, table) =>
          updateRemoteBrowserDbRow(id, tabIndex, rowId, patch, table),
        queryRemoteBrowserDbRows: async (_commandContext, id, tabIndex, tier) =>
          queryRemoteBrowserDbRows(id, tabIndex, tier),
        restartRemoteBrowserDb: async (_commandContext, id) => restartRemoteBrowserDb(id),
        deleteRemoteBrowserIndexedDbAndWaitForReload: async (_commandContext, id, dbName) =>
          deleteRemoteBrowserIndexedDbAndWaitForReload(id, dbName),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
        writeRealisticBrowserReport: async (_context, runId, report) => {
          mkdirSync(realisticBrowserBenchReportDir, { recursive: true });
          const reportFile = resolve(realisticBrowserBenchReportDir, `${runId}.json`);
          writeFileSync(reportFile, JSON.stringify(report), "utf8");
          return reportFile;
        },
      },
    },
    include: ["tests/browser/**/*.test.ts", "tests/browser/**/*.test.tsx"],
    exclude: excludeRealisticBrowserBench ? [REALISTIC_BROWSER_BENCH_TEST] : [],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
