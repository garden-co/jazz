import { testWasmDelivery } from "../../dev/gates/test-wasm-delivery.mjs";
import { createWorkerFaultBundleFixture } from "./tests/browser/worker-fault-bundle-node.js";
import { recoverPendingIndexedDbWrites } from "./tests/browser/indexeddb-pending-recovery-node.js";
import {
  liveAuthorityBackendOpen,
  liveAuthorityBackendInsert,
  liveAuthorityBackendClose,
} from "./tests/browser/live-authority-replay-node.js";
import {
  largeValueRewriteBackendOpen,
  largeValueRewriteBackendSeed,
  largeValueRewriteBackendAppend,
  largeValueRewriteBackendClose,
} from "./tests/browser/large-value-rewrite-node.js";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";
import { playwright } from "@vitest/browser-playwright";
import { readCorrectnessArtifactSnapshot } from "../../dev/artifacts/test-artifact-store.mjs";
import {
  jazzAppFront,
  jazzAppUser,
  jazzAppSignOut,
  jazzAppRequest,
  jazzServerInfo,
  stopJazzServerByUrl,
  blockJazzServerTransport,
  unblockJazzServerTransport,
  createJazzServerTransportControl,
  stopJazzServerTransportControl,
  downJazzServerTransport,
  upJazzServerTransport,
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

type QuotaPage = {
  context(): { newCDPSession(page: unknown): Promise<QuotaCdpSession> };
};
type QuotaCdpSession = {
  send(method: string, params: object): Promise<unknown>;
  detach(): Promise<void>;
};
// The override lasts as long as the CDP session that set it.
const quotaSessions = new Map<string, QuotaCdpSession>();
async function overrideStorageQuota(
  page: unknown,
  origin: string,
  quotaSize: number | null,
): Promise<void> {
  let session = quotaSessions.get(origin);
  if (!session) {
    session = await (page as QuotaPage).context().newCDPSession(page);
    quotaSessions.set(origin, session);
  }
  await session.send(
    "Storage.overrideQuotaForOrigin",
    quotaSize === null ? { origin } : { origin, quotaSize },
  );
  if (quotaSize === null) {
    quotaSessions.delete(origin);
    await session.detach();
  }
}

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
// A CDP quota override reaches IndexedDB only if nothing in the browser has
// used the origin's IndexedDB before, and every file here shares one origin.
// So each quota file runs only when named, in a browser of its own (see the
// test:browser:storage-quota script).
const STORAGE_QUOTA_TESTS = [
  "tests/browser/db.storage-quota.test.ts",
  "tests/browser/db.storage-quota-retry.test.ts",
];
const unnamedStorageQuotaTests = STORAGE_QUOTA_TESTS.filter(
  (test) => !process.argv.some((value) => value.replaceAll("\\", "/").endsWith(test)),
);
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
const workerFaultBundle = createWorkerFaultBundleFixture(
  sealedWasmPackage ?? correctnessSnapshot?.wasmPackage,
);

export default defineConfig({
  define: {
    __JAZZ_BROWSER_SOAK__: JSON.stringify(process.env.JAZZ_BROWSER_SOAK ?? ""),
    __JAZZ_ABSTRACT_BENCH__: JSON.stringify(abstractBench),
    __JAZZ_COLD_LOAD_BATCH_UPDATES__: JSON.stringify(
      process.env.JAZZ_COLD_LOAD_BATCH_UPDATES === "1",
    ),
    __JAZZ_COLD_LOAD_FIXTURE__: JSON.stringify(process.env.JAZZ_COLD_LOAD_FIXTURE_DIR ?? ""),
    __JAZZ_COLD_LOAD_RESPONSIVENESS__: JSON.stringify(
      process.env.JAZZ_COLD_LOAD_RESPONSIVENESS === "1",
    ),
    __JAZZ_REALISTIC_BROWSER_SCENARIOS__: JSON.stringify(realisticBrowserScenarios),
    __JAZZ_REALISTIC_BROWSER_RUN_ID__: JSON.stringify(realisticBrowserRunId),
    __JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON__: JSON.stringify(realisticBrowserLimitOverrides),
  },
  plugins: [
    workerFaultBundle.plugin,
    ...(sealedWasmPackage || correctnessSnapshot
      ? [
          testWasmDelivery(
            resolve(sealedWasmPackage ?? correctnessSnapshot!.wasmPackage, "jazz_wasm_bg.wasm"),
          ),
        ]
      : []),
    wasm(),
    topLevelAwait(),
    svelte(),
  ],
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
        readColdLoadFixture: async (_context, chunk?: number) => {
          const directory = process.env.JAZZ_COLD_LOAD_FIXTURE_DIR;
          if (!directory) throw new Error("JAZZ_COLD_LOAD_FIXTURE_DIR is required");
          if (chunk !== undefined && (!Number.isSafeInteger(chunk) || chunk < 0))
            throw new Error("Cold-load fixture chunk must be a non-negative integer");
          const file =
            chunk === undefined
              ? "cold-load-fixture-1500.json"
              : `cold-load-fixture-1500-pages-${chunk}.json`;
          return JSON.parse(readFileSync(resolve(directory, file), "utf8"));
        },
        workerFaultBundleUrl: async () => workerFaultBundle.url(),
        // Chromium only: caps (or, with `null`, restores) the storage quota of
        // `origin` through CDP, so a test can run IndexedDB out of space.
        overrideStorageQuota: async ({ page }, origin: string, quotaSize: number | null) =>
          overrideStorageQuota(page, origin, quotaSize),
        recoverPendingIndexedDbWrites: async ({ context, page }, config) =>
          recoverPendingIndexedDbWrites(context, page, config),
        writeBrowserStorageCorpus: async (_context, records: Record<string, string>) => {
          const output = process.env.JAZZ_BROWSER_CORPUS_OUT;
          if (!output) return null;
          // A reviewed source run exports to a new external candidate, never
          // overwriting a checked-in or previously produced physical receipt.
          writeFileSync(output, `${JSON.stringify(records, null, 2)}\n`, {
            flag: "wx",
          });
          return output;
        },
        liveAuthorityBackendOpen: async (_context, info) => liveAuthorityBackendOpen(info),
        liveAuthorityBackendInsert: async (_context, appId, seed, title) =>
          liveAuthorityBackendInsert(appId, seed, title),
        liveAuthorityBackendClose: async (_context, appId) => liveAuthorityBackendClose(appId),
        largeValueRewriteBackendOpen: async (_context, info) => largeValueRewriteBackendOpen(info),
        largeValueRewriteBackendSeed: async (_context, appId, conversationId, attachmentBytes) =>
          largeValueRewriteBackendSeed(appId, conversationId, attachmentBytes),
        largeValueRewriteBackendAppend: async (_context, appId, turnId, words, delayMs) =>
          largeValueRewriteBackendAppend(appId, turnId, words, delayMs),
        largeValueRewriteBackendClose: async (_context, appId) =>
          largeValueRewriteBackendClose(appId),
        jazzBrowserTopologyLog: async (_context, status, label, elapsedMs) => {
          console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
        },
        jazzServerInfo: async (_context, appId) => jazzServerInfo(appId),
        jazzAppFront: async (_context, coreUrl, app) => jazzAppFront(coreUrl, app),
        jazzAppUser: async (_context, frontUrl, userId, claims) =>
          jazzAppUser(frontUrl, userId, claims),
        jazzAppSignOut: async (_context, frontUrl, userUrl) => jazzAppSignOut(frontUrl, userUrl),
        jazzAppRequest: async (_context, url, init) => jazzAppRequest(url, init),
        jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
        jazzServerTransportControlCreate: async (_context, serverUrl) =>
          createJazzServerTransportControl(serverUrl),
        jazzServerTransportControlBlock: async (_context, url, direction: "both" | "inbound") =>
          blockJazzServerTransport(url, direction),
        jazzServerTransportControlUnblock: async (_context, url) => unblockJazzServerTransport(url),
        jazzServerTransportControlDown: async (_context, url) => downJazzServerTransport(url),
        jazzServerTransportControlUp: async (_context, url, serverUrl?: string) =>
          upJazzServerTransport(url, serverUrl),
        jazzServerTransportControlStop: async (_context, proxyUrl) =>
          stopJazzServerTransportControl(proxyUrl),
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
        writeRealisticBrowserReport: async (_context, runId, report) => {
          mkdirSync(realisticBrowserBenchReportDir, { recursive: true });
          const reportFile = resolve(realisticBrowserBenchReportDir, `${runId}.json`);
          writeFileSync(reportFile, JSON.stringify(report), "utf8");
          return reportFile;
        },
      },
    },
    include: ["tests/browser/**/*.test.ts", "tests/browser/**/*.test.tsx"],
    exclude: [
      ...(excludeRealisticBrowserBench ? [REALISTIC_BROWSER_BENCH_TEST] : []),
      ...unnamedStorageQuotaTests,
    ],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
