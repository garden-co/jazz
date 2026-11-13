import { describe, expect, it } from "vitest";
import { METRO_WORKER_START_HINT } from "../metro-setup-error.js";
import {
  BrowserWorkerScriptLoadError,
  describeBrokerWorkerLoadFailure,
} from "./broker-worker-load-failure.js";

const htmlAnswer = async () =>
  new Response("<!doctype html>", { status: 200, headers: { "content-type": "text/html" } });

describe("describeBrokerWorkerLoadFailure", () => {
  it("names Vite 6/7 pre-bundling and the fix when Jazz runs from .vite/deps", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      moduleUrl: "http://localhost:5173/node_modules/.vite/deps/chunk-7ATA2QW3.js?v=7d09ffa4",
    });
    expect(error).toBeInstanceOf(BrowserWorkerScriptLoadError);
    expect(error.name).toBe("BrowserWorkerScriptLoadError");
    expect(error.message).toBe(
      'Jazz\'s browser worker script did not load. Vite pre-bundled jazz-tools, and Vite 6 and 7 point the worker URL of a pre-bundled package at a path that does not exist, which the dev server answers with index.html. Add "jazz-tools" to optimizeDeps.exclude in your Vite config, next to "jazz-wasm", or upgrade to Vite 8. The local-first client cannot start without it.',
    );
  });

  it("recognises Vite's per-environment deps directories", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      moduleUrl: "http://localhost:5173/node_modules/.vite/deps_client/jazz-tools_local-first.js",
    });
    expect(error.message).toContain("Vite pre-bundled jazz-tools");
  });

  it("does not blame Vite for a package served from its own files", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      moduleUrl:
        "http://localhost:5173/node_modules/jazz-tools/dist/runtime/native-runtime/browser-shared-worker-connection.js",
    });
    expect(error.message).not.toContain("Vite");
    expect(error.message).toContain("a single-page-app fallback that answers unknown paths");
  });

  it("reports what the server answered for a URL Jazz resolved", async () => {
    const html = await describeBrokerWorkerLoadFailure({
      workerUrl: "https://app.example/jazz/worker/jazz-broker-worker.js",
      moduleUrl: "https://app.example/assets/index-abc.js",
      fetch: htmlAnswer,
    });
    expect(html.message).toContain(
      "Jazz's browser worker script https://app.example/jazz/worker/jazz-broker-worker.js did not load: the server answered with an HTML page (HTTP 200) instead of JavaScript.",
    );
    expect(html.message).toContain("point runtimeSources.brokerWorkerUrl at where it is served");

    const missing = await describeBrokerWorkerLoadFailure({
      workerUrl: "https://app.example/jazz/worker/jazz-broker-worker.js",
      moduleUrl: "https://app.example/assets/index-abc.js",
      fetch: async () => new Response("nope", { status: 404 }),
    });
    expect(missing.message).toContain("did not load: the server answered HTTP 404.");
  });

  it("still explains the failure when the diagnostic fetch itself fails", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      workerUrl: "https://app.example/jazz/worker/jazz-broker-worker.js",
      moduleUrl: "https://app.example/assets/index-abc.js",
      fetch: async () => {
        throw new TypeError("Failed to fetch");
      },
    });
    expect(error.message).toMatch(
      /^Jazz's browser worker script https:\/\/app\.example\/jazz\/worker\/jazz-broker-worker\.js did not load\. Serve/,
    );
  });

  it("does not report a JavaScript answer as the cause", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      workerUrl: "https://app.example/jazz/worker/jazz-broker-worker.js",
      moduleUrl: "https://app.example/assets/index-abc.js",
      fetch: async () =>
        new Response("export {}", { headers: { "content-type": "text/javascript" } }),
    });
    expect(error.message).toContain("jazz-broker-worker.js did not load. Serve");
  });

  // Under Expo's Metro the worker is Metro's chunk, and import.meta.url is
  // null: the dist/worker advice for other bundlers doesn't apply.
  it("points a Metro app at withJazz and its cache, not at dist/worker", async () => {
    const error = await describeBrokerWorkerLoadFailure({ moduleUrl: null, metro: true });
    expect(error.message).toContain(METRO_WORKER_START_HINT.trim());
    expect(error.message).not.toContain("dist/worker");
  });
});
