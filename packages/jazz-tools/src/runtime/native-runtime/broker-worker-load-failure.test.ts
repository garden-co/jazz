import { describe, expect, it } from "vitest";
import {
  BrowserWorkerScriptLoadError,
  describeBrokerWorkerLoadFailure,
} from "./broker-worker-load-failure.js";

const htmlAnswer = async () =>
  new Response("<!doctype html>", { status: 200, headers: { "content-type": "text/html" } });

describe("describeBrokerWorkerLoadFailure", () => {
  it("names the dev server's worker URL and the remaining fix when Jazz runs from .vite/deps", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      workerUrl: "http://localhost:5173/@id/jazz-tools/_worker/broker",
      moduleUrl: "http://localhost:5173/node_modules/.vite/deps/chunk-7ATA2QW3.js?v=7d09ffa4",
      fetch: async () =>
        new Response("<!doctype html>", { status: 404, headers: { "content-type": "text/html" } }),
    });
    expect(error).toBeInstanceOf(BrowserWorkerScriptLoadError);
    expect(error.name).toBe("BrowserWorkerScriptLoadError");
    expect(error.message).toBe(
      'Jazz\'s browser worker script http://localhost:5173/@id/jazz-tools/_worker/broker did not load: the server answered with an HTML page (HTTP 404) instead of JavaScript. Jazz runs from the Vite dev server\'s dependency pre-bundle, so it asks the dev server to resolve the worker from the jazz-tools package; that needs jazz-tools to be a dependency the dev server resolves from your project. Otherwise, add "jazz-tools" to optimizeDeps.exclude in your Vite config, next to "jazz-wasm". The local-first client cannot start without it.',
    );
  });

  it("recognises Vite's per-environment deps directories", async () => {
    const error = await describeBrokerWorkerLoadFailure({
      moduleUrl: "http://localhost:5173/node_modules/.vite/deps_client/jazz-tools_local-first.js",
    });
    expect(error.message).toContain("Vite dev server's dependency pre-bundle");
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
});
