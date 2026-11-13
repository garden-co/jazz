import { afterEach, describe, expect, it, vi } from "vitest";

// What a bundler says about the worker it built: null for bundlers that build
// it from the SharedWorker expression (browser-worker-assets.ts), a script
// type for Metro (its .metro.ts), which names the worker by URL.
const bundler = vi.hoisted(() => ({
  workerType: null as WorkerType | null,
  workerUrl: "https://app.test/assets/jazz-broker-worker.js",
}));
vi.mock("../browser-worker-assets.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../browser-worker-assets.js")>()),
  bundledBrowserWorkerType: () => bundler.workerType,
  bundledBrowserWorkerUrl: () => bundler.workerUrl,
}));

import { SharedBrowserForegroundNodeLease } from "./browser-shared-worker-connection.js";

afterEach(() => {
  bundler.workerType = null;
  vi.unstubAllGlobals();
});

/** Start a lease far enough to construct the broker worker, and report how. */
async function constructedWorker(
  runtimeSources?: Parameters<typeof SharedBrowserForegroundNodeLease.acquire>[0]["runtimeSources"],
): Promise<unknown[]> {
  const constructed: unknown[][] = [];
  vi.stubGlobal(
    "SharedWorker",
    class {
      constructor(...args: unknown[]) {
        constructed.push(args);
        throw new Error("constructed");
      }
    },
  );
  await expect(
    SharedBrowserForegroundNodeLease.acquire({
      dbName: "metro-construction",
      storageOwner: "owner",
      runtimeSources,
    }),
  ).rejects.toThrow("constructed");
  return constructed[0]!;
}

describe("broker SharedWorker construction under Metro", () => {
  it("starts the worker Metro built by URL, as a classic script", async () => {
    bundler.workerType = "classic";

    const [url, options] = await constructedWorker();

    expect(url).toBe("https://app.test/assets/jazz-broker-worker.js");
    expect(options).toMatchObject({ type: "classic" });
  });

  it("still starts the app's own worker URL as a module", async () => {
    bundler.workerType = "classic";

    const [url, options] = await constructedWorker({
      brokerWorkerUrl: "https://app.test/own/worker.js",
      wasmUrl: "https://app.test/own/jazz.wasm",
      wasmVersion: "deploy-42",
    });

    expect(url).toBe("https://app.test/own/worker.js?jazz-runtime-version=deploy-42");
    expect(options).toMatchObject({ type: "module" });
  });

  it("names the Metro setup when the bundler started a dedicated worker instead", async () => {
    // Expo's Metro without withJazz rewrites the SharedWorker expression into
    // a Worker, which has no port.
    const terminate = vi.fn();
    vi.stubGlobal(
      "SharedWorker",
      class {
        terminate = terminate;
      },
    );

    await expect(
      SharedBrowserForegroundNodeLease.acquire({
        dbName: "metro-construction",
        storageOwner: "owner",
        runtimeSources: { wasmSource: new Uint8Array([0, 97, 115, 109]) },
      }),
    ).rejects.toThrow("wrap your Metro config in withJazz from jazz-tools/expo/metro");
    expect(terminate).toHaveBeenCalledOnce();
  });
});
