import { describe, expect, it } from "vitest";
import { createInspector, type InspectorDeps } from "./inspect.js";
import { clientScope } from "../runtime/client-scope.js";
import { appScopedUrl } from "../runtime/url.js";

// #5170: the Inspector showed the app's id (its store's), while a browser's
// sync URLs carry /jazz/apps/<another id>. Both are right: a browser names
// the app by an id derived from the URL it syncs with, and the backend maps
// it to the store's. whoami now names both.

const STORE_APP_ID = "0b6f1f2e-3d4c-4b5a-9a8b-7c6d5e4f3a2b";

function whoami(publicUrls: string[], requestUrl: string) {
  const handle = createInspector({
    appId: STORE_APP_ID,
    basePath: "/jazz",
    publicUrls,
    options: {},
    schema: () => ({}),
    permissions: {},
    db: () => ({}),
    dbAs: async () => ({}),
    sessions: () => [],
    hook: () => ({ by: "ops@example.com", write: false }),
    tokenSecret: async () => null,
    remoteAddress: () => undefined,
    serveUi: async () => null,
    env: { NODE_ENV: "production" },
  } as unknown as InspectorDeps);
  return handle
    .handle(
      new Request(requestUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ op: "whoami" }),
      }),
      "/inspect/api",
    )
    .then((response) => response.json() as Promise<any>);
}

/** The app id in the sync URL a browser that syncs with `serverUrl` opens. */
function browsersAppId(serverUrl: string): string {
  const url = appScopedUrl(serverUrl, clientScope({ serverUrl }), "ws");
  return decodeURIComponent(/\/apps\/([^/]+)\/ws$/.exec(url)![1]!);
}

describe("the app ids whoami names", () => {
  it("is the store's id, and the id browsers name the app by in their sync URLs", async () => {
    const answer = await whoami([], "https://app.example.com/jazz/inspect/api");
    expect(answer.appId).toBe(STORE_APP_ID);
    expect(answer.clientAppId).toBe(browsersAppId("https://app.example.com/jazz"));
    expect(answer.clientAppId).not.toBe(STORE_APP_ID);
  });

  it("follows the public URL browsers reach the app at", async () => {
    const answer = await whoami(
      ["https://www.example.com/api/jazz"],
      "http://10.0.0.5:3000/jazz/inspect/api",
    );
    expect(answer.clientAppId).toBe(browsersAppId("https://www.example.com/api/jazz"));
  });
});
