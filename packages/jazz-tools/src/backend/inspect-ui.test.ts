import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createInspector, type InspectorDeps } from "./inspect.js";
import { createInspectorUi } from "./inspect-ui.js";

// The page as Vite builds it: every URL relative to the page.
const BUILT_PAGE = `<!doctype html><html><head><title>Jazz Inspector</title>
<script type="module" crossorigin src="./assets/backend-abc.js"></script>
<link rel="modulepreload" crossorigin href="./assets/src-def.js">
<link rel="stylesheet" crossorigin href="./assets/src-ghi.css">
</head><body><div id="root"></div></body></html>`;

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function builtUi() {
  const dir = await mkdtemp(join(tmpdir(), "jazz-inspect-ui-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  await mkdir(join(dir, "assets"));
  await writeFile(join(dir, "backend.html"), BUILT_PAGE);
  await writeFile(join(dir, "assets", "backend-abc.js"), "export {}");
  return dir;
}

function scriptUrls(html: string, pageUrl: string): string[] {
  return [...html.matchAll(/(?:src|href)="([^"]+)"/g)].map((m) => new URL(m[1]!, pageUrl).href);
}

describe("the Inspector page with and without a trailing slash", () => {
  it("is served at <basePath>/inspect without a redirect, with its assets resolving below /inspect/", async () => {
    const serveUi = createInspectorUi(await builtUi());
    const inspector = createInspector({
      basePath: "/jazz",
      serveUi,
      options: {},
    } as unknown as InspectorDeps);

    // Next.js answers `/jazz/inspect/` with a 308 to `/jazz/inspect`. Redirecting
    // back to the slash looped forever (ERR_TOO_MANY_REDIRECTS).
    for (const url of [
      "http://localhost:3000/jazz/inspect",
      "http://localhost:3000/jazz/inspect/",
    ]) {
      const response = await inspector.handle(new Request(url), new URL(url).pathname.slice(5));
      expect(response.status, url).toBe(200);
      expect(response.headers.get("location"), url).toBeNull();
      expect(response.headers.get("x-frame-options"), url).toBe("DENY");
      const urls = scriptUrls(await response.text(), url);
      expect(urls, url).toEqual([
        "http://localhost:3000/jazz/inspect/assets/backend-abc.js",
        "http://localhost:3000/jazz/inspect/assets/src-def.js",
        "http://localhost:3000/jazz/inspect/assets/src-ghi.css",
      ]);
    }

    // And those asset URLs are ones the backend serves.
    const asset = await inspector.handle(
      new Request("http://localhost:3000/jazz/inspect/assets/backend-abc.js"),
      "/inspect/assets/backend-abc.js",
    );
    expect(asset.status).toBe(200);
  });
});
