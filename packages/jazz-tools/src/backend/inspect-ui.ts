// The Inspector UI an app's backend serves at `<basePath>/inspect/`: the
// inspector package's `backend.html` build. Published jazz-tools carries it
// in its own dist (staged with the overlay's embedded build, see
// scripts/stage-inspector-overlay.mjs); in this monorepo it is read from the
// inspector package's build output.
//
// The page and its scripts are public code; what they can see is decided by
// the API next to them. Nothing here reads a credential.

import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join, normalize, sep } from "node:path";
import { fileURLToPath } from "node:url";

const PAGE = "backend.html";

const TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".woff2": "font/woff2",
};

/** The directory holding the backend Inspector build, or null when this install has none. */
export function resolveInspectorUiDir(): string | null {
  const here = dirname(fileURLToPath(import.meta.url)); // .../jazz-tools/{src,dist}/backend
  for (const dir of [
    join(here, "../dev/inspector-overlay/embedded"),
    join(here, "../../../inspector/dist-embedded"),
  ]) {
    if (existsSync(join(dir, PAGE))) return dir;
  }
  return null;
}

/** Headers for the page: no framing (the approval page must not be clickjacked), no referrer. */
const PAGE_HEADERS = {
  "Cache-Control": "no-store",
  "Content-Security-Policy": "frame-ancestors 'none'; base-uri 'none'; object-src 'none'",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "no-referrer",
  "X-Content-Type-Options": "nosniff",
};

export function createInspectorUi(dir: string | null = resolveInspectorUiDir()) {
  /** The file for `path` below `/inspect` (`/` is the page), or null. */
  return async function serveUi(_request: Request, path: string): Promise<Response | null> {
    if (!dir) return null;
    if (path === "/" || path === "") {
      return new Response(new Uint8Array(await readFile(join(dir, PAGE))), {
        headers: { "Content-Type": TYPES[".html"]!, ...PAGE_HEADERS },
      });
    }
    // Only the build's hashed assets, nothing else in the directory.
    let relative: string;
    try {
      relative = decodeURIComponent(path.slice(1));
    } catch {
      return null;
    }
    const file = normalize(relative);
    if (!file.startsWith(`assets${sep}`) || file.includes(`..`) || file.includes("\0")) {
      return null;
    }
    const type = TYPES[file.slice(file.lastIndexOf("."))];
    if (!type) return null;
    let body: Uint8Array<ArrayBuffer>;
    try {
      body = new Uint8Array(await readFile(join(dir, file)));
    } catch {
      return null;
    }
    return new Response(body, {
      headers: {
        "Content-Type": type,
        "Cache-Control": "public, max-age=31536000, immutable",
        "X-Content-Type-Options": "nosniff",
      },
    });
  };
}
