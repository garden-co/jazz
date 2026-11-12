// The Inspector UI an app's backend serves at `<basePath>/inspect/`: the
// inspector package's `backend.html` build, and at
// `<basePath>/inspect/embedded.html` the overlay for local-first pages.
// Published jazz-tools carries it as a module, `inspector-ui-files.js`
// (generated with the overlay's embedded build, see
// scripts/stage-inspector-overlay.mjs), so that it ships wherever the code
// does: Next.js's output file tracing leaves files read from disk behind. In
// this monorepo it is read from the inspector package's build output.
//
// The page and its scripts are public code; what they can see is decided by
// the API next to them. Nothing here reads a credential.

import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join, normalize, sep } from "node:path";
import { fileURLToPath } from "node:url";

const PAGE = "backend.html";
// The overlay a local-first page frames (`jazz-tools/inspector`). It reads only
// the framing page's own Db, through the window, never this backend's API.
const OVERLAY_PAGE = "embedded.html";

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

/** Headers for the overlay page: framed only by pages on this origin. */
const OVERLAY_HEADERS = {
  ...PAGE_HEADERS,
  "Content-Security-Policy": "frame-ancestors 'self'; base-uri 'none'; object-src 'none'",
  "X-Frame-Options": "SAMEORIGIN",
};

/**
 * The page as served at `<basePath>/inspect`, without the trailing slash.
 * Vite builds it with relative URLs (`./assets/...`), which there would
 * resolve beside `inspect` rather than below it, so point them below it.
 */
export function pageWithoutTrailingSlash(html: string): string {
  return html.replaceAll('="./', '="inspect/');
}

/** Reads a file of the UI build by its path below the build, or null. */
type UiFiles = (file: string) => Promise<Uint8Array<ArrayBuffer> | null>;

function filesInDir(dir: string): UiFiles {
  return async (file) => {
    try {
      return new Uint8Array(await readFile(join(dir, file)));
    } catch {
      return null;
    }
  };
}

function filesInModule(files: Readonly<Record<string, string>>): UiFiles {
  return async (file) => {
    const encoded = files[file.split(sep).join("/")];
    if (encoded === undefined) return null;
    const binary = atob(encoded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
  };
}

/** The installed UI: the generated module, else (in this monorepo) the build directory. */
async function installedUi(): Promise<UiFiles | null> {
  const { INSPECTOR_UI_FILES } = await import("./inspector-ui-files.js");
  if (INSPECTOR_UI_FILES) return filesInModule(INSPECTOR_UI_FILES);
  const dir = resolveInspectorUiDir();
  return dir ? filesInDir(dir) : null;
}

/**
 * Serve the Inspector UI: from `dir` when given (null: there is none), else
 * the one this install carries, loaded on the first request.
 */
export function createInspectorUi(dir?: string | null) {
  let ui: Promise<UiFiles | null> | undefined =
    dir === undefined ? undefined : Promise.resolve(dir === null ? null : filesInDir(dir));
  const read = async (file: string) => {
    ui ??= installedUi();
    const files = await ui;
    return files ? files(file) : null;
  };
  /**
   * The file for `path` below `/inspect`, or null. The page is at `/`, and at
   * `""` too: Next.js (and other hosts) redirect `/inspect/` to `/inspect`,
   * so redirecting back would loop.
   */
  return async function serveUi(_request: Request, path: string): Promise<Response | null> {
    if (path === "/" || path === "") {
      const page = await read(PAGE);
      if (!page) return null;
      const html = new TextDecoder().decode(page);
      return new Response(path === "" ? pageWithoutTrailingSlash(html) : html, {
        headers: { "Content-Type": TYPES[".html"]!, ...PAGE_HEADERS },
      });
    }
    if (path === `/${OVERLAY_PAGE}`) {
      const overlay = await read(OVERLAY_PAGE);
      if (!overlay) return null;
      return new Response(overlay, {
        headers: { "Content-Type": TYPES[".html"]!, ...OVERLAY_HEADERS },
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
    const body = await read(file);
    if (!body) return null;
    return new Response(body, {
      headers: {
        "Content-Type": type,
        "Cache-Control": "public, max-age=31536000, immutable",
        "X-Content-Type-Options": "nosniff",
      },
    });
  };
}
