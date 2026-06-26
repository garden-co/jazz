import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join, relative, isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

export const INSPECTOR_PACKAGE = "jazz-inspector";
export const OVERLAY_LOADER_PATH = "/__jazz/loader.js";
export const OVERLAY_EMBEDDED_PREFIX = "/__jazz/embedded";

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};
const ext = (p: string) => {
  const i = p.lastIndexOf(".");
  return i === -1 ? "" : p.slice(i);
};

export function resolveEmbeddedDir(appRoot: string): string | null {
  try {
    const requireFromApp = createRequire(join(appRoot, "noop.js"));
    // Resolve an existing emitted file (the build outputs embedded.html, not index.html).
    return dirname(requireFromApp.resolve(`${INSPECTOR_PACKAGE}/dist-embedded/embedded.html`));
  } catch {
    return null;
  }
}

let loaderScriptPromise: Promise<string> | null = null;
export function bundleOverlayLoaderScript(): Promise<string> {
  if (!loaderScriptPromise) {
    const here = dirname(fileURLToPath(import.meta.url));
    const tsEntry = join(here, "loader.ts");
    const entry = existsSync(tsEntry) ? tsEntry : join(here, "loader.js");
    loaderScriptPromise = build({
      entryPoints: [entry],
      bundle: true,
      format: "iife",
      platform: "browser",
      write: false,
      legalComments: "none",
    }).then((r) => r.outputFiles[0].text);
  }
  return loaderScriptPromise;
}

export interface OverlayResponse {
  setHeader(name: string, value: string): void;
  statusCode: number;
  end(body?: string | Buffer): void;
}
export interface OverlayHandlerOptions {
  appRoot: string;
}
// Minimal Vite-style dev server shape the overlay needs. Shared so the Vite and
// SvelteKit plugins wire the overlay the same way (one definition of the contract).
export interface OverlayDevServer {
  config: { root: string };
  middlewares?: {
    use(fn: (req: { url?: string }, res: OverlayResponse, next: () => void) => void): void;
  };
}

/** Register the overlay static-asset middleware (/__jazz/*) on a dev server. */
export function attachOverlayMiddleware(server: OverlayDevServer): void {
  const overlay = createOverlayHandler({ appRoot: server.config.root });
  server.middlewares?.use((req, res, next) => {
    void overlay(req, res).then((handled) => {
      if (!handled) next();
    });
  });
}

const OVERLAY_LOADER_TAG = {
  tag: "script",
  attrs: { type: "module", src: OVERLAY_LOADER_PATH },
  injectTo: "body" as const,
};

/** Vite `transformIndexHtml` hook that injects the overlay loader — dev-server only. */
export function overlayHtmlTransform(
  html: string,
  ctx: { server?: unknown },
): string | { html: string; tags: Array<typeof OVERLAY_LOADER_TAG> } {
  if (!ctx.server) return html;
  return { html, tags: [OVERLAY_LOADER_TAG] };
}

export function createOverlayHandler({ appRoot }: OverlayHandlerOptions) {
  let warnedMissing = false;
  // appRoot is fixed for the handler's lifetime, so resolve the embedded dir
  // once (require.resolve walks node_modules and hits disk) instead of per request.
  let dirCache: string | null | undefined;
  const embeddedDir = (): string | null =>
    dirCache !== undefined ? dirCache : (dirCache = resolveEmbeddedDir(appRoot));
  return async function handle(req: { url?: string }, res: OverlayResponse): Promise<boolean> {
    const url = (req.url ?? "").split("?")[0];
    if (url === OVERLAY_LOADER_PATH) {
      res.setHeader("Content-Type", MIME[".js"]);
      res.end(await bundleOverlayLoaderScript());
      return true;
    }
    if (url === OVERLAY_EMBEDDED_PREFIX || url.startsWith(OVERLAY_EMBEDDED_PREFIX + "/")) {
      const dir = embeddedDir();
      if (!dir) {
        if (!warnedMissing) {
          warnedMissing = true;
          console.log(
            `[jazz] Inspector overlay: install \`${INSPECTOR_PACKAGE}\` as a devDependency to enable it.`,
          );
        }
        res.statusCode = 404;
        res.end("Inspector not installed");
        return true;
      }
      const rel = url.slice(OVERLAY_EMBEDDED_PREFIX.length).replace(/^\//, "") || "embedded.html";
      const filePath = join(dir, rel);
      const within = relative(dir, filePath);
      if (within.startsWith("..") || isAbsolute(within)) {
        res.statusCode = 403;
        res.end("Forbidden");
        return true;
      }
      try {
        const body = await readFile(filePath);
        res.setHeader("Content-Type", MIME[ext(filePath)] ?? "application/octet-stream");
        res.end(body);
      } catch {
        res.statusCode = 404;
        res.end("Not found");
      }
      return true;
    }
    return false;
  };
}
