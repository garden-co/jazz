import { createRequire } from "node:module";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { dirname, join, relative, isAbsolute } from "node:path";

export const INSPECTOR_PACKAGE = "jazz-inspector";
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

export function resolveEmbeddedDir(): string | null {
  try {
    // `jazz-inspector` is a direct dependency of jazz-tools, so the embedded
    // build always sits in jazz-tools' own dependency tree — resolve it relative
    // to this module rather than the host app. The build is dev tooling, so this
    // runs lazily (dynamic resolution) and never gets bundled into app output.
    const require = createRequire(import.meta.url);
    // Resolve an existing emitted file (the build outputs embedded.html, not index.html).
    return dirname(require.resolve(`${INSPECTOR_PACKAGE}/dist-embedded/embedded.html`));
  } catch {
    return null;
  }
}

export interface OverlayResponse {
  setHeader(name: string, value: string): void;
  statusCode: number;
  end(body?: string | Buffer): void;
}
// Minimal Vite-style dev server shape the overlay needs. Shared so the Vite and
// SvelteKit plugins serve the inspector assets the same way (one definition).
export interface OverlayDevServer {
  config: { root: string; env?: Record<string, string> };
  middlewares?: {
    use(fn: (req: { url?: string }, res: OverlayResponse, next: () => void) => void): void;
  };
}

// Advertised by the dev plugins when the experimental inspector is enabled —
// there's no way to discover the overlay's toggle/shortcut otherwise.
export const OVERLAY_ENABLED_MESSAGE =
  "[jazz] Inspector overlay enabled — click the ⚡ button in your app (Alt+Shift+J).";

/**
 * Register the inspector embedded-asset middleware (/__jazz/embedded/*) on a dev
 * server. Both Vite and SvelteKit call this. Whether the overlay's toggle
 * actually mounts is gated separately by `experimental_inspector`; this passive
 * route only responds when the toggle's iframe requests it.
 */
export function attachOverlayMiddleware(server: OverlayDevServer): void {
  const overlay = createOverlayHandler();
  server.middlewares?.use((req, res, next) => {
    void overlay(req, res).then((handled) => {
      if (!handled) next();
    });
  });
}

/**
 * Experimental: when enabled, signal the client provider to mount the overlay
 * toggle by exposing VITE_JAZZ_INSPECTOR to the browser (Vite-family bundlers
 * surface VITE_*-prefixed keys via import.meta.env) and announce it. Shared by
 * the Vite and SvelteKit plugins, which inject it identically. (Next uses its
 * own NEXT_PUBLIC_ flag and announcement, since it has no Vite dev server here.)
 */
export function enableOverlayToggle(
  server: { config: { env?: Record<string, string> } },
  enabled: boolean | undefined,
): void {
  if (!enabled) return;
  server.config.env ??= {};
  server.config.env.VITE_JAZZ_INSPECTOR = "1";
  console.log(OVERLAY_ENABLED_MESSAGE);
}

/**
 * Attach the inspector overlay middleware and, when enabled, signal the client
 * provider to mount the toggle. The Vite and SvelteKit plugins always wire
 * these together (the middleware is passive; the flag only gates the toggle),
 * so they share this entry point to keep the pair in sync.
 */
export function wireInspectorOverlay(server: OverlayDevServer, enabled: boolean | undefined): void {
  attachOverlayMiddleware(server);
  enableOverlayToggle(server, enabled);
}

export interface OverlayAssetServer {
  /** Origin to proxy `/__jazz/embedded/*` requests to, e.g. http://127.0.0.1:54321 */
  origin: string;
  close(): Promise<void>;
}

/**
 * Start a tiny localhost HTTP server that serves the inspector's embedded build
 * at /__jazz/embedded/*, reusing the same handler as the Vite/SvelteKit
 * middleware. Next has no dev-middleware hook, so its plugin proxies to this
 * server via a dev-only rewrite instead of copying assets into the app. The
 * socket is unref()'d so it never keeps the dev process alive.
 */
export async function startOverlayAssetServer(): Promise<OverlayAssetServer> {
  const handle = createOverlayHandler();
  const server = createServer((req, res) => {
    void handle({ url: req.url }, res as unknown as OverlayResponse).then((handled) => {
      if (!handled) {
        res.statusCode = 404;
        res.end("Not found");
      }
    });
  });
  server.unref();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return {
    origin: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

export function createOverlayHandler() {
  let warnedMissing = false;
  // The embedded dir is fixed for the process lifetime, so resolve it once
  // (require.resolve walks node_modules and hits disk) instead of per request.
  let dirCache: string | null | undefined;
  const embeddedDir = (): string | null =>
    dirCache !== undefined ? dirCache : (dirCache = resolveEmbeddedDir());
  // The embedded assets are static for the process lifetime too, so memoize
  // their bytes — an iframe reload re-requests every chunk otherwise.
  const fileCache = new Map<string, Buffer>();
  return async function handle(req: { url?: string }, res: OverlayResponse): Promise<boolean> {
    const url = (req.url ?? "").split("?")[0];
    if (url !== OVERLAY_EMBEDDED_PREFIX && !url.startsWith(OVERLAY_EMBEDDED_PREFIX + "/")) {
      return false;
    }
    const dir = embeddedDir();
    if (!dir) {
      if (!warnedMissing) {
        warnedMissing = true;
        console.log(
          `[jazz] Inspector overlay: couldn't find the \`${INSPECTOR_PACKAGE}\` build. ` +
            "It ships with jazz-tools — try reinstalling dependencies.",
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
      let body = fileCache.get(filePath);
      if (body === undefined) {
        body = await readFile(filePath);
        fileCache.set(filePath, body);
      }
      res.setHeader("Content-Type", MIME[ext(filePath)] ?? "application/octet-stream");
      res.end(body);
    } catch {
      res.statusCode = 404;
      res.end("Not found");
    }
    return true;
  };
}
