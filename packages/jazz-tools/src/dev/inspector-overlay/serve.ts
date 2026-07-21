import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, relative, isAbsolute } from "node:path";

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
  const here = dirname(fileURLToPath(import.meta.url));
  // First: the inspector's embedded build bundled into jazz-tools' own dist next
  // to this module (staged at publish via scripts/stage-inspector-overlay.mjs) —
  // resolved relative to jazz-tools, so it works in any consumer install anywhere.
  // Fallback (monorepo dev only, never reached in a published install): read the
  // sibling inspector package directly, since we don't stage into dist during a
  // normal build. `here` is .../jazz-tools/{src,dist}/dev/inspector-overlay, so
  // four levels up lands on `packages/`.
  for (const dir of [join(here, "embedded"), join(here, "../../../../inspector/dist-embedded")]) {
    if (existsSync(join(dir, "embedded.html"))) return dir;
  }
  return null;
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

// Announced so the overlay's toggle/shortcut is discoverable — there's no other
// hint that it's there.
export const OVERLAY_ENABLED_MESSAGE =
  "[jazz] Inspector overlay enabled — click the 🎵 button in your app (Alt+Shift+J).";

/**
 * During dev, serve the inspector's embedded assets (/__jazz/embedded/*) and
 * signal the client that the jazz dev plugin is active by exposing
 * VITE_JAZZ_INSPECTOR (Vite surfaces VITE_*-prefixed keys to the browser via
 * import.meta.env). The provider reads that flag and mounts the overlay toggle,
 * so the inspector is on by default whenever the plugin is in use — and absent
 * when it isn't (nothing would serve the iframe otherwise). Both the Vite and
 * SvelteKit plugins call this.
 */
export function wireInspectorOverlay(server: OverlayDevServer): void {
  const overlay = createOverlayHandler();
  server.middlewares?.use((req, res, next) => {
    void overlay(req, res).then((handled) => {
      if (!handled) next();
    });
  });
  server.config.env ??= {};
  server.config.env.VITE_JAZZ_INSPECTOR = "1";
  console.log(OVERLAY_ENABLED_MESSAGE);
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
  // Resolve the embedded dir once at startup (hits disk). It ships inside
  // jazz-tools, so a miss means a broken install — say so once here rather than
  // re-checking per request.
  const dir = resolveEmbeddedDir();
  if (!dir) {
    console.log(
      "[jazz] Inspector overlay: couldn't find the embedded inspector build. " +
        "It ships with jazz-tools — try reinstalling dependencies.",
    );
  }
  return async function handle(req: { url?: string }, res: OverlayResponse): Promise<boolean> {
    const url = (req.url ?? "").split("?")[0];
    if (url !== OVERLAY_EMBEDDED_PREFIX && !url.startsWith(OVERLAY_EMBEDDED_PREFIX + "/")) {
      return false;
    }
    if (!dir) {
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
      // No in-process cache: this is a dev-only server and the OS page cache
      // already keeps these files hot, so a plain read stays fast and always
      // reflects a rebuild.
      const body = await readFile(filePath);
      res.setHeader("Content-Type", MIME[ext(filePath)] ?? "application/octet-stream");
      res.end(body);
    } catch {
      res.statusCode = 404;
      res.end("Not found");
    }
    return true;
  };
}
