// A CDN-like front for a production app server:
//
//   node cdn.mjs serve <frontPort> <backendPort> <urlPrefix> <staticDir>
//   node cdn.mjs precompress <staticDir> [<staticDir> ...]
//
// Requests under <urlPrefix> (the build's hashed assets: /_next/static/ or
// /assets/) are served from <staticDir> the way a CDN serves them: the real
// content type (`application/wasm` for the WASM runtime), precompressed brotli
// (quality 11) or gzip by Accept-Encoding, and `immutable` caching.
// Everything else (pages, API routes, public files, WebSocket upgrades) is
// proxied unchanged to the app server on <backendPort>.
//
// `vite preview` serves the WASM uncompressed with `no-cache` and `next start`
// gzips it again on every request, so neither is what a deployed app serves.
//
// Compressed copies are cached under .generated/cdn-cache by content hash.
// The build job runs `precompress`, so the measurement host only reads them.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { extname, join, normalize } from "node:path";
import zlib from "node:zlib";
import { generated } from "./apps.mjs";

const cacheDir = join(generated, "cdn-cache");

const types = {
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json",
  ".map": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webp": "image/webp",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".txt": "text/plain; charset=utf-8",
  ".html": "text/html; charset=utf-8",
};
const compressible = new Set([
  ".js",
  ".mjs",
  ".css",
  ".wasm",
  ".json",
  ".map",
  ".svg",
  ".txt",
  ".html",
]);

function walk(dir, rel = "", out = new Map()) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const r = rel ? `${rel}/${e.name}` : e.name;
    if (e.isDirectory()) walk(join(dir, e.name), r, out);
    else out.set(r, { file: join(dir, e.name) });
  }
  return out;
}

/** Reads every asset under staticDir, compressing (or reusing) each copy. */
export function loadAssets(staticDir) {
  mkdirSync(cacheDir, { recursive: true });
  const files = walk(staticDir);
  for (const [r, f] of files) {
    const body = readFileSync(f.file);
    const ext = extname(r);
    f.body = body;
    f.type = types[ext] ?? "application/octet-stream";
    if (!compressible.has(ext) || body.length <= 1024) continue;
    const hash = createHash("sha256").update(body).digest("hex").slice(0, 32);
    for (const [enc, compress] of [
      [
        "br",
        () =>
          zlib.brotliCompressSync(body, {
            params: {
              [zlib.constants.BROTLI_PARAM_QUALITY]: 11,
              [zlib.constants.BROTLI_PARAM_SIZE_HINT]: body.length,
            },
          }),
      ],
      ["gz", () => zlib.gzipSync(body, { level: 9 })],
    ]) {
      const cached = join(cacheDir, `${hash}.${enc}`);
      if (!existsSync(cached)) writeFileSync(cached, compress());
      f[enc] = readFileSync(cached);
    }
  }
  return files;
}

function serve(frontPort, backendPort, prefix, staticDir) {
  const t0 = Date.now();
  const files = loadAssets(staticDir);
  console.log(`cdn: ${files.size} assets ready in ${Date.now() - t0} ms`);
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://front");
    if (url.pathname.startsWith(prefix)) {
      const f = files.get(normalize(decodeURIComponent(url.pathname.slice(prefix.length))));
      if (f) {
        const accept = String(req.headers["accept-encoding"] ?? "");
        const [enc, body] =
          f.br && /\bbr\b/.test(accept)
            ? ["br", f.br]
            : f.gz && /\bgzip\b/.test(accept)
              ? ["gzip", f.gz]
              : [null, f.body];
        res.writeHead(200, {
          "content-type": f.type,
          "content-length": body.length,
          "cache-control": "public, max-age=31536000, immutable",
          vary: "Accept-Encoding",
          ...(enc ? { "content-encoding": enc } : {}),
        });
        res.end(req.method === "HEAD" ? undefined : body);
        return;
      }
    }
    const up = http.request(
      {
        host: "127.0.0.1",
        port: backendPort,
        path: req.url,
        method: req.method,
        headers: req.headers,
      },
      (r) => {
        res.writeHead(r.statusCode ?? 502, r.headers);
        r.pipe(res);
      },
    );
    up.on("error", () => res.destroy());
    req.pipe(up);
  });
  server.on("upgrade", (req, socket, head) => {
    const up = net.connect(backendPort, "127.0.0.1", () => {
      const headers = Object.entries(req.headers).map(([k, v]) => `${k}: ${v}`);
      up.write(`${req.method} ${req.url} HTTP/1.1\r\n${headers.join("\r\n")}\r\n\r\n`);
      if (head?.length) up.write(head);
      up.pipe(socket);
      socket.pipe(up);
    });
    const close = () => (up.destroy(), socket.destroy());
    up.on("error", close);
    socket.on("error", close);
  });
  server.keepAliveTimeout = 65_000;
  server.listen(frontPort, "127.0.0.1", () =>
    console.log(`CDN READY http://127.0.0.1:${frontPort}`),
  );
}

if (process.argv[1] === new URL(import.meta.url).pathname) {
  const [mode, ...args] = process.argv.slice(2);
  if (mode === "serve") {
    const [frontPort, backendPort, prefix, staticDir] = args;
    serve(Number(frontPort), Number(backendPort), prefix, staticDir);
  } else if (mode === "precompress") {
    for (const dir of args) {
      const t0 = Date.now();
      const files = loadAssets(dir);
      console.log(`precompressed ${files.size} assets of ${dir} in ${Date.now() - t0} ms`);
    }
  } else {
    console.error("usage: cdn.mjs serve <frontPort> <backendPort> <urlPrefix> <staticDir>");
    console.error("       cdn.mjs precompress <staticDir> [<staticDir> ...]");
    process.exit(2);
  }
}
