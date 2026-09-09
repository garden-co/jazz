import { readFileSync } from "node:fs";
import { posix, resolve } from "node:path";
import { gzipSync } from "node:zlib";

/**
 * Deliver the already-admitted correctness artifact without repeatedly streaming
 * its large debug/name sections uncompressed through Vite during concurrent tests.
 * Gzip preserves the exact executable and names covered by the producer receipt;
 * it changes neither browser assertions nor their operation deadlines.
 */
export function testWasmDelivery(wasmFile) {
  const assetPath = posix.join("/@fs", resolve(wasmFile).replaceAll("\\", "/"));
  const compressed = gzipSync(readFileSync(wasmFile), { level: 1 });
  return {
    name: "jazz-correctness-wasm-delivery",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        let path;
        try {
          path = decodeURIComponent((request.url ?? "").split("?")[0]);
        } catch {
          return next();
        }
        const acceptsGzip = (request.headers["accept-encoding"] ?? "").split(",").some((entry) => {
          const [encoding, ...parameters] = entry.trim().split(";");
          return (
            encoding === "gzip" &&
            !parameters.some((value) => /^\s*q\s*=\s*0(?:\.0*)?\s*$/.test(value))
          );
        });
        if (path !== assetPath || !["GET", "HEAD"].includes(request.method) || !acceptsGzip)
          return next();
        for (const [key, value] of Object.entries(server.config.server.headers ?? {}))
          response.setHeader(key, value);
        response.setHeader("Content-Type", "application/wasm");
        response.setHeader("Content-Encoding", "gzip");
        response.setHeader("Content-Length", compressed.length);
        response.setHeader("Vary", "Accept-Encoding");
        response.setHeader("Cache-Control", "no-cache");
        response.end(request.method === "HEAD" ? undefined : compressed);
      });
    },
  };
}
