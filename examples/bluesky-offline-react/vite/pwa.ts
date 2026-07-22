import type { Plugin } from "vite";

const manifest = {
  name: "Jazz ❤️ Bluesky",
  short_name: "Jazz ❤️ Bluesky",
  description: "A local-first Bluesky timeline powered by Jazz.",
  start_url: "/",
  scope: "/",
  display: "standalone",
  background_color: "#f5f7ff",
  theme_color: "#5865f2",
  icons: [
    { src: "/icons/icon-192.png", sizes: "192x192", type: "image/png", purpose: "any maskable" },
    { src: "/icons/icon-512.png", sizes: "512x512", type: "image/png", purpose: "any maskable" },
  ],
} as const;

function revisionFor(paths: string[]) {
  let hash = 5381;
  for (const character of paths.join("\n")) hash = (hash * 33) ^ character.charCodeAt(0);
  return (hash >>> 0).toString(36);
}

function serviceWorkerSource(shellAssets: string[]) {
  const revision = revisionFor(shellAssets);
  return `const shellCache = "jazz-bluesky-shell-${revision}";
const mediaCache = "jazz-bluesky-media-v1";
const mediaLimit = 100;
const mediaMaxAge = 7 * 24 * 60 * 60 * 1000;
const shellAssets = ${JSON.stringify(shellAssets)};
const metadataPath = "/__pwa-media-metadata__";

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(shellCache)
      .then((cache) => cache.addAll(shellAssets))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((names) => Promise.all(names
        .filter((name) => name.startsWith("jazz-bluesky-shell-") && name !== shellCache)
        .map((name) => caches.delete(name))))
      .then(() => self.clients.claim()),
  );
});

function metadataRequest(url) {
  return new Request(self.location.origin + metadataPath + "?url=" + encodeURIComponent(url));
}

async function trimMediaCache() {
  const cache = await caches.open(mediaCache);
  const now = Date.now();
  const retained = [];

  for (const request of await cache.keys()) {
    if (new URL(request.url).pathname === metadataPath) continue;
    const timestampResponse = await cache.match(metadataRequest(request.url));
    const timestamp = Number(timestampResponse ? await timestampResponse.text() : 0);
    if (!timestamp || now - timestamp > mediaMaxAge) {
      await Promise.all([cache.delete(request), cache.delete(metadataRequest(request.url))]);
    } else {
      retained.push({ request, timestamp });
    }
  }

  retained.sort((left, right) => left.timestamp - right.timestamp);
  for (const { request } of retained.slice(0, Math.max(0, retained.length - mediaLimit))) {
    await Promise.all([cache.delete(request), cache.delete(metadataRequest(request.url))]);
  }
}

async function updateMedia(request) {
  const response = await fetch(request);
  if (!response.ok && response.type !== "opaque") return response;
  const cache = await caches.open(mediaCache);
  await Promise.all([
    cache.put(request, response.clone()),
    cache.put(metadataRequest(request.url), new Response(String(Date.now()))),
  ]);
  await trimMediaCache();
  return response;
}

async function mediaResponse(request, event) {
  const cache = await caches.open(mediaCache);
  const cached = await cache.match(request);
  const update = updateMedia(request).catch(() => undefined);
  if (cached) {
    event.waitUntil(update);
    return cached;
  }
  return (await update) ?? new Response("", { status: 504, statusText: "Offline" });
}

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;
  const url = new URL(request.url);

  if (url.origin === self.location.origin
    && (url.pathname.startsWith("/api/") || url.pathname.startsWith("/xrpc/"))) return;

  if (request.mode === "navigate") {
    event.respondWith(fetch(request).catch(async () => (
      await caches.match("/index.html", { cacheName: shellCache })
      ?? await caches.match("/", { cacheName: shellCache })
      ?? new Response("Offline", { status: 503 })
    )));
    return;
  }

  if (url.origin === self.location.origin) {
    event.respondWith(
      caches.match(request, { cacheName: shellCache }).then((cached) => (
        cached ?? (request.destination === "image" ? mediaResponse(request, event) : fetch(request))
      )),
    );
    return;
  }

  if (request.destination === "image") {
    event.respondWith(mediaResponse(request, event));
  }
});
`;
}

export function createPwaAssets(bundleFiles: string[]) {
  const shellAssets = [
    ...new Set([
      "/",
      "/index.html",
      "/manifest.webmanifest",
      "/icons/icon-192.png",
      "/icons/icon-512.png",
      ...bundleFiles.map((file) => `/${file}`),
    ]),
  ].sort();
  return {
    manifest,
    serviceWorker: serviceWorkerSource(shellAssets),
  };
}

export function pwaPlugin(): Plugin {
  return {
    name: "bluesky-offline-pwa",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        if (request.url === "/manifest.webmanifest") {
          response.setHeader("content-type", "application/manifest+json");
          response.end(JSON.stringify(manifest));
          return;
        }
        next();
      });
    },
    generateBundle(_options, bundle) {
      const { manifest: generatedManifest, serviceWorker } = createPwaAssets(Object.keys(bundle));
      this.emitFile({
        type: "asset",
        fileName: "manifest.webmanifest",
        source: JSON.stringify(generatedManifest),
      });
      this.emitFile({ type: "asset", fileName: "service-worker.js", source: serviceWorker });
    },
  };
}
