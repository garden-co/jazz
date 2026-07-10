// PROTOTYPE — interceptor spike, throwaway. Not production code.
// Serves /files/* from Cache API ("staged"/"cached" bodies) or OPFS,
// synthesizing 206 responses for Range requests; falls through to network
// and writes the cache otherwise.

const CACHE = "spike-file-store";

self.addEventListener("install", (e) => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

async function bodyFromOpfs(path) {
  // path like "opfs/<name>" — read from OPFS root, proving SW-side OPFS access.
  const name = path.split("/").pop();
  try {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle(name);
    return await fh.getFile(); // a File (Blob)
  } catch {
    return null;
  }
}

function rangeResponse(blob, rangeHeader, contentType) {
  const m = /bytes=(\d*)-(\d*)/.exec(rangeHeader || "");
  if (!m) return null;
  const size = blob.size;
  let start = m[1] === "" ? size - Number(m[2]) : Number(m[1]);
  let end = m[2] === "" || m[1] === "" ? (m[1] === "" ? size - 1 : size - 1) : Number(m[2]);
  if (m[1] !== "" && m[2] !== "") end = Math.min(Number(m[2]), size - 1);
  if (isNaN(start) || start >= size) {
    return new Response(null, { status: 416, headers: { "Content-Range": `bytes */${size}` } });
  }
  const slice = blob.slice(start, end + 1);
  return new Response(slice, {
    status: 206,
    headers: {
      "Content-Type": contentType || "application/octet-stream",
      "Content-Range": `bytes ${start}-${end}/${size}`,
      "Accept-Ranges": "bytes",
      "Content-Length": String(end - start + 1),
      "X-Spike-Source": "sw",
    },
  });
}

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (!url.pathname.startsWith("/files/")) return; // not ours

  event.respondWith(
    (async () => {
      const range = event.request.headers.get("range");
      let blob = null;
      let ctype = "application/octet-stream";

      if (url.pathname.startsWith("/files/opfs/")) {
        blob = await bodyFromOpfs(url.pathname);
        ctype = "image/png";
      } else {
        const hit = await (await caches.open(CACHE)).match(url.pathname);
        if (hit) {
          ctype = hit.headers.get("Content-Type") || ctype;
          blob = await hit.blob();
        }
      }

      if (blob) {
        if (range) return rangeResponse(blob, range, ctype);
        return new Response(blob, {
          status: 200,
          headers: {
            "Content-Type": ctype,
            "Accept-Ranges": "bytes",
            "Content-Length": String(blob.size),
            "X-Spike-Source": "sw",
          },
        });
      }

      // fetch-through + cache write (200s only)
      const resp = await fetch(event.request);
      if (resp.ok && !range) {
        const cache = await caches.open(CACHE);
        cache.put(url.pathname, resp.clone());
      }
      return resp;
    })(),
  );
});
