import { describe, expect, it } from "vitest";
import { createPwaAssets } from "../../vite/pwa.js";

describe("PWA assets", () => {
  it("describes an installable app and precaches the complete application shell", () => {
    const { manifest, serviceWorker } = createPwaAssets([
      "assets/app.js",
      "assets/jazz.wasm",
      "assets/styles.css",
    ]);

    expect(manifest).toMatchObject({
      name: "Jazz ❤️ Bluesky",
      short_name: "Jazz ❤️ Bluesky",
      start_url: "/",
      display: "standalone",
      icons: [
        expect.objectContaining({ src: "/icons/icon-192.png", sizes: "192x192" }),
        expect.objectContaining({ src: "/icons/icon-512.png", sizes: "512x512" }),
      ],
    });
    expect(serviceWorker).toContain('"/assets/app.js"');
    expect(serviceWorker).toContain('"/assets/jazz.wasm"');
    expect(serviceWorker).toContain('"/assets/styles.css"');
    expect(serviceWorker).toContain('url.pathname.startsWith("/api/")');
    expect(serviceWorker).toContain('url.pathname.startsWith("/xrpc/")');
    expect(serviceWorker).toContain("caches.match(request, { cacheName: shellCache })");
    expect(serviceWorker).toContain("const mediaLimit = 100;");
    expect(serviceWorker).toContain("const mediaMaxAge = 7 * 24 * 60 * 60 * 1000;");
  });
});
