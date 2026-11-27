import type { S3BlobStore, VercelBlobStore } from "./blobs.js";
import type { JazzProvider } from "./core-provider.js";
import { networkTransport } from "./core-transport.js";
import { remote } from "./remote-provider.js";

/** Connect a main backend to the Jazz core actor for this deployment key. */
export function rivet(options: {
  blobs: "sqlite" | S3BlobStore | VercelBlobStore;
  endpoint: string;
  namespace: string;
  pool: string;
  key: string;
  token: string;
  secret: string;
}): JazzProvider {
  const { namespace, pool, key, token, secret } = options;
  const endpoint = new URL(options.endpoint);
  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash ||
    !(
      endpoint.protocol === "https:" ||
      (endpoint.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname))
    )
  ) {
    throw new Error("rivet(): use an HTTPS endpoint or loopback HTTP");
  }
  if (!namespace || !pool || !key || !token || typeof secret !== "string" || secret.length < 32) {
    throw new Error(
      "rivet(): namespace, pool, key, token and a core secret of at least 32 characters are required",
    );
  }
  const blobs = options.blobs;
  if (blobs !== "sqlite" && blobs?.kind !== "s3" && blobs?.kind !== "vercel-blob") {
    throw new Error('rivet(): blobs is required and must be "sqlite", s3() or vercelBlob()');
  }
  if (blobs !== "sqlite" && blobs.credentials !== undefined) {
    throw new Error("rivet(): configure blob credentials on the jazz-rivet runner");
  }
  const destination = JSON.stringify(blobs === "sqlite" ? { kind: "sqlite" } : blobs);
  const base = endpoint.href.replace(/\/+$/, "");
  return {
    async open(context) {
      const actor = await fetch(`${base}/actors?namespace=${encodeURIComponent(namespace)}`, {
        method: "PUT",
        redirect: "error",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ name: "jazz", key, pool, crash_policy: "sleep" }),
      });
      if (!actor.ok) throw new Error(`rivet(): opening the actor returned HTTP ${actor.status}`);
      const {
        actor: { actor_id: id, pool: actualPool },
      } = await actor.json();
      if (actualPool !== pool || typeof id !== "string" || !/^[a-zA-Z0-9_-]+$/.test(id)) {
        throw new Error("rivet(): unexpected actor or runner pool");
      }
      const gateway = `${base}/gateway/${id}`;
      const http = `${gateway}/request`;
      const network = networkTransport();
      const transport = {
        fetch(url: string, init: RequestInit) {
          const headers = new Headers(init.headers);
          headers.set("x-rivet-token", token);
          return network.fetch(url, { ...init, headers });
        },
        openWebSocket(url: string, headers: Record<string, string>) {
          const path = url.slice(http.replace(/^http/, "ws").length);
          return network.openWebSocket(`${gateway.replace(/^http/, "ws")}/websocket${path}`, {
            ...headers,
            "x-rivet-token": token,
          });
        },
      };
      const response = await transport.fetch(`${http}/app/blobs/v1`, {
        method: "PUT",
        body: destination,
        headers: { "x-jazz-backend-credential": secret, "content-type": "application/json" },
      });
      if (!response.ok)
        throw new Error(
          `rivet(): configuring blob storage returned HTTP ${response.status}: ${await response.text()}`,
        );
      const { appId } = await response.json();
      return remote({ url: http, appId, secret, transport }).open(context);
    },
  };
}
