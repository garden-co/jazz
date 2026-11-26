// Keep these literals independent of the writer. The node bytes and hash come
// from v1_codec_golden_bytes_decode_semantically_and_reject_alternates in jazz-types.
import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const hash = "16e2c8a19cfd936bb443835d260fcb915819d837f46f2c34e558c970cce16073";
const leaf = Buffer.from("00010076312d66697874757265", "hex");
const owner = Buffer.from("fixture-app\n");
const ownerKey = "jazz/fixture-app/.jazz-owner";
const leafKey = "jazz/fixture-app/" + hash;

const entries = [
  [ownerKey, owner],
  [leafKey, leaf],
];

for (const kind of ["r2", "s3", "vercel-blob"]) {
  test(kind + " reads and writes the Durable Object v1 blob layout", async () => {
    const objects = new Map();
    const mf = new Miniflare({
      modules: true,
      modulesRules: [{ type: "ESModule", include: ["**/*.js"] }],
      modulesRoot: fileURLToPath(new URL("../../", import.meta.url)),
      scriptPath: fileURLToPath(new URL("layout-worker.mjs", import.meta.url)),
      compatibilityDate: "2026-04-01",
      r2Buckets: ["BUCKET"],
      bindings: { KIND: kind },
      script: `
        import { LargeValueBucket } from "../../durable-object/host.js";
        import { blobStore } from "../../durable-object/blob-stores.js";
        export default {
          async fetch(request, env) {
            const options = {
              r2: { kind: "r2", bucket: env.BUCKET },
              s3: { kind: "s3", bucket: "bucket", region: "auto",
                credentials: { accessKeyId: "key", secretAccessKey: "secret" } },
              "vercel-blob": { kind: "vercel-blob", credentials: { readWriteToken: "vercel_blob_rw_fixture_token" } },
            };
            const adapter = blobStore(options[env.KIND], env, () => "fixture-signature");
            const app = "fixture-app";
            const reading = request.method === "GET";
            const claimed = { get: () => reading ? "jazz/fixture-app/" : null, set() {} };
            const bucket = new LargeValueBucket(adapter, LargeValueBucket.prefix(undefined, app), app, claimed);
            const key = new URL(request.url).pathname.slice(1);
            if (reading) {
              const { fetched } = await bucket.run({ fetches: [key] });
              return new Response(fetched[0]);
            }
            const bytes = new Uint8Array(await request.arrayBuffer());
            const { put } = await bucket.run({ puts: [key] }, () => bytes);
            return new Response(put[0]);
          },
        };
      `,
      async outboundService(request) {
        const url = new URL(request.url);
        if (kind === "s3") assert.ok(url.pathname.startsWith("/bucket/"));
        const key =
          kind === "s3"
            ? decodeURIComponent(url.pathname.slice("/bucket/".length))
            : (url.searchParams.get("pathname") ?? decodeURIComponent(url.pathname.slice(1)));
        if (request.method === "PUT") {
          objects.set(key, Buffer.from(await request.arrayBuffer()));
          return new Response();
        }
        assert.equal(request.method, "GET");
        return objects.has(key)
          ? new Response(objects.get(key))
          : new Response(null, { status: 404 });
      },
    });
    try {
      const r2 = await mf.getR2Bucket("BUCKET");
      const storage = kind === "r2" ? r2 : objects;
      // Start with objects from the fixture, before running the current writer.
      for (const [key, bytes] of entries) {
        if (kind === "r2") await r2.put(key, Uint8Array.from(bytes));
        else objects.set(key, bytes);
      }
      const url = "http://fixture/" + hash;
      const read = await mf.dispatchFetch(url);
      assert.deepEqual(Buffer.from(await read.arrayBuffer()), leaf);
      await storage.delete(ownerKey);
      await storage.delete(leafKey);
      const written = await mf.dispatchFetch(url, { method: "PUT", body: leaf });
      assert.equal(await written.text(), "");
      for (const [key, expected] of entries) {
        const bytes = kind === "r2" ? await (await r2.get(key)).arrayBuffer() : objects.get(key);
        assert.deepEqual(Buffer.from(bytes), expected, key);
      }
    } finally {
      await mf.dispose();
    }
  });
}
