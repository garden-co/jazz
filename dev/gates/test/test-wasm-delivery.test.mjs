import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { gunzipSync } from "node:zlib";
import { testWasmDelivery } from "../test-wasm-delivery.mjs";

test("sealed WASM delivery preserves bytes and limits compression to the selected asset", () => {
  const directory = mkdtempSync(join(tmpdir(), "jazz-wasm-delivery-"));
  try {
    const file = join(directory, "jazz_wasm_bg.wasm");
    const bytes = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);
    writeFileSync(file, bytes);
    let middleware;
    testWasmDelivery(file).configureServer({
      config: { server: { headers: { "Cross-Origin-Embedder-Policy": "require-corp" } } },
      middlewares: {
        use(fn) {
          middleware = fn;
        },
      },
    });
    function request(url, encoding = "gzip, deflate, br", method = "GET") {
      const headers = {};
      let body;
      let forwarded = false;
      middleware(
        { url, method, headers: { "accept-encoding": encoding } },
        {
          setHeader(key, value) {
            headers[key] = value;
          },
          end(value) {
            body = value;
          },
        },
        () => {
          forwarded = true;
        },
      );
      return { headers, body, forwarded };
    }
    const result = request(`/@fs${file}?v=sealed`);
    assert.equal(result.forwarded, false);
    assert.equal(result.headers["Content-Encoding"], "gzip");
    assert.equal(result.headers["Content-Type"], "application/wasm");
    assert.equal(result.headers["Cross-Origin-Embedder-Policy"], "require-corp");
    assert.deepEqual(gunzipSync(result.body), bytes);
    assert.equal(result.headers["Content-Length"], result.body.length);
    assert.equal(request(`/@fs${file}`, "gzip;q=0.0").forwarded, true);
    assert.equal(request(`/@fs${file}`, "identity").forwarded, true);
    assert.equal(request(`/@fs${file}-other`).forwarded, true);
    assert.equal(request(`/@fs${file}`, "gzip", "POST").forwarded, true);
    assert.equal(request(`/@fs${file}`, "gzip", "HEAD").body, undefined);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
