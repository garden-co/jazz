// Object transport for the DO host. The engine owns scheduling, integrity on
// reads and SQLite retention. None of these adapters persists credentials.
const hex = (bytes) =>
  Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
const digest = async (body) => hex(await crypto.subtle.digest("SHA-256", body));

function required(value, field) {
  if (typeof value !== "string" || !value.trim()) throw new Error(`missing or invalid ${field}`);
  return value;
}

function headerCredential(value, field) {
  required(value, field);
  if (!/^[!-~]+$/.test(value)) throw new Error(`invalid ${field}`);
  return value;
}

function r2Store(bucket) {
  if (!bucket) throw new Error("r2(): expected an R2 bucket binding");
  return {
    prefixOption: "JAZZ_BLOBS_PREFIX",
    async get(key, limit) {
      const object = await bucket.get(key);
      if (!object) return null;
      if (object.size > limit) throw new Error("R2 object exceeds the Jazz size limit");
      return new Uint8Array(await object.arrayBuffer());
    },
    createOwner: (key, bytes) => bucket.put(key, bytes, { onlyIf: { etagDoesNotMatch: "*" } }),
    put: (key, bytes, sha256) => bucket.put(key, bytes, { sha256 }),
    deleteMany: (keys) => bucket.delete(keys),
  };
}

function encodePath(key) {
  // URL parsers resolve dot segments before sending a request.
  if (key.split("/").some((part) => part === "." || part === "..")) {
    throw new Error("blob path contains a dot segment");
  }
  return key
    .split("/")
    .map((part) =>
      encodeURIComponent(part).replace(
        /[!'()*]/g,
        (char) => "%" + char.charCodeAt(0).toString(16).toUpperCase(),
      ),
    )
    .join("/");
}

async function send(url, options) {
  return fetch(url, { ...options, redirect: "manual", signal: AbortSignal.timeout(30000) });
}

async function check(response, operation, allowed = []) {
  if (!response.ok && !allowed.includes(response.status)) {
    await response.body?.cancel();
    throw new Error(`${operation} returned HTTP ${response.status}`);
  }
}

async function finish(response, operation, allowed) {
  await check(response, operation, allowed);
  await response.body?.cancel();
}

// A bad remote object must not allocate an unbounded buffer, even if it is
// compressed or streamed without Content-Length.
async function read(response, limit) {
  if (response.status === 404) {
    await response.body?.cancel();
    return null;
  }
  await check(response, "blob read");
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const parts = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.length;
    if (size > limit) {
      await reader.cancel();
      throw new Error("blob exceeds the Jazz size limit");
    }
    parts.push(value);
  }
  const bytes = new Uint8Array(size);
  let at = 0;
  for (const part of parts) {
    bytes.set(part, at);
    at += part.length;
  }
  return bytes;
}

function s3Store(options, env, sign) {
  const bucket = required(options.bucket ?? env.S3_BUCKET_NAME, "S3 bucket");
  const region = required(
    options.region ?? env.AWS_REGION ?? (options.endpoint ? "auto" : undefined),
    "S3 region",
  );
  if (!/^[a-z0-9-]+$/.test(region) || /[/\\]/.test(bucket) || bucket === "." || bucket === "..")
    throw new Error("invalid S3 bucket or region");
  const endpoint = new URL(options.endpoint ?? `https://s3.${region}.amazonaws.com`);
  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash ||
    !(
      endpoint.protocol === "https:" ||
      (endpoint.protocol === "http:" &&
        ["127.0.0.1", "[::1]", "localhost"].includes(endpoint.hostname))
    )
  ) {
    throw new Error("invalid S3 endpoint (use HTTPS or local HTTP)");
  }
  const base = endpoint.href.replace(/\/$/, "") + "/" + encodePath(bucket);
  const keys =
    options.credentials !== undefined
      ? options.credentials
      : {
          accessKeyId: env.AWS_ACCESS_KEY_ID,
          secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
          sessionToken: env.AWS_SESSION_TOKEN,
        };
  headerCredential(keys?.accessKeyId, "S3 credentials.accessKeyId");
  required(keys?.secretAccessKey, "S3 credentials.secretAccessKey");
  if (keys.sessionToken !== undefined)
    headerCredential(keys.sessionToken, "S3 credentials.sessionToken");
  async function request(method, key, body, hash, headers = {}) {
    const url = new URL(base + "/" + encodePath(key));
    const payload = hash ?? (await digest(body ?? new Uint8Array()));
    const signed = new Headers(headers);
    signed.set("host", url.host);
    signed.set("x-amz-date", new Date().toISOString().replace(/[:-]|\.\d{3}/g, ""));
    signed.set("x-amz-content-sha256", payload);
    if (keys.sessionToken) signed.set("x-amz-security-token", keys.sessionToken);
    signed.set("authorization", sign(method, url.pathname, [...signed], payload, keys, region));
    signed.delete("host"); // fetch supplies the URL's authority.
    return send(url, { method, headers: signed, body });
  }
  return {
    get: async (key, limit) => read(await request("GET", key), limit),
    async createOwner(key, bytes) {
      await finish(
        await request("PUT", key, bytes, undefined, { "if-none-match": "*" }),
        "S3 claim",
        [412],
      );
    },
    async put(key, bytes, hash) {
      const checksum = btoa(String.fromCharCode(...hash.match(/../g).map((v) => parseInt(v, 16))));
      await finish(
        await request("PUT", key, bytes, hash, { "x-amz-checksum-sha256": checksum }),
        "S3 upload",
      );
    },
    async remove(key) {
      await finish(await request("DELETE", key), "S3 delete", [404]);
    },
  };
}

function vercelStore(options, env) {
  const explicit = options.credentials !== undefined;
  const token = headerCredential(
    explicit ? options.credentials?.readWriteToken : env.BLOB_READ_WRITE_TOKEN,
    "Vercel Blob credentials.readWriteToken",
  );
  const tokenStore = /^vercel_blob_rw_([^_]+)_/.exec(token)?.[1];
  const id = required(
    options.storeId ?? (explicit ? tokenStore : undefined) ?? env.BLOB_STORE_ID ?? tokenStore,
    "Vercel Blob storeId",
  ).replace(/^store_/, "");
  if (!/^[a-zA-Z0-9-]+$/.test(id)) throw new Error("invalid Vercel Blob storeId");
  const auth = { authorization: "Bearer " + token };
  const headers = { ...auth, "x-api-version": "13", "x-vercel-blob-store-id": id };
  const api = "https://vercel.com/api/blob";
  async function upload(key, bytes, overwrite) {
    const response = await send(api + "/?pathname=" + encodeURIComponent(key), {
      method: "PUT",
      body: bytes,
      headers: {
        ...headers,
        "x-vercel-blob-access": "private",
        "x-add-random-suffix": "0",
        "x-allow-overwrite": overwrite ? "1" : "0",
        "x-content-type": "application/octet-stream",
        "x-content-length": String(bytes.length),
      },
    });
    await finish(response, "Vercel Blob upload");
  }
  return {
    async get(key, limit) {
      const url = `https://${id.toLowerCase()}.private.blob.vercel-storage.com/${encodePath(key)}`;
      return read(
        await send(url + (key.endsWith("/.jazz-owner") ? "?cache=0" : ""), { headers: auth }),
        limit,
      );
    },
    createOwner: (key, bytes) => upload(key, bytes, false),
    async put(key, bytes, hash) {
      if ((await digest(bytes)) !== hash) throw new Error("Vercel Blob upload checksum mismatch");
      await upload(key, bytes, true);
    },
    async deleteMany(keys) {
      const response = await send(api + "/delete", {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({ urls: keys }),
      });
      await finish(response, "Vercel Blob delete", [404]);
    },
  };
}

export function blobStore(options, env, sign) {
  if (options === "sqlite") return undefined;
  switch (options?.kind) {
    case "r2":
      return r2Store(options.bucket);
    case "s3":
      return s3Store(options, env, sign);
    case "vercel-blob":
      return vercelStore(options, env);
    default:
      throw new Error('createJazzCore(): select "sqlite", r2(), s3() or vercelBlob()');
  }
}
