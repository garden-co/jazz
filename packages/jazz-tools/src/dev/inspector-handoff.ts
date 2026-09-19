import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { appScopedUrl } from "../runtime/url.js";

export interface InspectorHandoffOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  inspectorUrl: string;
  operator?: string;
  capabilities?: ("inspector:read" | "inspector:edit" | "inspector:admin")[];
  timeoutMs?: number;
}

/** Root exchange is server-to-server; launch URL contains metadata and a single-use bootstrap code, never an access token. */
export async function startInspectorHandoff(options: InspectorHandoffOptions) {
  const inspector = new URL(options.inspectorUrl);
  if (
    inspector.username ||
    inspector.password ||
    inspector.search ||
    inspector.hash ||
    (inspector.protocol !== "https:" &&
      !(inspector.protocol === "http:" && ["localhost", "127.0.0.1"].includes(inspector.hostname)))
  )
    throw new Error("Inspector URL must use HTTPS or localhost");
  const endpoint = new URL(options.serverUrl);
  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash ||
    (endpoint.protocol !== "https:" &&
      !(
        endpoint.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname)
      ))
  )
    throw new Error("Jazz server must use HTTPS or localhost");
  const response = await fetch(
    appScopedUrl(options.serverUrl, options.appId, "admin/inspector/sessions"),
    {
      method: "POST",
      redirect: "error",
      cache: "no-store",
      signal: AbortSignal.timeout(10_000),
      headers: { "Content-Type": "application/json", "X-Jazz-Admin-Secret": options.adminSecret },
      body: JSON.stringify({
        operator: options.operator ?? "local-cli",
        capabilities: options.capabilities ?? ["inspector:read"],
      }),
    },
  );
  if (!response.ok) throw new Error(`Inspector session exchange failed (${response.status})`);
  const body = (await response.json()) as {
    accessToken?: unknown;
    expiresAt?: unknown;
    appId?: unknown;
    capabilities?: unknown;
  };
  if (
    typeof body.accessToken !== "string" ||
    !body.accessToken ||
    body.appId !== options.appId ||
    typeof body.expiresAt !== "number" ||
    !Number.isSafeInteger(body.expiresAt) ||
    body.expiresAt * 1000 <= Date.now() ||
    body.expiresAt * 1000 > Date.now() + 900_000 ||
    !Array.isArray(body.capabilities) ||
    !body.capabilities.includes("inspector:read") ||
    body.capabilities.some(
      (c) =>
        !["inspector:read", "inspector:edit", "inspector:admin"].includes(c) ||
        !(options.capabilities ?? ["inspector:read"]).includes(c),
    )
  )
    throw new Error("Invalid Inspector session response");
  let session: unknown = {
    accessToken: body.accessToken,
    expiresAt: body.expiresAt,
    appId: body.appId,
    capabilities: body.capabilities,
    serverUrl: options.serverUrl,
  };
  let launchCode: string | undefined = randomBytes(32).toString("base64url");
  const initialLaunchCode = launchCode;
  let binding: { challenge: string; code: string } | undefined;
  let used = false;
  let finish!: () => void;
  const done = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const server = createServer(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    const fail = () => {
      response.writeHead(403);
      response.end('{"error":"access_denied"}');
    };
    const address = server.address();
    if (
      !address ||
      typeof address === "string" ||
      request.headers.host !== `127.0.0.1:${address.port}` ||
      request.headers.origin !== inspector.origin
    )
      return fail();
    response.setHeader("Access-Control-Allow-Origin", inspector.origin);
    response.setHeader("Vary", "Origin");
    if (request.method === "OPTIONS") {
      response.setHeader("Access-Control-Allow-Methods", "POST");
      response.setHeader("Access-Control-Allow-Headers", "Content-Type");
      response.setHeader("Access-Control-Allow-Private-Network", "true");
      response.writeHead(204);
      response.end();
      return;
    }
    if (request.method !== "POST" || request.headers["content-type"] !== "application/json" || used)
      return fail();
    try {
      let data = "";
      for await (const chunk of request) {
        data += chunk.toString();
        if (data.length > 4096) {
          fail();
          request.destroy();
          return;
        }
      }
      const input = JSON.parse(data);
      if (
        request.url === "/challenge" &&
        !binding &&
        launchCode !== undefined &&
        typeof input.launch_code === "string" &&
        input.launch_code.length === launchCode.length &&
        timingSafeEqual(Buffer.from(input.launch_code), Buffer.from(launchCode)) &&
        typeof input.code_challenge === "string" &&
        /^[A-Za-z0-9_-]{43}$/.test(input.code_challenge)
      ) {
        launchCode = undefined;
        binding = { challenge: input.code_challenge, code: randomBytes(32).toString("base64url") };
        response.setHeader("Content-Type", "application/json");
        response.end(JSON.stringify({ code: binding.code }));
        return;
      }
      if (
        request.url !== "/token" ||
        !binding ||
        input.code !== binding.code ||
        typeof input.code_verifier !== "string" ||
        !/^[A-Za-z0-9_-]{43}$/.test(input.code_verifier)
      )
        return fail();
      const challenge = createHash("sha256").update(input.code_verifier).digest("base64url");
      if (!timingSafeEqual(Buffer.from(challenge), Buffer.from(binding.challenge))) return fail();
      used = true;
      binding = undefined;
      response.setHeader("Content-Type", "application/json");
      response.end(JSON.stringify(session));
      session = undefined;
      stop();
    } catch {
      if (!response.headersSent) fail();
    }
  });
  let timer: ReturnType<typeof setTimeout>;
  const stop = (force = false) => {
    used = true;
    launchCode = undefined;
    session = undefined;
    binding = undefined;
    clearTimeout(timer);
    server.close(() => finish());
    server.closeIdleConnections();
    if (force) server.closeAllConnections();
  };
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Inspector listener failed");
  timer = setTimeout(() => stop(true), Math.min(options.timeoutMs ?? 60_000, 60_000));
  inspector.hash = new URLSearchParams({
    appId: options.appId,
    handoff: `http://127.0.0.1:${address.port}`,
    launch: initialLaunchCode,
  }).toString();
  return { url: inspector.href, close: () => stop(true), done };
}
