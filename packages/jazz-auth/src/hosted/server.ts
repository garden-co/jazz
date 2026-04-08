import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";
import {
  createJazzHostedAuth,
  handleJazzHostedSignIn,
  handleJazzHostedSignOut,
  handleJazzHostedSignUp,
  type JazzHostedAuth,
  type JazzHostedAuthOptions,
} from "./index.js";

const DEFAULT_BIND_HOST = "127.0.0.1";

export interface JazzHostedAuthServerOptions extends JazzHostedAuthOptions {
  bindHost?: string;
  brandName?: string;
  port?: number;
}

export interface JazzHostedAuthServerHandle {
  close(): Promise<void>;
  hosted: JazzHostedAuth;
  port: number;
  url: string;
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderHostedPageDocument({
  action,
  alternateHref,
  alternateLabel,
  alternatePrompt,
  brandName,
  description,
  error,
  extraField,
  redirectTo,
  title,
}: {
  action: string;
  alternateHref: string;
  alternateLabel: string;
  alternatePrompt: string;
  brandName: string;
  description: string;
  error?: string | null;
  extraField?: string;
  redirectTo?: string | null;
  title: string;
}) {
  const errorBanner = error ? `<p class="error">${escapeHtml(error)}</p>` : "";

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)} · ${escapeHtml(brandName)}</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f4f7f5;
        --card: rgba(255, 255, 255, 0.96);
        --border: #d6e1dc;
        --text: #18343a;
        --muted: #5d7378;
        --primary: #147b73;
        --primary-foreground: #f4fffe;
        --error-bg: #fef2f2;
        --error-border: #fecaca;
        --error-text: #b91c1c;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: "Instrument Sans", "Avenir Next", "Segoe UI", sans-serif;
        background:
          radial-gradient(circle at top, rgba(20, 184, 166, 0.18), transparent 32%),
          linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(244, 247, 245, 0.98));
        color: var(--text);
      }

      main {
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 24px;
      }

      .card {
        width: min(100%, 460px);
        border-radius: 28px;
        border: 1px solid var(--border);
        background: var(--card);
        box-shadow: 0 32px 90px -48px rgba(15, 23, 42, 0.55);
        padding: 32px;
      }

      .eyebrow {
        margin: 0 0 10px;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.22em;
        text-transform: uppercase;
        color: var(--primary);
      }

      h1 {
        margin: 0;
        font-size: 32px;
        line-height: 1.1;
      }

      .description {
        margin: 12px 0 0;
        color: var(--muted);
        line-height: 1.6;
      }

      form {
        margin-top: 28px;
      }

      label {
        display: block;
        margin-bottom: 8px;
        font-size: 14px;
        font-weight: 600;
      }

      input {
        width: 100%;
        height: 44px;
        margin-bottom: 18px;
        border-radius: 14px;
        border: 1px solid var(--border);
        padding: 0 14px;
        font: inherit;
        color: inherit;
        background: #fff;
      }

      button {
        width: 100%;
        height: 46px;
        border: none;
        border-radius: 14px;
        font: inherit;
        font-weight: 700;
        background: var(--primary);
        color: var(--primary-foreground);
        cursor: pointer;
      }

      .footer {
        margin-top: 18px;
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: center;
        font-size: 14px;
        color: var(--muted);
      }

      .footer a {
        color: var(--primary);
        text-decoration: none;
        font-weight: 600;
      }

      .error {
        margin: 0 0 18px;
        border-radius: 14px;
        border: 1px solid var(--error-border);
        background: var(--error-bg);
        color: var(--error-text);
        padding: 12px 14px;
        line-height: 1.5;
      }
    </style>
  </head>
  <body>
    <main>
      <section class="card">
        <p class="eyebrow">${escapeHtml(brandName)}</p>
        <h1>${escapeHtml(title)}</h1>
        <p class="description">${escapeHtml(description)}</p>
        <form action="${escapeHtml(action)}" method="post">
          ${errorBanner}
          <input type="hidden" name="redirectTo" value="${escapeHtml(redirectTo ?? "")}" />
          ${extraField ?? ""}
          <label for="email">Email</label>
          <input id="email" name="email" type="email" autocomplete="email" placeholder="alice@example.com" required />
          <label for="password">Password</label>
          <input id="password" name="password" type="password" autocomplete="current-password" required />
          <button type="submit">${escapeHtml(title)}</button>
        </form>
        <div class="footer">
          <span>${escapeHtml(alternatePrompt)}</span>
          <a href="${escapeHtml(alternateHref)}">${escapeHtml(alternateLabel)}</a>
        </div>
      </section>
    </main>
  </body>
</html>`;
}

function buildNavHref(path: string, redirectTo?: string | null): string {
  if (!redirectTo) {
    return path;
  }

  const url = new URL(path, "https://jazz-auth.local");
  url.searchParams.set("redirectTo", redirectTo);
  return `${url.pathname}${url.search}`;
}

function renderSignInPage(hosted: JazzHostedAuth, brandName: string, requestUrl: URL): string {
  const redirectTo = requestUrl.searchParams.get("redirectTo");
  return renderHostedPageDocument({
    action: `${hosted.hostedBasePath}/sign-in/submit`,
    alternateHref: buildNavHref(`${hosted.hostedBasePath}/sign-up`, redirectTo),
    alternateLabel: "Create one",
    alternatePrompt: "Need an account?",
    brandName,
    description:
      "Use the hosted Jazz Auth flow, then redirect back to your app with a fresh session.",
    error: requestUrl.searchParams.get("error"),
    redirectTo,
    title: "Sign in",
  });
}

function renderSignUpPage(hosted: JazzHostedAuth, brandName: string, requestUrl: URL): string {
  const redirectTo = requestUrl.searchParams.get("redirectTo");
  return renderHostedPageDocument({
    action: `${hosted.hostedBasePath}/sign-up/submit`,
    alternateHref: buildNavHref(`${hosted.hostedBasePath}/sign-in`, redirectTo),
    alternateLabel: "Sign in",
    alternatePrompt: "Already have an account?",
    brandName,
    description: "New accounts can start syncing immediately after the hosted flow completes.",
    error: requestUrl.searchParams.get("error"),
    extraField:
      '<label for="name">Display name</label><input id="name" name="name" type="text" autocomplete="name" placeholder="Alice" required />',
    redirectTo,
    title: "Create account",
  });
}

function htmlResponse(html: string): Response {
  return new Response(html, {
    headers: {
      "content-type": "text/html; charset=utf-8",
    },
    status: 200,
  });
}

async function readNodeRequestBody(request: IncomingMessage): Promise<Buffer | undefined> {
  if (request.method === "GET" || request.method === "HEAD") {
    return undefined;
  }

  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks);
}

function buildFetchRequestUrl(request: IncomingMessage, fallbackBaseURL: string): string {
  const forwardedProto = request.headers["x-forwarded-proto"];
  const forwardedHost = request.headers["x-forwarded-host"];
  const protocol =
    typeof forwardedProto === "string"
      ? forwardedProto
      : new URL(fallbackBaseURL).protocol.replace(/:$/, "");
  const host =
    typeof forwardedHost === "string"
      ? forwardedHost
      : typeof request.headers.host === "string"
        ? request.headers.host
        : new URL(fallbackBaseURL).host;

  return new URL(request.url ?? "/", `${protocol}://${host}`).toString();
}

async function toFetchRequest(request: IncomingMessage, fallbackBaseURL: string): Promise<Request> {
  const headers = new Headers();

  for (const [key, value] of Object.entries(request.headers)) {
    if (value === undefined) {
      continue;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        headers.append(key, item);
      }
      continue;
    }

    headers.set(key, value);
  }

  const body = await readNodeRequestBody(request);

  return new Request(buildFetchRequestUrl(request, fallbackBaseURL), {
    body: body ? new Uint8Array(body) : undefined,
    headers,
    method: request.method ?? "GET",
  });
}

async function writeFetchResponse(nodeResponse: ServerResponse, response: Response): Promise<void> {
  const headers: Record<string, string | string[]> = {};

  for (const [key, value] of response.headers) {
    if (key.toLowerCase() === "set-cookie") {
      continue;
    }
    headers[key] = value;
  }

  const setCookies =
    typeof response.headers.getSetCookie === "function" ? response.headers.getSetCookie() : [];
  if (setCookies.length > 0) {
    headers["set-cookie"] = setCookies;
  }

  nodeResponse.writeHead(response.status, headers);

  if (!response.body) {
    nodeResponse.end();
    return;
  }

  await new Promise<void>((resolve, reject) => {
    Readable.fromWeb(response.body as unknown as Parameters<typeof Readable.fromWeb>[0])
      .on("error", reject)
      .on("end", () => resolve())
      .pipe(nodeResponse);
  });
}

async function handleBundledAuthRequest(
  hosted: JazzHostedAuth,
  brandName: string,
  request: Request,
): Promise<Response> {
  const requestUrl = new URL(request.url);
  const path = requestUrl.pathname;

  if (request.method === "GET" && path === "/health") {
    return Response.json({ status: "ok" });
  }

  if (
    request.method === "GET" &&
    (path === hosted.hostedBasePath || path === `${hosted.hostedBasePath}/`)
  ) {
    return Response.redirect(new URL(`${hosted.hostedBasePath}/sign-in`, request.url), 303);
  }

  if (request.method === "GET" && path === `${hosted.hostedBasePath}/sign-in`) {
    return htmlResponse(renderSignInPage(hosted, brandName, requestUrl));
  }

  if (request.method === "GET" && path === `${hosted.hostedBasePath}/sign-up`) {
    return htmlResponse(renderSignUpPage(hosted, brandName, requestUrl));
  }

  if (request.method === "POST" && path === `${hosted.hostedBasePath}/sign-in/submit`) {
    return handleJazzHostedSignIn(hosted, request);
  }

  if (request.method === "POST" && path === `${hosted.hostedBasePath}/sign-up/submit`) {
    return handleJazzHostedSignUp(hosted, request);
  }

  if (
    (request.method === "GET" || request.method === "POST") &&
    path === `${hosted.hostedBasePath}/sign-out`
  ) {
    return handleJazzHostedSignOut(hosted, request);
  }

  if (request.method === "GET" && (path === "/.well-known/jwks.json" || path === "/jwks")) {
    return hosted.auth.handler(
      new Request(new URL(`${hosted.apiBasePath}/jwks`, request.url), {
        headers: request.headers,
        method: "GET",
      }),
    );
  }

  if (path === hosted.apiBasePath || path.startsWith(`${hosted.apiBasePath}/`)) {
    return hosted.auth.handler(request);
  }

  return new Response("Not Found", { status: 404 });
}

export async function startJazzHostedAuthServer(
  options: JazzHostedAuthServerOptions,
): Promise<JazzHostedAuthServerHandle> {
  const bindHost = options.bindHost ?? DEFAULT_BIND_HOST;
  const port = options.port ?? 0;
  const brandName = options.brandName ?? "Jazz Auth";
  const hosted = createJazzHostedAuth({
    ...options,
    useNextCookies: false,
  });

  const server = createServer(async (request, response) => {
    try {
      const fetchRequest = await toFetchRequest(request, options.baseURL);
      const fetchResponse = await handleBundledAuthRequest(hosted, brandName, fetchRequest);
      await writeFetchResponse(response, fetchResponse);
    } catch (error) {
      response.writeHead(500, { "content-type": "application/json; charset=utf-8" });
      response.end(
        JSON.stringify({
          error: "auth_server_error",
          message: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  });

  await new Promise<void>((resolve, reject) => {
    server.listen(port, bindHost, (error?: Error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });

  const address = server.address() as AddressInfo;

  return {
    async close(): Promise<void> {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
    hosted,
    port: address.port,
    url: `http://${bindHost}:${address.port}`,
  };
}
