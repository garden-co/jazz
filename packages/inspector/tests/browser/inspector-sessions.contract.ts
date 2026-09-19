import { createHash, randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { test, expect } from "@playwright/test";
import ts from "typescript";
import { startInspectorHandoff } from "../../../jazz-tools/src/dev/inspector-handoff.js";

async function listen(handler: (req: IncomingMessage, res: ServerResponse) => void) {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("listener failed");
  return {
    origin: `http://127.0.0.1:${address.port}`,
    close: () => {
      server.close();
      server.closeAllConnections();
    },
  };
}

async function mockAdapter(sameOrigin = false) {
  let serveInspector: (req: IncomingMessage, res: ServerResponse) => void;
  let allowed = true,
    renewals = 0;
  let inspectorOrigin = "";
  const codes = new Map<string, { challenge: string; redirect: string }>();
  const session = () => ({
    accessToken: "synthetic-scoped-access",
    expiresAt: Math.floor(Date.now() / 1000) + 120,
    appId: "app",
    capabilities: ["inspector:read"],
    serverUrl: dashboard.origin,
  });
  const dashboard = await listen(async (req, res) => {
    const url = new URL(req.url!, "http://local");
    if (sameOrigin && ["/", "/browser-session.js", "/inspector/callback"].includes(url.pathname)) {
      serveInspector(req, res);
      return;
    }
    const json = (status: number, body: unknown) => {
      res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(body));
    };
    if (url.pathname === "/login") {
      res.setHeader("Set-Cookie", "dashboard=logged-in; HttpOnly; SameSite=Lax; Path=/");
      res.end("Logged in");
      return;
    }
    if (url.pathname === "/inspector/authorize") {
      if (
        req.headers.cookie !== "dashboard=logged-in" ||
        !allowed ||
        url.searchParams.get("app_id") !== "app" ||
        url.searchParams.get("redirect_uri") !== `${inspectorOrigin}/inspector/callback` ||
        url.searchParams.get("code_challenge_method") !== "S256"
      ) {
        json(403, { error: "access_denied" });
        return;
      }
      const code = randomBytes(24).toString("base64url");
      const redirect = url.searchParams.get("redirect_uri")!;
      codes.set(code, { challenge: url.searchParams.get("code_challenge")!, redirect });
      res.writeHead(302, {
        Location: `${redirect}?${new URLSearchParams({ code, state: url.searchParams.get("state")! })}`,
      });
      res.end();
      return;
    }
    if (url.pathname === "/apps/app/admin/inspector/sessions") {
      if (req.headers["x-jazz-admin-secret"] !== "synthetic-root") {
        json(401, {});
        return;
      }
      json(200, session());
      return;
    }
    const sameOriginSessionGet =
      url.pathname === "/inspector/session" &&
      req.method === "GET" &&
      !req.headers.origin &&
      req.headers["sec-fetch-site"] === "same-origin";
    if (req.headers.origin !== inspectorOrigin && !sameOriginSessionGet) {
      json(403, { error: "access_denied" });
      return;
    }
    res.setHeader("Access-Control-Allow-Origin", inspectorOrigin);
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader("Vary", "Origin");
    if (req.method === "OPTIONS") {
      res.setHeader("Access-Control-Allow-Methods", "GET, POST");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Inspector-CSRF");
      res.writeHead(204);
      res.end();
      return;
    }
    if (url.pathname === "/inspector/session") {
      json(req.headers.cookie === "dashboard=logged-in" ? 200 : 403, {
        csrfToken: "login-bound-csrf",
      });
      return;
    }
    let text = "";
    for await (const chunk of req) text += chunk;
    const body = JSON.parse(text || "{}");
    if (req.method !== "POST" || req.headers["content-type"] !== "application/json") {
      json(400, { error: "invalid_grant" });
      return;
    }
    if (url.pathname === "/inspector/token") {
      const binding = codes.get(body.code);
      if (
        !binding ||
        binding.redirect !== body.redirect_uri ||
        createHash("sha256")
          .update(body.code_verifier ?? "")
          .digest("base64url") !== binding.challenge ||
        !allowed ||
        req.headers.cookie
      ) {
        json(400, { error: "invalid_grant" });
        return;
      }
      codes.delete(body.code);
      json(200, session());
      return;
    }
    if (url.pathname === "/inspector/renew") {
      renewals++;
      if (
        req.headers.cookie !== "dashboard=logged-in" ||
        req.headers["x-inspector-csrf"] !== "login-bound-csrf" ||
        body.appId !== "app" ||
        !allowed
      ) {
        json(403, { error: "access_denied" });
        return;
      }
      json(200, session());
      return;
    }
    json(404, {});
  });
  const source = await readFile(
    new URL("../../src/session/browser-session.ts", import.meta.url),
    "utf8",
  );
  const javascript = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  }).outputText;
  serveInspector = (req, res) => {
    if (req.url === "/browser-session.js") {
      res.setHeader("Content-Type", "text/javascript");
      res.end(javascript);
      return;
    }
    res.setHeader("Content-Type", "text/html");
    res.end(`<button id="login">Sign in</button><button id="renew">Renew</button><button id="cli">CLI</button><output id="state">ready</output><script type="module">
      import { DashboardInspectorSession, completeInspectorCallback, receiveCliSession } from '/browser-session.js';
      const launch = new URLSearchParams(location.hash.slice(1)); history.replaceState(null, "", location.pathname + location.search);
      if (!completeInspectorCallback()) {
        const auth = new DashboardInspectorSession(${JSON.stringify(dashboard.origin)}, 'app', location.origin + '/inspector/callback');
        const run = async task => { try { window.session = await task(); document.querySelector('#state').textContent = 'connected'; } catch { window.session = null; document.querySelector('#state').textContent = 'denied'; } };
        document.querySelector('#login').onclick = () => run(() => auth.authorize());
        document.querySelector('#renew').onclick = () => run(() => auth.renew());
        document.querySelector('#cli').onclick = () => run(() => receiveCliSession(launch.get('handoff'), 'app', launch.get('launch')));
      }
    </script>`);
  };
  const inspector = sameOrigin
    ? { origin: dashboard.origin, close() {} }
    : await listen(serveInspector);
  inspectorOrigin = inspector.origin;
  return {
    dashboard,
    inspector,
    revoke: () => {
      allowed = false;
    },
    renewals: () => renewals,
    close: () => {
      dashboard.close();
      inspector.close();
    },
  };
}

for (const sameOrigin of [false, true])
  test(`real browser ${sameOrigin ? "same-origin" : "same-site cross-origin"} popup PKCE, cookie CSRF renewal and permission removal`, async ({
    page,
    context,
  }) => {
    const adapter = await mockAdapter(sameOrigin);
    try {
      await page.goto(`${adapter.dashboard.origin}/login`);
      await page.goto(adapter.inspector.origin);
      let popups = 0;
      page.on("popup", () => {
        popups++;
      });
      await page.getByText("Sign in", { exact: true }).click();
      await expect(page.locator("output")).toHaveText("connected");
      expect(popups).toBe(1);
      await page.getByText("Renew", { exact: true }).click();
      await expect.poll(adapter.renewals).toBe(1);
      await expect(page.locator("output")).toHaveText("connected");
      expect(popups).toBe(1);
      adapter.revoke();
      await page.getByText("Renew", { exact: true }).click();
      await expect(page.locator("output")).toHaveText("denied");
      expect(
        await page.evaluate(() => (window as unknown as { session: unknown }).session),
      ).toBeNull();
      expect(popups).toBe(1);
      const cookies = await context.cookies();
      expect(cookies[0]?.httpOnly).toBe(true);
      expect(page.url()).not.toContain("synthetic-scoped-access");
    } finally {
      adapter.close();
    }
  });

test("real browser redeems CLI loopback proof without a credential URL", async ({ page }) => {
  const adapter = await mockAdapter();
  const handoff = await startInspectorHandoff({
    appId: "app",
    serverUrl: adapter.dashboard.origin,
    inspectorUrl: adapter.inspector.origin,
    adminSecret: "synthetic-root",
  });
  try {
    expect(handoff.url).not.toMatch(/synthetic-root|synthetic-scoped-access/);
    await page.goto(handoff.url);
    await page.getByText("CLI", { exact: true }).click();
    await expect(page.locator("output")).toHaveText("connected");
    await handoff.done;
    expect(
      await page.evaluate(
        () => (window as unknown as { session: { appId: string } }).session.appId,
      ),
    ).toBe("app");
  } finally {
    handoff.close();
    adapter.close();
  }
});
