import { createHash, randomBytes } from "node:crypto";
import { createServer } from "node:http";
import { ADMIN_SECRET, APP_ID } from "./test-constants.js";

/** Synthetic dashboard adapter exchanging against the real lane-owned Jazz server. */
export async function startSessionTenantManager(serverUrl: string) {
  let inspectorOrigin = "";
  let allowed = true;
  let expiresIn = 900;
  let holdRenewal = false;
  const codes = new Map<
    string,
    { challenge: string; redirect: string; capabilities: string[]; expiresAt: number }
  >();
  const exchange = async (capabilities: string[]) => {
    const response = await fetch(`${serverUrl}/apps/${APP_ID}/admin/inspector/sessions`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Jazz-Admin-Secret": ADMIN_SECRET },
      body: JSON.stringify({ operator: "synthetic-browser-operator", capabilities, expiresIn }),
    });
    if (!response.ok) throw new Error("session exchange failed");
    return { ...(await response.json()), serverUrl };
  };
  const server = createServer(async (req, res) => {
    const json = (status: number, value: unknown) => {
      res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(value));
    };
    try {
      const url = new URL(req.url!, "http://fixture");
      if (url.pathname === "/login") {
        res.setHeader("Set-Cookie", "dashboard=synthetic-login; HttpOnly; SameSite=Lax; Path=/");
        res.end("Logged in");
        return;
      }
      if (url.pathname === "/test/control") {
        // Local fixture control; not part of the production adapter contract.
        let text = "";
        for await (const chunk of req) text += chunk;
        const value = JSON.parse(text);
        allowed = value.allowed ?? true;
        expiresIn = value.expiresIn ?? 900;
        holdRenewal = value.holdRenewal ?? false;
        json(200, {});
        return;
      }
      const loggedIn = req.headers.cookie === "dashboard=synthetic-login";
      if (url.pathname === "/inspector/authorize") {
        if (
          !allowed ||
          !loggedIn ||
          url.searchParams.get("app_id") !== APP_ID ||
          url.searchParams.get("redirect_uri") !== `${inspectorOrigin}/inspector/callback` ||
          url.searchParams.get("code_challenge_method") !== "S256"
        ) {
          json(403, { error: "access_denied" });
          return;
        }
        const code = randomBytes(24).toString("base64url");
        codes.set(code, {
          challenge: url.searchParams.get("code_challenge")!,
          redirect: url.searchParams.get("redirect_uri")!,
          capabilities: url.searchParams.get("capabilities")!.split(" "),
          expiresAt: Date.now() + 60_000,
        });
        res.writeHead(302, {
          Location: `${inspectorOrigin}/inspector/callback?${new URLSearchParams({ code, state: url.searchParams.get("state")! })}`,
        });
        res.end();
        return;
      }
      if (req.headers.origin !== inspectorOrigin) {
        json(403, { error: "access_denied" });
        return;
      }
      res.setHeader("Access-Control-Allow-Origin", inspectorOrigin);
      res.setHeader("Access-Control-Allow-Credentials", "true");
      if (req.method === "OPTIONS") {
        res.setHeader("Access-Control-Allow-Methods", "GET,POST");
        res.setHeader("Access-Control-Allow-Headers", "Content-Type,X-Inspector-CSRF");
        res.writeHead(204);
        res.end();
        return;
      }
      if (url.pathname === "/inspector/session") {
        json(loggedIn ? 200 : 403, { csrfToken: "synthetic-login-bound-csrf" });
        return;
      }
      if (req.method !== "POST" || req.headers["content-type"] !== "application/json") {
        json(400, { error: "invalid_grant" });
        return;
      }
      let text = "";
      for await (const chunk of req) text += chunk;
      const body = JSON.parse(text);
      if (url.pathname === "/inspector/token") {
        const binding = codes.get(body.code);
        if (
          !allowed ||
          !binding ||
          binding.expiresAt <= Date.now() ||
          binding.redirect !== body.redirect_uri ||
          createHash("sha256")
            .update(body.code_verifier ?? "")
            .digest("base64url") !== binding.challenge
        ) {
          json(400, { error: "invalid_grant" });
          return;
        }
        codes.delete(body.code);
        json(200, await exchange(binding.capabilities));
        return;
      }
      if (url.pathname === "/inspector/renew") {
        if (
          !allowed ||
          !loggedIn ||
          body.appId !== APP_ID ||
          req.headers["x-inspector-csrf"] !== "synthetic-login-bound-csrf"
        ) {
          json(403, { error: "access_denied" });
          return;
        }
        if (holdRenewal) return;
        json(200, await exchange(body.capabilities));
        return;
      }
      json(404, {});
    } catch {
      json(500, { error: "access_denied" });
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("tenant fixture failed to listen");
  return {
    origin: `http://127.0.0.1:${address.port}`,
    setInspectorOrigin(origin: string) {
      inspectorOrigin = origin;
    },
    close() {
      server.close();
      server.closeAllConnections();
    },
  };
}
