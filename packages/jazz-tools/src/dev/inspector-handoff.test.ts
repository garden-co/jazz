import { createServer, request } from "node:http";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { startInspectorHandoff } from "./inspector-handoff.js";

const cleanup: (() => void)[] = [];
afterEach(() => {
  cleanup.splice(0).forEach((close) => close());
});

async function setup(timeoutMs?: number) {
  const requests: { path?: string; root?: string; body: unknown }[] = [];
  const server = createServer(async (req, res) => {
    let text = "";
    for await (const chunk of req) text += chunk;
    requests.push({
      path: req.url,
      root: req.headers["x-jazz-admin-secret"] as string,
      body: JSON.parse(text),
    });
    res.setHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({
        appId: "test-app",
        accessToken: "short-lived-access",
        expiresAt: Math.floor(Date.now() / 1000) + 900,
        capabilities: ["inspector:read"],
        adminSecret: "must-not-forward",
      }),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanup.push(() => {
    server.close();
    server.closeAllConnections();
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("listen failed");
  const handoff = await startInspectorHandoff({
    appId: "test-app",
    serverUrl: `http://127.0.0.1:${address.port}`,
    adminSecret: "configured-root",
    inspectorUrl: "https://inspector.example",
    timeoutMs,
  });
  cleanup.push(handoff.close);
  const url = new URL(handoff.url);
  const endpoint = new URLSearchParams(url.hash.slice(1)).get("handoff")!;
  const launchCode = new URLSearchParams(url.hash.slice(1)).get("launch")!;
  const post = (path: string, body: unknown, origin = url.origin) =>
    fetch(`${endpoint}${path}`, {
      method: "POST",
      headers: { Origin: origin, "Content-Type": "application/json" },
      body: JSON.stringify({ launch_code: launchCode, ...(body as object) }),
    });
  return { handoff, requests, post, endpoint };
}

describe("CLI Inspector handoff", () => {
  it("exchanges root once, rejects foreign origins and wrong proofs, then consumes exactly once", async () => {
    const { handoff, requests, post } = await setup();
    expect(requests).toEqual([
      {
        path: "/apps/test-app/admin/inspector/sessions",
        root: "configured-root",
        body: { operator: "local-cli", capabilities: ["inspector:read"] },
      },
    ]);
    expect(handoff.url).not.toMatch(/configured-root|short-lived-access|must-not-forward/);
    const verifier = "a".repeat(43);
    const code_challenge = createHash("sha256").update(verifier).digest("base64url");
    expect((await post("/challenge", { code_challenge }, "https://other.example")).status).toBe(
      403,
    );
    expect((await post("/challenge", { code_challenge, launch_code: undefined })).status).toBe(403);
    expect((await post("/challenge", { code_challenge, launch_code: "x".repeat(43) })).status).toBe(
      403,
    );
    const challenge = await post("/challenge", { code_challenge });
    expect(challenge.headers.get("cache-control")).toBe("no-store");
    const { code } = await challenge.json();
    expect((await post("/challenge", { code_challenge })).status).toBe(403);
    expect((await post("/token", { code, code_verifier: "b".repeat(43) })).status).toBe(403);
    const token = await post("/token", { code, code_verifier: verifier });
    expect(token.status).toBe(200);
    const session = await token.json();
    expect(session.accessToken).toBe("short-lived-access");
    expect(session).not.toHaveProperty("adminSecret");
    await handoff.done;
    await expect(post("/token", { code, code_verifier: verifier })).rejects.toThrow();
  });
  it("rejects DNS-rebinding Host and non-JSON requests without consuming the challenge", async () => {
    const { endpoint, post } = await setup();
    const response = await fetch(`${endpoint}/challenge`, {
      method: "POST",
      headers: {
        Origin: "https://inspector.example",
        Host: "foreign.example",
        "Content-Type": "application/json",
      },
      body: "{}",
    });
    expect(response.status).toBe(403);
    expect(
      (
        await fetch(`${endpoint}/challenge`, {
          method: "POST",
          headers: { Origin: "https://inspector.example" },
          body: "{}",
        })
      ).status,
    ).toBe(403);
    expect((await post("/challenge", { code_challenge: "a".repeat(43) })).status).toBe(200);
  });
  it("closes an in-flight body at timeout and cannot reopen a challenge", async () => {
    const { handoff, endpoint, post } = await setup(30);
    const pending = request(`${endpoint}/challenge`, {
      method: "POST",
      headers: { Origin: "https://inspector.example", "Content-Type": "application/json" },
    });
    const closed = new Promise<void>((resolve) => {
      pending.on("error", () => resolve());
      pending.on("close", () => resolve());
    });
    pending.write('{"code_challenge":');
    await handoff.done;
    await closed;
    pending.destroy();
    await expect(post("/challenge", { code_challenge: "a".repeat(43) })).rejects.toThrow();
  });
});
