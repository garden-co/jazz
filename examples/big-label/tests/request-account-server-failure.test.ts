import { createServer } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { POST as bootstrap } from "../app/api/bootstrap/route.js";

const b64 = (value: string) => Buffer.from(value).toString("base64url");

/** A signed-looking JWT; verification never gets as far as its signature. */
function wellFormedJwt(issuer: string) {
  const head = b64(JSON.stringify({ alg: "RS256", typ: "JWT", kid: "big-label-key" }));
  const body = b64(JSON.stringify({ iss: issuer, sub: "reader" }));
  return `${head}.${body}.${b64("signature")}`;
}

/** A loopback origin nothing listens on. */
async function closedOrigin() {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("no test port");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return `http://127.0.0.1:${address.port}`;
}

afterEach(() => vi.unstubAllEnvs());

describe("BigLabel API server-side auth failures", () => {
  it("does not answer 401 when the JWKS endpoint is unreachable", async () => {
    const origin = await closedOrigin();
    vi.stubEnv("NEXT_PUBLIC_APP_ORIGIN", origin);
    vi.stubEnv("NEXT_PUBLIC_JAZZ_APP_ID", "big-label-jwks-outage");
    const request = new Request("http://127.0.0.1:3000/api", {
      method: "POST",
      headers: { authorization: `Bearer ${wellFormedJwt(origin)}` },
    });

    // The route throws, which Next answers with a logged 500: a signed-in
    // user is not told their session is invalid because the server is down.
    await expect(bootstrap(request)).rejects.toThrow(/Unable to fetch JWKS/);
  });
});
