import { expect, it } from "vitest";
import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import { app } from "../../schema";
import permissions from "../../permissions";

it("signs up, signs in, signs a session JWT and logs out through the application auth handler", async () => {
  const server = await startLocalJazzServer();
  const origin = "http://localhost:3000";
  const values = {
    NEXT_PUBLIC_JAZZ_APP_ID: server.appId,
    NEXT_PUBLIC_JAZZ_SERVER_URL: server.url,
    BACKEND_SECRET: server.backendSecret,
    NEXT_PUBLIC_APP_ORIGIN: origin,
  };
  const previous = Object.fromEntries(Object.keys(values).map((key) => [key, process.env[key]]));
  Object.assign(process.env, values);
  try {
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const { auth: pending } = await import("../../src/lib/auth");
    const auth = await pending;
    const call = (path: string, body: unknown, cookie?: string) =>
      auth.handler(
        new Request(`${origin}/api/auth/${path}`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Origin: origin,
            ...(cookie ? { Cookie: cookie } : {}),
          },
          body: JSON.stringify(body),
        }),
      );
    const email = `acceptance-${crypto.randomUUID()}@example.test`;
    const signup = await call("sign-up/email", {
      name: "Acceptance",
      email,
      password: "acceptance-password",
    });
    expect(signup.status).toBe(200);
    const signed = await signup.json();
    expect(signed.user.email).toBe(email);
    const signin = await call("sign-in/email", { email, password: "acceptance-password" });
    expect(signin.status).toBe(200);
    const logged = await signin.json();
    expect(logged.user.id).toBe(signed.user.id);
    const cookie = signin.headers
      .getSetCookie()
      .map((value) => value.split(";")[0])
      .join("; ");
    expect(cookie).not.toBe("");
    const current = await auth.handler(
      new Request(`${origin}/api/auth/get-session`, { headers: { Cookie: cookie } }),
    );
    expect(current.status).toBe(200);
    const token = current.headers.get("set-auth-jwt");
    expect(token?.split(".")).toHaveLength(3);
    const payload = JSON.parse(Buffer.from(token!.split(".")[1]!, "base64url").toString("utf8"));
    expect(payload).toMatchObject({ iss: origin, sub: signed.user.id });
    expect((await current.json()).user.id).toBe(signed.user.id);
    // get-session signs a JWT and persists the installed plugin's alg/crv metadata.
    const { authJazzClient } = await import("../../src/lib/auth-jazz-client");
    const keys = await (await authJazzClient()).db.all(app.better_auth_jwks);
    expect(keys).toEqual(
      expect.arrayContaining([expect.objectContaining({ alg: "ES256", crv: "P-256" })]),
    );
    const signout = await call("sign-out", {}, cookie);
    expect(signout.status).toBe(200);
    const ended = await auth.handler(
      new Request(`${origin}/api/auth/get-session`, { headers: { Cookie: cookie } }),
    );
    expect(await ended.json()).toBeNull();
  } finally {
    try {
      await globalThis.__authBetterAuthChatJazzSession?.then((session) => session.close());
    } finally {
      globalThis.__authBetterAuthChatJazzSession = undefined;
      try {
        await server.stop();
      } finally {
        for (const [key, value] of Object.entries(previous)) {
          if (value === undefined) delete process.env[key];
          else process.env[key] = value;
        }
      }
    }
  }
});
