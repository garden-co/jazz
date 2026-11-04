import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";

it("signs up, signs in to Jazz through the backend with the Better Auth cookie, and logs out", async () => {
  const origin = "http://localhost:3000";
  const appId = randomUUID();
  const values = { NEXT_PUBLIC_JAZZ_APP_ID: appId, NEXT_PUBLIC_APP_ORIGIN: origin };
  const previous = Object.fromEntries(Object.keys(values).map((key) => [key, process.env[key]]));
  Object.assign(process.env, values);
  try {
    const { auth: pending } = await import("../../src/lib/auth");
    const { jazz } = await import("../../src/lib/jazz.server");
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
    const jazzSignIn = async (cookie?: string) =>
      (await jazz()).fetch(
        new Request(`${origin}/jazz/apps/${appId}/accounts/backend-sign-in`, {
          method: "POST",
          headers: cookie ? { Cookie: cookie } : {},
        }),
      );
    const email = `acceptance-${randomUUID()}@example.test`;
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
    const cookie = signin.headers
      .getSetCookie()
      .map((value) => value.split(";")[0])
      .join("; ");
    expect(cookie).not.toBe("");

    // Jazz asks Better Auth who the cookie belongs to; the role becomes a claim.
    const admitted = await jazzSignIn(cookie);
    expect(admitted.status).toBe(200);
    expect(await admitted.json()).toMatchObject({
      identity: { issuer: `jazz:app:${appId}`, subject: signed.user.id },
      claims: { role: "member" },
    });

    const signout = await call("sign-out", {}, cookie);
    expect(signout.status).toBe(200);
    expect((await jazzSignIn(cookie)).status).toBe(401);
  } finally {
    try {
      await globalThis.__authBetterAuthChatJazz?.then((jazz) => jazz.close());
    } finally {
      globalThis.__authBetterAuthChatJazz = undefined;
      for (const [key, value] of Object.entries(previous)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }
});
