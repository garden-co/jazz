import React, { StrictMode } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { createAuthClient } from "better-auth/react";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import type { JWTAuth } from "../accounts/enrollment.js";
import { useBetterAuth } from "./auth-provider.js";
afterEach(cleanup);

it("uses real Better Auth atom hydration under StrictMode and releases only the connector", async () => {
  const admitted: string[] = [];
  let savedCredential: JWTAuth | undefined;
  const handle = {
    id: "account",
    identity: { issuer: "provider", subject: "one" },
  } as AccountHandle;
  const enroll = async (auth: JWTAuth) => {
    savedCredential = auth;
    admitted.push(typeof auth === "string" ? auth : await auth.getToken());
    return handle;
  };
  const accounts = new AccountManager({
    createLocalFirst: () => handle,
    restoreLocalFirst: () => handle,
    loginJWT: enroll,
    loginOrRegisterJWT: enroll,
    registerJWT: enroll,
    linkJWT: async (_old: AccountHandle, auth: JWTAuth) => enroll(auth),
    logout() {},
  });
  const session = await createJazzSessionOwner({
    accounts,
    async openClient() {
      return {
        async shutdown() {
          // A live client's final flush can need a refreshed credential after UI unmount.
          if (savedCredential && typeof savedCredential !== "string")
            await savedCredential.getToken();
        },
      };
    },
  });
  const data = {
    user: {
      id: "one",
      name: "One",
      email: "one@example.com",
      emailVerified: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    },
    session: {
      id: "session",
      userId: "one",
      token: "cookie",
      expiresAt: new Date(Date.now() + 600000),
      createdAt: new Date(),
      updatedAt: new Date(),
    },
  };
  const auth = createAuthClient({
    baseURL: "http://localhost:3000",
    fetchOptions: {
      customFetchImpl: async (request) =>
        new Response(
          JSON.stringify(String(request).endsWith("/token") ? { token: "jwt-one" } : data),
          { headers: { "Content-Type": "application/json" } },
        ),
    },
  });
  auth.hydrateSession(data);
  function View() {
    const state = useBetterAuth(session, auth);
    return <p>{state.error?.message ?? (state.ready ? "ready" : "pending")}</p>;
  }
  const view = render(
    <StrictMode>
      <View />
    </StrictMode>,
  );
  await waitFor(() => expect(view.getByText("ready")).toBeDefined());
  expect(admitted).toEqual(["jwt-one"]);
  view.unmount();
  expect(session.getSnapshot().status).toBe("ready");
  // A successor connection must be able to flush the predecessor's client.
  const next = render(<View />);
  await waitFor(() => expect(next.getByText("ready")).toBeDefined());
  next.unmount();
  await session.close();
  expect(session.getSnapshot().status).toBe("closed");
});
