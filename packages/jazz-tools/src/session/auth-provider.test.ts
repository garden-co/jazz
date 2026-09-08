import { describe, expect, it } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "./state.js";
import { connectAuthProvider } from "./auth-provider.js";
import { connectBetterAuth } from "./better-auth.js";
import type { JWTAuth } from "../accounts/enrollment.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
const tick = async () => {
  for (let i = 0; i < 60; i++) await Promise.resolve();
};
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
async function setup() {
  const events: string[] = [];
  let shutdownFailure = false;
  let admissionFailure = false;
  const handles = new Map<string, AccountHandle>();
  const account = (id: string) => {
    let result = handles.get(id);
    if (!result) {
      result = { id, identity: { issuer: "provider", subject: id } } as AccountHandle;
      handles.set(id, result);
    }
    return result;
  };
  const token = async (auth: JWTAuth) => (typeof auth === "string" ? auth : auth.getToken());
  const enroll = async (auth: JWTAuth) => {
    const id = await token(auth);
    events.push(`admit:${id}`);
    if (admissionFailure) throw new Error("registry offline");
    return account(id);
  };
  const accounts = new AccountManager({
    createLocalFirst: () => account("local"),
    restoreLocalFirst: () => account("local"),
    logout() {
      events.push("logout");
    },
    registerJWT: enroll,
    loginJWT: enroll,
    loginOrRegisterJWT: enroll,
    async linkJWT(_account: AccountHandle, auth: JWTAuth) {
      events.push("link");
      await token(auth);
      return account("local");
    },
  });
  const session = await createJazzSessionOwner({
    accounts,
    async openClient(selected) {
      events.push(`open:${selected.id}`);
      return {
        async shutdown() {
          events.push(`flush:${selected.id}`);
          if (shutdownFailure) throw new GracefulShutdownSyncError(new Error("offline"));
        },
      };
    },
  });
  return {
    session,
    events,
    failShutdown(value: boolean) {
      shutdownFailure = value;
    },
    failAdmission(value: boolean) {
      admissionFailure = value;
    },
  };
}

describe("provider connection with real Jazz session lifecycle", () => {
  it("waits for hydration and coalesces repeated identity and refresh events", async () => {
    const { session, events } = await setup();
    const auth = connectAuthProvider(session, { getToken: async () => "one" });
    auth.update({ key: null, isPending: true });
    await tick();
    expect(events).toEqual([]);
    auth.update({ key: "issuer:one" });
    await tick();
    auth.update({ key: "issuer:one" });
    auth.update({ key: "issuer:one" });
    await tick();
    expect(events).toEqual(["admit:one", "open:one"]);
    expect(auth.getSnapshot()).toMatchObject({ ready: true, isPending: false });
    auth.dispose();
    await session.close();
  });
  it("does not admit a JWT fetched for a superseded provider identity", async () => {
    const { session, events } = await setup();
    const jwt = deferred<string>();
    const auth = connectAuthProvider(session, { getToken: () => jwt.promise });
    auth.update({ key: "one" });
    await tick();
    auth.update({ key: null });
    jwt.resolve("one");
    await tick();
    expect(events).not.toContain("admit:one");
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(auth.getSnapshot().ready).toBe(true);
    auth.dispose();
    await session.close();
  });
  it("serializes explicit signout with admission and flushes before provider revocation", async () => {
    const { session, events } = await setup();
    const jwt = deferred<string>();
    const auth = connectAuthProvider(session, { getToken: () => jwt.promise });
    auth.update({ key: "one" });
    await tick();
    const leaving = auth.logout(async () => {
      events.push("revoke");
      auth.update({ key: null });
    });
    jwt.resolve("one");
    await leaving;
    expect(events).toEqual(["admit:one", "open:one", "flush:one", "logout", "revoke"]);
    expect(auth.getSnapshot().ready).toBe(true);
    auth.dispose();
    await session.close();
  });
  it("failed flush retains provider credentials and retries logout without reenrollment", async () => {
    const { session, events, failShutdown } = await setup();
    const auth = connectAuthProvider(session, { getToken: async () => "one" });
    auth.update({ key: "one" });
    await tick();
    failShutdown(true);
    await expect(
      auth.logout(async () => {
        events.push("revoke");
        auth.update({ key: null });
      }),
    ).rejects.toThrow();
    expect(events).not.toContain("revoke");
    auth.update({ key: "one" });
    await tick();
    expect(auth.getSnapshot().error).toBeDefined();
    failShutdown(false);
    await auth.retry();
    expect(events.filter((e) => e.startsWith("admit:"))).toEqual(["admit:one"]);
    expect(events.at(-1)).toBe("revoke");
    auth.dispose();
    await session.close();
  });
  it("exposes enrollment failures and retries the same identity", async () => {
    const { session, failAdmission } = await setup();
    const auth = connectAuthProvider(session, { getToken: async () => "one" });
    failAdmission(true);
    auth.update({ key: "one" });
    await tick();
    expect(auth.getSnapshot()).toMatchObject({
      ready: false,
      isPending: false,
      error: expect.any(Error),
    });
    failAdmission(false);
    await auth.retry();
    expect(auth.getSnapshot()).toMatchObject({ ready: true, error: undefined });
    auth.dispose();
    await session.close();
  });
  it("does not reenroll stale provider state after successful signout", async () => {
    const { session, events } = await setup();
    const auth = connectAuthProvider(session, { getToken: async () => "one" });
    auth.update({ key: "one" });
    await tick();
    await auth.logout(async () => {});
    auth.update({ key: "one" });
    await tick();
    expect(events.filter((e) => e.startsWith("admit:"))).toEqual(["admit:one"]);
    expect(auth.getSnapshot().ready).toBe(false);
    auth.update({ key: null });
    await tick();
    expect(auth.getSnapshot().ready).toBe(true);
    auth.dispose();
    await session.close();
  });
  it("disposal releases connection ownership, not the session, and ignores queued work", async () => {
    const { session, events } = await setup();
    const auth = connectAuthProvider(session, { getToken: async () => "one" });
    expect(() => connectAuthProvider(session, { getToken: async () => "two" })).toThrow("already");
    auth.update({ key: "one" });
    auth.dispose();
    await tick();
    expect(events).toEqual([]);
    const next = connectAuthProvider(session, { getToken: async () => "two" });
    next.update({ key: "two" });
    await tick();
    expect(session.getSnapshot().account?.id).toBe("two");
    next.dispose();
    expect(session.getSnapshot().status).toBe("ready");
    await session.close();
  });
  it("manual hybrid linking can precede any automatic enrollment", async () => {
    const { session, events } = await setup();
    await session.createLocalFirst();
    await session.linkJWT("new-provider");
    expect(events).toContain("link");
    expect(events.some((event) => event.startsWith("admit:"))).toBe(false);
    expect(session.getSnapshot().account?.id).toBe("local");
    await session.close();
  });
});
