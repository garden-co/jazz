import { afterEach, describe, expect, it, vi } from "vitest";
import { createAccountManagerWithRuntime } from "./enrollment.js";
import { requestAccountRegistry } from "./registry-client.js";
import { createJazzSessionOwner } from "../session/state.js";

const identity = { issuer: "https://issuer.example", subject: "test-user" };
const assignment = { account: "00000000-0000-4000-8000-000000000001", identity };
const token = `e30.${btoa(JSON.stringify({ iss: identity.issuer, sub: identity.subject }))}.signature`;
const registry = "https://core.example/apps/test/accounts";
// A single broad signature remains assignable to the DOM, Node and RN fetch
// overloads; Vitest Mock<typeof fetch> retains only the last overload.
type TestFetch = (input: unknown, init?: unknown) => Promise<Response>;
const never = () => new Promise<never>(() => {});

afterEach(() => vi.useRealTimers());

describe("account HTTP deadlines", () => {
  it.each(["headers", "json", "error text"])(
    "bounds stalled %s and permits immediate retry",
    async (phase) => {
      vi.useFakeTimers();
      const fetcher = vi
        .fn<TestFetch>()
        .mockImplementationOnce(async () => {
          if (phase === "headers") return never();
          return { ok: phase === "json", json: never, text: never } as unknown as Response;
        })
        .mockResolvedValueOnce(new Response(JSON.stringify(assignment)));
      const failed = expect(
        requestAccountRegistry(registry, "login", token, undefined, fetcher),
      ).rejects.toMatchObject({ code: "account_request_timeout" });
      await vi.advanceTimersByTimeAsync(30_000);
      await failed;
      expect(fetcher.mock.calls[0]![1]).toMatchObject({ signal: { aborted: true } });
      await expect(
        requestAccountRegistry(registry, "login", token, undefined, fetcher),
      ).resolves.toEqual(assignment);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("preserves an independent fetch cancellation", async () => {
    vi.useFakeTimers();
    const cancelled = new DOMException("Caller cancelled", "AbortError");
    const fetcher = vi.fn<TestFetch>().mockRejectedValue(cancelled);
    await expect(requestAccountRegistry(registry, "login", token, undefined, fetcher)).rejects.toBe(
      cancelled,
    );
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["logout", "close"] as const)(
    "allows session %s to finish after stalled enrollment",
    async (action) => {
      vi.useFakeTimers();
      const fetcher = vi
        .fn<TestFetch>()
        .mockImplementationOnce(never)
        .mockResolvedValueOnce(new Response(JSON.stringify(assignment)));
      const accounts = createAccountManagerWithRuntime({
        registry,
        fetch: fetcher,
        localFirst: {
          create: () => {
            throw new Error("unused");
          },
        },
      });
      const openClient = vi.fn(async () => ({ shutdown: async () => {} }));
      const session = await createJazzSessionOwner({ accounts, openClient });
      const admission = session.loginJWT(token).catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(0);
      expect(fetcher).toHaveBeenCalledTimes(1);
      let finished = false;
      const teardown = session[action]().then(() => {
        finished = true;
      });
      await vi.advanceTimersByTimeAsync(29_999);
      expect(finished).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      await teardown;
      expect(await admission).toBeInstanceOf(Error);
      expect(openClient).not.toHaveBeenCalled();
      expect(accounts.getLoggedIn()).toBeUndefined();
      if (action === "logout") {
        await session.loginJWT(token);
        expect(openClient).toHaveBeenCalledTimes(1);
        await session.close();
      }
    },
  );
});
