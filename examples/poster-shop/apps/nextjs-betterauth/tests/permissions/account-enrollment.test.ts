import { afterEach, describe, expect, it, vi } from "vitest";
import {
  bootstrapPersonalCanvas,
  inviteLinkFor,
  joinCanvasWithInvite,
  parseInviteFragment,
  prepareStudio,
} from "../../src/lib/account-enrollment";

afterEach(() => vi.unstubAllGlobals());

describe("PosterShop studio preparation", () => {
  it("passes the admitted bearer to bootstrap", async () => {
    const request = vi.fn().mockResolvedValue(new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", request);
    await bootstrapPersonalCanvas("admitted-jwt");
    expect(request).toHaveBeenCalledWith(
      "/api/bootstrap",
      expect.objectContaining({ headers: { authorization: "Bearer admitted-jwt" } }),
    );
  });

  it("keeps invite tokens in the fragment and the request body only", async () => {
    const invite = { canvasId: crypto.randomUUID(), token: crypto.randomUUID() };
    const link = new URL(inviteLinkFor("https://posters.test", invite));
    expect(link.pathname).toBe("/dashboard");
    expect(link.search).toBe("");
    expect(parseInviteFragment(link.hash)).toEqual(invite);
    expect(parseInviteFragment("#invite/not-a-uuid/also-not")).toBeNull();

    const request = vi.fn().mockResolvedValue(Response.json({ ok: true }));
    vi.stubGlobal("fetch", request);
    await joinCanvasWithInvite("jwt", invite);
    const [url, init] = request.mock.calls[0]!;
    expect(url).toBe("/api/join");
    expect(JSON.parse(init.body)).toEqual(invite);
  });

  it("shares one in-flight preparation between StrictMode mounts", async () => {
    const request = vi.fn().mockResolvedValue(new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", request);
    const getToken = vi.fn().mockResolvedValue("jwt");
    const [first, second] = await Promise.all([
      prepareStudio("user-1", getToken, null),
      prepareStudio("user-1", getToken, null),
    ]);
    expect(first).toEqual({ ok: true, joinedCanvasId: null, inviteRejected: false });
    expect(second).toBe(first);
    expect(request).toHaveBeenCalledOnce();
  });

  it("turns an unknown, used or revoked invite into the user's own studio", async () => {
    const invite = { canvasId: crypto.randomUUID(), token: crypto.randomUUID() };
    const request = vi.fn(async (url: string) =>
      url === "/api/join"
        ? Response.json({ error: "invite not found" }, { status: 404 })
        : Response.json({ ok: true }),
    );
    vi.stubGlobal("fetch", request);
    const result = await prepareStudio("user-2", async () => "jwt", invite);
    expect(result).toEqual({ ok: true, joinedCanvasId: null, inviteRejected: true });
  });

  it("keeps a transient join failure retryable", async () => {
    const invite = { canvasId: crypto.randomUUID(), token: crypto.randomUUID() };
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        url === "/api/join"
          ? Response.json({ error: "busy, retry" }, { status: 503 })
          : Response.json({ ok: true }),
      ),
    );
    expect(await prepareStudio("user-3", async () => "jwt", invite)).toEqual({ ok: false });
  });
});
