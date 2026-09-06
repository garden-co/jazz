import { AccountAuthError } from "jazz-tools";
import { describe, expect, it, vi } from "vitest";
import { bootstrapPersonalCanvas, loginOrRegister } from "../../src/lib/account-enrollment";

describe("PosterShop account bootstrap", () => {
  it("enrolls only after the registry says the identity is unassigned", async () => {
    const account = { id: crypto.randomUUID(), identity: { issuer: "issuer", subject: "user" } };
    const accounts = {
      loginJWT: vi.fn().mockRejectedValue(new AccountAuthError("identity_not_assigned")),
      registerJWT: vi.fn().mockResolvedValue(account),
    };
    await expect(loginOrRegister(accounts as never, { getToken: async () => "jwt" })).resolves.toBe(
      account,
    );
    expect(accounts.registerJWT).toHaveBeenCalledOnce();
  });

  it("does not turn a different registry failure into an enrollment", async () => {
    const accounts = {
      loginJWT: vi.fn().mockRejectedValue(new AccountAuthError("identity_not_authorized")),
      registerJWT: vi.fn(),
    };
    await expect(
      loginOrRegister(accounts as never, { getToken: async () => "jwt" }),
    ).rejects.toMatchObject({
      code: "identity_not_authorized",
    });
    expect(accounts.registerJWT).not.toHaveBeenCalled();
  });

  it("passes the admitted bearer to bootstrap", async () => {
    const request = vi.fn().mockResolvedValue(new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", request);
    await bootstrapPersonalCanvas("admitted-jwt");
    expect(request).toHaveBeenCalledWith(
      "/api/bootstrap",
      expect.objectContaining({ headers: { authorization: "Bearer admitted-jwt" } }),
    );
    vi.unstubAllGlobals();
  });
});
