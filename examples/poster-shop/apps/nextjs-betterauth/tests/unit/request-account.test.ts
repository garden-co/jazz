import type { Jazz } from "jazz-tools/backend";
import { expect, it, vi } from "vitest";
import { verifiedRequest } from "../../src/lib/request-account.js";

const request = () => new Request("https://posters.test/api/bootstrap", { method: "POST" });

it("returns the caller's account, claims and attributed backend db", async () => {
  const db = { getAuthState: () => ({ session: { claims: { name: "Ada" } } }) };
  const jazz = {
    accountFor: vi.fn().mockResolvedValue("account-1"),
    withAttribution: vi.fn().mockResolvedValue(db),
  } as unknown as Jazz;
  expect(await verifiedRequest(jazz, request())).toEqual({
    accountId: "account-1",
    claims: { name: "Ada" },
    db,
  });
});

it("returns null for a signed-out request, without opening a db", async () => {
  const withAttribution = vi.fn();
  const jazz = { accountFor: vi.fn().mockResolvedValue(null), withAttribution } as unknown as Jazz;
  expect(await verifiedRequest(jazz, request())).toBeNull();
  expect(withAttribution).not.toHaveBeenCalled();
});

it("rethrows backend failures instead of reporting them as signed out", async () => {
  const error = new Error("sync server unreachable");
  const jazz = { accountFor: vi.fn().mockRejectedValue(error) } as unknown as Jazz;
  await expect(verifiedRequest(jazz, request())).rejects.toBe(error);
});
