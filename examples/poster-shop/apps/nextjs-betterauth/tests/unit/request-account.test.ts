import { AccountAuthError } from "jazz-tools";
import type { JazzClient } from "jazz-tools/backend";
import { expect, it, vi } from "vitest";
import { verifiedRequest } from "../../src/lib/request-account.js";

const bearer = (token = "jwt") =>
  new Request("https://posters.test/api/bootstrap", {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
  });

function clientRejecting(error: unknown) {
  const withAttributionForRequest = vi.fn().mockRejectedValue(error);
  return {
    client: { withAttributionForRequest } as unknown as JazzClient,
    withAttributionForRequest,
  };
}

it("verifies the bearer once and returns the attributed backend db", async () => {
  const db = {
    getAuthState: () => ({ session: { user: { account: "account-1" }, claims: { name: "Ada" } } }),
  };
  const withAttributionForRequest = vi.fn().mockResolvedValue(db);
  const forRequest = vi.fn();
  const client = { withAttributionForRequest, forRequest } as unknown as JazzClient;
  const result = await verifiedRequest(client, bearer());
  expect(result).toEqual({ accountId: "account-1", claims: { name: "Ada" }, db });
  expect(withAttributionForRequest).toHaveBeenCalledOnce();
  expect(forRequest).not.toHaveBeenCalled();
});

it("returns null without a bearer, without asking the backend", async () => {
  const { client, withAttributionForRequest } = clientRejecting(new Error("unused"));
  const request = new Request("https://posters.test/api/bootstrap", { method: "POST" });
  expect(await verifiedRequest(client, request)).toBeNull();
  expect(withAttributionForRequest).not.toHaveBeenCalled();
});

it("returns null for rejected JWTs and unadmitted identities", async () => {
  for (const error of [
    new Error("Invalid JWT: JWT signature verification failed"),
    new Error("JWT has expired"),
    new Error("No matching JWK found"),
    new AccountAuthError("identity_not_assigned"),
  ]) {
    expect(await verifiedRequest(clientRejecting(error).client, bearer())).toBeNull();
  }
});

it("rethrows infrastructure failures instead of reporting them as sign-in problems", async () => {
  for (const error of [
    new Error("Unable to fetch JWKS: HTTP 503"),
    new Error("Invalid JWT public key"),
    new AccountAuthError("account_request_failed"),
  ]) {
    await expect(verifiedRequest(clientRejecting(error).client, bearer())).rejects.toBe(error);
  }
});
