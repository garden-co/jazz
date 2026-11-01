import { describe, expect, it } from "vitest";
import vectors from "../test-vectors.json" with { type: "json" };
import { verifyRequest } from "./server.js";

const { now, audience, cases } = vectors;
const request = (authorization?: string) =>
  new Request("https://app.example/jazz", authorization ? { headers: { authorization } } : {});

describe("verifyRequest", () => {
  it("returns the identity for a local-first bearer", () => {
    expect(verifyRequest(request(`Bearer ${cases[0]!.token}`), { audience, now })?.subject).toBe(
      cases[0]!.subject,
    );
  });
  it("returns null without a local-first bearer", () => {
    expect(verifyRequest(request(), { audience, now })).toBeNull();
    expect(verifyRequest(request("Bearer some.other.jwt"), { audience, now })).toBeNull();
    expect(verifyRequest(request("Basic abc"), { audience, now })).toBeNull();
  });
  it("throws for an invalid local-first bearer", () => {
    expect(() =>
      verifyRequest(request(`Bearer ${cases[0]!.token}`), {
        audience: "x",
        now,
      }),
    ).toThrow(/audience/);
  });
});
