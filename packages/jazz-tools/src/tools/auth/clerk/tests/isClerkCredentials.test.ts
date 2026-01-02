import { describe, expect, it } from "vitest";
import { isClerkCredentials } from "../types";

describe("isClerkCredentials", () => {
  it.each([
    {
      metadata: {
        jazzAccountID: "123",
        jazzAccountSecret: "456",
        jazzAccountSeed: [1, 2, 3],
      },
      description: "full credentials",
    },
    {
      metadata: {
        jazzAccountID: "123",
        jazzAccountSecret: "456",
      },
      description: "missing jazzAccountSeed",
    },
  ])("succeeds for valid credentials: $description", ({ metadata }) => {
    expect(isClerkCredentials(metadata)).toBe(true);
  });

  it.each([
    {
      metadata: {},
      description: "empty object",
    },
    {
      metadata: undefined,
      description: "undefined",
    },
    {
      metadata: {
        jazzAccountSecret: "456",
      },
      description: "missing jazzAccountID",
    },
    {
      metadata: {
        jazzAccountID: "123",
      },
      description: "missing jazzAccountSecret",
    },
  ])("fails for invalid credentials: $description", ({ metadata }) => {
    expect(isClerkCredentials(metadata)).toBe(false);
  });
});
