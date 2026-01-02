import { describe, expect, it } from "vitest";
import { isClerkAuthStateEqual } from "../types";

describe("isClerkAuthStateEqual", () => {
  const validCredentials = {
    jazzAccountID: "account-123",
    jazzAccountSecret: "secret-123",
    jazzAccountSeed: [1, 2, 3],
  };

  const differentCredentials = {
    jazzAccountID: "account-456",
    jazzAccountSecret: "secret-456",
    jazzAccountSeed: [4, 5, 6],
  };

  describe("both users null/undefined", () => {
    it.each([
      { previous: null, next: null, description: "both null" },
      { previous: undefined, next: undefined, description: "both undefined" },
      { previous: null, next: undefined, description: "null and undefined" },
      { previous: undefined, next: null, description: "undefined and null" },
    ])("returns true when $description", ({ previous, next }) => {
      expect(isClerkAuthStateEqual(previous, next)).toBe(true);
    });
  });

  describe("one user null, other exists", () => {
    it.each([
      {
        previous: null,
        next: { unsafeMetadata: validCredentials },
        description: "previous null, next exists",
      },
      {
        previous: { unsafeMetadata: validCredentials },
        next: null,
        description: "previous exists, next null",
      },
      {
        previous: undefined,
        next: { unsafeMetadata: validCredentials },
        description: "previous undefined, next exists",
      },
      {
        previous: { unsafeMetadata: validCredentials },
        next: undefined,
        description: "previous exists, next undefined",
      },
    ])("returns false when $description", ({ previous, next }) => {
      expect(isClerkAuthStateEqual(previous, next)).toBe(false);
    });
  });

  describe("same jazzAccountID", () => {
    it("returns true when both users have the same jazzAccountID", () => {
      const previous = { unsafeMetadata: validCredentials };
      const next = {
        unsafeMetadata: {
          ...validCredentials,
          jazzAccountSecret: "different-secret",
        },
      };
      expect(isClerkAuthStateEqual(previous, next)).toBe(true);
    });
  });

  describe("different jazzAccountID", () => {
    it("returns false when users have different jazzAccountID", () => {
      const previous = { unsafeMetadata: validCredentials };
      const next = { unsafeMetadata: differentCredentials };
      expect(isClerkAuthStateEqual(previous, next)).toBe(false);
    });
  });

  describe("neither user has valid credentials", () => {
    it.each([
      {
        previous: { unsafeMetadata: {} },
        next: { unsafeMetadata: {} },
        description: "both have empty metadata",
      },
      {
        previous: { unsafeMetadata: { someOtherField: "value" } },
        next: { unsafeMetadata: { anotherField: "value" } },
        description: "both have non-credential metadata",
      },
      {
        previous: { unsafeMetadata: { jazzAccountID: "123" } },
        next: { unsafeMetadata: { jazzAccountSecret: "456" } },
        description: "both have incomplete credentials",
      },
    ])("returns true when $description", ({ previous, next }) => {
      expect(isClerkAuthStateEqual(previous, next)).toBe(true);
    });
  });

  describe("one has credentials, other doesn't", () => {
    it.each([
      {
        previous: { unsafeMetadata: validCredentials },
        next: { unsafeMetadata: {} },
        description: "previous has credentials, next empty",
      },
      {
        previous: { unsafeMetadata: {} },
        next: { unsafeMetadata: validCredentials },
        description: "previous empty, next has credentials",
      },
      {
        previous: { unsafeMetadata: validCredentials },
        next: { unsafeMetadata: { jazzAccountID: "123" } },
        description: "previous has credentials, next has incomplete",
      },
      {
        previous: { unsafeMetadata: { jazzAccountSecret: "456" } },
        next: { unsafeMetadata: validCredentials },
        description: "previous has incomplete, next has credentials",
      },
    ])("returns false when $description", ({ previous, next }) => {
      expect(isClerkAuthStateEqual(previous, next)).toBe(false);
    });
  });
});
