import { describe, expect, it } from "vitest";
import { getClerkUsername } from "../getClerkUsername.js";
import type { ClerkUser } from "../types.js";

describe("getClerkUsername", () => {
  it("should return fullName if available", () => {
    expect(
      getClerkUsername({
        fullName: "John Doe",
        firstName: "John",
        lastName: "Doe",
        username: "johndoe",
      } as ClerkUser),
    ).toBe("John Doe");
  });

  it("should return firstName + lastName if available and no fullName", () => {
    expect(
      getClerkUsername({
        firstName: "John",
        lastName: "Doe",
        username: "johndoe",
      } as ClerkUser),
    ).toBe("John Doe");
  });

  it("should return firstName if available and no lastName or fullName", () => {
    expect(
      getClerkUsername({
        firstName: "John",
        username: "johndoe",
      } as ClerkUser),
    ).toBe("John");
  });

  it("should return username if available and no names", () => {
    expect(
      getClerkUsername({
        username: "johndoe",
      } as ClerkUser),
    ).toBe("johndoe");
  });

  it("should return email username if available and no other identifiers", () => {
    expect(
      getClerkUsername({
        primaryEmailAddress: {
          emailAddress: "john.doe@example.com",
        },
      } as ClerkUser),
    ).toBe("john.doe");
  });

  it("should return user id as last resort", () => {
    expect(
      getClerkUsername({
        id: "user_123",
      } as ClerkUser),
    ).toBe("user_123");
  });
});
