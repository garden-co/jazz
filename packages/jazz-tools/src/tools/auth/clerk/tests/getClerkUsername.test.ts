import { describe, expect, it } from "vitest";
import { getClerkUsername } from "../getClerkUsername.js";
import type { MinimalClerkClient } from "../types.js";

describe("getClerkUsername", () => {
  it("should return null if no user", () => {
    const mockClerk = {
      user: null,
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe(null);
  });

  it("should return fullName if available", () => {
    const mockClerk = {
      user: {
        fullName: "John Doe",
        firstName: "John",
        lastName: "Doe",
        username: "johndoe",
      },
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe("John Doe");
  });

  it("should return firstName + lastName if available and no fullName", () => {
    const mockClerk = {
      user: {
        firstName: "John",
        lastName: "Doe",
        username: "johndoe",
      },
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe("John Doe");
  });

  it("should return firstName if available and no lastName or fullName", () => {
    const mockClerk = {
      user: {
        firstName: "John",
        username: "johndoe",
      },
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe("John");
  });

  it("should return username if available and no names", () => {
    const mockClerk = {
      user: {
        username: "johndoe",
      },
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe("johndoe");
  });

  it("should return email username if available and no other identifiers", () => {
    const mockClerk = {
      user: {
        primaryEmailAddress: {
          emailAddress: "john.doe@example.com",
        },
      },
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe("john.doe");
  });

  it("should return user id as last resort", () => {
    const mockClerk = {
      user: {
        id: "user_123",
      },
    } as MinimalClerkClient;

    expect(getClerkUsername(mockClerk)).toBe("user_123");
  });
});
