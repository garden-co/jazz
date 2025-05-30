import { describe, expect, it } from "vitest";
import { getWorkOSUsername } from "../getWorkOSUsername.js";
import type { MinimalWorkOSClient } from "../types.js";

describe("getWorkOSUsername", () => {
  it("should return null if no user", () => {
    const mockWorkOS = {
      user: null,
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe(null);
  });

  it("should return fullName if available", () => {
    const mockWorkOS = {
      user: {
        firstName: "John",
        lastName: "Doe",
      },
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe("John Doe");
  });

  it("should return firstName + lastName if available and no fullName", () => {
    const mockWorkOS = {
      user: {
        firstName: "John",
        lastName: "Doe",
        email: "johndoe@example.com",
      },
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe("John Doe");
  });

  it("should return firstName if available and no lastName or fullName", () => {
    const mockWorkOS = {
      user: {
        firstName: "John",
        email: "johndoe@example.com",
      },
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe("John");
  });

  it("should return username if available and no names", () => {
    const mockWorkOS = {
      user: {
        email: "johndoe@example.com",
      },
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe("johndoe");
  });

  it("should return email username if available and no other identifiers", () => {
    const mockWorkOS = {
      user: {
        email: "john.doe@example.com",
      },
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe("john.doe");
  });

  it("should return user id as last resort", () => {
    const mockWorkOS = {
      user: {
        id: "user_123",
      },
    } as MinimalWorkOSClient;

    expect(getWorkOSUsername(mockWorkOS)).toBe("user_123");
  });
});
