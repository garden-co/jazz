import { describe, expect, it } from "vitest";
import { isRequestAuthenticationError } from "./request-auth.js";

describe("backend request auth", () => {
  it("recognises the error from another copy of jazz-tools by name", () => {
    const foreign = Object.assign(new Error("Missing or invalid Authorization header"), {
      name: "RequestAuthenticationError",
    });
    expect(isRequestAuthenticationError(foreign)).toBe(true);
    expect(isRequestAuthenticationError(new Error("registry down"))).toBe(false);
    expect(isRequestAuthenticationError("RequestAuthenticationError")).toBe(false);
  });
});
