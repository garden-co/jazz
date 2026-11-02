import { afterEach, describe, expect, it } from "vitest";
import { serverSecret } from "../src/lib/server-secret.js";

const originalNodeEnv = process.env.NODE_ENV;
const originalSecret = process.env.BETTER_AUTH_SECRET;

afterEach(() => {
  process.env.NODE_ENV = originalNodeEnv;
  if (originalSecret === undefined) delete process.env.BETTER_AUTH_SECRET;
  else process.env.BETTER_AUTH_SECRET = originalSecret;
});

describe("serverSecret", () => {
  it("accepts an injected production secret but never has a production fallback", () => {
    process.env.NODE_ENV = "production";
    process.env.BETTER_AUTH_SECRET = "injected-production-secret";
    expect(serverSecret("BETTER_AUTH_SECRET", "development-fixture")).toBe(
      "injected-production-secret",
    );

    delete process.env.BETTER_AUTH_SECRET;
    expect(() => serverSecret("BETTER_AUTH_SECRET", "development-fixture")).toThrow(
      "BETTER_AUTH_SECRET must be configured in production",
    );
  });

  it("keeps deterministic fixtures limited to non-production runs", () => {
    process.env.NODE_ENV = "test";
    delete process.env.BETTER_AUTH_SECRET;
    expect(serverSecret("BETTER_AUTH_SECRET", "development-fixture")).toBe("development-fixture");
  });
});
