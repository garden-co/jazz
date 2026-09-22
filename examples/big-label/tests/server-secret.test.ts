import { afterEach, describe, expect, it } from "vitest";
import { serverSecret } from "../src/lib/server-secret.js";

const originalNodeEnv = process.env.NODE_ENV;
const originalBackendSecret = process.env.BACKEND_SECRET;

afterEach(() => {
  process.env.NODE_ENV = originalNodeEnv;
  if (originalBackendSecret === undefined) delete process.env.BACKEND_SECRET;
  else process.env.BACKEND_SECRET = originalBackendSecret;
});

describe("serverSecret", () => {
  it("accepts an injected production secret but never has a production fallback", () => {
    process.env.NODE_ENV = "production";
    process.env.BACKEND_SECRET = "injected-production-secret";
    expect(serverSecret("BACKEND_SECRET", "development-fixture")).toBe(
      "injected-production-secret",
    );

    delete process.env.BACKEND_SECRET;
    expect(() => serverSecret("BACKEND_SECRET", "development-fixture")).toThrow(
      "BACKEND_SECRET must be configured in production",
    );
  });

  it("keeps deterministic fixtures limited to non-production runs", () => {
    process.env.NODE_ENV = "test";
    delete process.env.BACKEND_SECRET;
    expect(serverSecret("BACKEND_SECRET", "development-fixture")).toBe("development-fixture");
  });
});
