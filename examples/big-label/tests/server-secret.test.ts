import { afterEach, describe, expect, it } from "vitest";
import { serverSecret } from "../src/lib/server-secret.js";

const originalEnvironment = process.env.NODE_ENV;
const secretName = "BIG_LABEL_TEST_SERVER_SECRET";
const originalSecret = process.env[secretName];

afterEach(() => {
  if (originalEnvironment === undefined) delete process.env.NODE_ENV;
  else process.env.NODE_ENV = originalEnvironment;
  if (originalSecret === undefined) delete process.env[secretName];
  else process.env[secretName] = originalSecret;
});

describe("BigLabel server secrets", () => {
  it("requires configured production credentials instead of using source-visible fallbacks", () => {
    process.env.NODE_ENV = "production";
    delete process.env[secretName];

    expect(() => serverSecret(secretName, "development-only")).toThrow(
      `${secretName} must be configured in production.`,
    );
  });

  it("uses an explicit secret in every environment and a fallback only in development", () => {
    process.env.NODE_ENV = "production";
    process.env[secretName] = "configured-production-secret";
    expect(serverSecret(secretName, "development-only")).toBe("configured-production-secret");

    delete process.env[secretName];
    process.env.NODE_ENV = "development";
    expect(serverSecret(secretName, "development-only")).toBe("development-only");
  });
});
