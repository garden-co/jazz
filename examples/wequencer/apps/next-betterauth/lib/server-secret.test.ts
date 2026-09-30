import { afterEach, describe, expect, it, vi } from "vitest";
import { serverSecret } from "./server-secret";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("server secrets", () => {
  it("uses the development fallback outside production", () => {
    vi.stubEnv("NODE_ENV", "development");
    vi.stubEnv("BACKEND_SECRET", "");
    expect(serverSecret("BACKEND_SECRET", "dev-only")).toBe("dev-only");
  });

  it("fails closed in production when the secret is missing", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("BETTER_AUTH_SECRET", "");
    expect(() => serverSecret("BETTER_AUTH_SECRET", "dev-only")).toThrow(/must be configured/);
  });

  it("prefers a configured secret", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("BACKEND_SECRET", "from-env");
    expect(serverSecret("BACKEND_SECRET", "dev-only")).toBe("from-env");
  });
});
