import { describe, expect, it, vi } from "vitest";
import {
  checkApiReachable,
  connectivityStatus,
  reachabilityAfterHealthCheck,
} from "../../../src/hooks/use-connectivity.js";

describe("API connectivity", () => {
  it("distinguishes checking from confirmed online and offline states", () => {
    expect(connectivityStatus(true, undefined)).toBe("checking");
    expect(connectivityStatus(true, true)).toBe("online");
    expect(connectivityStatus(true, false)).toBe("offline");
    expect(connectivityStatus(false, undefined)).toBe("offline");
  });

  it("reports whether the BFF health endpoint is reachable", async () => {
    await expect(checkApiReachable(vi.fn().mockResolvedValue({ ok: true }))).resolves.toBe(true);
    await expect(checkApiReachable(vi.fn().mockResolvedValue({ ok: false }))).resolves.toBe(false);
    await expect(checkApiReachable(vi.fn().mockRejectedValue(new Error("offline")))).resolves.toBe(
      false,
    );
  });

  it("uses the health endpoint to report BFF connectivity", () => {
    expect(reachabilityAfterHealthCheck(undefined, true)).toBe(true);
    expect(reachabilityAfterHealthCheck(true, true)).toBe(true);
    expect(reachabilityAfterHealthCheck(undefined, false)).toBe(false);
  });
});
