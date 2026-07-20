import { describe, expect, it, vi } from "vitest";
import { checkApiReachable } from "./use-connectivity.js";

describe("API connectivity", () => {
  it("reports whether the BFF health endpoint is reachable", async () => {
    await expect(checkApiReachable(vi.fn().mockResolvedValue({ ok: true }))).resolves.toBe(true);
    await expect(checkApiReachable(vi.fn().mockResolvedValue({ ok: false }))).resolves.toBe(false);
    await expect(checkApiReachable(vi.fn().mockRejectedValue(new Error("offline")))).resolves.toBe(false);
  });
});
