import { describe, expect, it } from "vitest";
import { mountInspector } from "./index.js";

describe("mountInspector", () => {
  it("imports anywhere, and says it needs a browser page when there is none", async () => {
    await expect(mountInspector({} as never)).rejects.toThrow(/needs a browser page/);
  });
});
