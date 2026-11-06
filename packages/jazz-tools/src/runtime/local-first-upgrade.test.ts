import { expect, it } from "vitest";
import { Db } from "./db.js";

it("refuses a destructured upgradeToLocalFirst instead of resolving as if there were nothing to do", async () => {
  // `const { upgradeToLocalFirst } = db; await upgradeToLocalFirst(loader)`
  const { upgradeToLocalFirst } = Db.prototype;
  const loader = () => Promise.reject(new Error("never loaded"));
  await expect(upgradeToLocalFirst(loader)).rejects.toThrow(
    /call it as db\.upgradeToLocalFirst\(loader\)/,
  );
});
