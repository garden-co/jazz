import { expect, it } from "vitest";
import { accountAppId, localFirstAccountId } from "./local-first.js";

it("matches the Rust founding identity fixture", () => {
  expect(accountAppId("account-fixture")).toBe("81b22349-11e4-58e9-870b-1eef809fda42");
  expect(localFirstAccountId("account-fixture", "00000000-0000-4000-8000-000000000001")).toBe(
    "30f0ec9a-0a5a-5940-b329-274194a6e24b",
  );
});
