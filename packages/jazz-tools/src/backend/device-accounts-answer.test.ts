import { describe, expect, it } from "vitest";
import * as backend from "./index.js";

describe("DeviceAccountsAnswer", () => {
  it("its alpha.68 name LocalFirstGuest was removed in beta.0", () => {
    // @ts-expect-error LocalFirstGuest was removed: use DeviceAccountsAnswer.
    type Removed = backend.LocalFirstGuest;
    void (null as unknown as Removed);
    expect(backend.acceptDeviceAccounts(new Request("http://localhost/"))).toBeTruthy();
  });
});
