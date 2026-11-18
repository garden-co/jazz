import { describe, expectTypeOf, it } from "vitest";
import { acceptDeviceAccounts, type DeviceAccountsAnswer, type LocalFirstGuest } from "./index.js";

describe("LocalFirstGuest (deprecated name of DeviceAccountsAnswer)", () => {
  it("is assignable to and from DeviceAccountsAnswer", () => {
    expectTypeOf<LocalFirstGuest>().toMatchTypeOf<DeviceAccountsAnswer>();
    expectTypeOf<DeviceAccountsAnswer>().toMatchTypeOf<LocalFirstGuest>();
    const answer: LocalFirstGuest = acceptDeviceAccounts(new Request("http://localhost/"));
    const renamed: DeviceAccountsAnswer = answer;
    expectTypeOf(renamed).toEqualTypeOf<DeviceAccountsAnswer>();
  });
});
