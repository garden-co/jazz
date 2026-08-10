jest.mock("../NativeJazzRn", () => ({
  __esModule: true,
  default: { installRustCrate: jest.fn() },
}));

jest.mock("../generated/jazz_rn", () => ({
  __esModule: true,
  default: { initialize: jest.fn() },
  mintAnonymousToken: jest.fn(() => "anonymous-token"),
}));

import installer from "../NativeJazzRn";
import * as generated from "../generated/jazz_rn";
import jazzRn, { mintAnonymousToken, uniffiInitAsync } from "../index";

describe("jazz-rn package entrypoint", () => {
  it("installs and initializes the generated crate before exposing bindings", async () => {
    expect(installer.installRustCrate).toHaveBeenCalledTimes(1);
    expect(generated.default.initialize).toHaveBeenCalledTimes(1);
    expect(mintAnonymousToken("secret", "app", 60, 123n)).toBe("anonymous-token");
    expect(generated.mintAnonymousToken).toHaveBeenCalledWith("secret", "app", 60, 123n);
    expect(jazzRn.jazz_rn.mintAnonymousToken).toBe(generated.mintAnonymousToken);
    await expect(uniffiInitAsync()).resolves.toBeUndefined();
  });
});
