import { describe, expect, it } from "vitest";
import * as root from "./index.js";
import * as react from "./react/index.js";
import * as vue from "./vue/index.js";
import * as backend from "./backend/index.js";
import { REPLACED, UPGRADE_FROM_ALPHA_60_URL } from "./removed-api.js";

describe("exports alpha.61 removed", () => {
  it("throw when called, naming the replacement, and are no working aliases", () => {
    for (const [name, stub] of [
      ["useJazzAuth", react.useJazzAuth],
      ["useLocalFirstAuth", vue.useLocalFirstAuth],
      ["createAccountManager", root.createAccountManager],
      ["createDb", root.createDb],
      ["acceptLocalFirstAuth", backend.acceptLocalFirstAuth],
    ] as const) {
      expect(stub.name).toBe(name);
      let thrown: unknown;
      try {
        (stub as unknown as () => unknown)();
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toMatchObject({
        code: "JAZZ_API_REMOVED",
        message: `[JAZZ_API_REMOVED] ${name} was removed in jazz-tools alpha.61: use ${REPLACED[name]}. See ${UPGRADE_FROM_ALPHA_60_URL}`,
      });
    }
    // The one adopters hit: Turbopack suggested useJazzClient.
    expect(() => (react.useJazzAuth as unknown as () => unknown)()).toThrow(
      /useJazzAuth was removed in jazz-tools alpha\.61: use useJazzSession\(\)/,
    );
    expect(() => (backend.acceptLocalFirstAuth as unknown as () => unknown)()).toThrow(
      /use acceptDeviceAccounts\./,
    );
  });

  it("throw when constructed, as the removed classes were", () => {
    expect(() => new (root.AccountManager as unknown as new () => unknown)()).toThrow(
      /\[JAZZ_API_REMOVED\] AccountManager was removed in jazz-tools alpha\.61: use createJazzClient\(config\)/,
    );
    expect(() => new (root.BrowserAuthSecretStore as unknown as new () => unknown)()).toThrow(
      /use device accounts on the session: createDeviceAccount\(\), exportDeviceKey\(\)/,
    );
  });
});
