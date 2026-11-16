import { describe, expect, it } from "vitest";
import { refuseInvalidSessionConfig } from "./config-options.js";

const options = { serverUrl: true, initial: true } as const;

describe("refuseInvalidSessionConfig", () => {
  it("names what it got instead of a config object, and the options", () => {
    for (const [config, got] of [
      [undefined, "nothing"],
      [null, "null"],
      ["/jazz", "string"],
      [[], "object"],
    ] as const) {
      expect(() => refuseInvalidSessionConfig(config, options)).toThrow(
        new TypeError(
          `createJazzClient needs a config object, e.g. createJazzClient({ serverUrl: "/jazz" }), or createJazzClient({}) for the defaults; got ${got}. Options: initial, serverUrl.`,
        ),
      );
    }
  });

  it("takes the options it lists, and refuses any other by name", () => {
    expect(() => refuseInvalidSessionConfig({}, options)).not.toThrow();
    expect(() =>
      refuseInvalidSessionConfig({ serverUrl: "/jazz", initial: "device" }, options),
    ).not.toThrow();
    expect(() => refuseInvalidSessionConfig({ serverUrl: "/jazz", typo: 1 }, options)).toThrow(
      new TypeError('createJazzClient has no option "typo". Options: initial, serverUrl.'),
    );
  });

  it("says what replaced a likely old name", () => {
    expect(() => refuseInvalidSessionConfig({ guestMode: true }, options)).toThrow(
      'createJazzClient has no option "guestMode": guests are device accounts: use `initial: "device"`. Options: initial, serverUrl.',
    );
    expect(() => refuseInvalidSessionConfig({ peer: "wss://x" }, options)).toThrow(
      /no option "peer": the endpoint the client syncs with is `serverUrl`/,
    );
  });

  it("keeps alpha.60's own messages for its removed options", () => {
    expect(() => refuseInvalidSessionConfig({ linkGuestOnSignUp: false }, options)).toThrow(
      /linkGuestOnSignUp is now linkDeviceAccountOnSignUp/,
    );
  });
});
