import { describe, expect, it, vi } from "vitest";

import { requireBandChatBrowserCommands } from "../browser/browser-command-contract.js";

describe("BandChat browser command contract", () => {
  it("accepts and preserves the configured bootstrap command", async () => {
    const bootstrap = vi.fn(async () => undefined);
    const configured = { jazzBandChatBootstrapProfile: bootstrap };

    const commands = requireBandChatBrowserCommands(configured);
    await commands.jazzBandChatBootstrapProfile(
      { appId: "019d-test", serverUrl: "ws://localhost:4200" },
      "musician@example.test",
      "Musician",
    );

    expect(commands).toBe(configured);
    expect(bootstrap).toHaveBeenCalledWith(
      { appId: "019d-test", serverUrl: "ws://localhost:4200" },
      "musician@example.test",
      "Musician",
    );
  });

  it.each([undefined, null, {}, { jazzBandChatBootstrapProfile: "not a function" }])(
    "fails helpfully when the command is not configured (%j)",
    (configured) => {
      expect(() => requireBandChatBrowserCommands(configured)).toThrow(
        "BandChat browser tests are missing the jazzBandChatBootstrapProfile command. " +
          "Configure it in vitest.config.browser.ts.",
      );
    },
  );
});
