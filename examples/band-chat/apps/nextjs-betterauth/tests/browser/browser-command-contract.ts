export interface BandChatBrowserCommands {
  jazzBandChatBootstrapProfile(
    server: { appId: string; serverUrl: string },
    userId: string,
    displayName: string,
  ): Promise<void>;
}

export function requireBandChatBrowserCommands(value: unknown): BandChatBrowserCommands {
  if (
    typeof value !== "object" ||
    value === null ||
    !("jazzBandChatBootstrapProfile" in value) ||
    typeof Reflect.get(value, "jazzBandChatBootstrapProfile") !== "function"
  ) {
    throw new Error(
      "BandChat browser tests are missing the jazzBandChatBootstrapProfile command. " +
        "Configure it in vitest.config.browser.ts.",
    );
  }

  return value as BandChatBrowserCommands;
}
