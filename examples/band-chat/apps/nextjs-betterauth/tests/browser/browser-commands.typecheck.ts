import { bandChatBrowserCommands } from "./browser-commands.js";

const commands = bandChatBrowserCommands();
void commands.jazzBandChatBootstrapProfile(
  { appId: "019d-test", serverUrl: "ws://localhost:4200" },
  "musician@example.test",
  "Musician",
);

// @ts-expect-error The app command requires the server locator, subject, and display name.
void commands.jazzBandChatBootstrapProfile({ appId: "019d-test" }, "Musician");
