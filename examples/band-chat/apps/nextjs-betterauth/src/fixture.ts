/** Public, deterministic fixture data. No real people, bands, or identifiers. */
export const BAND_CHAT_FIXTURE_VERSION = 1;
export const demoRoom = {
  name: "Neon Soundcheck",
  welcome: "Soundcheck at 19:00 — bring your loudest ideas.",
};

export function deterministicBandFixture(seed = "band-chat-smoke") {
  return {
    version: BAND_CHAT_FIXTURE_VERSION,
    seed,
    room: demoRoom,
    messages: [demoRoom.welcome],
  } as const;
}
