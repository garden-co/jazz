/** Public, deterministic fixture data. No real people, bands, or identifiers. */
export const BAND_CHAT_FIXTURE_VERSION = 1;
export const demoRoom = {
  name: "Neon Soundcheck",
  welcome: "Soundcheck at 19:00 — bring your loudest ideas.",
};

/** Stable synthetic external subjects; Better Auth user ids are Jazz row ids. */
export const bandChatFixtureUsers = {
  owner: "019d4349-24b0-72a9-ae86-8ed24a7e3a90",
  peer: "019d4349-24b0-72a9-ae86-8ed24a7e3a91",
  outsider: "019d4349-24b0-72a9-ae86-8ed24a7e3a92",
  largeOwner: "019d4349-24b0-72a9-ae86-8ed24a7e3a93",
  largePeer: "019d4349-24b0-72a9-ae86-8ed24a7e3a94",
} as const;

export function deterministicBandFixture(seed = "band-chat-smoke") {
  return {
    version: BAND_CHAT_FIXTURE_VERSION,
    seed,
    room: demoRoom,
    messages: [demoRoom.welcome],
  } as const;
}
