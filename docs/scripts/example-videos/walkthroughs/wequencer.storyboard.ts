import type { Storyboard } from "../storyboard.ts";

const address = "wequencer.example.com";
const KICK = ["Kick, step 4", "Kick, step 8", "Kick, step 14"];

export default {
  id: "wequencer",
  summary:
    "Two bandmates in one session: it appears in the second one's list live, then pattern edits, Play and tempo follow on both screens, and steps added while he's offline arrive when his Wi-Fi is back.",
  devices: {
    a: { name: "Ada's laptop", address },
    b: { name: "Ben's laptop", address, color: "#ea580c" },
  },
  // Ben signs up first (which also compiles the dashboard), then Ada.
  offCamera: [
    { do: "signUp", on: "b", args: ["Ben"] },
    { do: "signUp", on: "a", args: ["Ada"] },
  ],
  opening: [{ split: ["a", "b"], scale: 0.8 }],
  beats: [
    {
      title: "Wequencer",
      text: "A shared step sequencer: one pattern, one transport, one mix. Next.js + Better Auth + Jazz.",
      hold: 2600,
    },
    { caption: "Ada and Ben are signed in; each starts on an empty dashboard", hold: 2200 },
    { caption: "" },
    { do: "createSession", on: "a", args: ["Friday jam"] },
    { wait: 1200 },

    { caption: "Ada adds Ben as an editor by his account ID" },
    { do: "addCollaborator", on: "a", args: ["b"] },
    // Ben's session list is a live query of the sessions he can see.
    { do: "waitForSessionLink", on: "b", args: ["Friday jam"] },
    {
      caption: "Ben's session list is a live query: “Friday jam” appears as he's added",
      hold: 2600,
    },
    { do: "openSession", on: "b", args: ["Friday jam"] },
    { wait: 800 },
    { caption: "" },

    { caption: "Both program the same pattern, live" },
    {
      do: "toggle",
      on: "b",
      args: [["Snare, step 2", "Snare, step 10", "Snare, step 12", "Snare, step 13"]],
    },
    {
      do: "toggle",
      on: "a",
      args: [["Open hat, step 3", "Open hat, step 7", "Open hat, step 11"]],
    },
    { do: "waitForSamePads", on: "a", args: ["b", ["Snare, step 13", "Open hat, step 11"]] },
    { poster: true },
    { wait: 1200 },

    { caption: "One shared transport: Ben presses Play, and Ada's sequencer plays too" },
    { do: "play", on: "b" },
    { do: "waitForPlaying", on: "a" },
    { wait: 2500 },

    { caption: "Ada sets the tempo to 150 for everyone" },
    { do: "setTempo", on: "a", args: [150] },
    { do: "waitForTempo", on: "b", args: [150] },
    { wait: 1800 },

    // Wequencer waits for the server to confirm each pad edit, so the one who
    // goes offline here is Ben, while Ada keeps editing.
    { caption: "Ben's Wi-Fi drops…" },
    { wifi: "off", on: "b" },
    { do: "rememberPads", on: "b", args: [KICK] },
    { caption: "…while Ada programs the kick" },
    { do: "toggle", on: "a", args: [KICK] },
    { wait: 1500 },
    { do: "expectPadsUnchanged", on: "b", args: [[KICK[2]]] },
    { caption: "Ben's pattern doesn't have those steps yet", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "b" },
    { do: "waitForSamePads", on: "b", args: ["a", [KICK[2]], { timeout: 30_000, required: true }] },
    { caption: "…and Ada's kick arrives in Ben's pattern", hold: 2400 },

    { caption: "Ada adds a second pattern; it shows up for Ben" },
    { do: "addPattern", on: "a" },
    { see: "Pattern 2", on: "b", exact: true },
    { wait: 1800 },

    { caption: "Ada reloads: pattern, mix and transport are all still there" },
    { do: "reloadSession", on: "a", args: ["Friday jam"] },
    { wait: 2500 },
    { do: "stop", on: "a" },
    { caption: "" },
  ],
} satisfies Storyboard;
