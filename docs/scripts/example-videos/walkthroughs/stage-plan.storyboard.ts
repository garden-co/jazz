import type { Storyboard } from "../storyboard.ts";

const address = "stageplan.example.com";
const SHOW = "The Late Lanterns: album launch";

export default {
  id: "stage-plan",
  summary:
    "A crew chief and a crew member in two browsers: the invite link, card moves and task comments arriving live, edits made with the Wi-Fi off syncing once it's back, and a live checklist filter.",
  devices: {
    a: { name: "Mia's laptop", address, color: "#2563eb" },
    b: { name: "Cole's laptop", address, color: "#ea580c" },
  },
  // The first visitor gets the demo show; no sign-up, the account is local-first.
  offCamera: [{ do: "openDemoShow", on: "a", args: [SHOW, "Mia"] }],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "StagePlan",
      text: "A crew prepares shows on a live board per show. React + Jazz, local-first accounts.",
      hold: 2600,
    },
    {
      caption:
        "No sign-up: the browser holds a local-first account, and the demo show is already here",
      hold: 3000,
    },
    { caption: "" },
    { do: "openShow", on: "a", args: [SHOW] },
    { wait: 1000 },

    { do: "addTask", on: "a", args: ["Tape the set list to the floor"] },
    { caption: "Writes apply instantly, locally first, and sync in the background", hold: 1500 },
    { do: "moveCard", on: "a", args: ["Soundcheck", "todo", "doing"] },
    { wait: 1200 },
    { caption: "" },

    // Task detail: comments and activity.
    { do: "openCard", on: "a", args: ["Soundcheck", { after: 900 }] },
    { do: "comment", on: "a", args: ["Band arrives at five. Drums first.", { after: 1200 }] },
    { do: "scroll", on: "a", args: [500] },
    { wait: 1400 },
    { do: "closeCard", on: "a" },
    { wait: 600 },

    { do: "showInviteLink", on: "a" },
    { caption: "Only the crew chief can read the invite code", hold: 2400 },
    { caption: "" },

    // Cole, on another laptop, opens the link (off camera until it lands).
    { do: "joinWithInvite", on: "b", args: ["Cole"] },
    { split: ["a", "b"], scale: 0.64 },
    { see: "Cole", on: "a" },
    {
      caption: "Cole joined through the invite link, and Mia's crew list updated live",
      hold: 2800,
    },
    { caption: "" },
    { do: "openTab", on: "a", args: ["Board"] },

    { do: "moveCard", on: "b", args: ["Line check", "doing", "done"] },
    { do: "waitForCard", on: "a", args: ["Line check", "done"] },
    { caption: "Cole moves “Line check” to Done, and it moves on Mia's board too", hold: 2600 },

    // Live query: Mia's open task shows just that task's comments, live.
    { caption: "Mia opens soundcheck: a live query for just its comments" },
    { do: "openCard", on: "a", args: ["Soundcheck"] },
    { do: "openCard", on: "b", args: ["Soundcheck"] },
    { caption: "Cole adds a comment to that task…" },
    { do: "comment", on: "b", args: ["Kick drum mic is live"] },
    { see: "Kick drum mic is live", on: "a" },
    { poster: true },
    { caption: "…and it appears in Mia's open task immediately", hold: 2400 },
    { do: "closeCard", on: "b" },
    { do: "closeCard", on: "a" },

    // Wi-Fi off: local edits, then catch-up.
    { caption: "Mia turns off her laptop's Wi-Fi…" },
    { wifi: "off", on: "a" },
    { caption: "…and keeps working: her edits apply locally" },
    { do: "addTask", on: "a", args: ["Check the fire exits"] },
    { do: "moveCard", on: "a", args: ["Hazer", "blocked", "done"] },
    { wait: 1200 },
    { caption: "Cole's board doesn't have them yet", hold: 2200 },
    { notSee: "Check the fire exits", on: "b", because: "Mia's Wi-Fi is off" },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "a" },
    { do: "waitForCard", on: "b", args: ["Check the fire exits", "todo"] },
    { caption: "…and Cole's board catches up", hold: 2400 },
    { caption: "" },

    // The private checklist, with a live filter.
    { full: "a" },
    { do: "openTab", on: "a", args: ["Checklist"] },
    { caption: "A private checklist for show day. Only its owner can read it." },
    {
      do: "addChecklistItems",
      on: "a",
      args: [["In-ears", "Spare batteries", "Spare gaffer tape"]],
    },
    { do: "filterChecklist", on: "a", args: ["Spare"] },
    { caption: "The filter is part of the query…", hold: 1600 },
    { do: "addChecklistItem", on: "a", args: ["Spare strings"] },
    { caption: "…so a new matching item shows up in the filtered list right away", hold: 2800 },
    { caption: "" },
  ],
} satisfies Storyboard;
