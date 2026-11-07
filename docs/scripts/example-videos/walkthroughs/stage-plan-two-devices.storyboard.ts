import type { Storyboard } from "../storyboard.ts";

// The homepage clip: StagePlan on two laptops side by side, a crew chief and a
// crew member on the same show board. It plays at about half size, so the
// subtitles are larger, and it has no title card.
const address = "stageplan.example.com";

export default {
  id: "stage-plan-two-devices",
  stage: { captionSize: 30 },
  devices: {
    a: { name: "Mia's laptop", address, color: "#2563eb" },
    b: { name: "Cole's laptop", address, color: "#ea580c" },
  },
  // Mia's first run creates the demo show; Cole joins it through her invite link.
  offCamera: [
    { do: "openDemoShow", on: "a", args: ["The Late Lanterns: album launch", "Mia"] },
    { do: "readInviteLink", on: "a" },
    { do: "joinWithInvite", on: "b", args: ["Cole"] },
    { do: "reopenBoard", on: "a", args: [2] },
  ],
  opening: [
    { split: ["a", "b"], scale: 0.8 },
    { do: "waitForCard", on: "a", args: ["Soundcheck with the band"] },
    { do: "waitForCard", on: "b", args: ["Soundcheck with the band"] },
    { wait: 1200 },
  ],
  beats: [
    { caption: "Two crew members, one live show board", hold: 2200 },

    { caption: "Mia adds a task…" },
    { do: "addTask", on: "a", args: ["Tape down the cable runs", { after: 200 }] },
    { do: "waitForCard", on: "b", args: ["Tape down the cable runs", "todo"] },
    { caption: "…and it appears on Cole's board right away", hold: 2200 },

    { caption: "Cole starts on soundcheck…" },
    { do: "dragCard", on: "b", args: ["Soundcheck with the band", "doing"] },
    { do: "waitForCard", on: "a", args: ["Soundcheck with the band", "doing"] },
    { caption: "…and Mia's board follows", hold: 2000 },

    // A filtered live query: Mia's open task shows only that task's comments.
    // (StagePlan's other filter, on the checklist, only ever sees its owner's
    // items, so a second person can't add to it.) Cole first comments on another
    // task, which stays out of Mia's view, then on hers, which shows up at once.
    { caption: "Mia opens soundcheck: a live query for just its comments" },
    { do: "openCard", on: "a", args: ["Soundcheck with the band"] },
    { do: "openCard", on: "b", args: ["Tape down the cable runs"] },
    { caption: "Cole comments on a different task: not in Mia's query" },
    { do: "comment", on: "b", args: ["Gaffer tape is in the van"] },
    { wait: 1400 },
    { notSee: "Gaffer tape is in the van", on: "a", because: "it's a comment on another task" },
    { do: "closeCard", on: "b" },
    { do: "openCard", on: "b", args: ["Soundcheck with the band"] },
    { caption: "Then on soundcheck: it matches…" },
    { do: "comment", on: "b", args: ["Drums are miked"] },
    { see: "Drums are miked", on: "a" },
    { caption: "…so it shows up in Mia's open task immediately", hold: 2600 },
    { do: "closeCard", on: "b" },
    { do: "closeCard", on: "a" },

    { caption: "Mia turns off her laptop's Wi-Fi" },
    { wifi: "off", on: "a" },
    { caption: "She keeps working: edits apply locally" },
    { do: "addTask", on: "a", args: ["Top up the hazer fluid", { after: 200 }] },
    { do: "dragCard", on: "a", args: ["Print setlists and tape them down", "doing"] },
    { caption: "Cole doesn't have them yet", hold: 2400 },
    { do: "expectNoCard", on: "b", args: ["Top up the hazer fluid"] },
    { do: "expectNoCard", on: "b", args: ["Print setlists and tape them down", "doing"] },

    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "a" },
    { do: "waitForCard", on: "b", args: ["Top up the hazer fluid", "todo"] },
    { do: "waitForCard", on: "b", args: ["Print setlists and tape them down", "doing"] },
    { caption: "…and Cole's board catches up", hold: 2400 },
    { caption: "Every change: local first, then synced", hold: 2400 },
  ],
} satisfies Storyboard;
