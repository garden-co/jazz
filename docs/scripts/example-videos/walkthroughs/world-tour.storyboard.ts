import type { Storyboard } from "../storyboard.ts";

const address = "worldtour.example.com";

export default {
  id: "world-tour",
  summary:
    "The tour manager sees every stop while a fan with the public link sees only the confirmed ones; a stop the manager confirms appears on the fan's globe live, and one confirmed with the Wi-Fi off arrives once it's back.",
  // The globe is WebGL, which needs software rendering on the recording machine.
  stage: { webgl: true },
  devices: {
    a: { name: "Tour manager's laptop", address },
    b: { name: "Fan's laptop", address, color: "#ea580c" },
    p: { kind: "phone", address, color: "#ea580c", viewport: { width: 375, height: 732 } },
  },
  // The first visitor to an empty server gets the seeded demo tour.
  offCamera: [{ do: "openDemoTour", on: "a" }],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "World Tour",
      text: "A band plans its tour on a globe; fans follow the confirmed dates. Vue + Jazz.",
      hold: 2600,
    },
    { caption: "The tour manager's view: all 12 stops, tentative ones included", hold: 1800 },
    { do: "playTour", on: "a" },
    { caption: "" },

    { do: "openFirstStop", on: "a" },
    {
      caption: "A stop: calendar, venue, and private notes only band members can read",
      hold: 2600,
    },
    { caption: "" },
    { do: "close", on: "a", args: [{ after: 900 }] },

    { do: "readBandLinks", on: "a" },
    { caption: "Only the owner can read the invite code", hold: 1800 },
    { caption: "" },
    { do: "close", on: "a", args: [{ after: 600 }] },

    // A fan with the public link, on another laptop with its own account.
    { do: "openPublicLink", on: "b" },
    { wait: 1500 },
    { split: ["a", "b"], scale: 0.62 },
    {
      caption: "A fan with the public link gets only confirmed dates: the server filters the rows",
      hold: 3000,
    },
    { do: "exploreGlobe", on: "b", args: [{ after: 1000 }] },
    { caption: "" },
    { do: "findTentativeStops", on: "b" },

    // A row starts matching the fan's filtered query: it appears live.
    { caption: "Confirm “{tentative.0}”…" },
    { do: "confirmStop", on: "a", args: [0] },
    { do: "waitForStop", on: "b", args: [0] },
    { caption: "…and it shows up on the fan's globe, live", hold: 1200 },
    { do: "openStop", on: "b", args: [0] },
    { poster: true },
    { wait: 2200 },
    { caption: "" },
    { do: "close", on: "a", args: [{ after: 300 }] },
    { do: "closeIfOpen", on: "b" },

    // Wi-Fi off: the change waits on the laptop until it's back online.
    { caption: "On the road, the tour manager's Wi-Fi drops…" },
    { wifi: "off", on: "a" },
    { caption: "…confirming “{tentative.1}” still works, locally" },
    { do: "confirmStop", on: "a", args: [1] },
    { wait: 1500 },
    { do: "expectNoStop", on: "b", args: [1] },
    { caption: "The fan doesn't see it yet", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "a" },
    { do: "waitForStop", on: "b", args: [1] },
    { caption: "…and the confirmed date reaches the fan", hold: 2400 },
    { caption: "" },
    { do: "close", on: "a", args: [{ after: 300 }] },

    // The fan joins with the invite link and becomes a member.
    { full: "a" },
    { do: "openBand", on: "a", args: [{ after: 300 }] },
    { caption: "The tour manager sends the fan the invite link" },
    { do: "openInviteLink", on: "b" },
    { split: ["a", "b"], scale: 0.62 },
    { caption: "The fan opens it", hold: 800 },
    { do: "joinBand", on: "b", args: ["Robin"] },
    { do: "waitForMember", on: "a", args: ["Robin"] },
    {
      caption: "The server accepts the membership; the owner's member list updates live",
      hold: 2200,
    },
    { do: "waitForAllStops", on: "b" },
    { caption: "As a member, Robin now sees every stop and the private notes", hold: 2500 },
    { caption: "" },

    // The phone. Stop the other two globes first so the phone renders smoothly.
    { do: "blank", on: "a" },
    { do: "blank", on: "b" },
    { do: "openPublicLink", on: "p" },
    { wait: 1200 },
    { show: [{ id: "p", x: 452, y: 20, w: 375, h: 760 }] },
    { caption: "The public tour page on a phone", hold: 1800 },
    { do: "exploreGlobe", on: "p", args: [{ after: 4500 }] },
    { caption: "" },
  ],
} satisfies Storyboard;
