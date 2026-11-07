import type { Storyboard } from "../storyboard.ts";

const address = "biglabel.example.com";
const LABEL = "Low Tide Records";

export default {
  id: "big-label",
  summary:
    "An admin adds a viewer by email; the label appears in the viewer's menu live, read-only, a new artist shows up in the viewer's filtered search without a reload, and a rename reaches him once his Wi-Fi is back.",
  devices: {
    a: { name: "Ada's laptop", address },
    b: { name: "Bo's laptop", address, color: "#ea580c" },
  },
  // Both sign up with Better Auth; each gets a personal label.
  offCamera: [
    { do: "openSignIn", on: "a" },
    { do: "signUp", on: "b", args: ["Bo"] },
    { do: "signUp", on: "a", args: ["Ada"] },
    { wait: 1000 },
  ],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "BigLabel",
      text: "Multi-tenant operations for record labels: members, roles, artists, releases. Next.js + Better Auth + Jazz.",
      hold: 2600,
    },
    {
      caption:
        "Ada signed in with Better Auth; the server bootstrapped her personal label, with her as admin",
      hold: 3000,
    },
    { caption: "" },

    { do: "loadDemoData", on: "a" },
    { caption: "Demo data: three more labels, with Ada as admin of each", hold: 2400 },
    { caption: "" },

    { do: "switchLabel", on: "a", args: [LABEL] },
    { do: "open", on: "a", args: ["Overview", { after: 1200 }] },
    { caption: `${LABEL}: counts and latest releases, each a live Jazz query`, hold: 2800 },
    { caption: "" },

    { do: "open", on: "a", args: ["Releases", { after: 1000 }] },
    { do: "search", on: "a", args: ["the"] },
    { wait: 1000 },
    { caption: "Search, filter, sort and paging are bounded, ordered queries", hold: 2000 },
    { do: "clearSearch", on: "a" },
    { wait: 600 },
    { caption: "" },
    { do: "openFirstRelease", on: "a" },
    { wait: 1000 },

    { do: "open", on: "a", args: ["People", { after: 900 }] },
    { do: "fillNewMember", on: "a", args: ["Bo", "Viewer"] },
    {
      caption:
        "The server looks up the email, then writes the membership as Ada: permissions.ts decides",
    },
    { do: "addMember", on: "a", args: ["Bo"] },
    { caption: "" },

    { split: ["a", "b"], scale: 0.6 },
    { do: "openLabelMenu", on: "b", args: [LABEL] },
    { caption: `${LABEL} appeared in Bo's label menu, live`, hold: 1800 },
    { do: "pickLabel", on: "b", args: [LABEL] },
    { do: "open", on: "b", args: ["Artists", { after: 1000 }] },
    { caption: "As a viewer, Bo can read the label but not change it", hold: 2200 },

    // A live filtered query: Bo searches; a matching artist appears as Ada adds it.
    { do: "search", on: "b", args: ["aard", { delay: 110 }] },
    { caption: "Bo searches the artists for “aard”: nothing yet", hold: 1800 },
    { do: "open", on: "a", args: ["Artists", { after: 600 }] },
    { caption: "Ada adds a matching artist…" },
    { do: "addArtist", on: "a", args: ["Aardvark Choir"] },
    { see: "Aardvark Choir", on: "b" },
    { poster: true },
    { caption: "…and it appears in Bo's filtered list immediately", hold: 2600 },

    // BigLabel waits for the server to confirm each write, so the one who goes
    // offline here is Bo, the reader.
    { caption: "Bo's Wi-Fi drops…" },
    { wifi: "off", on: "b" },
    { caption: "…while Ada renames the artist" },
    { do: "renameArtist", on: "a", args: ["Aardvark Choir and Strings"] },
    { wait: 1200 },
    { notSee: "Aardvark Choir and Strings", on: "b", because: "Bo's Wi-Fi is off" },
    { caption: "Bo's list still has the old name", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "b" },
    { see: "Aardvark Choir and Strings", on: "b" },
    { caption: "…and Bo's filtered list catches up", hold: 2400 },
    { caption: "" },

    { full: "a" },
    { do: "open", on: "a", args: ["Teams", { after: 1400 }] },
    { caption: "Teams: groups of members with their own roles", hold: 2000 },
    { caption: "" },
  ],
} satisfies Storyboard;
