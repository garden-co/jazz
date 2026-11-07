import type { Storyboard } from "../storyboard.ts";

const address = "bandbook.example.com";

export default {
  id: "band-book",
  summary:
    'A bandmate shares one song with a "Can edit" link; the guest sees only that song and its subpage, typing shows up in both copies live, and edits made while his Wi-Fi is off merge once it’s back.',
  devices: {
    a: { name: "Ada's laptop", address },
    b: { name: "Bo's laptop", address, color: "#ea580c" },
  },
  offCamera: [
    { do: "signUp", on: "b", args: ["Bo"] },
    { do: "signUp", on: "a", args: ["Ada"] },
  ],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "BandBook",
      text: "A Notion-style notebook for running a band, with an issue tracker inside. Next.js + Better Auth + Jazz.",
      hold: 2600,
    },
    {
      caption:
        "On sign-up the server bootstrapped a demo band in one transaction: pages, songs, tour notes, issues",
      hold: 2600,
    },
    { caption: "" },

    { do: "openPage", on: "a", args: ["Setlist: spring tour", { after: 1200 }] },
    { do: "tick", on: "a", args: ["Night bus home"] },
    { do: "openPage", on: "a", args: ["Songs"] },
    { do: "openPage", on: "a", args: ["Harbour lights"] },
    { do: "addTextBlock", on: "a", args: ["Bridge: stay on the IV chord"] },
    { wait: 800 },
    { caption: "Pages nest, and blocks nest inside blocks", hold: 1800 },
    { caption: "" },

    { do: "openIssues", on: "a" },
    { caption: "The band's issue tracker is a database of pages: a table…", hold: 2000 },
    { do: "showBoardView", on: "a" },
    { caption: "…and a board", hold: 1800 },
    { caption: "" },

    { do: "openPage", on: "a", args: ["Harbour lights"] },
    { do: "createEditLink", on: "a" },
    {
      caption: "A “Can edit” link for this song only. Row policies in permissions.ts enforce it.",
      hold: 2800,
    },
    { caption: "" },
    { do: "closeShareDialog", on: "a" },

    { split: ["a", "b"], scale: 0.62 },
    { caption: "Bo, signed in on his own laptop, opens the link" },
    { do: "openEditLink", on: "b" },
    { wait: 600 },
    {
      caption: "Bo sees the shared song and the pages inside it, and nothing else of the band",
      hold: 2800,
    },

    { do: "append", on: "a", args: ["Key of D", " Count in on four."] },
    { do: "waitForText", on: "b", args: ["Count in on four."] },
    { poster: true },
    { caption: "Ada types, and Bo's copy updates as she types", hold: 2000 },

    { caption: "Bo's Wi-Fi drops…" },
    { wifi: "off", on: "b" },
    { caption: "…and both keep editing the same song" },
    { do: "append", on: "b", args: ["Bridge: stay on the IV chord", ", then back to D"] },
    { do: "append", on: "a", args: ["Count in on four.", " Tuning: drop D."] },
    { wait: 1200 },
    { do: "expectNoText", on: "b", args: ["drop D"] },
    { caption: "Each side has only its own edit for now", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "b" },
    { do: "waitForText", on: "b", args: ["drop D."] },
    { do: "waitForText", on: "a", args: ["then back to D"] },
    {
      caption: "…and both edits arrive. Each keystroke is a small text splice, so they merge.",
      hold: 3000,
    },
    { caption: "" },
  ],
} satisfies Storyboard;
