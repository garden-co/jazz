import type { Storyboard } from "../storyboard.ts";

const address = "postershop.example.com";

export default {
  id: "poster-shop",
  summary:
    "A second editor joins by invite link; her cursor and edits arrive live, edits she makes with the Wi-Fi off sync once it's back, then a named checkpoint.",
  devices: {
    a: { name: "Ada's laptop", address },
    b: { name: "Grace's laptop", address, color: "#ea580c" },
  },
  // Each signs up on their own laptop; Ada's first sign-in seeds a demo poster.
  offCamera: [
    { do: "signUp", on: "b", args: ["Grace"] },
    { do: "signUp", on: "a", args: ["Ada"] },
  ],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "PosterShop",
      text: "Design gig posters together: layers, shapes, images and checkpoints. Next.js + Better Auth + Jazz.",
      hold: 2600,
    },
    {
      caption: "Ada's first sign-in seeded a demo poster: layers, an SVG artboard and an inspector",
      hold: 2600,
    },
    { do: "selectSun", on: "a" },
    {
      caption: "Select, recolour, add and move shapes. Every change is a local write that syncs.",
    },
    { do: "pickColour", on: "a", args: ["Sun", { after: 500 }] },
    { do: "addRectangle", on: "a" },
    { do: "dragShape", on: "a", args: ["added", 140, -260] },
    { do: "pickColour", on: "a", args: ["Pink", { after: 800 }] },
    { caption: "" },

    { do: "createInviteLink", on: "a" },
    { caption: "An invite link: can edit or can view, single use by default", hold: 2400 },
    { do: "press", on: "a", args: ["Escape"] },
    { wait: 400 },
    { caption: "" },

    { split: ["a", "b"], scale: 0.62 },
    { caption: "Grace opens the link on her laptop and joins as an editor" },
    { do: "openInviteLink", on: "b" },
    { wait: 1000 },

    { caption: "Her cursor shows up on Ada's poster, live" },
    { do: "hoverSun", on: "b" },
    { wait: 1500 },
    { caption: "Grace recolours the sun, and the change arrives in Ada's window" },
    { do: "recolourSun", on: "b", args: ["Violet", "a"] },
    { do: "waitForSunChange", on: "a" },
    { poster: true },
    { wait: 1800 },

    { caption: "Grace's Wi-Fi drops…" },
    { wifi: "off", on: "b" },
    { caption: "…she keeps designing: moves and recolours Ada's new shape" },
    { do: "rememberShape", on: "a", args: ["added"] },
    { do: "selectShape", on: "b", args: ["added"] },
    { do: "pickColour", on: "b", args: ["Teal", { after: 300 }] },
    { do: "dragShape", on: "b", args: ["added", -120, 60] },
    { wait: 1200 },
    { do: "expectShapeUnchanged", on: "a", args: ["added"] },
    { caption: "Ada doesn't have those edits yet", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "b" },
    { do: "waitForShapeChange", on: "a", args: ["added"] },
    { caption: "…and they sync to Ada's poster", hold: 2400 },
    { caption: "" },

    { full: "a" },
    { do: "saveCheckpoint", on: "a", args: ["First draft"] },
    { caption: "Named checkpoints keep a copy of the poster to preview later", hold: 2400 },
    { caption: "" },
  ],
} satisfies Storyboard;
