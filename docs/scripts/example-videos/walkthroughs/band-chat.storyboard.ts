import type { Storyboard } from "../storyboard.ts";

const address = "bandchat.example.com";

export default {
  id: "band-chat",
  summary:
    "A guest asks to join a room and the creator admits them; the room and its history appear in his list, replies arrive live, and a message he sends with the Wi-Fi off arrives once it's back.",
  devices: {
    a: { name: "Olive's laptop", address },
    b: { name: "Gus's laptop", address, color: "#ea580c" },
  },
  offCamera: [
    { do: "signUp", on: "b", args: ["Gus Moreno"] },
    { do: "signUp", on: "a", args: ["Olive Park"] },
  ],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "BandChat",
      text: "A band's group chat: rooms, join requests, attachments. Next.js + Better Auth + Jazz.",
      hold: 2600,
    },
    {
      caption: "Olive is signed in with Better Auth; Jazz enrolled her account from its JWT",
      hold: 2400,
    },
    { caption: "" },
    { do: "createRoom", on: "a", args: ["Rehearsal"] },
    { caption: "Every message is a local write first, so it shows at once" },
    { do: "send", on: "a", args: ["Soundcheck moved to 7. Bring the new in-ear packs."] },
    { do: "pickStagePlot", on: "a" },
    { caption: "Attachments stream into the message row" },
    { wait: 900 },
    { do: "sendAttachment", on: "a", args: ["stage-plot.png"] },
    { wait: 1500 },
    { caption: "" },

    { split: ["a", "b"], scale: 0.62 },
    { caption: "Gus opens the room link. A link only lets him ask to join." },
    { do: "askToJoin", on: "b" },
    { do: "waitForJoinRequest", on: "a" },
    { caption: "The request shows up for Olive, live", hold: 1800 },
    { do: "openInvites", on: "a" },
    { caption: "Only the room creator can admit: Jazz permissions enforce it, not the UI" },
    { do: "admit", on: "a" },
    // Gus's room list is a live query of the rooms he belongs to.
    { do: "waitForRoom", on: "b", args: ["Rehearsal", "Soundcheck moved to 7"] },
    {
      caption:
        "Admitted: the room appears in Gus's live room list, history and attachment included",
      hold: 2600,
    },
    { caption: "Gus replies, and it lands in Olive's window live" },
    { do: "send", on: "b", args: ["In. I'll bring the spare snare too."] },
    { see: "I'll bring the spare snare", on: "a" },
    { poster: true },
    { wait: 1600 },

    { caption: "Gus's Wi-Fi drops…" },
    { wifi: "off", on: "b" },
    { caption: "…he keeps chatting: the message commits on his laptop" },
    { do: "send", on: "b", args: ["Running 10 min late, start without me"] },
    { wait: 1200 },
    { notSee: "Running 10 min late", on: "a", because: "Gus's Wi-Fi is off" },
    { caption: "Olive doesn't have it yet", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "b" },
    { see: "Running 10 min late", on: "a" },
    { caption: "…and it arrives in Olive's room", hold: 2400 },
    { caption: "" },
  ],
} satisfies Storyboard;
