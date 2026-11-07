import type { Storyboard } from "../storyboard.ts";

const address = "epicdrop.example.com";

export default {
  id: "epic-drop",
  summary:
    'Uploads and previews in a shared folder; a second account joins by "Can edit" link, his upload appears in the open folder live, and a rename made while he’s offline reaches him once his Wi-Fi is back.',
  devices: {
    a: { name: "Alice's laptop", address },
    b: { name: "Bob's laptop", address, color: "#ea580c" },
  },
  // Each browser is an anonymous local-first account: no sign-up.
  offCamera: [
    { do: "openApp", on: "a" },
    { do: "openApp", on: "b" },
  ],
  opening: [{ full: "a" }],
  beats: [
    {
      title: "EpicDrop",
      text: "A file browser for a band's demos, built on Jazz large values. React + Jazz.",
      hold: 2400,
    },
    { caption: "Each browser is an anonymous local-first Jazz account", hold: 1500 },
    { do: "createFolder", on: "a", args: ["Tour demos"] },
    { wait: 600 },

    { caption: "Uploads stream File.stream() into a Jazz bytes column, chunk by chunk" },
    { do: "uploadDemos", on: "a" },
    { wait: 1500 },

    { caption: "Previews read only the byte range they show: select({ contents: { from, to } })" },
    { do: "previewText", on: "a", args: ["set-list.txt"] },
    { caption: "Images, audio, video and PDFs load the whole value into a Blob" },
    { do: "preview", on: "a", args: ["cover.png", { after: 2000 }] },
    { caption: "Anything else: a hex dump of the first 512 bytes" },
    { do: "preview", on: "a", args: ["stems.bin", { after: 2200 }] },
    { do: "closePreview", on: "a" },

    { caption: "Sharing: an invite link, checked by a permission at the sync server" },
    { do: "createEditLink", on: "a" },
    { wait: 1600 },
    { do: "press", on: "a", args: ["Escape"] },
    { wait: 400 },

    { split: ["a", "b"], scale: 0.6 },
    { caption: "Bob opens the link on his laptop" },
    { do: "joinFolder", on: "b", args: ["Tour demos"] },
    { caption: "Bob now sees the folder and its files, with edit access", hold: 2400 },

    { caption: "Alice's open folder is a live query for its files…" },
    { do: "uploadIdea", on: "b" },
    { do: "waitForFile", on: "a", args: ["bob-idea.txt"] },
    { poster: true },
    { caption: "…so Bob's upload appears for Alice without a reload", hold: 2400 },

    { caption: "Bob's Wi-Fi drops…" },
    { wifi: "off", on: "b" },
    { caption: "…while Alice renames a file" },
    { do: "rename", on: "a", args: ["set-list.txt", "set-list-final.txt"] },
    { wait: 1500 },
    { do: "expectNoFile", on: "b", args: ["set-list-final.txt"] },
    { caption: "Bob still has the old name", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "b" },
    { do: "waitForFile", on: "b", args: ["set-list-final.txt"] },
    { caption: "…and the new name syncs in", hold: 2200 },

    { caption: "Bob reloads: his copy is stored locally and syncs back in" },
    { do: "reload", on: "b" },
    { do: "waitForFile", on: "b", args: ["set-list-final.txt", { timeout: 60_000 }] },
    { wait: 1800 },
    { caption: "" },
  ],
} satisfies Storyboard;
