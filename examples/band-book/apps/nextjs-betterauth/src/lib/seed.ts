import type { BlockKind, IssuePriority, IssueStatus } from "@/schema";

/**
 * The demo workspace every new account starts with. It is plain data so the
 * bootstrap route, the tests and the benchmarks can all share it, and it is
 * deterministic: the same account always gets the same ids and content.
 */

export type SeedBlock = {
  kind: BlockKind;
  text?: string;
  checked?: boolean;
  children?: SeedBlock[];
};

export type SeedIssue = {
  status: IssueStatus;
  priority: IssuePriority;
  labels: string[];
};

/**
 * A comment on a song sheet, written the way a person (or an LLM) would quote
 * it: the words commented on and a little text around them, no positions.
 */
export type SeedComment = {
  before?: string;
  matched: string;
  after?: string;
  text: string;
};

export type SeedPage = {
  key: string;
  title: string;
  kind: "doc" | "issues" | "issue" | "song";
  blocks?: SeedBlock[];
  issue?: SeedIssue;
  /** A song page's rich-text body, as Markdown. */
  sheet?: string;
  comments?: SeedComment[];
  children?: SeedPage[];
};

export const DEMO_WORKSPACE_NAME = "The Late Shift";

export const DEMO_PAGES: SeedPage[] = [
  {
    key: "setlist",
    title: "Setlist: spring tour",
    kind: "doc",
    blocks: [
      { kind: "heading", text: "Main set" },
      { kind: "todo", text: "Harbour lights", checked: true },
      { kind: "todo", text: "Paper moon", checked: true },
      { kind: "todo", text: "Night bus home" },
      { kind: "todo", text: "Slow burn" },
      { kind: "divider" },
      { kind: "heading", text: "Encore" },
      { kind: "bullet", text: "Last orders (acoustic)" },
      { kind: "quote", text: "Keep the encore under six minutes. Curfew is strict in Porto." },
    ],
  },
  {
    key: "songs",
    title: "Songs",
    kind: "doc",
    blocks: [
      { kind: "paragraph", text: "Lyrics, chords and arrangement notes. One page per song." },
    ],
    children: [
      {
        key: "harbour-lights",
        title: "Harbour lights",
        kind: "doc",
        blocks: [
          { kind: "paragraph", text: "Key of D, 92 bpm. Capo 2 on the acoustic." },
          { kind: "heading", text: "Verse 1" },
          {
            kind: "quote",
            text: "Salt on the window, the ferry's late again\nWe count the harbour lights like they were friends",
          },
          { kind: "heading", text: "Chorus" },
          {
            kind: "quote",
            text: "Hold on, hold on, the tide will turn around\nKeep one light burning on the edge of town",
          },
        ],
        children: [
          {
            key: "harbour-lights-arrangement",
            title: "Arrangement notes",
            kind: "doc",
            blocks: [
              { kind: "bullet", text: "Intro: bass and brushes only for eight bars" },
              {
                kind: "bullet",
                text: "Chorus 2: organ enters",
                children: [
                  { kind: "bullet", text: "Keep the pad below the vocal" },
                  { kind: "bullet", text: "Drop out again for the bridge" },
                ],
              },
              { kind: "bullet", text: "Outro: hold the last chord, let the room ring" },
            ],
          },
        ],
      },
      {
        key: "night-bus",
        title: "Night bus home (song sheet)",
        kind: "song",
        sheet: [
          "**Key of E, 104 bpm.** Written on the N29 after the [Lisbon show](https://example.com/lisbon).",
          "",
          "## Verse 1",
          "",
          "> Last bus out of Camden, the windows fogged and gold\\",
          "> You draw a map of nowhere on the glass against the cold",
          "",
          "## Chorus",
          "",
          "> Ride it to the end of the line\\",
          "> Every stop is a reason to stay",
          "",
          "## Chords",
          "",
          "```",
          "Verse   E  | C#m | A  | B",
          "Chorus  A  | E   | B  | C#m",
          "```",
          "",
          "## Arrangement",
          "",
          "1. Intro: guitar alone, palm muted",
          "2. Verse 1: *drums on the second line*",
          "3. Chorus: everyone in, harmonies on **end of the line**",
          "",
          "- Decide on a bridge",
          "- Try it a tone lower for the acoustic set",
        ].join("\n"),
        comments: [
          {
            before: "the windows ",
            matched: "fogged and gold",
            text: "Love this image. Keep it even if the rest of the verse changes.",
          },
          {
            matched: "Every stop is a reason to stay",
            text: "Feels a syllable short when sung. Try “every single stop”?",
          },
          {
            before: "Chorus: everyone in, ",
            matched: "harmonies on **end of the line**",
            text: "Who takes the high part? Let's try it at soundcheck.",
          },
        ],
      },
      {
        key: "paper-moon",
        title: "Paper moon",
        kind: "doc",
        blocks: [
          { kind: "paragraph", text: "Key of A minor, 120 bpm." },
          { kind: "heading", text: "Verse 1" },
          {
            kind: "quote",
            text: "Cut it out of paper, pin it to the sky\nNobody will notice if we tell them it is high",
          },
          { kind: "heading", text: "Bridge" },
          { kind: "paragraph", text: "Still unwritten. See the issue in the tracker." },
        ],
      },
    ],
  },
  {
    key: "tour",
    title: "Tour notes",
    kind: "doc",
    blocks: [{ kind: "paragraph", text: "Venues, load-in times and who to call." }],
    children: [
      {
        key: "tour-lisbon",
        title: "Lisbon, 12 April",
        kind: "doc",
        blocks: [
          { kind: "heading", text: "Load-in" },
          { kind: "paragraph", text: "Doors 20:00, load-in 16:00 through the side entrance." },
          { kind: "todo", text: "Send stage plot to the venue", checked: true },
          { kind: "todo", text: "Book two hotel rooms near the venue" },
        ],
      },
      {
        key: "tour-porto",
        title: "Porto, 14 April",
        kind: "doc",
        blocks: [
          { kind: "heading", text: "Load-in" },
          { kind: "paragraph", text: "Shared backline with the support act. Curfew 23:00." },
          { kind: "todo", text: "Confirm drum kit with the promoter" },
        ],
      },
    ],
  },
  {
    key: "issues",
    title: "Issues",
    kind: "issues",
    children: [
      {
        key: "issue-restring",
        title: "Restring the bass before Lisbon",
        kind: "issue",
        issue: { status: "todo", priority: "high", labels: ["Gear"] },
        blocks: [{ kind: "paragraph", text: "Flatwounds, the spare set is in the van." }],
      },
      {
        key: "issue-backline",
        title: "Confirm backline with Porto venue",
        kind: "issue",
        issue: { status: "in_progress", priority: "urgent", labels: ["Logistics"] },
        blocks: [
          { kind: "paragraph", text: "They list a kit but no bass amp." },
          { kind: "todo", text: "Email the promoter" },
          { kind: "todo", text: "Ask the support act about sharing" },
        ],
      },
      {
        key: "issue-bridge",
        title: "Finish bridge lyrics for Paper moon",
        kind: "issue",
        issue: { status: "backlog", priority: "medium", labels: ["Writing"] },
        blocks: [
          { kind: "paragraph", text: "Something about the tide, to tie it to Harbour lights." },
        ],
      },
      {
        key: "issue-print",
        title: "Print setlists",
        kind: "issue",
        issue: { status: "done", priority: "low", labels: ["Logistics"] },
        blocks: [],
      },
    ],
  },
];

/** Depth-first, parents before children: the order the bootstrap writes them. */
export function flattenSeedPages(
  pages: SeedPage[] = DEMO_PAGES,
  parentKey: string | null = null,
): Array<SeedPage & { parentKey: string | null }> {
  return pages.flatMap((page) => [
    { ...page, parentKey },
    ...flattenSeedPages(page.children ?? [], page.key),
  ]);
}
