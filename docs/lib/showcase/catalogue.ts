import { formatTime } from "../perf-timeline/model.ts";

/** Looks up another benchmark's headline seconds, for metrics that compare two cases. */
export type Lookup = (benchmarkName: string) => number | null;

export type HeroMetric = {
  /** Exact CodSpeed benchmark name; its reviewed definition lives in dev/benchmarks/metadata. */
  benchmark: string;
  label: string;
  /**
   * Show the headline per operation: the run's seconds ÷ `count`, labelled
   * "per <unit>". For benchmarks that time many identical operations.
   */
  per?: { count: number; unit: string };
  /** One plain-language sentence about what the measured seconds mean for the app. */
  interpret: (seconds: number, lookup: Lookup) => string;
};

export type HeroVideo = {
  /** Written by `pnpm --filter docs capture:example-videos`. */
  src: string;
  poster: string;
  caption: string;
};

export type HeroExample = {
  id: string;
  title: string;
  tagline: string;
  description: string;
  /** Interactions the walkthrough should show, also listed beside the video. */
  highlights: string[];
  /** Repository-relative source directories. */
  sources: { label: string; path: string }[];
  video: HeroVideo | null;
  /** What the placeholder should promise until a capture exists. */
  plannedVideo?: string;
  metrics: HeroMetric[];
  /** Shown instead of metric cards while an example has no wallclock benchmarks on CodSpeed. */
  plannedMetrics?: string;
};

const rate = (count: number, seconds: number) =>
  Math.round(count / seconds).toLocaleString("en-US");
const each = (count: number, seconds: number) => formatTime(seconds / count);
// A 60 Hz frame. Only used to phrase a result that is already below it.
const frame = 1 / 60;

export const heroExamples: HeroExample[] = [
  {
    id: "todo",
    title: "Todos",
    tagline: "Local-first basics: instant writes, live queries, shared across devices.",
    description:
      "A React todo list with anonymous local-first identity and IndexedDB persistence. Every write lands locally first and syncs in the background; every device subscribed to the list sees it immediately. Row-level permissions let anyone read every todo, while only its owner may change or delete it.",
    highlights: [
      "Two devices add todos and see each other's changes live",
      "Checking off your own todo syncs instantly",
      "Trying to change someone else's todo is refused by the permission policy",
      "Live filters are just queries: typing narrows the list as you type",
    ],
    sources: [
      { label: "React app", path: "examples/todo-client-localfirst-react" },
      { label: "Benchmarks", path: "examples/todo-client-localfirst-ts/benchmarks" },
    ],
    video: {
      src: "/examples/videos/todo-two-devices.mp4",
      poster: "/examples/videos/todo-two-devices.jpg",
      caption:
        "Two independent browser profiles (separate storage and identities) syncing through a local Jazz server. Recorded automatically with Playwright from the example app.",
    },
    metrics: [
      {
        benchmark: "sequential_insert_1350_rocksdb",
        label: "Add a todo",
        per: { count: 1350, unit: "insert" },
        interpret: (s) =>
          `Each insert is its own durable transaction, persisted and delivered back to the live list${s / 1350 < frame ? " well inside one 60 fps frame" : ""}. Averaged over 1,350 inserts in a row (${formatTime(s)} total).`,
      },
      {
        benchmark: "sequential_update_1350_rocksdb",
        label: "Check off a todo",
        per: { count: 1350, unit: "update" },
        interpret: (s) =>
          `Each check-off is its own transaction, persisted and delivered back to the live list. Averaged over 1,350 updates in a row (${formatTime(s)} total).`,
      },
      {
        benchmark: "batch_update_1350_rocksdb",
        label: "Bulk edit 1,350 todos",
        interpret: (s) =>
          `Changing 1,350 rows in one transaction takes ${formatTime(s)}, about ${rate(1350, s)} rows per second.`,
      },
      {
        benchmark: "reopen_1500_rocksdb",
        label: "Reopen with 1,500 todos",
        interpret: (s) =>
          `Opening the app again and showing all 1,500 stored todos takes ${formatTime(s)}, including the fixed cost of opening storage.`,
      },
    ],
  },
  {
    id: "chat",
    title: "Chat",
    tagline: "Rooms, invites, reactions and shared drawing canvases, synced live.",
    description:
      "A React chat app with public rooms and private chats joined by invite code, emoji reactions and collaborative drawing canvases attached to a chat. Every message, reaction and stroke is a local write that Jazz replicates in the background; row-level policies in the schema decide who can read and change what, so components carry no auth logic.",
    highlights: [
      "Messages and reactions appear on every member's screen as they're sent",
      "Join a private chat with its invite code",
      "Draw together on a canvas attached to the chat",
    ],
    sources: [{ label: "React app", path: "examples/chat-react" }],
    video: null,
    plannedVideo:
      "Walkthrough capture is planned: two people chatting, reacting and drawing together.",
    metrics: [],
    plannedMetrics: "This example has no benchmark variant yet.",
  },
  {
    id: "band-chat",
    title: "BandChat",
    tagline: "Private rooms with membership boundaries, attachments and fast resume.",
    description:
      "A Next.js app with Better Auth sign-in, where a room's creator admits or removes members and only members can read or post. Messages carry inline attachments and are created local-first. Its benchmark covers the reads a chat does constantly: a room's timeline page, unread rooms by recent activity, one author's history, and resuming a caught-up client.",
    highlights: [
      "Create a room and admit a bandmate",
      "Send a message with an inline attachment",
      "Removing a member stops their later writes",
    ],
    sources: [
      { label: "Next.js app", path: "examples/band-chat/apps/nextjs-betterauth" },
      { label: "Benchmarks", path: "examples/band-chat/benchmarks" },
    ],
    video: null,
    plannedVideo: "Walkthrough capture is planned: two bandmates in a private room.",
    metrics: [],
    plannedMetrics:
      "Benchmarked on CodSpeed in simulation mode (instruction counts), which this page doesn't show yet.",
  },
  {
    id: "world-tour",
    title: "World Tour",
    tagline: "Tour management on a live globe: dates, venues and a public calendar.",
    description:
      "A Vue app for planning a band's tour, with the schedule drawn on a dot-art globe. Members see the full calendar; the public sees confirmed dates only. Both are ordered, bounded three-week itinerary reads that include each stop's venue.",
    highlights: [
      "The tour's stops are drawn on the globe",
      "Members see every date; the public calendar shows confirmed dates only",
    ],
    sources: [{ label: "Vue app and benchmarks", path: "examples/world-tour" }],
    video: null,
    plannedVideo:
      "Walkthrough capture is planned: planning a tour stop and checking the public calendar.",
    metrics: [],
    plannedMetrics:
      "Benchmarked on CodSpeed in simulation mode (instruction counts), which this page doesn't show yet.",
  },
  {
    id: "wequencer",
    title: "Wequencer",
    tagline: "A collaborative step sequencer: many small subscriptions, many concurrent edits.",
    description:
      "Bandmates edit a shared pattern together, watch its transport state and see who else is around. It makes hard local-first shapes concrete: an ordered, windowed step grid with many small subscriptions, concurrent edits to nearby and identical steps, and editor and viewer roles on one shared session.",
    highlights: [
      "Two bandmates toggle steps in the same pattern at once",
      "Go offline, keep editing, and reconnect",
      "A viewer's edit is refused",
    ],
    sources: [
      { label: "Next.js app", path: "examples/wequencer/apps/next-betterauth" },
      { label: "Benchmarks", path: "examples/wequencer/benchmarks" },
    ],
    video: null,
    plannedVideo: "Walkthrough capture is planned: two bandmates editing one pattern.",
    metrics: [],
    plannedMetrics: "Its benchmark variant exists but doesn't run on CodSpeed yet.",
  },
  {
    id: "poster-shop",
    title: "PosterShop",
    tagline: "A collaborative gig-poster canvas with layers, shapes and live cursors.",
    description:
      "The durable data a tldraw-like canvas needs, without tying it to a renderer: canvases, ordered layers, shapes, asset metadata and checkpoints. Layers, canvas, cursors, assets and checkpoints are independently subscribed, so a stream of cursor updates never re-runs the whole canvas query.",
    highlights: [
      "Two editors add shapes to the same canvas",
      "Cursors move live without touching history",
      "Offline edits replay to peers after reconnecting",
    ],
    sources: [
      { label: "Next.js app", path: "examples/poster-shop/apps/nextjs-betterauth" },
      { label: "Benchmarks", path: "examples/poster-shop/benchmarks" },
    ],
    video: null,
    plannedVideo: "Walkthrough capture is planned: two editors designing one poster.",
    metrics: [],
    plannedMetrics: "Its benchmark variant exists but doesn't run on CodSpeed yet.",
  },
  {
    id: "record-player",
    title: "RecordPlayer",
    tagline: "Shared playlists over streamed audio, with listener and editor invitations.",
    description:
      "Albums are browsed in a CoverFlow view and collected into shared playlists. Audio is uploaded as streamed bytes, and invitations grant listener or editor access to a playlist. Concurrent additions from two people converge on the same list.",
    highlights: [
      "Invite a friend to listen, or to edit",
      "Two people add tracks to one playlist at once",
      "Offline additions flush on reconnect",
    ],
    sources: [
      { label: "Next.js app", path: "examples/record-player/apps/next-betterauth" },
      { label: "Benchmarks", path: "examples/record-player/benchmarks" },
    ],
    video: null,
    plannedVideo: "Walkthrough capture is planned: browsing albums and sharing a playlist.",
    metrics: [],
    plannedMetrics: "Its benchmark variant exists but doesn't run on CodSpeed yet.",
  },
  {
    id: "epic-drop",
    title: "EpicDrop",
    tagline: "A file browser for large binary values, streamed straight from the browser.",
    description:
      "Uploads stream a browser File directly into Jazz, and a folder lists each file's name, type and size without loading any file contents. A cancelled upload publishes nothing, and a retry starts clean.",
    highlights: [
      "Drop a large file and watch it stream in",
      "Browse a folder without downloading its files",
    ],
    sources: [{ label: "App and benchmarks", path: "examples/epic-drop" }],
    video: null,
    plannedVideo: "Walkthrough capture is planned: uploading and browsing files.",
    metrics: [],
    plannedMetrics: "Its benchmark variant exists but doesn't run on CodSpeed yet.",
  },
  {
    id: "jamazon-warehouse",
    title: "Jamazon Warehouse",
    tagline: "An operations console for an instrument store, shaped like TPC-C.",
    description:
      "Warehouses, districts, stock, customers, orders and payments, with multi-row checkout, ordered and bounded operational reads, local-first retry, and an idempotent hand-off to external effects. Anyone can watch stock and orders; only a warehouse's operator can change them.",
    highlights: [
      "Place an order that reserves stock across several rows",
      "Watch stock levels update live on the console",
    ],
    sources: [{ label: "App and benchmarks", path: "examples/jamazon-warehouse" }],
    video: null,
    plannedVideo: "Walkthrough capture is planned: checkout and the live stock console.",
    metrics: [],
    plannedMetrics: "Its benchmark variant exists but doesn't run on CodSpeed yet.",
  },
  {
    id: "music-agent",
    title: "MusicAgent",
    tagline: "An LLM agent transcript: streamed turns, tool calls and audio attachments.",
    description:
      "A provider-free agent harness that records a conversation, streams a long assistant turn, logs tool invocations and keeps uploaded audio as bytes. A deterministic fake agent makes the whole flow run without an API key.",
    highlights: [
      "A long assistant reply streams in as it's written",
      "Tool calls and attachments are part of the transcript",
    ],
    sources: [
      { label: "TypeScript app", path: "examples/music-agent/apps/ts-localfirst" },
      { label: "Benchmarks", path: "examples/music-agent/benchmarks" },
    ],
    video: null,
    plannedVideo: "Walkthrough capture is planned: one agent conversation with a tool call.",
    metrics: [],
    plannedMetrics: "Its benchmark variant exists but doesn't run on CodSpeed yet.",
  },
  {
    id: "moon-lander",
    title: "Moon Lander",
    tagline: "A multiplayer game: positions, fuel and inventory synced with no netcode.",
    description:
      "Players descend onto a shared lunar surface, collect fuel, share it with nearby astronauts and launch back into space. Positions, fuel deposits, inventory and chat all sync through Jazz, with no custom networking code.",
    highlights: ["Two players land on the same moon", "Share fuel with a nearby astronaut"],
    sources: [{ label: "React app", path: "examples/moon-lander-react" }],
    video: null,
    plannedVideo: "Walkthrough capture is planned: two players landing and sharing fuel.",
    metrics: [],
    plannedMetrics: "This example has no benchmark variant yet.",
  },
  {
    id: "task-board",
    title: "Team task board",
    tagline: "A permissioned project tracker with boards, task details, comments and activity.",
    description:
      "The W1 workload models a team project tracker: users, projects, 3,000 tasks, 12,000 comments and 9,000 activity rows, with team-inherited read permissions. Its benchmarks time the reads a real board UI makes, and a dashboard that opens one overview plus many independently mounted lists at once.",
    highlights: [
      "Open a project board and a task's detail view",
      "A dashboard mounts dozens of live lists at once",
      "Another team's tasks are invisible, enforced by inherited permissions",
    ],
    sources: [{ label: "Workload and benchmarks", path: "examples/benchmarks/w1" }],
    video: null,
    plannedVideo:
      "A board UI for this workload does not exist yet. The walkthrough follows once it does.",
    metrics: [
      {
        benchmark: "query_board_profile_s_rocksdb",
        label: "Open a project board",
        interpret: (s) =>
          `Filtering and ordering a project's tasks from 3,000 on disk takes ${formatTime(s)}.`,
      },
      {
        benchmark: "query_task_detail_profile_s_rocksdb",
        label: "Open a task",
        interpret: (s) => `Loading a task's detail view (two queries) takes ${formatTime(s)}.`,
      },
      {
        benchmark: "subscription_fanout_memory[(600, 60)]",
        label: "Dashboard with 60 live lists",
        interpret: (s) =>
          `Opening one overview plus 60 permissioned board lists until all have settled takes ${formatTime(s)}, about ${each(61, s)} per subscription.`,
      },
    ],
  },
  {
    id: "big-label",
    title: "BigLabel",
    tagline: "A multi-tenant record-label SaaS: organizations, teams, roles, artists and releases.",
    description:
      "BigLabel is a Next.js app with Better Auth sign-in, where each organization manages its artists and releases under admin-checked membership policies. It exercises what SaaS apps do all day: tenant-scoped lists, relations, cold loads and bulk imports.",
    highlights: [
      "Sign in and land in your own organization",
      "Invite teammates and assign roles",
      "Import a catalogue of releases in bulk",
    ],
    sources: [{ label: "App and benchmarks", path: "examples/big-label" }],
    video: null,
    plannedVideo: "Walkthrough capture is planned: sign-in, team setup and a bulk release import.",
    metrics: [
      {
        benchmark: "ingest_walltime_10k",
        label: "Import 10,000 releases",
        interpret: (s) =>
          `Ten batches of 1,000 releases are inserted in ${formatTime(s)}, about ${rate(10000, s)} rows per second.`,
      },
      {
        benchmark: "ingest_walltime_100k",
        label: "Import 100,000 releases",
        interpret: (s, lookup) => {
          const small = lookup("ingest_walltime_10k");
          const scaling = small
            ? ` (${(s / small).toFixed(1)}× the 10k import for 10× the rows)`
            : "";
          return `A hundred batches of 1,000 take ${formatTime(s)}${scaling}.`;
        },
      },
    ],
  },
  {
    id: "permissioned-resources",
    title: "Permissioned resources",
    tagline: "A deep-permission resource catalogue synced onto a fresh device.",
    description:
      "A synthetic catalogue with deeply inherited access rules, shaped like a real adopter's fixture. The benchmark brings a brand-new device from empty to a fully settled, permission-filtered view through a device-local persistence relay, with every row authorized by the server.",
    highlights: [
      "A fresh device signs in and syncs its visible slice",
      "Access is inherited through parent resources",
      "The view settles once every subscription is complete",
    ],
    sources: [{ label: "Workload and benchmarks", path: "examples/permissioned-resources" }],
    video: null,
    plannedVideo: "This example has no UI yet, so there is nothing to record.",
    metrics: [
      {
        benchmark: "first_sync_local_relay_27518_rocksdb",
        label: "First sync, 27,518 rows",
        interpret: (s) =>
          `A new device goes from empty to a settled view of 27,518 authorized rows across 39 subscriptions in ${formatTime(s)}, about ${rate(27518, s)} rows per second.`,
      },
    ],
  },
  {
    id: "policy-documents",
    title: "Policy-scoped documents",
    tagline: "What row-level security costs: the same page of documents with and without a policy.",
    description:
      "100,000 documents owned by 100 people in 25 organizations. A user may read their own documents and those of organizations they were admitted to. The benchmarks load the first page of 50 documents with the policy and with an explicit allow-all policy, so the difference is the price of authorization.",
    highlights: [
      "Show my documents, newest first",
      "Show my organization's documents",
      "Keep the page live as documents change",
    ],
    sources: [{ label: "Workload and benchmarks", path: "examples/policy-scoped-documents" }],
    video: null,
    plannedVideo: "This example has no UI yet, so there is nothing to record.",
    metrics: [
      {
        benchmark: "owner_or_org_policy_org_page50[100000]",
        label: "Organization page, with policy",
        interpret: (s, lookup) => {
          const free = lookup("policy_free_org_page50[100000]");
          const overhead = free
            ? ` The same page without a policy takes ${formatTime(free)}, so authorization adds ${Math.round((s / free - 1) * 100)}%.`
            : "";
          return `The first 50 of an organization's documents load in ${formatTime(s)}.${overhead}`;
        },
      },
      {
        benchmark: "subscribe_owner_or_org_policy_org_page50[100000]",
        label: "Live organization page",
        interpret: (s) =>
          `Subscribing to the same permissioned page and receiving its first result takes ${formatTime(s)}.`,
      },
    ],
  },
];

export const heroBenchmarkNames = new Set(
  heroExamples.flatMap((example) => example.metrics.map((metric) => metric.benchmark)),
);
