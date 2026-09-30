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
  /**
   * An H.264 MP4 under docs/public, at most MAX_BYTES (scripts/example-videos/encode.mjs).
   * Written by `pnpm --filter docs capture:example-videos` or encoded from a walkthrough recording.
   */
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
  /**
   * Repository-relative directory of the example's benchmark suite. Every
   * benchmark whose metadata source lies under it is listed with the example
   * (the metric cards first, then the rest in a table).
   */
  benchmarks?: string;
  video: HeroVideo | null;
  /** What the placeholder should promise until a capture exists. */
  plannedVideo?: string;
  metrics: HeroMetric[];
  /** Shown instead of metric cards while an example has no wallclock benchmarks on CodSpeed. */
  plannedMetrics?: string;
};

// Interpretations get the /5 estimate, so every time and rate they print
// carries the page's "*" marker. Ratios need none: the divisor cancels out.
const t = (seconds: number) => `${formatTime(seconds)}*`;
const rate = (count: number, seconds: number) =>
  `${Math.round(count / seconds).toLocaleString("en-US")}*`;
const each = (count: number, seconds: number) => t(seconds / count);
// A 60 Hz frame. Only used to phrase a result that is already below it.
const frame = 1 / 60;

export const heroExamples: HeroExample[] = [
  {
    id: "band-chat",
    title: "BandChat",
    tagline: "Private rooms with membership boundaries, attachments and fast resume.",
    description:
      "A Next.js app with Better Auth sign-in, where a room's creator admits or removes members and only members can read or post. Messages carry inline attachments and are created local-first. Its benchmarks cover what a chat does constantly: opening a private room through its membership policy, sending messages, scrolling back, unread rooms by recent activity, a new message while many rooms are open, and resuming a caught-up client.",
    highlights: [
      "Create a room and admit a bandmate",
      "Send a message with an inline attachment",
      "Removing a member stops their later writes",
    ],
    sources: [
      { label: "Next.js app", path: "examples/band-chat/apps/nextjs-betterauth" },
      { label: "Benchmarks", path: "examples/band-chat/benchmarks" },
    ],
    benchmarks: "examples/band-chat/benchmarks",
    video: {
      src: "/examples/videos/band-chat.mp4",
      poster: "/examples/videos/band-chat.jpg",
      caption:
        "A guest asks to join a room, the creator admits them, history appears, and after removal the guest's offline send is rejected.",
    },
    metrics: [
      {
        benchmark: "band_chat_open_room[10000]",
        label: "Open a room",
        interpret: (s) =>
          `A member opens a private room and sees its newest 21 messages, with their senders, in ${t(s)}, with 10,000 messages in the app. Today this grows with the room's whole history, not just the page.`,
      },
      {
        benchmark: "band_chat_send_100[10000]",
        label: "Send a message",
        per: { count: 100, unit: "message" },
        interpret: (s) =>
          `Each message is checked against the room's insert policy, accepted and shown at the top of the open room before the next one is sent. Averaged over 100 messages (${t(s)} total).`,
      },
      {
        benchmark: "band_chat_timeline_second_page[4096]",
        label: "Scroll back in a room",
        interpret: (s) =>
          `Loading the second page of a busy room (25 messages, newest first) takes ${t(s)}, with 4,096 messages across 256 rooms. Today this grows with the whole message table, not just the page.`,
      },
      {
        benchmark: "band_chat_caught_up_fast_resume[10000]",
        label: "Reconnect when up to date",
        interpret: (s, lookup) => {
          const small = lookup("band_chat_caught_up_fast_resume[100]");
          const scale = small ? ` With 100 messages it takes ${t(small)}.` : "";
          return `A client that has already seen all 10,000 messages reconnects in ${t(s)}: the server confirms it is current without resending any message.${scale}`;
        },
      },
    ],
  },
  {
    id: "stage-plan",
    title: "StagePlan",
    tagline: "A stage crew's task board: shows, departments, live lists and permissions.",
    description:
      "A crew prepares shows together: each show has a board of stage-prep tasks with discussion and an activity log, and every crew member's dashboard mounts dozens of live department lists at once. Every write lands locally first and syncs in the background. Crews only see their own shows, enforced by inherited row-level permissions, and tasks can be archived and restored.",
    highlights: [
      "Two crew members add and check off tasks and see each other's changes live",
      "Moving a card to Done updates every open filtered view",
      "A dashboard mounts dozens of live department lists at once",
      "Another crew's shows are invisible, enforced by inherited permissions",
    ],
    sources: [
      { label: "App", path: "examples/stage-plan" },
      { label: "Benchmarks", path: "examples/stage-plan/benchmarks" },
    ],
    benchmarks: "examples/stage-plan/benchmarks",
    video: {
      src: "/examples/videos/stage-plan.mp4",
      poster: "/examples/videos/stage-plan.jpg",
      caption:
        "A crew chief and a crew member in two browsers: the invite link, card moves on each other's board, and edits made with Sync off arriving once it's back on.",
    },
    metrics: [
      {
        benchmark: "stage_plan_add_task_1350",
        label: "Add a task",
        per: { count: 1350, unit: "task" },
        interpret: (s) =>
          `Each task is its own durable transaction, persisted and delivered back to the live list${s / 1350 < frame ? " well inside one 60 fps frame" : ""}. Averaged over 1,350 additions in a row (${t(s)} total).`,
      },
      {
        benchmark: "stage_plan_open_board",
        label: "Open a show's board",
        interpret: (s) => `Filtering and ordering a show's tasks from 3,000 on disk takes ${t(s)}.`,
      },
      {
        benchmark: "stage_plan_move_card_to_done",
        label: "Move a card to Done",
        interpret: (s) =>
          `Changing a task's status so it leaves one live filtered view and enters another, until the view has the change, takes ${t(s)}.`,
      },
      {
        benchmark: "stage_plan_crew_dashboard[(600, 60)]",
        label: "Crew dashboard with 60 live lists",
        interpret: (s) =>
          `Opening one overview plus 60 permissioned department lists until all have settled takes ${t(s)}, about ${each(61, s)} per subscription.`,
      },
    ],
  },
  {
    id: "band-book",
    title: "BandBook",
    tagline: "A Notion-style workspace with pages and issues, scoped by row-level policies.",
    description:
      "A band's shared notebook: members write pages, track issues, and belong to workspaces. A member may read their own pages plus those of every workspace they were admitted to. Its benchmarks load the newest 50 pages of a 100,000-page notebook under an explicit allow-all policy, an owner-only policy and the own-or-workspace policy, so the difference is the price of authorization.",
    highlights: [
      "Show my pages, newest first",
      "Show my workspace's pages and issues",
      "Keep the page list live as pages change",
    ],
    sources: [
      { label: "App", path: "examples/band-book" },
      { label: "Benchmarks", path: "examples/band-book/benchmarks" },
    ],
    benchmarks: "examples/band-book/benchmarks",
    video: {
      src: "/examples/videos/band-book.mp4",
      poster: "/examples/videos/band-book.jpg",
      caption:
        'A bandmate shares one song with a "Can edit" link; the guest sees only that song and its subpage, and typing shows up in both copies live.',
    },
    metrics: [
      {
        benchmark: "band_book_workspace_pages[100000]",
        label: "Workspace pages, with policy",
        interpret: (s, lookup) => {
          const free = lookup("band_book_workspace_pages_unrestricted[100000]");
          const overhead = free
            ? ` The same list under an allow-all policy takes ${t(free)}, so authorization adds ${Math.round((s / free - 1) * 100)}%.`
            : "";
          return `The newest 50 of a workspace's pages load in ${t(s)} from 100,000 pages.${overhead}`;
        },
      },
      {
        benchmark: "band_book_workspace_pages_live[100000]",
        label: "Live workspace page list",
        interpret: (s) =>
          `Subscribing to the same permissioned list and receiving its first result takes ${t(s)}.`,
      },
    ],
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
    benchmarks: "examples/world-tour/benchmarks",
    video: {
      src: "/examples/videos/world-tour.mp4",
      poster: "/examples/videos/world-tour.jpg",
      caption:
        "The tour manager sees all 12 stops while a fan with the public link sees only the confirmed ones; a stop the manager confirms appears on the fan's globe live.",
    },
    metrics: [
      {
        benchmark: "world_tour_public_calendar_window[4096]",
        label: "Open the public calendar",
        interpret: (s) =>
          `The first 12 stops of a fan's next three weeks (confirmed stops only, each with its venue) load in ${t(s)} from a tour of 4,096 stops. Today this grows with the whole tour, not just the window.`,
      },
    ],
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
    benchmarks: "examples/wequencer/benchmarks",
    video: {
      src: "/examples/videos/wequencer.mp4",
      poster: "/examples/videos/wequencer.jpg",
      caption:
        "Two bandmates in one session: pattern edits, Play, tempo and mutes follow on both screens.",
    },
    metrics: [
      {
        benchmark: "wequencer_open_pattern",
        label: "Open a pattern",
        interpret: (s) =>
          `Opening a 16-track, 64-step pattern (17 live subscriptions, 1,024 pads) until every one has its first result takes ${t(s)}.`,
      },
      {
        benchmark: "wequencer_toggle_pad",
        label: "Toggle a pad",
        interpret: (s) =>
          `Flipping one pad on a live grid, until that track's subscription delivers the change, takes ${t(s)}. Syncing it to bandmates isn't included.`,
      },
      {
        benchmark: "wequencer_open_pattern_views[100]",
        label: "100 bandmates open their patterns",
        interpret: (s) =>
          `100 pattern views of the same query shape, each bound to a different pattern, open and hydrate in ${t(s)}, about ${each(100, s)} per view.`,
      },
    ],
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
    benchmarks: "examples/poster-shop/benchmarks",
    video: {
      src: "/examples/videos/poster-shop.mp4",
      poster: "/examples/videos/poster-shop.jpg",
      caption:
        "A second editor joins by invite link; her cursor and edits arrive live, then an image upload, a checkpoint and a reload with everything kept.",
    },
    metrics: [
      {
        benchmark: "poster_shop_open_canvas[4096]",
        label: "Open a poster",
        interpret: (s) =>
          `Opening a 4,096-shape poster, with its shapes, layers, cursors, asset shelf and checkpoints each subscribed live, takes ${t(s)} until all five have their first result.`,
      },
      {
        benchmark: "poster_shop_add_shape[4096]",
        label: "Draw a shape",
        interpret: (s) =>
          `Adding one shape to that live canvas, until the canvas subscription delivers it, takes ${t(s)}. Today this grows with the number of shapes on the canvas.`,
      },
      {
        benchmark: "poster_shop_move_cursor[4096]",
        label: "A collaborator's cursor moves",
        interpret: (s) =>
          `A cursor update reaches the live cursor subscription in ${t(s)}, without waking the 4,096-shape canvas.`,
      },
    ],
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
    benchmarks: "examples/record-player/benchmarks",
    video: null,
    plannedVideo: "Walkthrough capture is planned: browsing albums and sharing a playlist.",
    metrics: [
      {
        benchmark: "record_player_open_coverflow[4096]",
        label: "Open the library",
        interpret: (s) =>
          `Opening CoverFlow (a 20-album shelf plus the focused album's tracks) from a 4,096-track library takes ${t(s)}, without loading any audio.`,
      },
      {
        benchmark: "record_player_add_to_playlist[4096]",
        label: "Add a track",
        interpret: (s) =>
          `Inserting a track into the visible part of a live 4,096-track playlist, until the window delivers it, takes ${t(s)}.`,
      },
      {
        benchmark: "record_player_scrub_track_64mb",
        label: "Scrub a track",
        interpret: (s) =>
          `Reading 64 KiB from the middle of a 64 MiB track, as the player does when you drag the playhead, takes ${t(s)}. Today this grows with the whole track's size.`,
      },
    ],
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
    benchmarks: "examples/epic-drop/benchmarks",
    video: {
      src: "/examples/videos/epic-drop.mp4",
      poster: "/examples/videos/epic-drop.jpg",
      caption:
        'Uploads and previews in a shared folder; a second account joins by "Can edit" link, and uploads and renames sync both ways.',
    },
    metrics: [
      {
        benchmark: "epic_drop_upload_64mb",
        label: "Upload a 64 MiB file",
        interpret: (s) =>
          `Streaming a 64 MiB file into a folder until it is stored locally takes ${t(s)}, about ${rate(64, s)} MiB per second.`,
      },
      {
        benchmark: "epic_drop_folder_listing_100_files",
        label: "List a folder",
        interpret: (s) =>
          `Listing a folder of 100 files by name, with each file's type and size, takes ${t(s)}. Today this still grows with the size of the files, not just their number.`,
      },
      {
        benchmark: "epic_drop_download_4mb",
        label: "Download a 4 MiB file",
        interpret: (s) =>
          `Reading a whole 4 MiB file back from storage takes ${t(s)}, about ${rate(4, s)} MiB per second.`,
      },
    ],
  },
  {
    id: "jamazon",
    title: "Jamazon",
    tagline: "An instrument storefront: catalogue, search, cart and checkout.",
    description:
      "The shop front of the Jamazon instrument store: browse and search a large catalogue, keep a cart that follows you across devices, and check out against live stock. It shares its data with the Jamazon Warehouse operations console.",
    highlights: [
      "Browse and search the catalogue",
      "A cart that follows you across devices",
      "Check out against live stock levels",
    ],
    sources: [{ label: "App", path: "examples/jamazon" }],
    video: {
      src: "/examples/videos/jamazon.mp4",
      poster: "/examples/videos/jamazon.jpg",
      caption:
        "A guest cart carried into a new account, a quantity change arriving from a second device, an offline edit, then checkout and the order's timeline updating live.",
    },
    metrics: [],
    plannedMetrics:
      "Storefront benchmarks (catalogue browsing and search, cart sync) will be added here once they measure an area no other example owns. Checkout is measured by Jamazon Warehouse.",
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
    benchmarks: "examples/jamazon-warehouse/benchmarks",
    video: null,
    plannedVideo: "Walkthrough capture is planned: checkout and the live stock console.",
    metrics: [
      {
        benchmark: "jamazon_checkout_100",
        label: "Check out",
        per: { count: 100, unit: "checkout" },
        interpret: (s) =>
          `Each checkout is one transaction that reads and updates stock, the district's order counter and the customer's balance, then records the order and its payment. Averaged over 100 checkouts in a row (${t(s)} total). Today this grows with order history.`,
      },
      {
        benchmark: "jamazon_pending_orders_10k",
        label: "Load pending orders",
        interpret: (s) =>
          `The console's first page, the district's 20 oldest pending orders, loads in ${t(s)} from a history of 10,000 orders. Today this grows with order history, not just the page.`,
      },
    ],
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
    benchmarks: "examples/music-agent/benchmarks",
    video: null,
    plannedVideo: "Walkthrough capture is planned: one agent conversation with a tool call.",
    metrics: [
      {
        benchmark: "music_agent_stream_reply_1000_chunks",
        label: "Stream a reply",
        per: { count: 1000, unit: "chunk" },
        interpret: (s) =>
          `Each streamed chunk is appended to a long reply and stored locally. Averaged over 1,000 chunks (${t(s)} total). Today each append grows with the reply's size.`,
      },
      {
        benchmark: "music_agent_open_transcript_200_turns",
        label: "Open a conversation",
        interpret: (s) =>
          `Reading a 200-turn conversation in order, including a 128 KiB streamed reply, takes ${t(s)}.`,
      },
      {
        benchmark: "music_agent_reopen_transcript_200_turns",
        label: "Reopen after a restart",
        interpret: (s) =>
          `Reopening storage after an app restart and reading the same conversation takes ${t(s)}, including the fixed cost of opening the database.`,
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
    benchmarks: "examples/big-label/benchmarks",
    video: {
      src: "/examples/videos/big-label.mp4",
      poster: "/examples/videos/big-label.jpg",
      caption:
        "An admin adds a viewer by email; the label appears in the viewer's menu live, read-only, and a new artist shows up without a reload.",
    },
    metrics: [
      {
        benchmark: "big_label_label_load[4096]",
        label: "Open a label's releases",
        interpret: (s) =>
          `A label page loads all 512 of its releases, newest first, in ${t(s)}, out of 4,096 releases across 8 labels.`,
      },
      {
        benchmark: "big_label_releases_live_view_100k",
        label: "Live view in a huge table",
        interpret: (s) =>
          `A label's live view of its newest 50 releases opens in ${t(s)} from a table holding 100,000 releases of every tenant, following the label index instead of scanning the table.`,
      },
      {
        benchmark: "ingest_walltime_100k",
        label: "Import 100,000 releases",
        interpret: (s) =>
          `A hundred batches of 1,000 releases are inserted in ${t(s)}, about ${rate(100000, s)} rows per second.`,
      },
    ],
  },
];

/** "More benchmarks": the areas no hero example owns. */
export type BenchmarkSection = {
  id: string;
  title: string;
  description: string;
  /** Repository-relative directories whose metadata sources belong here. */
  sources: string[];
  /**
   * Benchmarks without metadata (the engine benches) listed here, by CodSpeed
   * benchmark id: one bench name repeats once per scenario module, so a name
   * alone does not identify a row.
   */
  benchmarks?: readonly EngineBenchmark[];
};

/** One engine row: a CodSpeed benchmark id and the scenario that tells it apart. */
export type EngineBenchmark = {
  /** CodSpeed's benchmark id, stable for the URI below. */
  id: string;
  /** CodSpeed name, shared by every scenario of the same bench. */
  name: string;
  scenario: string;
  /** CodSpeed URI: `crates/groove/benches/<bench>.rs::<scenario module>::<name>`. */
  uri: string;
};

const scenarioLabels: Record<string, string> = {
  author_posts: "Author posts",
  feed: "Feed",
  feed_top20: "Top-20 feed",
  tasks: "Tasks",
};

function engineBenchmark(id: string, bench: string, module: string, name: string): EngineBenchmark {
  return {
    id,
    name,
    scenario: scenarioLabels[module],
    uri: `crates/groove/benches/${bench}.rs::${module}::${name}`,
  };
}

/**
 * The Groove cases CodSpeed measures on every merge, and only those: the IVM
 * engines at the measured size. Smaller sizes and the reference engines run
 * in the nightly CodSpeed run (`GROOVE_BENCH_SWEEP=1`); they are not listed,
 * so a size that stops being measured is not shown with a frozen number.
 */
export const engineBenchmarks: readonly EngineBenchmark[] = [
  engineBenchmark(
    "6ab494d0ad9a6239bfdd8982",
    "pull_vs_snapshot",
    "author_posts",
    "prepared_warm[5000]",
  ),
  engineBenchmark("6ab494d0ad9a6239bfdd8992", "pull_vs_snapshot", "feed", "prepared_warm[5000]"),
  engineBenchmark(
    "6ab494d0ad9a6239bfdd898a",
    "pull_vs_snapshot",
    "feed_top20",
    "prepared_warm[5000]",
  ),
  engineBenchmark(
    "6ab494d0ad9a6239bfdd8980",
    "pull_vs_snapshot",
    "author_posts",
    "prepared_cold[5000]",
  ),
  engineBenchmark("6ab494d0ad9a6239bfdd8990", "pull_vs_snapshot", "feed", "prepared_cold[5000]"),
  engineBenchmark(
    "6ab494d0ad9a6239bfdd8988",
    "pull_vs_snapshot",
    "feed_top20",
    "prepared_cold[5000]",
  ),
  engineBenchmark("6ab4a076ad9a6239bfddd0c7", "steady_state", "feed", "ivm[100]"),
  engineBenchmark("6ab4a076ad9a6239bfddd0bd", "steady_state", "feed_top20", "ivm[100]"),
  engineBenchmark("6ab4a076ad9a6239bfddd0d1", "steady_state", "tasks", "ivm[100]"),
];

export const moreBenchmarkSections: BenchmarkSection[] = [
  {
    id: "adopter-workloads",
    title: "Anonymized adopter workloads",
    description:
      "Synthetic fixtures shaped like real adopters' schemas and data, with invented names and values. Permissioned resources brings a brand-new device from empty to a fully settled, permission-filtered view of a deep-permission resource catalogue through a device-local persistence relay, with every row authorized by the server.",
    sources: ["examples/permissioned-resources/"],
  },
  {
    id: "engine",
    title: "Engine",
    description:
      "What no product owns: Groove's incremental view maintenance measured directly, for one-shot reads through prepared shapes (author posts, feed and top-20 feed) and for keeping 100 live subscriptions current as writes arrive (feed, top-20 feed and tasks). The reference engines they are compared against (SQLite re-query, hand-written pull plans, snapshot re-runs) and the smaller sizes run in the nightly CodSpeed run, not on every merge.",
    sources: ["crates/"],
    benchmarks: engineBenchmarks,
  },
];

/**
 * Where the examples page lists a benchmark with metadata: the id of the hero
 * example whose suite holds its metadata source, the id of a "More benchmarks"
 * section, or null for a result no current suite produces (a retired name
 * still in the CodSpeed history). Engine rows have no metadata and are listed
 * by id instead (`BenchmarkSection.benchmarks`).
 */
export function placeBenchmark(source: string | undefined): string | null {
  if (!source) return null;
  const hero = heroExamples.find(
    (example) => example.benchmarks && source.startsWith(`${example.benchmarks}/`),
  );
  if (hero) return hero.id;
  const section = moreBenchmarkSections.find((candidate) =>
    candidate.sources.some((prefix) => source.startsWith(prefix)),
  );
  return section?.id ?? null;
}

/** A row of a benchmark table: a benchmark and, for engine rows, its scenario. */
export type Placed<E> = E & { scenario?: string };

/**
 * Every current benchmark that is not a metric card, grouped by the hero
 * example or "More benchmarks" section that lists it, sorted by name (then
 * scenario). `byName` holds the newest result per name for benchmarks with
 * metadata; engine rows come from `byId`, one per catalogued id. Retired
 * names and unmeasured engine sizes are left out.
 */
export function groupBenchmarks<E extends { bench: { id: string; name: string } }>(
  byName: ReadonlyMap<string, E>,
  byId: ReadonlyMap<string, E>,
  sourceOf: (name: string) => string | undefined,
): Map<string, Placed<E>[]> {
  const grouped = new Map<string, Placed<E>[]>();
  const add = (place: string, entry: Placed<E>) =>
    grouped.set(place, [...(grouped.get(place) ?? []), entry]);
  for (const [name, entry] of byName) {
    if (heroBenchmarkNames.has(name)) continue;
    const place = placeBenchmark(sourceOf(name));
    if (place) add(place, entry);
  }
  for (const section of moreBenchmarkSections) {
    for (const row of section.benchmarks ?? []) {
      const entry = byId.get(row.id);
      if (entry) add(section.id, { ...entry, scenario: row.scenario });
    }
  }
  for (const entries of grouped.values())
    entries.sort(
      (a, b) =>
        a.bench.name.localeCompare(b.bench.name) ||
        (a.scenario ?? "").localeCompare(b.scenario ?? ""),
    );
  return grouped;
}

export const heroBenchmarkNames = new Set(
  heroExamples.flatMap((example) => example.metrics.map((metric) => metric.benchmark)),
);
