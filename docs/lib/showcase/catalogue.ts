import { formatTime } from "../perf-timeline/model.ts";

/** Looks up another benchmark's headline seconds, for metrics that compare two cases. */
export type Lookup = (benchmarkName: string) => number | null;

export type HeroMetric = {
  /** Exact CodSpeed benchmark name; its reviewed definition lives in dev/benchmarks/metadata. */
  benchmark: string;
  label: string;
  /** One plain-language sentence about what the measured seconds mean for the app. */
  interpret: (seconds: number, lookup: Lookup) => string;
};

export type HeroVideo = {
  src: string;
  poster?: string;
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
      src: "/examples/videos/todo-two-devices.webm",
      poster: "/examples/videos/todo-two-devices.png",
      caption:
        "Two independent browsers (separate origins, storage and identities) syncing through a local Jazz server. Captured automatically with Playwright.",
    },
    metrics: [
      {
        benchmark: "sequential_insert_1350_rocksdb",
        label: "Add a todo",
        interpret: (s) =>
          `Each insert is its own durable transaction and reaches the UI again in about ${each(1350, s)}${s / 1350 < frame ? ", well inside one 60 fps frame" : ""}.`,
      },
      {
        benchmark: "sequential_update_1350_rocksdb",
        label: "Check off a todo",
        interpret: (s) =>
          `A single update, persisted and delivered back to the live list, takes about ${each(1350, s)}.`,
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
