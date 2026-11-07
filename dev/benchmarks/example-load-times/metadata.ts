import type { BenchmarkMetadata } from "../metadata/types.ts";

type Kind = "cold" | "reload" | "invite" | "live" | "flow";

type Case = {
  app: string;
  /** The scenario's key in scenarios/<app>.mjs. */
  scenario: string;
  kind: Kind;
  /** What the user does, then what they wait to see. */
  title: string;
  /** The user action that starts the timer. */
  start: string;
  /** What must be visible (or true) when the timer stops. */
  done: string;
  /** Untimed state this case starts from, beyond the app's fixture users. */
  from: string;
};

const c = (
  app: string,
  scenario: string,
  kind: Kind,
  title: string,
  start: string,
  done: string,
  from: string,
): Case => ({ app, scenario, kind, title, start, done, from });

const signedUp = "A fixture owner, signed up and on the measured content";
const invitee = "The invited fixture member, on the shared content";
const pair = "The owner and the invited member, each on the shared content in their own browser";

// The docs examples page's load-time scenarios, in its order.
export const loadTimeCases: Case[] = [
  c(
    "band-book",
    "cold_signup",
    "cold",
    "Sign up and see the starter band",
    "Submit the filled sign-up form",
    'The seeded "Setlist: spring tour" page in the sidebar',
    "A fresh browser on the filled sign-up form",
  ),
  c(
    "band-book",
    "reload",
    "reload",
    "Reload and see the page",
    "Reload the page",
    'The "Harbour lights" page text containing "Key of D"',
    signedUp,
  ),
  c(
    "band-book",
    "invite",
    "invite",
    "Open an invite and see the shared content",
    "Navigate to the owner's edit link",
    'The shared page text containing "Key of D"',
    "A newly signed-up member on their own workspace; the owner's edit link to the page",
  ),
  c(
    "band-book",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the page",
    "Reload the page",
    'The shared page text containing "Key of D"',
    invitee,
  ),
  c(
    "band-book",
    "live_edit",
    "live",
    "An edit appears on another device",
    "Type a few words into the owner's text block",
    "The words in the invited member's text block",
    pair,
  ),

  c(
    "band-chat",
    "cold_signup",
    "cold",
    "Sign up until the app is ready",
    "Submit the filled sign-up form",
    '"Set up your profile"',
    "A fresh browser on the filled sign-up form",
  ),
  c(
    "band-chat",
    "reload",
    "reload",
    "Reload and see the latest message",
    "Reload the room",
    "The room's first message",
    signedUp,
  ),
  c(
    "band-chat",
    "invite_prompt",
    "invite",
    "Open an invite and see the join prompt",
    "Navigate to the room link",
    'The "Ask to join" button',
    "A newly signed-up user on their room list",
  ),
  c(
    "band-chat",
    "invite_admit",
    "invite",
    "Get admitted and see the room",
    "The owner clicks Admit",
    "The room's first message on the new member's screen",
    "The new user is waiting to be admitted; the owner's invite panel shows the request",
  ),
  c(
    "band-chat",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the latest message",
    "Reload the room",
    "The room's first message",
    invitee,
  ),
  c(
    "band-chat",
    "live_message",
    "live",
    "A message appears on another device",
    "The owner sends a message",
    "The message on the invited member's screen",
    pair,
  ),

  c(
    "big-label",
    "cold_signup",
    "cold",
    "Sign up and see the overview",
    "Submit the filled sign-up form",
    'The "Overview" navigation link',
    "A fresh browser on the filled sign-up form",
  ),
  c(
    "big-label",
    "reload",
    "reload",
    "Reload and see the releases",
    "Reload the releases table",
    "A release row",
    "A fixture owner with the small demo data set, on the Low Tide Records releases table",
  ),
  c(
    "big-label",
    "invite_pick_label",
    "invite",
    "Invited member picks the label and sees its releases",
    "Pick Low Tide Records in the label menu",
    "A release row",
    "A newly signed-up user added to the label as a viewer, with the label menu open",
  ),
  c(
    "big-label",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the releases",
    "Reload the releases table",
    "A release row",
    invitee,
  ),

  c(
    "epic-drop",
    "cold",
    "cold",
    "First visit until the folders show",
    "Navigate to the app",
    '"No folders yet"',
    "A fresh browser",
  ),
  c(
    "epic-drop",
    "reload",
    "reload",
    "Reload and see the files",
    "Reload the folder",
    'The "set-list.txt" file',
    "A fixture owner on a folder with a set list, a cover image and 3 MB of stems",
  ),
  c(
    "epic-drop",
    "invite_join",
    "invite",
    "Join through an invite and see the shared content",
    'Click "Join folder"',
    'The "set-list.txt" file',
    "A new visitor on the owner's edit link, with the join button enabled",
  ),
  c(
    "epic-drop",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the files",
    "Reload the folder",
    'The "set-list.txt" file',
    invitee,
  ),
  c(
    "epic-drop",
    "upload_3mb",
    "flow",
    "Upload a 3 MB file until it is stored",
    "Hand the folder the walkthrough's upload: 3 MB of stems, a 4,000-line set list and a cover image",
    "The stems are listed and the upload queue has drained",
    "A new visitor on a new, empty folder",
  ),
  c(
    "epic-drop",
    "live_upload",
    "live",
    "A change appears on another device",
    "The owner adds a small text file",
    "The file in the invited member's file list",
    pair,
  ),

  c(
    "jamazon",
    "cold_search",
    "cold",
    "First anonymous visit until search is usable",
    "Navigate to the store",
    "The search box",
    "A fresh browser",
  ),
  c(
    "jamazon",
    "cold_first_product",
    "cold",
    "First anonymous visit until a product shows",
    "Navigate to the store",
    "A product link",
    "A fresh browser",
  ),
  c(
    "jamazon",
    "cold_signup",
    "cold",
    "Sign up and see the store",
    "Submit the filled sign-up form",
    "A product link, or the empty cart",
    "A browser that browsed the store anonymously, on the filled sign-up form",
  ),
  c(
    "jamazon",
    "second_device_cart",
    "flow",
    "Open a second device and see the cart",
    "Navigate to the cart",
    '"Your cart is empty"',
    "A second browser just signed in to a new account",
  ),
  c(
    "jamazon",
    "return_visit",
    "reload",
    "Return visit until a product shows",
    "Navigate to the store",
    "A product link",
    "A signed-in fixture customer with a snare in the cart, on the cart page",
  ),
  c(
    "jamazon",
    "cart_reload",
    "reload",
    "Reload and see the cart",
    "Reload the cart",
    "The snare's quantity field",
    "A signed-in fixture customer with a snare in the cart, on the cart page",
  ),

  c(
    "poster-shop",
    "cold_signup",
    "cold",
    "Sign up and see the poster",
    "Submit the filled sign-up form",
    "The starter poster's second ellipse",
    "A fresh browser on the filled sign-up form",
  ),
  c(
    "poster-shop",
    "reload",
    "reload",
    "Reload and see the poster",
    "Reload the editor",
    "The poster's sun",
    signedUp,
  ),
  c(
    "poster-shop",
    "invite",
    "invite",
    "Open an invite and see the shared poster",
    "Navigate to a new single-use invite link",
    "The owner's sun on the invited member's canvas",
    "A newly signed-up user on their own poster",
  ),
  c(
    "poster-shop",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the poster",
    "Reload the editor",
    "The shared poster's sun",
    invitee,
  ),
  c(
    "poster-shop",
    "live_edit",
    "live",
    "An edit appears on another device",
    "The owner picks a new colour for the sun",
    "The sun's new colour on the invited member's canvas",
    pair,
  ),

  c(
    "record-player",
    "cold_signup",
    "cold",
    "Sign up and see the library",
    "Submit the sign-up form",
    "The library: empty, or with the demo albums",
    "A fresh browser on the filled sign-in form",
  ),
  c(
    "record-player",
    "reload",
    "reload",
    "Reload and see the albums",
    "Reload the library",
    'The "Sine studies" album',
    "A fixture listener with the demo library added",
  ),
  c(
    "record-player",
    "second_user_signup",
    "cold",
    "Second user signs up and sees the shared library",
    "Submit the sign-up form",
    'The "Sine studies" album',
    "A fresh browser on the filled sign-in form; the demo library already exists",
  ),

  c(
    "stage-plan",
    "cold",
    "cold",
    "First visit until the demo show opens",
    "Navigate to the app",
    "The demo show's link",
    "A fresh browser",
  ),
  c(
    "stage-plan",
    "reload",
    "reload",
    "Reload and see the board",
    "Reload the board",
    'The "Soundcheck with the band" card',
    "A fixture crew chief on the demo show's board",
  ),
  c(
    "stage-plan",
    "invite",
    "invite",
    "Open an invite and see the show",
    "Navigate to the invite link",
    'The "Soundcheck with the band" card',
    "A fresh browser; the chief's invite link",
  ),
  c(
    "stage-plan",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the board",
    "Reload the board",
    'The "Soundcheck with the band" card',
    invitee,
  ),
  c(
    "stage-plan",
    "live_edit",
    "live",
    "An edit appears on another device",
    "The chief adds a task",
    "The task's card on the invited member's board",
    pair,
  ),

  c(
    "wequencer",
    "cold_signup",
    "cold",
    "Sign up until a session can be started",
    "Submit the filled sign-up form",
    'The "New session" button',
    "A fresh browser on the filled sign-up form",
  ),
  c(
    "wequencer",
    "reload",
    "reload",
    "Reload and see the pads",
    "Reload the session",
    'The "Kick, step 1" pad',
    "A fixture owner in a new session",
  ),
  c(
    "wequencer",
    "invite_open",
    "invite",
    "Open an invite and see the shared pads",
    "Open the shared session from the dashboard",
    'The "Kick, step 1" pad',
    "A newly signed-up user, added as a collaborator, with the session listed on their dashboard",
  ),
  c(
    "wequencer",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the pads",
    "Reload the session",
    'The "Kick, step 1" pad',
    invitee,
  ),

  c(
    "world-tour",
    "cold",
    "cold",
    "First visit until the whole tour shows",
    "Navigate to the app",
    "All 12 demo stops on the globe",
    "A fresh browser and a fresh sync server, so this visitor claims the demo band",
  ),
  c(
    "world-tour",
    "reload",
    "reload",
    "Reload and see the whole tour",
    "Reload the app",
    "All the band's stops",
    "The fixture owner of the demo band",
  ),
  c(
    "world-tour",
    "public_link",
    "flow",
    "Open a public link and see the poster",
    "Navigate to the band's public link",
    "The poster dialog",
    "A fresh browser",
  ),
  c(
    "world-tour",
    "invite_prompt",
    "invite",
    "Open an invite and see the join dialog",
    "Navigate to the invite link",
    "The join dialog",
    "A fresh browser",
  ),
  c(
    "world-tour",
    "invite_join",
    "invite",
    "Join the tour and see its stops",
    'Click "Join band"',
    "All the band's stops",
    "A fresh browser on the join dialog, with a name filled in",
  ),
  c(
    "world-tour",
    "invitee_reload",
    "reload",
    "Invited member reloads and sees the tour",
    "Reload the app",
    "All the band's stops",
    invitee,
  ),
  c(
    "world-tour",
    "live_edit",
    "live",
    "An edit appears on another device",
    "The owner renames the band",
    "The new name on the invited member's screen",
    pair,
  ),
];

/** The CodSpeed name: `<app>_<scenario>_browser`, with underscores. */
export const benchmarkName = ({ app, scenario }: Pick<Case, "app" | "scenario">) =>
  `${app.replaceAll("-", "_")}_${scenario}_browser`;

const label = (app: string) =>
  app
    .split("-")
    .map((w) => w[0]!.toUpperCase() + w.slice(1))
    .join("");

export const loadTimeBenchmarks: BenchmarkMetadata[] = loadTimeCases.map((m) => ({
  name: benchmarkName(m),
  harness: "vitest",
  title: `${label(m.app)} · ${m.title.toLowerCase()} (production build)`,
  description: `${m.start}, then wait until this is visible: ${m.done}. Drives the example's production build in Chromium through Playwright; one user action per iteration.`,
  fixture: m.from,
  storage:
    "Chromium with a fresh profile per new device; the app's production server (next start or vite preview) behind a local CDN-like front serving brotli-compressed hashed assets with immutable caching; a localhost JazzServer with a fresh on-disk data dir running the app's own schema and permissions",
  includes: [
    m.kind === "live"
      ? "The write on one device"
      : "The user action and everything the app does after it",
    m.kind === "cold"
      ? "Page load from an empty browser cache, WASM download and compile, and the first sync"
      : m.kind === "reload"
        ? "Page reload with a warm browser cache and the app's local data"
        : m.kind === "live"
          ? "Sync through the local server to the other browser and its re-render"
          : "Navigation, authentication and sync as the app does them",
    "Playwright observation of the awaited content",
  ],
  excludes: [
    "App builds, asset precompression, server startup and browser launch",
    "Fixture sign-ups and every step that sets up the starting state",
    "Network latency: everything runs on localhost",
  ],
  work: {
    count: 1,
    unit: m.kind === "live" ? "synced edits/s" : "user flows/s",
    explanation: "One complete user flow per iteration.",
  },
  source: `dev/benchmarks/example-load-times/scenarios/${m.app}.mjs`,
}));
