# Walkthrough videos

The walkthrough videos on the homepage and the examples page are recorded from
the real example apps, running locally, with nothing mocked. Each video is
described by a **storyboard**, and one command renders any or all of them:

```sh
pnpm build:core                                   # once: jazz-tools, WASM and NAPI
cd docs
pnpm render:walkthroughs                          # all of them
pnpm render:walkthroughs jamazon band-chat        # some of them
pnpm render:walkthroughs --list                   # the ids
```

Each run writes `public/examples/videos/<id>.mp4` and `<id>.jpg` (the poster).
Commit the videos together with the storyboard change that produced them.

Rendering needs ffmpeg and a Chromium for Playwright (`CHROMIUM_PATH` picks a
specific build). With `DEBUG_DIR` set, a failed run leaves a screenshot of
every device there. `WALK_TRACE=1` logs each beat and caption with its time.

## Files

| File                              | What it is                                                                             |
| --------------------------------- | -------------------------------------------------------------------------------------- |
| `walkthroughs/<id>.storyboard.ts` | The story: devices, then the beats in order, with captions and holds. Plain data.      |
| `walkthroughs/<id>.mjs`           | The app: which example and dev server to start, and the named actions the beats call.  |
| `storyboard.ts`                   | The storyboard format (TypeScript types).                                              |
| `walkthrough.mjs`                 | Plays a storyboard: starts the server, the stage and the devices, then encodes.        |
| `stage.mjs`                       | The stage: device frames, the Wi-Fi switch, subtitles, title cards and the capture.    |
| `encode.mjs`                      | The MP4 and poster encoder, under a size budget (`MAX_BYTES`), and dev server helpers. |
| `render.mjs`                      | `pnpm render:walkthroughs`.                                                            |

The ids are `band-book`, `band-chat`, `big-label`, `epic-drop`, `jamazon`,
`poster-shop`, `stage-plan`, `stage-plan-two-devices` (the homepage clip),
`wequencer` and `world-tour`.

## The stage

Every video uses the same stage, so they look alike and stay that way.

- **Devices.** Each person is a device with their own browser context: their
  own storage, account and Jazz client. A laptop is drawn with a menu bar (its
  owner's name, Wi-Fi icon and clock) and a slim browser toolbar (the
  storyboard's `address`). A phone has a status bar and no toolbar, and runs as
  a touch device. Each device has its own cursor colour.
- **Layout.** One device fills the stage (`full`), two sit side by side
  (`split`, with the app laid out at `1 / scale` of its box), or devices go at
  explicit boxes (`show`), as with Jamazon's laptop and phone.
- **Wi-Fi, outside the app.** The stage's own cursor opens the device's Wi-Fi
  menu in its menu bar and flips the switch. This really cuts the network:
  each device reaches the servers through its own small local proxy, and
  turning Wi-Fi off drops its open connections, including the sync WebSocket,
  and refuses new ones until it is turned back on. The browser context also
  goes offline, so the app sees `navigator.onLine === false`. Ports in
  `keepPorts` (a Vite dev server's) stay reachable, because Vite's hot-reload
  client would otherwise reload the page when it reconnects. A device that is
  offline gets a red outline.
- **Subtitles** sit in their own band below the devices, in a rounded,
  bordered box, so they never cover an app. The homepage clip uses larger
  subtitles (`captionSize: 30`) because it plays at about half size.
- **Title cards** fill the stage with the app's name and one line about it.
- **Look.** Every device opens in dark mode, on a pure black background. The
  examples-page videos are 1280×892 and the homepage one 1280×926: an 800 px
  device area plus the subtitle band.
- **Capture.** During the run, each device's screen is streamed and its frames
  are saved with the time they arrived. Every change to the stage (a
  subtitle, a title card, the Wi-Fi menu, the stage cursor) is logged with
  its time too. Afterwards the stage is rebuilt offline and stepped frame by
  frame at 30 fps: each frame shows each device's latest screen and replays
  the logged changes. Recording the stage live instead competed with the apps
  for the CPU and dropped motion to a few frames a second.
- **WebGL** runs in software only for World Tour's globe
  (`stage: { webgl: true }`), because it slows every page down.
- **Poster.** The `{ poster: true }` beat picks the poster frame; without one,
  the poster is the last frame.

## Storyboards

A storyboard is a list of beats. Each beat does one thing:

| Beat                                       | Does                                                   |
| ------------------------------------------ | ------------------------------------------------------ |
| `{ title, text, hold }`                    | A title card for `hold` ms.                            |
| `{ caption, hold }`                        | Shows a subtitle, then waits `hold` ms. `""` hides it. |
| `{ do, on, args }`                         | Runs the named action from `<id>.mjs` on device `on`.  |
| `{ wait }`                                 | Waits, in ms.                                          |
| `{ wifi: "off" \| "on", on }`              | Flips a device's Wi-Fi from its menu bar.              |
| `{ full }`, `{ split, scale }`, `{ show }` | Lays the devices out.                                  |
| `{ poster: true }`                         | The poster frame is now.                               |
| `{ see, on }`                              | Waits until the text is visible on the device.         |
| `{ notSee, on, because }`                  | Fails the recording if the text is on the device.      |

Beats run in three groups: `offCamera` (sign-ups, seeding, compiling routes),
`opening` (the first layout, before the video starts) and `beats` (the video).
A caption can name a value an action kept, such as `"Confirm “{tentative.0}”…"`.

From `walkthroughs/band-chat.storyboard.ts`:

```ts
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
```

A storyboard's `summary` is the sentence shown under the video on the examples
page: `lib/showcase/catalogue.ts` reads it from here, so the description and
the video change together.

### Changing a walkthrough

- **A caption, a hold or the order of beats:** edit the storyboard and render
  that id.
- **Something new happening in the app:** add a named action to `<id>.mjs`
  and call it from a beat. Actions get `{ stage, on, page, pages, state }`
  and the beat's `args`. `page` is the beat's device, `pages` holds every
  device, and `state` carries values from one beat to a later one (an invite
  link, a shape's id). Actions use `click`, `type` and `pointAt` from
  `stage.mjs`, which move the device's cursor the way a person would.
- **Assertions stay in.** Each walkthrough checks what its captions claim: an
  edit made offline must not arrive early, and must arrive once the Wi-Fi is
  back. Use `see` and `notSee` beats, or an action that throws.
- **Never show sign-up or sign-in.** Do it in `offCamera`, and open the video
  with everyone signed in.

### Adding a walkthrough

1. Write `walkthroughs/<id>.storyboard.ts` (`satisfies Storyboard`) and
   `walkthroughs/<id>.mjs`, which exports `app` (the example's directory),
   `server` (from `viteServer` or `nextServer` in `walkthrough.mjs`), and
   `actions`. Export `deviceOptions = { keepPorts: [port] }` for a Vite app.
   Pick a port no other walkthrough uses.
2. `pnpm render:walkthroughs <id>`, then look at the MP4 before committing it.
3. For the examples page, give the catalogue entry in
   `lib/showcase/catalogue.ts` a `video` whose `caption` is the storyboard's
   `summary`. `lib/showcase/catalogue.test.ts` checks that every video has a
   storyboard, every beat is known, every action exists, and every video is
   under the size budget.
