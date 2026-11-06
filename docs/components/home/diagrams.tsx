import type { ReactNode } from "react";

// Static explainer diagrams for the homepage, drawn in the style of the Jazz
// print material: square boxes with mono labels, thin wires with rounded
// elbows and open arrowheads. Colour carries meaning throughout:
//   blue       = local (on a device, instantly visible, mergeable)
//   mint       = global (the cloud, authoritative, exclusive)
//   periwinkle = in between (sync messages in flight, a second version)
//   blue/mint dashed = shared by both
// Everything draws with theme tokens (see `.home-diagram` in app/global.css),
// so the diagrams follow light and dark mode.

type Tone = "ink" | "blue" | "mint" | "periwinkle" | "shared" | "muted";
type Point = [number, number];

function Diagram({
  label,
  viewBox,
  children,
}: {
  label: string;
  viewBox: string;
  children: ReactNode;
}) {
  return (
    <svg
      viewBox={viewBox}
      role="img"
      aria-label={label}
      className="home-diagram block h-auto w-full"
      xmlns="http://www.w3.org/2000/svg"
    >
      <defs>
        {(["ink", "blue", "mint", "periwinkle", "muted"] as const).map((tone) => (
          <marker
            key={tone}
            id={`dg-arrow-${tone}`}
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="10"
            markerHeight="10"
            markerUnits="userSpaceOnUse"
            orient="auto-start-reverse"
          >
            <path d="M2 1 L9 5 L2 9" className={`dg-chevron dg-stroke-${tone}`} />
          </marker>
        ))}
      </defs>
      {children}
    </svg>
  );
}

/** An orthogonal path through `points` with rounded corners of radius `r`. */
function rounded(points: Point[], r = 10): string {
  let d = `M${points[0][0]} ${points[0][1]}`;
  for (let i = 1; i < points.length - 1; i++) {
    const [px, py] = points[i - 1];
    const [cx, cy] = points[i];
    const [nx, ny] = points[i + 1];
    const inLen = Math.hypot(cx - px, cy - py);
    const outLen = Math.hypot(nx - cx, ny - cy);
    const k = Math.min(r, inLen / 2, outLen / 2);
    const ax = cx - ((cx - px) / inLen) * k;
    const ay = cy - ((cy - py) / inLen) * k;
    const bx = cx + ((nx - cx) / outLen) * k;
    const by = cy + ((ny - cy) / outLen) * k;
    d += ` L${ax} ${ay} Q${cx} ${cy} ${bx} ${by}`;
  }
  const [lx, ly] = points[points.length - 1];
  return `${d} L${lx} ${ly}`;
}

function Wire({
  points,
  tone = "ink",
  start,
  end = true,
  dashed,
}: {
  points: Point[];
  tone?: Exclude<Tone, "shared">;
  start?: boolean;
  end?: boolean;
  dashed?: boolean;
}) {
  return (
    <path
      d={rounded(points)}
      className={`dg-wire dg-stroke-${tone}${dashed ? " dg-wire-dashed" : ""}`}
      markerStart={start ? `url(#dg-arrow-${tone})` : undefined}
      markerEnd={end ? `url(#dg-arrow-${tone})` : undefined}
    />
  );
}

function Frame({ x, y, w, h, tone }: { x: number; y: number; w: number; h: number; tone: Tone }) {
  if (tone === "shared") {
    // Mint underneath, blue dashes on top: reads as alternating blue/mint.
    return (
      <g>
        <rect x={x} y={y} width={w} height={h} rx={2} className="dg-box dg-stroke-mint" />
        <rect x={x} y={y} width={w} height={h} rx={2} className="dg-box-dash dg-stroke-blue" />
      </g>
    );
  }
  return <rect x={x} y={y} width={w} height={h} rx={2} className={`dg-box dg-stroke-${tone}`} />;
}

/** A box with a mono title and optional mono detail lines. */
function Box({
  x,
  y,
  w,
  h,
  tone = "ink",
  title,
  note,
  lines,
  center,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  tone?: Tone;
  title: string;
  /** Muted aside after the title, in the detail size. */
  note?: string;
  lines?: (string | { text: string; tone: Tone })[];
  center?: boolean;
}) {
  const textTone = tone === "shared" ? "blue" : tone;
  return (
    <g>
      <Frame x={x} y={y} w={w} h={h} tone={tone} />
      <text
        x={center ? x + w / 2 : x + 12}
        y={lines ? y + 23 : y + h / 2 + 5}
        textAnchor={center ? "middle" : undefined}
        className={`dg-title dg-fill-${textTone}`}
      >
        {title}
        {note ? (
          <tspan dx={8} className="dg-detail">
            {note}
          </tspan>
        ) : null}
      </text>
      {lines?.map((line, index) => {
        const { text, tone: lineTone } = typeof line === "string" ? { text: line } : line;
        return (
          <text
            key={text}
            x={x + 12}
            y={y + 45 + index * 16}
            className={lineTone ? `dg-detail dg-fill-${lineTone}` : "dg-detail"}
          >
            {text}
          </text>
        );
      })}
    </g>
  );
}

/** A small mono label, e.g. on a wire. */
function Label({
  x,
  y,
  children,
  tone = "ink",
  anchor,
}: {
  x: number;
  y: number;
  children: string;
  tone?: Tone;
  anchor?: "start" | "middle" | "end";
}) {
  return (
    <text x={x} y={y} textAnchor={anchor} className={`dg-label dg-fill-${tone}`}>
      {children}
    </text>
  );
}

/** A plain table: header row, then rows; `tones` colours individual cells. */
function Grid({
  x,
  y,
  cols,
  rows,
  tone = "ink",
  cellTone,
  rowH = 24,
}: {
  x: number;
  y: number;
  cols: { label: string; w: number; tone?: Tone }[];
  rows: string[][];
  tone?: Tone;
  cellTone?: (row: number, col: number) => Tone | undefined;
  rowH?: number;
}) {
  const w = cols.reduce((sum, col) => sum + col.w, 0);
  const h = rowH * (rows.length + 1);
  const colX = cols.map((_, index) => x + cols.slice(0, index).reduce((sum, c) => sum + c.w, 0));
  return (
    <g>
      <Frame x={x} y={y} w={w} h={h} tone={tone} />
      {rows.map((_, index) => (
        <line
          key={`r${index}`}
          x1={x}
          x2={x + w}
          y1={y + rowH * (index + 1)}
          y2={y + rowH * (index + 1)}
          className={
            index === 0 ? `dg-rule dg-stroke-${tone === "shared" ? "blue" : tone}` : "dg-grid"
          }
        />
      ))}
      {colX.slice(1).map((cx) => (
        <line key={`c${cx}`} x1={cx} x2={cx} y1={y} y2={y + h} className="dg-grid" />
      ))}
      {cols.map((col, index) => (
        <text
          key={col.label}
          x={colX[index] + 8}
          y={y + rowH / 2 + 4}
          className={`dg-label dg-fill-${col.tone ?? "muted"}`}
        >
          {col.label}
        </text>
      ))}
      {rows.map((row, r) =>
        row.map((cell, c) => (
          <text
            key={`${r}-${c}`}
            x={colX[c] + 8}
            y={y + rowH * (r + 1) + rowH / 2 + 4}
            className={`dg-label dg-fill-${cellTone?.(r, c) ?? "ink"}`}
          >
            {cell}
          </text>
        )),
      )}
    </g>
  );
}

export type DeploymentScenario = "vercel" | "cloudflare" | "aws" | "server";

type Platform = {
  label: string;
  /** Your backend's runtime, under "Your Backend". */
  runtime: string[];
  /** The account the dashed boundary names; none for your own machine. */
  account?: string;
  /** Above the shard row. */
  scaling: string;
  /** Each shard: the platform's box title lines, and the Jazz box inside. */
  shard: { title: string[]; inner: string[]; count: number };
  store: string;
};

const LARGE_VALUES = ["jazz large value chunks", "(blobs, streams, JSON, richtext)"];

const deploymentScenarios: Record<DeploymentScenario, Platform> = {
  vercel: {
    label:
      "On Vercel, everything stays in your Vercel account: jazz SQLite shards in Vercel Sandboxes, auto-scaling and hibernating, jazz large value chunks in Vercel Blobs, and the jazz core in your backend's Vercel functions, next to your normal API routes and behind your auth. Web pages, web apps, native apps, backend workers and agent VMs get efficient realtime sync through the mounted /jazz sync endpoint; traditional clients make manual HTTP requests and streaming.",
    runtime: ["(Vercel functions)"],
    account: "Your vercel account",
    scaling: "auto-scaling and hibernating",
    shard: { title: ["Vercel", "Sandbox"], inner: ["jazz SQLite", "shard"], count: 4 },
    store: "Vercel Blobs",
  },
  cloudflare: {
    label:
      "On Cloudflare, coming soon: everything stays in your Cloudflare account: jazz shards in Durable Objects with their own SQLite, auto-scaling and hibernating, jazz large value chunks in R2, and the jazz core in your backend's Workers, next to your normal API routes and behind your auth. Clients get efficient realtime sync through the mounted /jazz sync endpoint; traditional clients make manual HTTP requests and streaming.",
    runtime: ["(CF Workers)"],
    account: "Your Cloudflare account",
    scaling: "auto-scaling and hibernating",
    shard: { title: ["CF DO", "SQLite"], inner: ["jazz", "shard"], count: 7 },
    store: "R2",
  },
  aws: {
    label:
      "On AWS, everything stays in your AWS account: jazz SQLite shards on EC2 instances, manually scaled and long running, jazz large value chunks in S3, and the jazz core in your backend on AWS Lambda, next to your normal API routes and behind your auth. Clients get efficient realtime sync through the mounted /jazz sync endpoint; traditional clients make manual HTTP requests and streaming.",
    runtime: ["(AWS Lambda)"],
    account: "Your AWS account",
    scaling: "manually scaled, long running",
    shard: { title: ["EC2 instance"], inner: ["jazz SQLite", "shard"], count: 2 },
    store: "S3",
  },
  server: {
    label:
      "On a local machine or your own server: your backend holds local SQLite and local files with the jazz large value chunks, and the jazz core next to your normal API routes, behind your auth. Clients get efficient realtime sync through the mounted /jazz sync endpoint; traditional clients make manual HTTP requests and streaming.",
    runtime: ["(local machine", "or your server)"],
    scaling: "",
    shard: { title: [], inner: [], count: 0 },
    store: "local files",
  },
};

export const deploymentScenarioNames: Record<DeploymentScenario, string> = {
  vercel: "Vercel",
  cloudflare: "Cloudflare (coming soon)",
  aws: "AWS",
  server: "Your own server",
};

/** Lines of text from `y`, `step` apart, in one style. */
function Lines({
  x,
  y,
  lines,
  className,
  step = 18,
  anchor,
}: {
  x: number;
  y: number;
  lines: string[];
  className: string;
  step?: number;
  anchor?: "middle";
}) {
  return (
    <text x={x} y={y} textAnchor={anchor} className={className}>
      {lines.map((line, index) => (
        <tspan key={line} x={x} dy={index === 0 ? 0 : step}>
          {line}
        </tspan>
      ))}
    </text>
  );
}

/** A platform box: its title, then Jazz's part in blue below it. */
function Nested({
  x,
  y,
  w,
  h,
  title,
  inner,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  title: string[];
  inner: string[];
}) {
  const pad = w < 70 ? 5 : w < 90 ? 8 : 12;
  return (
    <g>
      <Frame x={x} y={y} w={w} h={h} tone="ink" />
      <Lines x={x + pad} y={y + 23} lines={title} className="dg-title dg-fill-ink" />
      <Lines
        x={x + pad}
        y={y + 23 + title.length * 18 + 8}
        lines={inner}
        step={16}
        className="dg-detail dg-fill-blue"
      />
    </g>
  );
}

type Client = { name: string[]; inner: string[]; tone: Tone };

const clients: Client[] = [
  {
    name: ["traditional", "client"],
    inner: ["your own request", "& state handling"],
    tone: "ink",
  },
  { name: ["web page"], inner: ["thin jazz client"], tone: "blue" },
  {
    name: ["web app"],
    inner: ["local-first", "jazz client", "(SQLite on", "indexeddb)"],
    tone: "blue",
  },
  {
    name: ["native app"],
    inner: ["local-first", "jazz client", "(native SQLite)"],
    tone: "blue",
  },
  { name: ["backend worker"], inner: ["thin jazz client"], tone: "blue" },
  { name: ["agent VM"], inner: ["thin jazz client"], tone: "blue" },
];

/** A client: its name, then its Jazz client (or your own code) below. */
function ClientBox({
  x,
  y,
  w,
  h,
  client,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  client: Client;
}) {
  // Each box fits its contents.
  const boxH = Math.min(h, 23 + client.name.length * 18 + client.inner.length * 16 + 12);
  return (
    <g>
      <Frame x={x} y={y} w={w} h={boxH} tone="ink" />
      <Lines x={x + 12} y={y + 23} lines={client.name} className="dg-title dg-fill-ink" />
      <Lines
        x={x + 12}
        y={y + 23 + client.name.length * 18 + 8}
        lines={client.inner}
        step={16}
        className={client.tone === "blue" ? "dg-detail dg-fill-blue" : "dg-detail"}
      />
    </g>
  );
}

/**
 * Fig. 1: where Jazz runs, drawn narrow enough for a phone. Your platform
 * account on top (the large-value store, then the shards), your backend with
 * the jazz core and its /jazz mount behind your auth, then a row of three
 * client slots. Each slot takes turns between two of the six kinds of client
 * (staggered, so one changes at a time), bringing its own wire along; the
 * dashed large-value reads come down the left edge.
 * In this figure blue marks Jazz's own parts and mint your auth.
 */
export function DeploymentDiagram({ scenario }: { scenario: DeploymentScenario }) {
  const { label, runtime, account, scaling, shard, store } = deploymentScenarios[scenario];
  const local = !account;
  const W = 560;
  const storeBox = { x: 16, y: 32, w: 528, h: 70 };
  const shardRow = { x: 16, y: 120, w: 484, h: 104 };
  const backend = { x: 16, y: local ? 12 : 264, w: 484 };
  const backendBottom = 392;
  const authH = 24;
  const authBottom = backendBottom + authH;
  const core = { x: 232, y: 276, w: 256, h: 44 };
  const api = { x: 32, y: 336, w: 180, h: 44 };
  const mount = { x: 224, y: 336, w: 264, h: 44 };
  const cw = 168;
  const colX = [16, 196, 376];
  const center = (col: number) => colX[col] + cw / 2;
  const rowY = 480;
  const clientH = 120;
  // Three slots, each alternating between two clients. Slot 0 swaps the
  // traditional client (plain requests to your API) for a web page (sync).
  const slots = [
    [clients[0], clients[1]],
    [clients[2], clients[4]],
    [clients[3], clients[5]],
  ];
  const gap = shard.count > 4 ? 6 : 10;
  const sw = shard.count ? (shardRow.w - gap * (shard.count - 1)) / shard.count : 0;
  const shardXs = Array.from({ length: shard.count }, (_, i) => shardRow.x + i * (sw + gap));
  return (
    <Diagram viewBox={`0 0 ${W} ${rowY + clientH + 6}`} label={label}>
      {account ? <rect x={4} y={4} width={552} height={418} rx={2} className="dg-region" /> : null}
      <Frame x={backend.x} y={backend.y} w={backend.w} h={backendBottom - backend.y} tone="ink" />
      <Lines
        x={backend.x + 16}
        y={backend.y + 24}
        lines={["Your Backend", ...runtime]}
        step={18}
        className="dg-title dg-fill-ink"
      />
      {account ? (
        <g>
          <Label x={16} y={22} tone="blue">
            {scaling}
          </Label>
          <Label x={544} y={22} tone="ink" anchor="end">
            {account}
          </Label>
          <Nested {...storeBox} title={[store]} inner={LARGE_VALUES} />
          {shardXs.map((x) => (
            <Nested
              key={x}
              x={x}
              y={shardRow.y}
              w={sw}
              h={shardRow.h}
              title={shard.title}
              inner={shard.inner}
            />
          ))}
          {shardXs.map((x, index) => (
            <Wire
              key={`w${x}`}
              points={[
                [x + sw / 2, shardRow.y + shardRow.h],
                [x + sw / 2, 244],
                [360, 244],
                [360, core.y],
              ]}
              start
              end={index === 0}
              tone="blue"
            />
          ))}
          {/* The store reaches the core around the right of the shards. */}
          <Wire
            points={[
              [516, storeBox.y + storeBox.h],
              [516, core.y + core.h / 2],
              [core.x + core.w, core.y + core.h / 2],
            ]}
            tone="blue"
            dashed
            start
          />
          {/* Clients read large values from the store directly (all but
              the traditional client). */}
          <Wire
            points={[
              [storeBox.x, storeBox.y + storeBox.h / 2],
              [8, storeBox.y + storeBox.h / 2],
              [8, 470],
              [colX[2] + 12, 470],
            ]}
            tone="blue"
            dashed
            start
            end={false}
          />
          <g className="dg-cycle dg-cycle-b">
            <Wire
              points={[
                [colX[0] + 12, 470],
                [colX[0] + 12, rowY],
              ]}
              tone="blue"
              dashed
            />
          </g>
          {[1, 2].map((col) => (
            <Wire
              key={`d${col}`}
              points={[
                [colX[col] + 12, 470],
                [colX[col] + 12, rowY],
              ]}
              tone="blue"
              dashed
            />
          ))}
        </g>
      ) : (
        <g>
          <Box
            x={32}
            y={96}
            w={452}
            h={70}
            tone="blue"
            title="local files"
            lines={LARGE_VALUES.map((text) => ({ text, tone: "blue" as const }))}
          />
          <Box x={32} y={186} w={408} h={50} tone="blue" title="local SQLite" center />
          <Wire
            points={[
              [464, 166],
              [464, core.y],
            ]}
            tone="blue"
            start
          />
          <Wire
            points={[
              [300, 236],
              [300, core.y],
            ]}
            tone="blue"
            start
          />
        </g>
      )}
      <Box {...core} tone="blue" title="jazz core" center />
      <Box {...api} title="normal API route" center />
      <Box {...mount} tone="blue" title="mounted /jazz sync endpoint" center />
      <Wire
        points={[
          [206, api.y],
          [206, core.y + core.h / 2],
          [core.x, core.y + core.h / 2],
        ]}
        tone="blue"
        start
      />
      <Frame x={backend.x} y={backendBottom} w={backend.w} h={authH} tone="mint" />
      <Label x={backend.x + backend.w / 2} y={backendBottom + 16} tone="mint" anchor="middle">
        your auth
      </Label>
      {/* The traditional client makes ordinary requests (and streams) to your
          API routes; every other client keeps a live sync connection. */}
      {[1, 2].map((col) => (
        <Wire
          key={`s${col}`}
          points={[
            [center(col), rowY],
            [center(col), authBottom],
          ]}
          tone="blue"
          start
        />
      ))}
      <Lines
        x={center(2) + 8}
        y={440}
        lines={["efficient", "realtime", "sync"]}
        step={14}
        className="dg-label dg-fill-blue"
      />
      {slots.map((pair, col) =>
        pair.map((client, turn) => (
          <g
            key={client.name.join(" ")}
            className={`dg-cycle ${turn ? "dg-cycle-b" : "dg-cycle-a"}`}
            style={{ animationDelay: `${col * 2}s` }}
          >
            {col === 0 && turn === 0 ? (
              <g>
                <Wire
                  points={[
                    [center(0), rowY],
                    [center(0), api.y + api.h],
                  ]}
                  start
                />
                <Lines
                  x={center(0) + 6}
                  y={432}
                  lines={["manual http", "requests &", "streaming"]}
                  step={14}
                  className="dg-label dg-fill-ink"
                />
              </g>
            ) : null}
            {col === 0 && turn === 1 ? (
              <Wire
                points={[
                  [center(0), rowY],
                  [center(0), 446],
                  [240, 446],
                  [240, authBottom],
                ]}
                tone="blue"
                start
              />
            ) : null}
            <ClientBox x={colX[col]} y={rowY} w={cw} h={clientH} client={client} />
          </g>
        )),
      )}
    </Diagram>
  );
}

/** Node-message-state: a write's local state, its sync message, and its confirmed fate. */
export function PermissionsDiagram() {
  const tasks = [
    ["plan launch", "false", "you"],
    ["draft pricing", "false", "sam"],
    ["review pr", "true", "you"],
    ["book venue", "false", "you"],
    ["fix login", "false", "ana"],
  ];
  const matches = (row: string[]) => row[1] === "false" && row[2] === "you";
  return (
    <Diagram
      viewBox="0 0 640 224"
      label="A tasks table with title, done and createdBy columns. The read policy, createdBy equals you, and your query, done equals false, run as one combined query. Only the two matching tasks reach you."
    >
      <Label x={16} y={28} tone="mint">
        tasks · all rows
      </Label>
      <Grid
        x={16}
        y={40}
        cols={[
          { label: "title", w: 110 },
          { label: "done", w: 52, tone: "blue" },
          { label: "$createdBy", w: 88, tone: "mint" },
        ]}
        rows={tasks}
        tone="mint"
        cellTone={(r) => (matches(tasks[r]) ? "ink" : "muted")}
      />
      <Wire
        points={[
          [266, 112],
          [292, 112],
        ]}
      />
      <Box
        x={292}
        y={58}
        w={176}
        h={108}
        title="combined query"
        lines={[
          { text: "read policy", tone: "muted" },
          { text: "$createdBy = you", tone: "mint" },
          { text: "your query", tone: "muted" },
          { text: "done = false", tone: "blue" },
        ]}
      />
      <Wire
        points={[
          [468, 112],
          [496, 112],
        ]}
        tone="blue"
      />
      <Label x={496} y={64} tone="blue">
        synced to you
      </Label>
      <Grid
        x={496}
        y={76}
        cols={[{ label: "title", w: 128 }]}
        rows={tasks.filter(matches).map((row) => [row[0]])}
        tone="blue"
        cellTone={() => "blue"}
      />
      <Label x={16} y={214} tone="muted">
        one plan · rows a user may not read never leave your backend
      </Label>
    </Diagram>
  );
}

/** Git-like history for one row: main, a draft branch, and a merge. */
export function HistoryDiagram() {
  const mainY = 70;
  const draftY = 170;
  const commit = (x: number, y: number, tone: Tone, label: string, below?: boolean) => (
    <g key={`${x}-${y}`}>
      <rect
        x={x - 7}
        y={y - 7}
        width={14}
        height={14}
        rx={2}
        className={`dg-box dg-stroke-${tone}`}
      />
      <Label x={x} y={below ? y + 28 : y - 16} tone={tone} anchor="middle">
        {label}
      </Label>
    </g>
  );
  return (
    <Diagram
      viewBox="0 0 640 250"
      label="One row's history: edits on the main branch, a draft branch edited by an agent, and a merge back into main."
    >
      <Label x={16} y={mainY + 5} tone="mint">
        main
      </Label>
      <Label x={16} y={draftY + 5} tone="blue">
        draft
      </Label>
      <line x1={80} x2={624} y1={mainY} y2={mainY} className="dg-lane dg-stroke-mint" />
      <path
        d={rounded(
          [
            [210, mainY],
            [250, mainY],
            [250, draftY],
            [470, draftY],
            [470, mainY],
            [513, mainY],
          ],
          18,
        )}
        className="dg-lane dg-stroke-blue"
      />
      {commit(110, mainY, "mint", "ana")}
      {commit(210, mainY, "mint", "ana")}
      {commit(310, draftY, "blue", "agent", true)}
      {commit(390, draftY, "blue", "agent", true)}
      {commit(410, mainY, "mint", "sam")}
      <rect x={513} y={mainY - 7} width={14} height={14} rx={2} className="dg-box dg-stroke-mint" />
      <rect
        x={513}
        y={mainY - 7}
        width={14}
        height={14}
        rx={2}
        className="dg-box-dash dg-stroke-blue"
      />
      <Label x={520} y={mainY - 16} anchor="middle">
        merge
      </Label>
      <Label x={16} y={236} tone="muted">
        read any version · compare branches · see who changed what
      </Label>
    </Diagram>
  );
}

/** One raw table holds rows from every schema version; each app reads it through its lens. */
export function SchemaDiagram() {
  const rows = [
    ["buy milk", "true", "", "v1"],
    ["call ana", "false", "", "v1"],
    ["ship v2", "", "doing", "v2"],
    ["fix bug", "", "done", "v2"],
  ];
  const tone = (version: string): Tone => (version === "v1" ? "blue" : "periwinkle");
  return (
    <Diagram
      viewBox="0 0 640 336"
      label="A raw table holds the superset of columns from every schema version: title, done from version 1 and status from version 2. Rows written by each version fill only their own columns. App v1 reads and writes it through a lens that maps status to done; app v2 through a lens that maps done to status."
    >
      <Box x={16} y={16} w={200} h={56} tone="blue" title="app v1" lines={["title · done"]} />
      <Box
        x={424}
        y={16}
        w={200}
        h={56}
        tone="periwinkle"
        title="app v2"
        lines={["title · status"]}
      />
      <Wire
        points={[
          [116, 72],
          [116, 108],
        ]}
        tone="blue"
        start
      />
      <Wire
        points={[
          [524, 72],
          [524, 108],
        ]}
        tone="periwinkle"
        start
      />
      <Box x={16} y={108} w={200} h={56} tone="blue" title="lens v1" lines={["status → done"]} />
      <Box
        x={424}
        y={108}
        w={200}
        h={56}
        tone="periwinkle"
        title="lens v2"
        lines={["done → status"]}
      />
      <Wire
        points={[
          [116, 164],
          [116, 272],
          [160, 272],
        ]}
        tone="blue"
        start
      />
      <Wire
        points={[
          [524, 164],
          [524, 272],
          [480, 272],
        ]}
        tone="periwinkle"
        start
      />
      <Label x={160} y={204} tone="muted">
        raw table · superset of all versions
      </Label>
      <Grid
        x={160}
        y={216}
        cols={[
          { label: "title", w: 104 },
          { label: "done", w: 68, tone: "blue" },
          { label: "status", w: 76, tone: "periwinkle" },
          { label: "written", w: 72 },
        ]}
        rows={rows.map((row) => row.map((cell) => cell || "·"))}
        rowH={20}
        cellTone={(r, c) => (c === 0 ? "ink" : rows[r][c] ? tone(rows[r][3]) : "muted")}
      />
    </Diagram>
  );
}

/** Large values are chunked, so appends, range reads and edits touch only a few chunks. */
export function LargeValuesDiagram() {
  const columns: {
    name: string;
    kind: string;
    chunks: number;
    hot: number[];
    append?: boolean;
    op: string;
  }[] = [
    {
      name: "events",
      kind: "stream",
      chunks: 8,
      hot: [],
      append: true,
      op: "append · stream out",
    },
    {
      name: "video",
      kind: "binary · 2GB",
      chunks: 10,
      hot: [4, 5, 6],
      op: "read a byte range",
    },
    {
      name: "settings",
      kind: "json",
      chunks: 6,
      hot: [2],
      op: "read /theme by pointer",
    },
    {
      name: "body",
      kind: "markdown",
      chunks: 9,
      hot: [5],
      op: "edit mid-document",
    },
  ];
  const chunkX = (index: number) => 176 + index * 26;
  return (
    <Diagram
      viewBox="0 0 640 290"
      label="Four columns of one table: an events stream, a 2GB binary video, a JSON settings document and a markdown body. Each is stored as chunks. Appending to the stream, reading a byte range of the video, reading one JSON pointer and editing the middle of the document each touch only a few chunks."
    >
      <Label x={16} y={24} tone="muted">
        column
      </Label>
      <Label x={176} y={24} tone="muted">
        stored as chunks
      </Label>
      <Label x={456} y={24} tone="muted">
        stays fast
      </Label>
      {columns.map((column, row) => {
        const y = 44 + row * 54;
        return (
          <g key={column.name}>
            <text x={16} y={y + 16} className="dg-title dg-fill-ink">
              {column.name}
            </text>
            <text x={16} y={y + 34} className="dg-detail">
              {column.kind}
            </text>
            {Array.from({ length: column.chunks }, (_, index) => (
              <rect
                key={index}
                x={chunkX(index)}
                y={y + 6}
                width={20}
                height={28}
                rx={2}
                className={`dg-box dg-stroke-${column.hot.includes(index) ? "blue" : "muted"}${column.hot.includes(index) ? " dg-hot" : ""}`}
              />
            ))}
            {column.append ? (
              <>
                <rect
                  x={chunkX(column.chunks)}
                  y={y + 6}
                  width={20}
                  height={28}
                  rx={2}
                  className="dg-box-dash dg-stroke-blue"
                />
                <Wire
                  points={[
                    [chunkX(column.chunks) + 26, y + 20],
                    [chunkX(column.chunks) + 50, y + 20],
                  ]}
                  tone="blue"
                  start
                  end={false}
                />
              </>
            ) : null}
            <text x={456} y={y + 25} className="dg-label dg-fill-blue">
              {column.op}
            </text>
          </g>
        );
      })}
      <Label x={16} y={278} tone="muted">
        only touched chunks sync · permissions apply as on any column
      </Label>
    </Diagram>
  );
}

/** A typical backend stack compared with what Jazz covers. */
export function BackendDiagram() {
  const typical = [
    "CRUD api endpoints",
    "requests and reconnects",
    "client data state handling",
    "websocket fan-out",
    "cache + invalidation",
    "permission checks",
    "message queue",
    "blob storage + cdn",
    "database",
  ];
  const jazz = [
    "sync + live queries",
    "declarative permissions",
    "durable streams",
    "files + blobs",
    "database",
  ];
  const row = 32;
  const listY = 82;
  const listBottom = listY + (typical.length - 1) * (row + 6) + row;
  // The jazz frame hugs its items, so it reads as the smaller stack it is.
  const jazzStep = row + 6;
  const frameH = 34 + (jazz.length - 1) * jazzStep + row + 12;
  return (
    <Diagram
      viewBox={`0 0 640 ${listBottom + 16}`}
      label="A typical stack needs CRUD API endpoints, request and reconnect handling, client data state handling, WebSocket fan-out, caching, permission checks, a queue, blob storage and a database. With Jazz, your business logic sits on one layer that covers sync, permissions, streams, files and the database."
    >
      <Label x={16} y={24} tone="muted">
        typical stack
      </Label>
      <Label x={344} y={24} tone="muted">
        with jazz
      </Label>
      <Box x={16} y={36} w={280} h={row + 4} title="business logic" />
      {typical.map((item, index) => (
        <Box
          key={item}
          x={16}
          y={listY + index * (row + 6)}
          w={280}
          h={row}
          tone="muted"
          title={item}
        />
      ))}
      <Box x={344} y={36} w={280} h={row + 4} title="business logic" />
      <Frame x={344} y={listY} w={280} h={frameH} tone="blue" />
      <Label x={356} y={listY + 22} tone="blue">
        jazz
      </Label>
      {jazz.map((item, index) => (
        <Box
          key={item}
          x={356}
          y={listY + 34 + index * jazzStep}
          w={256}
          h={row}
          tone="blue"
          title={item}
        />
      ))}
    </Diagram>
  );
}

export function FrontendDiagram() {
  const typical = [
    "fetch calls per endpoint",
    "loading + error states",
    "client state store",
    "cache invalidation + refetch",
    "websocket subscriptions",
    "optimistic updates",
    "rollback on failure",
    "offline queue + retries",
    "local persistence",
  ];
  const thin = ["one reactive query hook", "live updates", "writes as plain calls"];
  const localFirst = [
    "instant optimistic writes",
    "durable local copy",
    "offline + background sync",
  ];
  const row = 32;
  const gap = 6;
  const listY = 82;
  const listBottom = listY + (typical.length - 1) * (row + gap) + row;
  const frameGap = 12;
  const inner = (count: number) => 34 + count * row + (count - 1) * gap + 12;
  const thinH = inner(thin.length);
  const localY = listY + thinH + frameGap;
  const localH = listBottom - localY;
  const boxes = (items: string[], y: number, tone: "mint" | "blue") =>
    items.map((item, index) => (
      <Box
        key={item}
        x={356}
        y={y + 34 + index * (row + gap)}
        w={256}
        h={row}
        tone={tone}
        title={item}
      />
    ));
  return (
    <Diagram
      viewBox={`0 0 640 ${listBottom + 16}`}
      label="A typical client needs fetch calls per endpoint, loading and error states, a client state store, cache invalidation, WebSocket subscriptions, optimistic updates with rollback, an offline queue and local persistence. A thin Jazz client replaces the fetching, caching and subscriptions with one reactive query hook. A local-first Jazz client also takes over optimistic writes, a durable local copy, offline use and background sync."
    >
      <Label x={16} y={24} tone="muted">
        typical client
      </Label>
      <Label x={344} y={24} tone="muted">
        with jazz
      </Label>
      <Box x={16} y={36} w={280} h={row + 4} title="ui components" />
      {typical.map((item, index) => (
        <Box
          key={item}
          x={16}
          y={listY + index * (row + gap)}
          w={280}
          h={row}
          tone="muted"
          title={item}
        />
      ))}
      <Box x={344} y={36} w={280} h={row + 4} title="ui components" />
      <Frame x={344} y={listY} w={280} h={thinH} tone="mint" />
      <Label x={356} y={listY + 22} tone="mint">
        thin client
      </Label>
      {boxes(thin, listY, "mint")}
      <Frame x={344} y={localY} w={280} h={localH} tone="blue" />
      <Label x={356} y={localY + 22} tone="blue">
        + local-first
      </Label>
      {boxes(localFirst, localY, "blue")}
    </Diagram>
  );
}
