import type { ReactNode } from "react";

// Static explainer diagrams for the homepage, drawn in the style of the Jazz
// print material: square boxes with mono labels, thin wires with rounded
// elbows and open arrowheads. Colour carries meaning throughout:
//   blue  = local (on a device, instantly visible, mergeable)
//   green = global (Core, authoritative, exclusive)
//   blue/green dashed = shared by both
// Everything draws with theme tokens (see `.home-diagram` in app/global.css),
// so the diagrams follow light and dark mode.

type Tone = "ink" | "blue" | "green" | "shared" | "muted";
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
        {(["ink", "blue", "green", "muted"] as const).map((tone) => (
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
    // Green underneath, blue dashes on top: reads as alternating blue/green.
    return (
      <g>
        <rect x={x} y={y} width={w} height={h} rx={2} className="dg-box dg-stroke-green" />
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

/** Clients and server modules each hold a partial copy; Jazz Cloud holds all data. */
export function StackDiagram() {
  const copy = (where: string) => [
    { text: "partial local copy", tone: "blue" as const },
    { text: `(${where})`, tone: "blue" as const },
  ];
  const peers = [
    { x: 16, title: "web app", lines: ["browser", ...copy("on disk")] },
    { x: 214, title: "mobile app", lines: ["react native", ...copy("on disk")] },
    { x: 412, title: "backend", lines: ["typescript · rust", ...copy("in memory")] },
    { x: 610, title: "agents & jobs", lines: ["any server", ...copy("in memory")] },
  ];
  const coreX = 250;
  const coreW = 300;
  const busY = 134;
  const peerY = 164;
  return (
    <Diagram
      viewBox="0 0 800 300"
      label="Web apps, mobile apps, backends and agents each keep a partial local copy of the data they use, on disk or in memory, and sync it with Jazz Cloud, which authorizes every write and holds all data."
    >
      <Box
        x={coreX}
        y={16}
        w={coreW}
        h={82}
        title="jazz cloud"
        note="(or self-hosted via CLI)"
        lines={["authorizes every write", { text: "all data", tone: "blue" }]}
      />
      {peers.map((peer) => {
        const cx = peer.x + 87;
        return (
          <Wire
            key={peer.title}
            points={[
              [400, 98],
              [400, busY],
              [cx, busY],
              [cx, peerY],
            ]}
            start={peer.x === 16}
          />
        );
      })}
      <Label x={410} y={120}>
        sync
      </Label>
      {peers.map((peer) => (
        <Box
          key={peer.title}
          x={peer.x}
          y={peerY}
          w={174}
          h={98}
          title={peer.title}
          lines={peer.lines}
        />
      ))}
      <Label x={16} y={288} tone="muted">
        the clients and server modules making up your app
      </Label>
    </Diagram>
  );
}

/** A write is visible locally at once and globally once Core accepts it. */
export function ConsistencyDiagram() {
  return (
    <Diagram
      viewBox="0 0 640 290"
      label="A write applies on the device immediately. wait with tier local resolves once it is saved locally; wait with tier global resolves once Core has authorized and stored it."
    >
      <Label x={16} y={24} tone="muted">
        device
      </Label>
      <line x1={16} x2={624} y1={34} y2={34} className="dg-region" />
      <Box x={16} y={52} w={150} h={40} tone="ink" title="db.insert(…)" center />
      <Wire
        points={[
          [166, 72],
          [206, 72],
        ]}
      />
      <Box
        x={206}
        y={52}
        w={200}
        h={74}
        tone="blue"
        title='tier: "local"'
        lines={["visible to local queries", "works offline"]}
      />
      <Box
        x={446}
        y={52}
        w={178}
        h={74}
        tone="green"
        title='tier: "global"'
        lines={["accepted everywhere"]}
      />
      <Label x={16} y={170} tone="muted">
        core
      </Label>
      <line x1={16} x2={624} y1={180} y2={180} className="dg-region" />
      <Wire
        points={[
          [306, 126],
          [306, 232],
          [380, 232],
        ]}
        end={false}
      />
      <Box x={380} y={206} w={170} h={52} tone="green" title="authorize, store" center />
      <Wire
        points={[
          [550, 232],
          [590, 232],
          [590, 126],
        ]}
        tone="green"
      />
      <Label x={316} y={222}>
        sync
      </Label>
      <Label x={16} y={280} tone="muted">
        offline: local resolves, global waits for core
      </Label>
    </Diagram>
  );
}

/** The read policy and the query run as one plan; only allowed rows sync. */
export function PermissionsDiagram() {
  const rows = [
    { title: "plan launch", kept: true },
    { title: "draft pricing", kept: false },
    { title: "review pr", kept: true },
    { title: "book venue", kept: false },
  ];
  return (
    <Diagram
      viewBox="0 0 640 300"
      label="A query and the table's read policy are optimized together as one plan, so only rows the user may read are synced."
    >
      <Box x={16} y={40} w={96} h={40} title="todos" center />
      <Wire
        points={[
          [112, 60],
          [150, 60],
        ]}
      />
      <Box x={150} y={40} w={196} h={40} tone="green" title="owner_id = you" center />
      <Label x={158} y={28} tone="green">
        read policy
      </Label>
      <Wire
        points={[
          [346, 60],
          [384, 60],
        ]}
      />
      <Box x={384} y={40} w={150} h={40} tone="blue" title="done = false" center />
      <Label x={392} y={28} tone="blue">
        your query
      </Label>
      <Wire
        points={[
          [534, 60],
          [566, 60],
        ]}
      />
      <Box x={566} y={40} w={58} h={40} tone="blue" title="ui" center />
      <path d="M150 96 V104 H534 V96" className="dg-bracket" />
      <Label x={342} y={122} anchor="middle">
        one plan, optimized together
      </Label>
      {rows.map((row, index) => {
        const y = 146 + index * 34;
        return (
          <g key={row.title}>
            <Frame x={150} y={y} w={384} h={28} tone={row.kept ? "blue" : "muted"} />
            <text
              x={162}
              y={y + 19}
              className={`dg-label dg-fill-${row.kept ? "blue" : "muted"}${row.kept ? "" : " dg-struck"}`}
            >
              {row.title}
            </text>
            <text
              x={522}
              y={y + 19}
              textAnchor="end"
              className={`dg-label dg-fill-${row.kept ? "blue" : "muted"}`}
            >
              {row.kept ? "synced" : "never leaves core"}
            </text>
          </g>
        );
      })}
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
      <Label x={16} y={mainY + 5} tone="green">
        main
      </Label>
      <Label x={16} y={draftY + 5} tone="blue">
        draft
      </Label>
      <line x1={80} x2={624} y1={mainY} y2={mainY} className="dg-lane dg-stroke-green" />
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
      {commit(110, mainY, "green", "ana")}
      {commit(210, mainY, "green", "ana")}
      {commit(310, draftY, "blue", "agent", true)}
      {commit(390, draftY, "blue", "agent", true)}
      {commit(410, mainY, "green", "sam")}
      <rect
        x={513}
        y={mainY - 7}
        width={14}
        height={14}
        rx={2}
        className="dg-box dg-stroke-green"
      />
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

/** Two app versions share one table through a migration lens. */
export function SchemaDiagram() {
  return (
    <Diagram
      viewBox="0 0 640 270"
      label="Clients on schema version 1 and version 2 read and write the same data. A migration lens translates the done column to a status column in both directions."
    >
      <Box
        x={16}
        y={24}
        w={176}
        h={96}
        tone="blue"
        title="app v1"
        lines={["still running", "title: string", "done: boolean"]}
      />
      <Box
        x={448}
        y={24}
        w={176}
        h={96}
        tone="blue"
        title="app v2"
        lines={["just shipped", "title: string", "status: enum"]}
      />
      <Box
        x={232}
        y={44}
        w={176}
        h={56}
        tone="shared"
        title="migration lens"
        lines={["done ⇄ status"]}
      />
      <Wire
        points={[
          [192, 72],
          [232, 72],
        ]}
        start
      />
      <Wire
        points={[
          [448, 72],
          [408, 72],
        ]}
        start
      />
      <Wire
        points={[
          [320, 100],
          [320, 170],
        ]}
        start
        tone="green"
      />
      <Box
        x={200}
        y={170}
        w={240}
        h={60}
        tone="green"
        title="todos"
        lines={["one table, both versions live"]}
      />
      <Label x={16} y={258} tone="muted">
        no stop-the-world migration · old clients keep working
      </Label>
    </Diagram>
  );
}

/** A typical backend stack compared with what Jazz covers. */
export function BackendDiagram() {
  const typical = [
    "api endpoints",
    "websocket fan-out",
    "cache + invalidation",
    "permission checks",
    "message queue",
    "blob storage + cdn",
    "database",
  ];
  const jazz = [
    "sync + live queries",
    "row-level permissions",
    "durable streams",
    "files + blobs",
    "database",
  ];
  const row = 32;
  return (
    <Diagram
      viewBox="0 0 640 358"
      label="A typical stack needs API endpoints, WebSocket fan-out, caching, permission checks, a queue, blob storage and a database. With Jazz, your business logic sits on one layer that covers sync, permissions, streams, files and the database."
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
          y={82 + index * (row + 6)}
          w={280}
          h={row}
          tone="muted"
          title={item}
        />
      ))}
      <Box x={344} y={36} w={280} h={row + 4} title="business logic" />
      <Frame x={344} y={82} w={280} h={260} tone="shared" />
      <Label x={356} y={104} tone="blue">
        jazz
      </Label>
      {jazz.map((item, index) => (
        <Box
          key={item}
          x={356}
          y={116 + index * (row + 13)}
          w={256}
          h={row}
          tone={index === jazz.length - 1 ? "green" : "blue"}
          title={item}
        />
      ))}
    </Diagram>
  );
}
