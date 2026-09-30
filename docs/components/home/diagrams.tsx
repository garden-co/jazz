import type { ReactNode } from "react";

// Static explainer diagrams for the homepage, drawn in the style of the Jazz
// print material: square boxes with mono labels, thin wires with rounded
// elbows and open arrowheads. Colour carries meaning throughout:
//   blue  = local (on a device, instantly visible, mergeable)
//   green = global (the cloud, authoritative, exclusive)
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

/** Clients and server modules each hold a partial copy; Jazz Cloud holds all data. */
export function StackDiagram() {
  const copy = (where: string) => [
    { text: "partial local copy", tone: "blue" as const },
    { text: `(${where})`, tone: "blue" as const },
  ];
  const w = 188;
  const peers = [
    { x: 8, title: "web app", lines: [...copy("on disk"), "react · svelte · vue · …"] },
    { x: 206, title: "mobile app", lines: [...copy("on disk"), "react native"] },
    { x: 404, title: "backend", lines: [...copy("in memory"), "typescript · rust"] },
    { x: 602, title: "agents & jobs", lines: [...copy("in memory"), "any server"] },
  ];
  const coreX = 250;
  const coreW = 300;
  const busY = 134;
  const peerY = 164;
  return (
    <Diagram
      viewBox="0 0 800 300"
      label="Web apps, mobile apps, backends and agents each keep a partial local copy of the data they use, on disk or in memory, and sync it with Jazz Cloud, which holds all data and authorizes every write."
    >
      <Box
        x={coreX}
        y={16}
        w={coreW}
        h={82}
        title="jazz cloud"
        note="(or self-hosted via CLI)"
        lines={[{ text: "all data", tone: "blue" }, "authorizes every write"]}
      />
      {peers.map((peer) => {
        const cx = peer.x + w / 2;
        return (
          <Wire
            key={peer.title}
            points={[
              [400, 98],
              [400, busY],
              [cx, busY],
              [cx, peerY],
            ]}
            start={peer.x === 8}
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
          w={w}
          h={98}
          title={peer.title}
          lines={peer.lines}
        />
      ))}
      <Label x={8} y={288} tone="muted">
        the clients and server modules making up your app
      </Label>
    </Diagram>
  );
}

/** Node-message-state: a write's local state, its sync message, and its confirmed fate. */
export function ConsistencyDiagram() {
  const device = 116;
  const cloud = 524;
  return (
    <Diagram
      viewBox="0 0 640 340"
      label="On the device, a write is applied to local state and visible at once; the local tier resolves. It syncs to the cloud with a permission check, which authorizes and stores it as new remote state. The cloud sends back the write's fate, and the device reaches confirmed state; the global tier resolves."
    >
      <Label x={device} y={20} tone="blue" anchor="middle">
        device
      </Label>
      <Label x={cloud} y={20} tone="green" anchor="middle">
        cloud
      </Label>
      <line x1={device} x2={device} y1={30} y2={330} className="dg-region" />
      <line x1={cloud} x2={cloud} y1={30} y2={330} className="dg-region" />
      <Box
        x={16}
        y={44}
        w={200}
        h={74}
        tone="blue"
        title="local state"
        lines={["write visible at once", 'tier "local" resolves']}
      />
      <Wire
        points={[
          [216, 96],
          [424, 150],
        ]}
        tone="blue"
      />
      <Label x={236} y={86} tone="blue">
        sync + permission check
      </Label>
      <Box
        x={424}
        y={136}
        w={200}
        h={74}
        tone="green"
        title="remote state"
        lines={["authorized", "stored durably"]}
      />
      <Wire
        points={[
          [424, 196],
          [216, 250],
        ]}
        tone="green"
      />
      <Label x={300} y={252} tone="green">
        fate confirmation
      </Label>
      <Box
        x={16}
        y={236}
        w={200}
        h={74}
        tone="shared"
        title="confirmed state"
        lines={['tier "global" resolves', "or rolled back"]}
      />
    </Diagram>
  );
}

/** The read policy and the query run as one plan; only matching rows reach you. */
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
      <Label x={16} y={28} tone="green">
        tasks · all rows
      </Label>
      <Grid
        x={16}
        y={40}
        cols={[
          { label: "title", w: 110 },
          { label: "done", w: 52, tone: "blue" },
          { label: "$createdBy", w: 88, tone: "green" },
        ]}
        rows={tasks}
        tone="green"
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
          { text: "$createdBy = you", tone: "green" },
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
        one plan · rows you may not read never leave the cloud
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

/** One raw table holds rows from every schema version; each app reads it through its lens. */
export function SchemaDiagram() {
  const rows = [
    ["buy milk", "true", "", "v1"],
    ["call ana", "false", "", "v1"],
    ["ship v2", "", "doing", "v2"],
    ["fix bug", "", "done", "v2"],
  ];
  const tone = (version: string): Tone => (version === "v1" ? "blue" : "green");
  return (
    <Diagram
      viewBox="0 0 640 336"
      label="A raw table holds the superset of columns from every schema version: title, done from version 1 and status from version 2. Rows written by each version fill only their own columns. App v1 reads and writes it through a lens that maps status to done; app v2 through a lens that maps done to status."
    >
      <Box x={16} y={16} w={200} h={56} tone="blue" title="app v1" lines={["title · done"]} />
      <Box x={424} y={16} w={200} h={56} tone="green" title="app v2" lines={["title · status"]} />
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
        tone="green"
        start
      />
      <Box x={16} y={108} w={200} h={56} tone="blue" title="lens v1" lines={["status → done"]} />
      <Box x={424} y={108} w={200} h={56} tone="green" title="lens v2" lines={["done → status"]} />
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
        tone="green"
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
          { label: "status", w: 76, tone: "green" },
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
    { name: "events", kind: "stream", chunks: 8, hot: [], append: true, op: "append · stream out" },
    { name: "video", kind: "binary · 2GB", chunks: 10, hot: [4, 5, 6], op: "read a byte range" },
    { name: "settings", kind: "json", chunks: 6, hot: [2], op: "read /theme by pointer" },
    { name: "body", kind: "markdown", chunks: 9, hot: [5], op: "edit mid-document" },
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
    "row-level permissions",
    "durable streams",
    "files + blobs",
    "database",
  ];
  const row = 32;
  const listY = 82;
  const listBottom = listY + (typical.length - 1) * (row + 6) + row;
  const frameH = listBottom - listY;
  const jazzStep = (frameH - 34 - row - 16) / (jazz.length - 1);
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
