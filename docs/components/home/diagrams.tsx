import type { ReactNode } from "react";

// Static explainer diagrams for the homepage. They draw with theme tokens
// (see `.home-diagram` in app/global.css), so they follow light and dark mode.

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
        <marker
          id="home-arrow"
          viewBox="0 0 8 8"
          refX="7"
          refY="4"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M0 0 L8 4 L0 8 z" className="dg-arrow-head" />
        </marker>
        <marker
          id="home-arrow-accent"
          viewBox="0 0 8 8"
          refX="7"
          refY="4"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M0 0 L8 4 L0 8 z" className="dg-arrow-head-accent" />
        </marker>
      </defs>
      {children}
    </svg>
  );
}

function Node({
  x,
  y,
  w,
  h,
  title,
  sub,
  emphasis,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  title: string;
  sub?: string;
  emphasis?: boolean;
}) {
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx={6}
        className={emphasis ? "dg-node-strong" : "dg-node"}
      />
      <text x={x + 12} y={y + 26} className="dg-title">
        {title}
      </text>
      {sub ? (
        <text x={x + 12} y={y + 46} className="dg-sub">
          {sub}
        </text>
      ) : null}
    </g>
  );
}

function Pill({ x, y, w, children }: { x: number; y: number; w: number; children: string }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={26} rx={3} className="dg-pill" />
      <text x={x + 8} y={y + 17} className="dg-code">
        {children}
      </text>
    </g>
  );
}

/** Frontend, backend and cloud each hold a synced copy of the data. */
export function StackDiagram() {
  const peers = [
    { x: 16, title: "Web app", sub: "browser" },
    { x: 213, title: "Mobile app", sub: "React Native" },
    { x: 411, title: "Your backend", sub: "TypeScript or Rust" },
    { x: 608, title: "Agents & jobs", sub: "any server" },
  ];
  return (
    <Diagram
      viewBox="0 0 800 330"
      label="Web apps, mobile apps, backends and agents each keep a local copy of the data they use and sync it with Jazz Core, in Jazz Cloud or self-hosted."
    >
      <rect x={16} y={16} width={768} height={100} rx={9} className="dg-band" />
      <text x={32} y={44} className="dg-sub">
        Jazz Cloud or self-hosted
      </text>
      <Node
        x={250}
        y={34}
        w={300}
        h={64}
        title="Core"
        sub="authorizes and durably stores writes"
        emphasis
      />
      {peers.map((peer) => (
        <path
          key={peer.title}
          d={`M${peer.x + 88} 226 C ${peer.x + 88} 170, 400 164, 400 98`}
          className="dg-link-dashed"
        />
      ))}
      <rect x={380} y={150} width={40} height={22} rx={3} className="dg-pill" />
      <text x={400} y={166} textAnchor="middle" className="dg-sub-sm">
        sync
      </text>
      {peers.map((peer) => (
        <g key={peer.title}>
          <Node x={peer.x} y={226} w={176} h={94} title={peer.title} sub={peer.sub} />
          <Pill x={peer.x + 12} y={284} w={152}>
            local copy
          </Pill>
        </g>
      ))}
    </Diagram>
  );
}

/** A write is visible locally at once; `wait({ tier })` picks the confirmation. */
export function ConsistencyDiagram() {
  return (
    <Diagram
      viewBox="0 0 640 270"
      label="A write applies on the device immediately. wait with tier local resolves once it is saved locally; wait with tier global resolves once Core has authorized and stored it."
    >
      <text x={16} y={77} className="dg-title">
        Device
      </text>
      <text x={16} y={197} className="dg-title">
        Core
      </text>
      <line x1={110} y1={72} x2={624} y2={72} className="dg-lane" />
      <line x1={110} y1={192} x2={624} y2={192} className="dg-lane" />

      <text x={138} y={44} className="dg-code-strong">
        db.insert(…)
      </text>
      <path
        d="M150 78 C 150 160, 300 192, 424 192"
        className="dg-link-accent"
        markerEnd="url(#home-arrow-accent)"
      />
      <circle cx={150} cy={72} r={7} className="dg-dot-accent" />
      <text x={178} y={106} className="dg-sub">
        visible to local queries at once
      </text>

      <circle cx={248} cy={72} r={5} className="dg-dot" />
      <text x={236} y={44} className="dg-code">
        {'wait({ tier: "local" })'}
      </text>

      <circle cx={430} cy={192} r={7} className="dg-dot-accent" />
      <text x={430} y={226} textAnchor="middle" className="dg-sub">
        authorized and durably stored
      </text>
      <path d="M434 186 L 540 80" className="dg-link" markerEnd="url(#home-arrow)" />
      <circle cx={544} cy={72} r={5} className="dg-dot" />
      <text x={624} y={44} textAnchor="end" className="dg-code">
        {'wait({ tier: "global" })'}
      </text>

      <text x={16} y={258} className="dg-sub">
        Offline, only local resolves; global waits until Core accepts or rejects.
      </text>
    </Diagram>
  );
}

/** The user's query and the table's read policy compile into one plan. */
export function PermissionsDiagram() {
  const rows = [
    { title: "Plan launch", kept: true },
    { title: "Draft pricing", kept: false },
    { title: "Review PR", kept: true },
    { title: "Book venue", kept: false },
  ];
  return (
    <Diagram
      viewBox="0 0 640 280"
      label="A query and the table's read policy are optimized together as one plan, so only rows the user may read are synced."
    >
      <text x={16} y={26} className="dg-sub">
        your query
      </text>
      <Pill x={16} y={36} w={300}>
        {"todos.where({ done: false })"}
      </Pill>
      <text x={16} y={100} className="dg-sub">
        read policy
      </text>
      <Pill x={16} y={110} w={300}>
        {"{ owner_id: session.user.account }"}
      </Pill>
      <path d="M316 48 C 332 48, 332 98, 340 98" className="dg-link" />
      <path
        d="M316 122 C 332 122, 332 98, 340 98"
        className="dg-link"
        markerEnd="url(#home-arrow)"
      />
      <Node x={344} y={66} w={138} h={64} title="One plan" sub="query + policy" emphasis />
      <path d="M482 98 L 496 98" className="dg-link-accent" markerEnd="url(#home-arrow-accent)" />
      {rows.map((row, index) => {
        const y = 16 + index * 52;
        return (
          <g key={row.title} opacity={row.kept ? 1 : 0.5}>
            <rect
              x={500}
              y={y}
              width={124}
              height={44}
              rx={3}
              className={row.kept ? "dg-row-kept" : "dg-row"}
            />
            <text x={510} y={y + 19} className="dg-title-sm">
              {row.title}
            </text>
            <text x={510} y={y + 35} className="dg-sub-sm">
              {row.kept ? "owner: you" : "not synced"}
            </text>
          </g>
        );
      })}
      <text x={16} y={246} className="dg-sub">
        Core only syncs rows the policy allows, so the client can query
      </text>
      <text x={16} y={266} className="dg-sub">
        its local copy with no round trip to check access.
      </text>
    </Diagram>
  );
}

/** Each row keeps a branching history of every edit. */
export function HistoryDiagram() {
  const main = [
    { x: 80, who: "Ana" },
    { x: 180, who: "Ana" },
    { x: 400, who: "Sam" },
    { x: 570, who: "merge" },
  ];
  const draft = [
    { x: 260, who: "agent" },
    { x: 350, who: "agent" },
    { x: 470, who: "Ana" },
  ];
  return (
    <Diagram
      viewBox="0 0 640 250"
      label="One row's history: edits on the main branch, a draft branch edited by an agent, and a merge back into main."
    >
      <text x={16} y={26} className="dg-sub">
        History of one row
      </text>
      <text x={16} y={96} className="dg-code">
        main
      </text>
      <text x={16} y={176} className="dg-code">
        draft
      </text>
      <line x1={80} y1={92} x2={620} y2={92} className="dg-lane-strong" />
      <path
        d="M180 92 C 220 92, 220 172, 260 172 L 470 172 C 520 172, 520 92, 570 92"
        className="dg-link-accent"
      />
      {main.map((commit) => (
        <g key={`m-${commit.x}`}>
          <circle
            cx={commit.x}
            cy={92}
            r={7}
            className={commit.who === "merge" ? "dg-dot-accent" : "dg-dot-hollow"}
          />
          <text x={commit.x} y={70} textAnchor="middle" className="dg-sub-sm">
            {commit.who}
          </text>
        </g>
      ))}
      {draft.map((commit) => (
        <g key={`d-${commit.x}`}>
          <circle cx={commit.x} cy={172} r={7} className="dg-dot-hollow-accent" />
          <text x={commit.x} y={202} textAnchor="middle" className="dg-sub-sm">
            {commit.who}
          </text>
        </g>
      ))}
      <text x={16} y={240} className="dg-sub">
        Read any version, compare branches, and see who changed what.
      </text>
    </Diagram>
  );
}

/** Migrations translate between live schema versions instead of stopping the world. */
export function SchemaDiagram() {
  return (
    <Diagram
      viewBox="0 0 640 270"
      label="Clients on schema version 1 and version 2 read and write the same data. A migration lens translates the done column to a status column in both directions."
    >
      <Node x={16} y={16} w={176} h={128} title="App v1" sub="still running" />
      <Pill x={28} y={72} w={152}>
        title: string
      </Pill>
      <Pill x={28} y={104} w={152}>
        done: boolean
      </Pill>

      <Node x={448} y={16} w={176} h={128} title="App v2" sub="just shipped" />
      <Pill x={460} y={72} w={152}>
        title: string
      </Pill>
      <Pill x={460} y={104} w={152}>
        status: enum
      </Pill>

      <rect x={236} y={52} width={168} height={56} rx={6} className="dg-node-strong" />
      <text x={320} y={76} textAnchor="middle" className="dg-title">
        Migration lens
      </text>
      <text x={320} y={96} textAnchor="middle" className="dg-code">
        {"done ⇄ status"}
      </text>
      <path
        d="M196 80 L 232 80"
        className="dg-link-accent"
        markerStart="url(#home-arrow-accent)"
        markerEnd="url(#home-arrow-accent)"
      />
      <path
        d="M408 80 L 444 80"
        className="dg-link-accent"
        markerStart="url(#home-arrow-accent)"
        markerEnd="url(#home-arrow-accent)"
      />

      <path d="M320 108 L 320 176" className="dg-link" markerEnd="url(#home-arrow)" />
      <rect x={180} y={180} width={280} height={44} rx={6} className="dg-band" />
      <text x={320} y={207} textAnchor="middle" className="dg-title-sm">
        One table, both versions live
      </text>
      <text x={16} y={258} className="dg-sub">
        Old and new clients keep reading and writing the same rows.
      </text>
    </Diagram>
  );
}

/** The parts of a typical backend that Jazz takes on. */
export function BackendDiagram() {
  const typical = [
    "API endpoints",
    "WebSocket fan-out",
    "Cache and invalidation",
    "Permission checks",
    "Message queue",
    "Blob storage and CDN",
    "Database",
  ];
  const withJazz = [
    "Sync and live queries",
    "Row-level permissions",
    "Durable streams",
    "Files and blobs",
    "Database",
  ];
  const rowH = 28;
  const step = rowH + 4;
  return (
    <Diagram
      viewBox="0 0 640 316"
      label="A typical stack needs API endpoints, WebSocket fan-out, caching, permission checks, a queue, blob storage and a database. With Jazz, your business logic sits on one layer that covers sync, permissions, streams, files and the database."
    >
      <text x={16} y={26} className="dg-title">
        Typical stack
      </text>
      <rect x={16} y={40} width={284} height={32} rx={3} className="dg-node" />
      <text x={28} y={61} className="dg-title-sm">
        Business logic
      </text>
      {typical.map((layer, index) => (
        <g key={layer}>
          <rect x={16} y={80 + index * step} width={284} height={rowH} rx={3} className="dg-row" />
          <text x={28} y={80 + index * step + 19} className="dg-sub">
            {layer}
          </text>
        </g>
      ))}

      <text x={340} y={26} className="dg-title">
        With Jazz
      </text>
      <rect x={340} y={40} width={284} height={32} rx={3} className="dg-node" />
      <text x={352} y={61} className="dg-title-sm">
        Business logic
      </text>
      <rect
        x={340}
        y={80}
        width={284}
        height={withJazz.length * step + 36}
        rx={6}
        className="dg-node-strong"
      />
      <text x={352} y={102} className="dg-title-sm">
        Jazz
      </text>
      {withJazz.map((layer, index) => (
        <g key={layer}>
          <rect
            x={352}
            y={112 + index * step}
            width={260}
            height={rowH}
            rx={3}
            className="dg-row-kept"
          />
          <text x={364} y={112 + index * step + 19} className="dg-sub">
            {layer}
          </text>
        </g>
      ))}
    </Diagram>
  );
}
