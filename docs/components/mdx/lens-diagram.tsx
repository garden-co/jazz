"use client";

import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { cn } from "@/lib/cn";

import {
  type Anchors,
  Graph,
  type GraphNode,
  type GraphOverlayCtx,
  useDiagramTraces,
} from "./diagram";
import { PhoneChrome } from "./diagram/phone-chrome";
import {
  buildPath,
  CLIENT_ID,
  collectCards,
  dataId,
  type Direction,
  getDirection,
  lensId,
  schemaId,
  type Version,
} from "./lens-diagram-path";

type Row =
  | { title: string; completed?: never; done?: never }
  | { title: string; completed: boolean; done?: never }
  | { title: string; done: boolean; completed?: never };

const SCHEMAS: Record<Version, { hash: string; fields: string[]; sample: Row }> = {
  1: { hash: "a01f5c", fields: ["title: s.string()"], sample: { title: "Buy milk" } },
  2: {
    hash: "311995",
    fields: ["title: s.string()", "completed: s.boolean()"],
    sample: { title: "Pay rent", completed: true },
  },
  3: {
    hash: "73b65d",
    fields: ["title: s.string()", "done: s.boolean()"],
    sample: { title: "Walk dog", done: false },
  },
};

type RowField = "completed" | "done";
type Migration = { forward: (r: Row) => Row; backward: (r: Row) => Row };

function addField(key: RowField, value: boolean): Migration {
  return {
    forward: (r) => ({ ...r, [key]: value }) as Row,
    backward: (r) => {
      const copy = { ...r } as Record<string, unknown>;
      delete copy[key];
      return copy as Row;
    },
  };
}

function renameField(from: RowField, to: RowField): Migration {
  const move = (src: RowField, dst: RowField) => (r: Row) => {
    const copy = { ...r } as Record<string, unknown>;
    copy[dst] = copy[src];
    delete copy[src];
    return copy as Row;
  };
  return { forward: move(from, to), backward: move(to, from) };
}

type Lens = Migration & { forwardLabel: string; backwardLabel: string };

const LENSES: Lens[] = [
  {
    forwardLabel: "Add 'completed', default false",
    backwardLabel: "Drop 'completed'",
    ...addField("completed", false),
  },
  {
    forwardLabel: "Rename 'completed' to 'done'",
    backwardLabel: "Rename 'done' to 'completed'",
    ...renameField("completed", "done"),
  },
];

function project(row: Row, fromV: Version, toV: Version): Row {
  if (fromV === toV) return row;
  let r = row;
  if (fromV < toV) {
    for (let v = fromV; v < toV; v++) r = LENSES[v - 1].forward(r);
  } else {
    for (let v = fromV; v > toV; v--) r = LENSES[v - 2].backward(r);
  }
  return r;
}

function rowAsText(row: Row): string {
  return Object.entries(row)
    .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
    .join("\n");
}

function isSchemaOnPath(version: Version, fromV: Version, toV: Version): boolean {
  const lo = Math.min(fromV, toV);
  const hi = Math.max(fromV, toV);
  return version >= lo && version <= hi;
}

function isLensOnPath(lensIdx: number, fromV: Version, toV: Version): boolean {
  if (fromV === toV) return false;
  const lo = Math.min(fromV, toV);
  const hi = Math.max(fromV, toV);
  return lensIdx + 1 >= lo && lensIdx + 2 <= hi;
}

function Header({ children }: { children: ReactNode }) {
  return (
    <div className="text-[10px] uppercase tracking-wide text-fd-muted-foreground font-semibold pb-1">
      {children}
    </div>
  );
}

function DataCard({
  version,
  row,
  isActive,
  onSelect,
}: {
  version: Version;
  row: Row;
  isActive: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        "text-left rounded-lg border-2 bg-fd-card p-3 transition-all duration-200 cursor-pointer w-full h-full flex flex-col justify-center hover:-translate-y-0.5 hover:shadow-md",
        isActive
          ? "border-[#146aff] shadow-md"
          : "border-fd-border hover:bg-fd-accent opacity-70 hover:opacity-100",
      )}
    >
      <span className="text-xs font-semibold text-fd-foreground text-balance">
        Created with schema v{version}
      </span>
      <small className="text-[10px] font-mono text-fd-muted-foreground leading-4">
        {SCHEMAS[version].hash}
      </small>
      <pre className="text-[10px] font-mono text-fd-muted-foreground whitespace-pre-wrap m-0 p-0 leading-4">
        {rowAsText(row)}
      </pre>
    </button>
  );
}

function SchemaCard({ version, isOnPath }: { version: Version; isOnPath: boolean }) {
  return (
    <div
      className={cn(
        "rounded-lg border-2 bg-fd-card p-3 h-full flex flex-col justify-center transition-all duration-300 leading-4",
        isOnPath ? "border-transparent shadow-md bg-clip-padding" : "border-fd-border opacity-50",
      )}
    >
      <div className="text-xs font-semibold text-fd-foreground">Schema v{version}</div>
      <pre className="text-[10px] font-mono text-fd-muted-foreground whitespace-pre-wrap m-0 p-0 bg-transparent">
        {SCHEMAS[version].hash}
        <br />
        {SCHEMAS[version].fields.join("\n")}
      </pre>
    </div>
  );
}

function LensCard({
  lens,
  isOnPath,
  direction,
}: {
  lens: Lens;
  isOnPath: boolean;
  direction: Direction;
}) {
  const spacerLabel =
    lens.forwardLabel.length >= lens.backwardLabel.length ? lens.forwardLabel : lens.backwardLabel;
  const visibleLabel =
    isOnPath && direction === "backward" ? lens.backwardLabel : lens.forwardLabel;
  return (
    <div
      style={{ borderRadius: "50% / 35%" }}
      className={cn(
        "rounded-md border-2 bg-fd-card px-3 py-1.5 text-xs max-w-full transition-all duration-300",
        isOnPath
          ? "border-transparent shadow-md opacity-100 bg-clip-padding"
          : "border-dashed border-fd-border opacity-50",
      )}
    >
      <div className="flex text-center gap-2 justify-center flex-col relative">
        <span className="invisible italic block text-balance">{spacerLabel}</span>
        <span
          className={cn(
            "absolute italic transition-colors text-center w-full text-balance",
            isOnPath ? "text-fd-foreground" : "text-fd-muted-foreground/60",
          )}
        >
          {visibleLabel}
        </span>
      </div>
    </div>
  );
}

const PROJECTION_CSS = `
@keyframes lens-projection-in {
  from { opacity: 0; transform: translateX(-4px); }
  to { opacity: 1; transform: translateY(0); }
}
.projection-row { animation: lens-projection-in 200ms ease-out 350ms both; }
`;

function ClientDevice({
  client,
  setClient,
  projectedRow,
  dataVersion,
}: {
  client: Version;
  setClient: (v: Version) => void;
  projectedRow: Row;
  dataVersion: Version;
}) {
  return (
    <PhoneChrome className="h-full">
      <style href="lens-projection" precedence="default">
        {PROJECTION_CSS}
      </style>
      <div>
        <div className="flex items-center gap-2 pt-2">
          <span className="text-xs font-medium text-fd-foreground">Schema:</span>
          <div className="flex gap-1 rounded-lg border border-fd-border bg-fd-card p-1">
            {([1, 2, 3] as const).map((v) => (
              <button
                key={v}
                type="button"
                onClick={() => setClient(v)}
                className={cn(
                  "px-3 py-1 rounded text-xs font-medium transition-colors",
                  client === v
                    ? "bg-fd-primary text-fd-primary-foreground"
                    : "text-fd-foreground hover:bg-fd-accent",
                )}
              >
                v{v}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex-1 rounded-lg border-2 border-[#146aff] bg-fd-card p-2 flex flex-col gap-2 justify-between">
        <div key={`${client}-${dataVersion}`}>
          <div className="text-xs uppercase tracking-wide text-[#146aff] font-semibold mb-2">
            My To Dos
          </div>
          <div className="projection-row text-xs">
            <label className="mb-2 flex items-center gap-1">
              {"completed" in projectedRow ? (
                <input type="checkbox" checked={Boolean(projectedRow.completed)} readOnly />
              ) : "done" in projectedRow ? (
                <input type="checkbox" checked={Boolean(projectedRow.done)} readOnly />
              ) : null}{" "}
              {projectedRow.title}
            </label>
          </div>
          <div className="text-xs uppercase tracking-wide text-fd-muted-foreground font-semibold mt-6 mb-2">
            JSON
          </div>
          <div className="text-xs font-mono text-fd-foreground whitespace-pre-wrap m-0 border-2 projection-row bg-fd-muted rounded-xl p-1">
            {JSON.stringify(projectedRow, null, 1)}
          </div>
        </div>
        <div className="text-xs text-fd-muted-foreground italic text-balance">
          {client === dataVersion
            ? `Read directly from v${dataVersion} data.`
            : `v${dataVersion} row, read through the lens chain by a client on schema v${client}.`}
        </div>
      </div>
    </PhoneChrome>
  );
}

export function LensDiagram() {
  const [client, setClient] = useState<Version>(3);
  const [dataVersion, setDataVersion] = useState<Version>(1);
  const direction = getDirection(dataVersion, client);
  const projectedRow = project(SCHEMAS[dataVersion].sample, dataVersion, client);

  const traces = useDiagramTraces();
  const [anchors, setAnchors] = useState<Record<string, Anchors>>({});
  const onGeometry = useCallback((ctx: GraphOverlayCtx) => setAnchors(ctx.anchors), []);

  const pathD = useMemo(() => {
    return buildPath({
      cards: collectCards(dataVersion, client),
      anchors,
      dataId: dataId(dataVersion),
      clientId: CLIENT_ID,
      direction,
    });
  }, [anchors, dataVersion, client, direction]);

  // Draw on a selection change; snap (no replay) when geometry changes for the
  // same selection (resize).
  const lastKey = useRef("");
  useEffect(() => {
    if (!pathD) return;
    const key = `${dataVersion}-${client}`;
    if (lastKey.current === key) {
      traces.snap("lens");
      return;
    }
    lastKey.current = key;
    traces.play({
      id: "lens",
      d: pathD,
      follow: true,
      timing: { min: 700, max: 4000, perPx: 2.9 },
    });
  }, [pathD, dataVersion, client, traces]);

  // Path runs behind the cards; the tip dot rides above them.
  const overlay = useCallback(
    () => (
      <path
        ref={traces.pathRef("lens")}
        className="diagram-path"
        d={pathD}
        fill="none"
        stroke="var(--diagram-accent, #146aff)"
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    ),
    [pathD, traces],
  );

  const overlayFront = useCallback(
    () => (
      <circle
        ref={traces.dotRef("lens")}
        className="diagram-dot"
        r={4}
        cx={0}
        cy={0}
        fill="var(--diagram-accent, #146aff)"
        style={{ opacity: 0 }}
      />
    ),
    [traces],
  );

  const nodes: GraphNode[] = [
    { id: "hdr-data", slot: { row: "1 / 2", col: "1 / 2" }, content: <Header>Data</Header> },
    {
      id: "hdr-mid",
      slot: { row: "1 / 2", col: "2 / 3" },
      content: <Header>Schemas + lenses</Header>,
    },
    { id: "hdr-client", slot: { row: "1 / 2", col: "3 / 4" }, content: <Header>Client</Header> },
    ...([1, 2, 3] as const).flatMap((v): GraphNode[] => {
      const rowLine = `${2 * v} / ${2 * v + 1}`;
      return [
        {
          id: dataId(v),
          slot: { row: rowLine, col: "1 / 2" },
          content: (
            <DataCard
              version={v}
              row={SCHEMAS[v].sample}
              isActive={dataVersion === v}
              onSelect={() => setDataVersion(v)}
            />
          ),
        },
        {
          id: schemaId(v),
          slot: { row: rowLine, col: "2 / 3" },
          content: <SchemaCard version={v} isOnPath={isSchemaOnPath(v, dataVersion, client)} />,
        },
      ];
    }),
    ...([0, 1] as const).map(
      (idx): GraphNode => ({
        id: lensId(idx),
        slot: { row: `${2 * (idx + 1) + 1} / ${2 * (idx + 1) + 2}`, col: "2 / 3" },
        content: (
          <LensCard
            lens={LENSES[idx]}
            isOnPath={isLensOnPath(idx, dataVersion, client)}
            direction={direction}
          />
        ),
      }),
    ),
    {
      id: CLIENT_ID,
      slot: { row: "2 / 7", col: "3 / 4" },
      content: (
        <ClientDevice
          client={client}
          setClient={setClient}
          projectedRow={projectedRow}
          dataVersion={dataVersion}
        />
      ),
    },
  ];

  return (
    <Graph
      eyebrow="Interactive Demo"
      description={
        <>
          Choose data from the left, then pick a schema version on the client device. The row is
          loaded as-is, and the client applies all the lenses needed to interpret it using its
          schema.
        </>
      }
      direction="LR"
      nodes={nodes}
      edges={[]}
      naturalWidth={560}
      grid={{
        columns: "minmax(120px, 0.9fr) minmax(160px, 1fr) minmax(220px, 1.1fr)",
        rows: "auto auto auto auto auto auto",
        gap: "0.5rem 1rem",
      }}
      nodeAlign="stretch"
      arrows={false}
      traces={traces}
      overlay={overlay}
      overlayBehindNodes
      overlayFront={overlayFront}
      onGeometry={onGeometry}
    />
  );
}
