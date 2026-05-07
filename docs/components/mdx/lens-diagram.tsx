"use client";

import { Fragment, useEffect, useLayoutEffect, useRef, useState } from "react";

import { cn } from "@/lib/cn";

import {
  buildPath,
  collectCards,
  getDirection,
  type Direction,
  type Version,
} from "./lens-diagram-path";

type Row =
  | { title: string; completed?: never; done?: never }
  | { title: string; completed: boolean; done?: never }
  | { title: string; done: boolean; completed?: never };

const SCHEMAS: Record<Version, { hash: string; fields: string[]; sample: Row }> = {
  1: {
    hash: "a01f5c",
    fields: ["title: s.string()"],
    sample: { title: "Buy milk" },
  },
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

function DataCard({
  version,
  row,
  isActive,
  onSelect,
  innerRef,
}: {
  version: Version;
  row: Row;
  isActive: boolean;
  onSelect: () => void;
  innerRef: (el: HTMLButtonElement | null) => void;
}) {
  return (
    <button
      ref={innerRef}
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

function SchemaCard({
  version,
  isOnPath,
  innerRef,
}: {
  version: Version;
  isOnPath: boolean;
  innerRef: (el: HTMLDivElement | null) => void;
}) {
  return (
    <div
      ref={innerRef}
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
  innerRef,
}: {
  lens: Lens;
  isOnPath: boolean;
  direction: Direction;
  innerRef: (el: HTMLDivElement | null) => void;
}) {
  // Pick the longer of the two labels to reserve consistent height regardless
  // of direction or active state — prevents layout shift when toggling.
  const spacerLabel =
    lens.forwardLabel.length >= lens.backwardLabel.length ? lens.forwardLabel : lens.backwardLabel;
  // Off-path lenses always show the forward label; the direction only flips
  // the label when this lens is actually being walked.
  const visibleLabel =
    isOnPath && direction === "backward" ? lens.backwardLabel : lens.forwardLabel;
  return (
    <div
      ref={innerRef}
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

function StatusBarIcons() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="80"
      height="18"
      fill="currentColor"
      viewBox="0 0 80 18"
    >
      <path
        fillRule="evenodd"
        d="M19.528 4.033c0-.633-.477-1.146-1.066-1.146h-1.067c-.59 0-1.067.513-1.067 1.146v9.934c0 .633.478 1.146 1.067 1.146h1.067c.589 0 1.066-.513 1.066-1.146zm-7.434 1.3h1.067c.589 0 1.066.525 1.066 1.173v7.434c0 .648-.477 1.173-1.066 1.173h-1.067c-.59 0-1.067-.525-1.067-1.173V6.506c0-.648.478-1.174 1.067-1.174M7.762 7.98H6.696c-.59 0-1.067.532-1.067 1.189v4.755c0 .656.477 1.188 1.067 1.188h1.066c.59 0 1.067-.532 1.067-1.188V9.17c0-.657-.478-1.189-1.067-1.189m-5.3 2.446H1.394c-.59 0-1.067.524-1.067 1.171v2.344c0 .647.478 1.171 1.067 1.171H2.46c.59 0 1.067-.524 1.067-1.171v-2.344c0-.647-.477-1.171-1.067-1.171M36.1 5.302c2.487 0 4.879.923 6.681 2.576a.355.355 0 0 0 .487-.004l1.297-1.263a.34.34 0 0 0-.003-.494c-4.73-4.375-12.195-4.375-16.926 0a.342.342 0 0 0-.003.494l1.298 1.263c.133.13.35.132.486.004 1.803-1.654 4.195-2.576 6.683-2.576m-.004 4.22c1.358 0 2.667.512 3.673 1.436.136.131.35.129.483-.006l1.287-1.32a.367.367 0 0 0-.005-.518 7.9 7.9 0 0 0-10.873 0 .367.367 0 0 0-.005.519l1.287 1.319a.343.343 0 0 0 .483.006 5.43 5.43 0 0 1 3.67-1.435m2.525 2.794a.4.4 0 0 1-.103.28l-2.176 2.456a.32.32 0 0 1-.242.112.32.32 0 0 1-.242-.112l-2.177-2.455a.4.4 0 0 1-.102-.28.4.4 0 0 1 .113-.277c1.39-1.314 3.426-1.314 4.816 0 .07.071.11.17.113.276"
        clipRule="evenodd"
      />
      <path
        d="M71.17 14.5v1h-12v-1zm5.5-5.5c0-1.039 0-1.767-.04-2.338-.04-.56-.113-.894-.223-1.152a3.3 3.3 0 0 0-1.747-1.747c-.258-.11-.591-.184-1.152-.223-.57-.04-1.299-.04-2.338-.04h-12c-1.04 0-1.767 0-2.338.04-.56.04-.894.113-1.152.223a3.3 3.3 0 0 0-1.747 1.747c-.11.258-.184.591-.223 1.152-.04.57-.04 1.299-.04 2.338s0 1.768.04 2.338c.04.561.113.894.223 1.152.334.787.96 1.413 1.747 1.748.258.11.591.183 1.152.222.57.04 1.299.04 2.338.04v1l-1.358-.005c-1.192-.016-1.92-.08-2.524-.338a4.3 4.3 0 0 1-2.19-2.085l-.085-.19c-.343-.806-.343-1.831-.343-3.882 0-1.922 0-2.943.282-3.727l.06-.155a4.3 4.3 0 0 1 2.087-2.19l.19-.085c.604-.257 1.331-.322 2.523-.338L59.17 2.5h12c2.05 0 3.076 0 3.882.343a4.3 4.3 0 0 1 2.275 2.275c.343.806.343 1.832.343 3.882s0 3.076-.343 3.882l-.086.19a4.3 4.3 0 0 1-2.19 2.085l-.154.061c-.784.282-1.805.282-3.727.282v-1c1.04 0 1.767 0 2.338-.04.56-.039.894-.113 1.152-.223a3.3 3.3 0 0 0 1.747-1.747c.11-.258.184-.591.223-1.152.04-.57.04-1.299.04-2.338"
        opacity=".35"
      />
      <path
        d="M78.67 7.281v4.076a2.21 2.21 0 0 0 1.328-2.038c0-.89-.523-1.693-1.328-2.038"
        opacity=".4"
      />
      <path d="M54.67 8.5c0-1.4 0-2.1.272-2.635a2.5 2.5 0 0 1 1.093-1.092C56.57 4.5 57.27 4.5 58.67 4.5h13c1.4 0 2.1 0 2.635.273a2.5 2.5 0 0 1 1.092 1.092c.273.535.273 1.235.273 2.635v1c0 1.4 0 2.1-.273 2.635a2.5 2.5 0 0 1-1.092 1.093c-.535.272-1.235.272-2.635.272h-13c-1.4 0-2.1 0-2.635-.272a2.5 2.5 0 0 1-1.093-1.093C54.67 11.6 54.67 10.9 54.67 9.5z" />
    </svg>
  );
}

function ClientDevice({
  client,
  setClient,
  projectedRow,
  dataVersion,
  innerRef,
}: {
  client: Version;
  setClient: (v: Version) => void;
  projectedRow: Row;
  dataVersion: Version;
  innerRef: (el: HTMLDivElement | null) => void;
}) {
  const [time, setTime] = useState(() => new Date());
  useEffect(() => {
    let timeoutId: ReturnType<typeof setTimeout>;
    const tick = () => {
      setTime(new Date());
      timeoutId = setTimeout(tick, 60_000 - (Date.now() % 60_000));
    };
    timeoutId = setTimeout(tick, 60_000 - (Date.now() % 60_000));
    return () => clearTimeout(timeoutId);
  }, []);
  return (
    <div
      ref={innerRef}
      className="rounded-2xl border-5 border-fd-primary bg-fd-card p-2 pt-4 h-full flex flex-col gap-2"
    >
      <div className="grid grid-cols-3 w-full absolute h-4 top-0 left-0 rounded-b-lg gap-2">
        <div className="col-span-1 ps-5 flex flex-col text-xs pt-1.5 uppercase">
          {time.toLocaleTimeString("en-GB", {
            hour: "numeric",
            minute: "2-digit",
            hour12: false,
          })}
        </div>
        <div className="bg-fd-primary rounded-b-lg"></div>
        <div className="flex justify-end pt-1.5 pe-4 items-center">
          <StatusBarIcons />
        </div>
      </div>
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
    </div>
  );
}

export function LensDiagram() {
  const [client, setClient] = useState<Version>(3);
  const [dataVersion, setDataVersion] = useState<Version>(1);

  const direction = getDirection(dataVersion, client);
  const projectedRow = project(SCHEMAS[dataVersion].sample, dataVersion, client);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const dataRefs = useRef<Partial<Record<Version, HTMLButtonElement | null>>>({});
  const schemaRefs = useRef<Partial<Record<Version, HTMLDivElement | null>>>({});
  const lensRefs = useRef<Partial<Record<number, HTMLDivElement | null>>>({});
  const clientRef = useRef<HTMLDivElement | null>(null);

  const [pathD, setPathD] = useState("");
  const [svgSize, setSvgSize] = useState({ w: 0, h: 0 });
  const arrowPathRef = useRef<SVGPathElement | null>(null);
  const tipDotRef = useRef<SVGCircleElement | null>(null);
  const lastAnimatedKey = useRef<string>("");

  useLayoutEffect(() => {
    function measure() {
      const container = containerRef.current;
      const dataEl = dataRefs.current[dataVersion];
      const clientEl = clientRef.current;
      if (!container || !dataEl || !clientEl) return;

      const cards = collectCards(dataVersion, client, schemaRefs.current, lensRefs.current);
      if (!cards) return;

      const c = container.getBoundingClientRect();
      const d = dataEl.getBoundingClientRect();
      const cl = clientEl.getBoundingClientRect();
      const rects = cards.map((card) => card.el.getBoundingClientRect());

      setSvgSize((prev) =>
        prev.w === c.width && prev.h === c.height ? prev : { w: c.width, h: c.height },
      );
      setPathD(
        buildPath({
          cards,
          rects,
          container: c,
          data: d,
          client: cl,
          direction,
        }),
      );
    }

    measure();
    const ro = new ResizeObserver(measure);
    if (containerRef.current) ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, [client, dataVersion]);

  useLayoutEffect(() => {
    const el = arrowPathRef.current;
    if (!el || !pathD) return;
    const len = el.getTotalLength();
    if (len <= 0) return;

    // Re-trigger the draw animation only on user interaction (when the
    // selected version pair changes). Path updates from a window resize
    // arrive on the same key and should snap to the fully-drawn state.
    const key = `${dataVersion}-${client}`;
    if (lastAnimatedKey.current === key) {
      el.style.transition = "none";
      el.style.strokeDasharray = `${len}`;
      el.style.strokeDashoffset = "0";
      const dot = tipDotRef.current;
      if (dot) {
        const end = el.getPointAtLength(len);
        dot.setAttribute("cx", String(end.x));
        dot.setAttribute("cy", String(end.y));
      }
      return;
    }
    lastAnimatedKey.current = key;

    el.style.transition = "none";
    el.style.strokeDasharray = `${len}`;
    el.style.strokeDashoffset = `${len}`;
    void el.getBoundingClientRect();
    const duration = Math.max(500, Math.min(len * 1.43, 3000));
    el.style.transition = `stroke-dashoffset ${duration}ms ease-out`;
    el.style.strokeDashoffset = "0";

    const dot = tipDotRef.current;
    if (!dot) return;
    let rafId = 0;
    const tick = () => {
      const offset = parseFloat(getComputedStyle(el).strokeDashoffset);
      if (Number.isNaN(offset)) return;
      const distance = Math.max(0, len - offset);
      const point = el.getPointAtLength(distance);
      dot.setAttribute("cx", String(point.x));
      dot.setAttribute("cy", String(point.y));
      if (offset > 0.5) {
        rafId = requestAnimationFrame(tick);
      }
    };
    rafId = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafId);
  }, [pathD]);

  return (
    <div className="my-6 rounded-xl border border-fd-border bg-fd-card/30 p-5 not-prose hidden lg:block">
      <style precedence="default">{`
        .lens-arrow-path {
          stroke-dasharray: 99999;
          stroke-dashoffset: 99999;
        }
        @keyframes lens-projection-in {
          from { opacity: 0; transform: translateX(-4px); }
          to { opacity: 1; transform: translateY(0); }
        }
        .projection-row {
          animation: lens-projection-in 200ms ease-out 350ms both;
        }
      `}</style>

      <p className="uppercase text-xs font-bold text-fd-primary">Interactive Demo</p>
      <p className="text-sm text-fd-muted-foreground mb-4">
        Choose data from the left, then pick a schema version on the client device. The row is
        loaded as-is, and the client applies all the lenses needed to interpret it using its schema.
      </p>

      <div
        ref={containerRef}
        className="relative grid items-stretch gap-x-4"
        style={{
          gridTemplateColumns: "minmax(120px, 0.9fr) minmax(160px, 1fr) minmax(220px, 1.1fr)",
          gridTemplateRows: "auto auto auto auto auto auto",
          rowGap: "0.5rem",
        }}
      >
        <div
          className="text-[10px] uppercase tracking-wide text-fd-muted-foreground font-semibold pb-1"
          style={{ gridRow: "1 / 2", gridColumn: "1 / 2" }}
        >
          Data
        </div>
        <div
          className="text-[10px] uppercase tracking-wide text-fd-muted-foreground font-semibold pb-1"
          style={{ gridRow: "1 / 2", gridColumn: "2 / 3" }}
        >
          Schemas + lenses
        </div>
        <div
          className="text-[10px] uppercase tracking-wide text-fd-muted-foreground font-semibold pb-1"
          style={{ gridRow: "1 / 2", gridColumn: "3 / 3" }}
        >
          Client
        </div>
        {([1, 2, 3] as const).map((v) => {
          const dataRow = `${2 * v} / ${2 * v + 1}`;
          const lensIdx = v - 1;
          return (
            <Fragment key={v}>
              <div style={{ gridRow: dataRow, gridColumn: "1 / 2" }} className="relative z-10">
                <DataCard
                  version={v}
                  row={SCHEMAS[v].sample}
                  isActive={dataVersion === v}
                  onSelect={() => setDataVersion(v)}
                  innerRef={(el) => {
                    dataRefs.current[v] = el;
                  }}
                />
              </div>
              <div className="relative z-10" style={{ gridRow: dataRow, gridColumn: "2 / 3" }}>
                <SchemaCard
                  version={v}
                  isOnPath={isSchemaOnPath(v, dataVersion, client)}
                  innerRef={(el) => {
                    schemaRefs.current[v] = el;
                  }}
                />
              </div>
              {v < 3 && (
                <div
                  style={{
                    gridRow: `${2 * v + 1} / ${2 * v + 2}`,
                    gridColumn: "2 / 3",
                  }}
                  className="relative z-10 flex justify-center"
                >
                  <LensCard
                    lens={LENSES[lensIdx]}
                    isOnPath={isLensOnPath(lensIdx, dataVersion, client)}
                    direction={direction}
                    innerRef={(el) => {
                      lensRefs.current[lensIdx] = el;
                    }}
                  />
                </div>
              )}
            </Fragment>
          );
        })}
        <div
          className="relative z-10 self-stretch"
          style={{ gridRow: "2 / 7", gridColumn: "3 / 4" }}
        >
          <ClientDevice
            client={client}
            setClient={setClient}
            projectedRow={projectedRow}
            dataVersion={dataVersion}
            innerRef={(el) => {
              clientRef.current = el;
            }}
          />
        </div>
        {svgSize.w > 0 && (
          <>
            <svg
              className="absolute pointer-events-none inset-0 text-[#146aff]"
              style={{ zIndex: 0 }}
              width={svgSize.w}
              height={svgSize.h}
              viewBox={`0 0 ${svgSize.w} ${svgSize.h}`}
            >
              <path
                ref={arrowPathRef}
                className="lens-arrow-path"
                d={pathD}
                stroke="currentColor"
                strokeWidth="2"
                fill="none"
              />
            </svg>
            <svg
              className="absolute pointer-events-none inset-0 text-[#146aff]"
              style={{ zIndex: 20 }}
              width={svgSize.w}
              height={svgSize.h}
              viewBox={`0 0 ${svgSize.w} ${svgSize.h}`}
            >
              <circle ref={tipDotRef} r="4" cx="0" cy="0" fill="currentColor" />
            </svg>
          </>
        )}
      </div>
    </div>
  );
}
