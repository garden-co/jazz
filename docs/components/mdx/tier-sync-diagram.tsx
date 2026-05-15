"use client";

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Cloud, Plus, Server, Smartphone, type LucideIcon } from "lucide-react";

import { cn } from "@/lib/cn";

import {
  anchorsFor,
  buildConnectionPath,
  buildStaticTopology,
  type Anchors,
} from "./tier-sync-path";
import { DiagramFrame } from "./diagram/frame";
import { DiagramStyles } from "./diagram/styles";
import { drawPath, trackDotAlongPath } from "./diagram/trace-anim";

type Tier = "global" | "edge" | "local";
type NodeKey = "global" | "edge1" | "edge2" | "alice" | "bob" | "charlie";

type NodeMeta = { label: string; subtitle: string; tier: Tier };

const NODES: Record<NodeKey, NodeMeta> = {
  global: { label: "Global", subtitle: "global core", tier: "global" },
  edge1: { label: "Edge", subtitle: "sync server", tier: "edge" },
  edge2: { label: "Edge", subtitle: "sync server", tier: "edge" },
  alice: { label: "Alice", subtitle: "local", tier: "local" },
  bob: { label: "Bob", subtitle: "local", tier: "local" },
  charlie: { label: "Charlie", subtitle: "local", tier: "local" },
};

const TIER_ICONS: Record<Tier, LucideIcon> = {
  global: Cloud,
  edge: Server,
  local: Smartphone,
};

const TOPOLOGY: Record<NodeKey, NodeKey[]> = {
  alice: ["edge1"],
  bob: ["edge1"],
  charlie: ["edge2"],
  edge1: ["alice", "bob", "global"],
  edge2: ["charlie", "global"],
  global: ["edge1", "edge2"],
};

const CLIENTS: NodeKey[] = ["alice", "bob", "charlie"];

type Hop = { from: NodeKey; to: NodeKey };

const ALL_HOPS: Hop[] = (() => {
  const out: Hop[] = [];
  for (const [from, neighbors] of Object.entries(TOPOLOGY) as Array<[NodeKey, NodeKey[]]>) {
    for (const to of neighbors) out.push({ from, to });
  }
  return out;
})();

function hopId(h: Hop): string {
  return `${h.from}-${h.to}`;
}

function bfsFrom(writer: NodeKey): Array<Hop[]> {
  const visited = new Set<NodeKey>([writer]);
  const stages: Array<Hop[]> = [];
  let frontier: NodeKey[] = [writer];
  while (frontier.length > 0) {
    const nextFrontier: NodeKey[] = [];
    const hops: Hop[] = [];
    for (const node of frontier) {
      for (const neighbor of TOPOLOGY[node]) {
        if (visited.has(neighbor)) continue;
        visited.add(neighbor);
        hops.push({ from: node, to: neighbor });
        nextFrontier.push(neighbor);
      }
    }
    if (hops.length > 0) stages.push(hops);
    frontier = nextFrontier;
  }
  return stages;
}

// Targets that aren't forwarded onward — leaves of the BFS spanning tree.
function terminalsOf(stages: Hop[][]): Set<NodeKey> {
  const sources = new Set<NodeKey>();
  const targets = new Set<NodeKey>();
  for (const stage of stages) {
    for (const hop of stage) {
      sources.add(hop.from);
      targets.add(hop.to);
    }
  }
  const out = new Set<NodeKey>();
  for (const t of targets) if (!sources.has(t)) out.add(t);
  return out;
}

function tierDepth(tier: Tier): number {
  return tier === "global" ? 0 : tier === "edge" ? 1 : 2;
}

const STAGE_DURATION = 1100;
const TRACE_FADE_MS = 700;
const PULSE_ANIMATION_MS = 1400;
const PRIMARY = "#146aff";

type PulseState = Record<NodeKey, number>;
type ColorState = Record<NodeKey, string | null>;

const ZERO_PULSES: PulseState = {
  global: 0,
  edge1: 0,
  edge2: 0,
  alice: 0,
  bob: 0,
  charlie: 0,
};
const INITIAL_COLOR = "#146aff";
const INITIAL_COLORS: ColorState = {
  global: INITIAL_COLOR,
  edge1: INITIAL_COLOR,
  edge2: INITIAL_COLOR,
  alice: INITIAL_COLOR,
  bob: INITIAL_COLOR,
  charlie: INITIAL_COLOR,
};

const PALETTE = [
  "#ef4444",
  "#f97316",
  "#eab308",
  "#22c55e",
  "#06b6d4",
  "#3b82f6",
  "#8b5cf6",
  "#ec4899",
];

function pickNextColor(current: string | null): string {
  const choices = PALETTE.filter((c) => c !== current);
  return choices[Math.floor(Math.random() * choices.length)];
}

type WriteEvent = {
  id: number;
  writer: NodeKey;
  value: string;
  stages: Hop[][];
  startedAt: number;
};

function NodeCard({
  nodeKey,
  color,
  pulseKey,
  onWrite,
  innerRef,
}: {
  nodeKey: NodeKey;
  color: string | null;
  pulseKey: number;
  onWrite?: () => void;
  innerRef: (el: HTMLDivElement | null) => void;
}) {
  const meta = NODES[nodeKey];
  const Icon = TIER_ICONS[meta.tier];
  return (
    <div
      ref={innerRef}
      className="rounded-lg border-2 border-fd-border bg-fd-card px-3 py-2 w-[10rem] flex flex-col items-center text-center relative"
    >
      {pulseKey > 0 && (
        <span
          key={pulseKey}
          className="diagram-pulse pointer-events-none absolute inset-[-2px] rounded-lg"
        />
      )}
      <Icon className="h-4 w-4 mb-1 text-fd-muted-foreground" />
      <div className="text-xs font-semibold text-fd-foreground leading-tight">{meta.label}</div>
      <div className="text-[10px] text-fd-muted-foreground leading-tight">{meta.subtitle}</div>
      <div
        className="mt-2 w-full rounded px-2 py-1 flex items-center justify-between gap-1.5 transition-colors duration-200"
        style={{ backgroundColor: color ? `${color}26` : undefined }}
      >
        <span className="text-[10px] font-mono text-fd-muted-foreground">color</span>
        <span
          className="inline-block overflow-hidden text-xs font-mono tabular-nums text-fd-foreground leading-none"
          style={{ height: "1em" }}
          aria-live="polite"
        >
          <span key={`hex-${color ?? "none"}`} className="block diagram-tally">
            {color ?? "—"}
          </span>
        </span>
        <span
          className="inline-block overflow-hidden rounded border border-fd-border flex-shrink-0"
          style={{ height: "0.9rem", width: "0.9rem" }}
        >
          <span
            key={`sw-${color ?? "none"}`}
            className="block diagram-tally h-full w-full"
            style={{ backgroundColor: color ?? "transparent" }}
          />
        </span>
      </div>
      {onWrite && (
        <button
          type="button"
          aria-label={`New colour from ${meta.label}`}
          onClick={onWrite}
          className="mt-2 block w-full rounded border border-fd-border bg-fd-card px-2 py-1 text-xs font-medium text-fd-foreground hover:bg-fd-accent cursor-pointer flex items-center justify-center gap-1"
        >
          <Plus className="h-3 w-3" />
          <span>New colour</span>
        </button>
      )}
    </div>
  );
}

export function TierSyncDiagram() {
  const [colors, setColors] = useState<ColorState>(INITIAL_COLORS);
  const [pulses, setPulses] = useState<PulseState>(ZERO_PULSES);
  const [events, setEvents] = useState<WriteEvent[]>([]);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const nodeRefs = useRef<Partial<Record<NodeKey, HTMLDivElement | null>>>({});

  const [svgSize, setSvgSize] = useState({ w: 0, h: 0 });
  const [pathDs, setPathDs] = useState<Record<string, string>>({});
  const [staticD, setStaticD] = useState("");

  // One <path> per directed hop, hidden — used only for getTotalLength / getPointAtLength.
  const measurePathRefs = useRef<Record<string, SVGPathElement | null>>({});
  const lengthCache = useRef<Record<string, number>>({});
  const destReachCache = useRef<Record<string, number>>({});

  // One animated <path> + <circle> per (event, hop).
  const eventPathRefs = useRef<Record<string, SVGPathElement | null>>({});
  const eventDotRefs = useRef<Record<string, SVGCircleElement | null>>({});

  const eventIdRef = useRef(0);
  const scheduledEventsRef = useRef<Set<number>>(new Set());
  const eventResourcesRef = useRef<
    Map<number, { timers: ReturnType<typeof setTimeout>[]; cleanups: Array<() => void> }>
  >(new Map());

  const lastInteractionRef = useRef(performance.now());
  const fireWriteRef = useRef<(writer: NodeKey, isUserAction?: boolean) => void>(() => {});
  const colorsRef = useRef<ColorState>(INITIAL_COLORS);

  // Per-node LWW guard: every node remembers the highest event id it has seen
  // and rejects arrivals with an older id, so concurrent writes converge.
  const lastSeenIdRef = useRef<Record<NodeKey, number>>({
    global: 0,
    edge1: 0,
    edge2: 0,
    alice: 0,
    bob: 0,
    charlie: 0,
  });

  useLayoutEffect(() => {
    function measure() {
      const container = containerRef.current;
      if (!container) return;
      const c = container.getBoundingClientRect();
      if (c.width === 0) return;

      const anchors: Partial<Record<NodeKey, Anchors>> = {};
      for (const key of Object.keys(NODES) as NodeKey[]) {
        const a = anchorsFor(nodeRefs.current[key], c);
        if (a) anchors[key] = a;
      }
      if (
        !anchors.global ||
        !anchors.edge1 ||
        !anchors.edge2 ||
        !anchors.alice ||
        !anchors.bob ||
        !anchors.charlie
      ) {
        return;
      }

      const next: Record<string, string> = {};
      const dests: Record<string, number> = {};
      for (const hop of ALL_HOPS) {
        const src = anchors[hop.from]!;
        const tgt = anchors[hop.to]!;
        const fromDepth = tierDepth(NODES[hop.from].tier);
        const toDepth = tierDepth(NODES[hop.to].tier);
        const direction = fromDepth > toDepth ? "up" : "down";
        const upper = fromDepth < toDepth ? src : tgt;
        const lower = fromDepth < toDepth ? tgt : src;
        const armY = (upper.bottom + lower.top) / 2;
        const { d, destReachDistance } = buildConnectionPath({
          source: src,
          target: tgt,
          direction,
          armY,
          includeSourceLoop: false,
        });
        next[hopId(hop)] = d;
        dests[hopId(hop)] = destReachDistance;
      }

      const staticPath = buildStaticTopology([
        {
          parent: anchors.global,
          children: [anchors.edge1, anchors.edge2],
          armY: (anchors.global.bottom + anchors.edge1.top) / 2,
        },
        {
          parent: anchors.edge1,
          children: [anchors.alice, anchors.bob],
          armY: (anchors.edge1.bottom + anchors.alice.top) / 2,
        },
        {
          parent: anchors.edge2,
          children: [anchors.charlie],
          armY: (anchors.edge2.bottom + anchors.charlie.top) / 2,
        },
      ]);

      setSvgSize((prev) =>
        prev.w === c.width && prev.h === c.height ? prev : { w: c.width, h: c.height },
      );
      setPathDs(next);
      setStaticD(staticPath);
      destReachCache.current = dests;
    }

    measure();
    const ro = new ResizeObserver(measure);
    if (containerRef.current) ro.observe(containerRef.current);
    for (const key of Object.keys(NODES) as NodeKey[]) {
      const el = nodeRefs.current[key];
      if (el) ro.observe(el);
    }
    return () => ro.disconnect();
  }, []);

  // Cache total length per directed hop using the hidden measurement paths.
  useLayoutEffect(() => {
    for (const hop of ALL_HOPS) {
      const id = hopId(hop);
      const el = measurePathRefs.current[id];
      if (!el) continue;
      lengthCache.current[id] = el.getTotalLength();
    }
  }, [pathDs]);

  function scheduleEvent(event: WriteEvent) {
    const resources: {
      timers: ReturnType<typeof setTimeout>[];
      cleanups: Array<() => void>;
    } = {
      timers: [],
      cleanups: [],
    };
    eventResourcesRef.current.set(event.id, resources);

    let stageDelay = 0;
    for (let s = 0; s < event.stages.length; s++) {
      const currentStageDelay = stageDelay;
      for (const hop of event.stages[s]) {
        const id = hopId(hop);
        const len = lengthCache.current[id] ?? 0;
        const destDist = destReachCache.current[id] ?? len;
        const duration = Math.max(700, Math.min(len * 1.3, STAGE_DURATION - 60));
        const arrivalTime = currentStageDelay + duration * (destDist / Math.max(1, len));
        const target = hop.to;
        const value = event.value;

        // Receiver pulse + counter update fire exactly when the leading dot
        // crosses the target edge — but only if this event is newer than
        // anything the target has already seen (LWW by event id).
        resources.timers.push(
          setTimeout(() => {
            const lastSeen = lastSeenIdRef.current[target] ?? 0;
            if (event.id <= lastSeen) return;
            lastSeenIdRef.current = { ...lastSeenIdRef.current, [target]: event.id };
            colorsRef.current = { ...colorsRef.current, [target]: value };
            setColors((c) => ({ ...c, [target]: value }));
            setPulses((p) => ({ ...p, [target]: p[target] + 1 }));
          }, arrivalTime),
        );

        // Kick off the trace draw + dot animation at the start of this stage.
        resources.timers.push(
          setTimeout(() => {
            const key = `${event.id}-${id}`;
            const pathEl = eventPathRefs.current[key];
            const dotEl = eventDotRefs.current[key];
            if (!pathEl) return;

            drawPath(pathEl, duration);

            // Path fades after the draw completes.
            resources.timers.push(
              setTimeout(() => {
                if (!pathEl.isConnected) return;
                pathEl.style.transition = `opacity ${TRACE_FADE_MS}ms ease-out`;
                pathEl.style.opacity = "0";
              }, duration),
            );

            if (!dotEl) return;
            // Full opacity along the arm; linear fade as the dot wraps the
            // receiver, hitting 0 exactly when the trace finishes drawing.
            const loopLength = Math.max(1, len - destDist);
            const stop = trackDotAlongPath(pathEl, dotEl, {
              opacityForDistance: (distance) =>
                distance <= destDist ? 1 : Math.max(0, 1 - (distance - destDist) / loopLength),
            });
            resources.cleanups.push(stop);
          }, currentStageDelay),
        );
      }
      stageDelay += STAGE_DURATION;
    }

    // Drop the event from state once the trace, pulse, and dot fade have all
    // had time to finish.
    resources.timers.push(
      setTimeout(
        () => {
          setEvents((evts) => evts.filter((e) => e.id !== event.id));
        },
        stageDelay + PULSE_ANIMATION_MS + 400,
      ),
    );
  }

  // Schedule animations for newly added events; clean up resources for removed ones.
  useEffect(() => {
    for (const event of events) {
      if (scheduledEventsRef.current.has(event.id)) continue;
      scheduledEventsRef.current.add(event.id);
      scheduleEvent(event);
    }
    const liveIds = new Set(events.map((e) => e.id));
    for (const id of Array.from(scheduledEventsRef.current)) {
      if (liveIds.has(id)) continue;
      const res = eventResourcesRef.current.get(id);
      if (res) {
        res.timers.forEach((t) => clearTimeout(t));
        res.cleanups.forEach((fn) => fn());
        eventResourcesRef.current.delete(id);
      }
      scheduledEventsRef.current.delete(id);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events]);

  // Component unmount cleanup.
  useEffect(() => {
    return () => {
      for (const res of eventResourcesRef.current.values()) {
        res.timers.forEach((t) => clearTimeout(t));
        res.cleanups.forEach((fn) => fn());
      }
      eventResourcesRef.current.clear();
      scheduledEventsRef.current.clear();
    };
  }, []);

  function fireWrite(writer: NodeKey, isUserAction = true) {
    if (isUserAction) {
      lastInteractionRef.current = performance.now();
    }
    const newEventId = ++eventIdRef.current;
    const newValue = pickNextColor(colorsRef.current[writer]);
    // Keep the ref ahead of React's commit so rapid clicks see the new value.
    colorsRef.current = { ...colorsRef.current, [writer]: newValue };
    // Writer immediately stamps its own row with this event id so later
    // arrivals from older waves are rejected.
    lastSeenIdRef.current = { ...lastSeenIdRef.current, [writer]: newEventId };

    const event: WriteEvent = {
      id: newEventId,
      writer,
      value: newValue,
      stages: bfsFrom(writer),
      startedAt: performance.now(),
    };

    setColors((prev) => ({ ...prev, [writer]: newValue }));
    setEvents((evts) => (evts.some((e) => e.id === newEventId) ? evts : [...evts, event]));
    setPulses((p) => ({ ...p, [writer]: p[writer] + 1 }));
  }

  fireWriteRef.current = fireWrite;

  // Demo on mount + ambient pulses when the user goes idle. Each iteration
  // waits 5s±3s; if there has been no user interaction in the last 5s, a
  // random client fires a write.
  useEffect(() => {
    const demoTimer = setTimeout(() => fireWriteRef.current("alice", false), 900);
    let loopTimer: ReturnType<typeof setTimeout>;
    function loop() {
      const sinceInteraction = performance.now() - lastInteractionRef.current;
      if (sinceInteraction >= 5000) {
        const writer = CLIENTS[Math.floor(Math.random() * CLIENTS.length)];
        fireWriteRef.current(writer, false);
        const delay = 7000 + Math.random() * 6000; // 10s ± 3s
        loopTimer = setTimeout(loop, delay);
      } else {
        loopTimer = setTimeout(loop, 5000 - sinceInteraction + 200);
      }
    }
    loopTimer = setTimeout(loop, 5000);
    return () => {
      clearTimeout(demoTimer);
      clearTimeout(loopTimer);
    };
  }, []);

  return (
    <DiagramFrame
      eyebrow="Live sync"
      description={
        <>
          Click <span className="font-mono">New colour</span> on any client to set a new colour for
          the row. The change propagates: up to that client's edge, across to siblings and the
          global core, then down through the other edge. If two clients write at once, the most
          recent write wins, and every node eventually ends up showing the same colour.
        </>
      }
    >
      <DiagramStyles />
      <div
        ref={containerRef}
        className="relative grid grid-cols-3 gap-x-6"
        style={{ rowGap: "4.5rem" }}
      >
        {svgSize.w > 0 && (
          <svg
            className="absolute inset-0 pointer-events-none"
            style={{ zIndex: 0 }}
            width={svgSize.w}
            height={svgSize.h}
            viewBox={`0 0 ${svgSize.w} ${svgSize.h}`}
          >
            <path
              d={staticD}
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              fill="none"
              className="text-fd-border"
            />
            {/* Hidden measurement paths — kept here for getPointAtLength. */}
            <g style={{ display: "none" }}>
              {ALL_HOPS.map((hop) => {
                const id = hopId(hop);
                return (
                  <path
                    key={id}
                    ref={(el) => {
                      measurePathRefs.current[id] = el;
                    }}
                    d={pathDs[id] ?? ""}
                  />
                );
              })}
            </g>
          </svg>
        )}

        <div className="col-span-3 relative z-10 flex justify-center">
          <NodeCard
            nodeKey="global"
            color={colors.global}
            pulseKey={pulses.global}
            innerRef={(el) => {
              nodeRefs.current.global = el;
            }}
          />
        </div>

        <div className="col-span-2 relative z-10 flex justify-center">
          <NodeCard
            nodeKey="edge1"
            color={colors.edge1}
            pulseKey={pulses.edge1}
            innerRef={(el) => {
              nodeRefs.current.edge1 = el;
            }}
          />
        </div>
        <div className="col-span-1 relative z-10 flex justify-center">
          <NodeCard
            nodeKey="edge2"
            color={colors.edge2}
            pulseKey={pulses.edge2}
            innerRef={(el) => {
              nodeRefs.current.edge2 = el;
            }}
          />
        </div>

        {CLIENTS.map((client) => (
          <div key={client} className="col-span-1 relative z-10 flex justify-center">
            <NodeCard
              nodeKey={client}
              color={colors[client]}
              pulseKey={pulses[client]}
              onWrite={() => fireWrite(client)}
              innerRef={(el) => {
                nodeRefs.current[client] = el;
              }}
            />
          </div>
        ))}

        {svgSize.w > 0 && (
          <>
            <svg
              className="absolute inset-0 pointer-events-none"
              style={{ zIndex: 30 }}
              width={svgSize.w}
              height={svgSize.h}
              viewBox={`0 0 ${svgSize.w} ${svgSize.h}`}
            >
              {events.flatMap((event) =>
                event.stages.flatMap((stage) =>
                  stage.map((hop) => {
                    const id = hopId(hop);
                    const key = `${event.id}-${id}`;
                    return (
                      <path
                        key={key}
                        ref={(el) => {
                          eventPathRefs.current[key] = el;
                        }}
                        className="diagram-path"
                        d={pathDs[id] ?? ""}
                        stroke={PRIMARY}
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        fill="none"
                      />
                    );
                  }),
                ),
              )}
            </svg>
            <svg
              className="absolute inset-0 pointer-events-none"
              style={{ zIndex: 40 }}
              width={svgSize.w}
              height={svgSize.h}
              viewBox={`0 0 ${svgSize.w} ${svgSize.h}`}
            >
              {events.flatMap((event) =>
                event.stages.flatMap((stage) =>
                  stage.map((hop) => {
                    const id = hopId(hop);
                    const key = `${event.id}-${id}`;
                    return (
                      <circle
                        key={key}
                        ref={(el) => {
                          eventDotRefs.current[key] = el;
                        }}
                        className="diagram-dot"
                        r="5"
                        cx="0"
                        cy="0"
                        fill={PRIMARY}
                        style={{ opacity: 0 }}
                      />
                    );
                  }),
                ),
              )}
            </svg>
          </>
        )}
      </div>
    </DiagramFrame>
  );
}
