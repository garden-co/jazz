"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Cloud, Plus, Server, Smartphone, type LucideIcon } from "lucide-react";

import {
  bfsWaves,
  Graph,
  type GraphEdge,
  type GraphNode,
  type GraphOverlayCtx,
  type Hop,
  hopEdgeId,
  type RoutedEdge,
  NodeAction,
  NodeFooter,
  NodeIcon,
  NodeShell,
  NodeSubtitle,
  NodeTitle,
  useDiagramTraces,
} from "./diagram";
import { INITIAL_COLOR, pickNextColor } from "./colour";

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

const SLOTS: Record<NodeKey, { row: number; col: string }> = {
  global: { row: 1, col: "1 / 4" },
  edge1: { row: 2, col: "1 / 3" },
  edge2: { row: 2, col: "3 / 4" },
  alice: { row: 3, col: "1 / 2" },
  bob: { row: 3, col: "2 / 3" },
  charlie: { row: 3, col: "3 / 4" },
};

const TIER_DEPTH: Record<Tier, number> = { global: 0, edge: 1, local: 2 };
const STAGE_DURATION = 1100;
const PULSE_MS = 1400;

type ColorState = Record<NodeKey, string | null>;
const INITIAL_COLORS: ColorState = {
  global: INITIAL_COLOR,
  edge1: INITIAL_COLOR,
  edge2: INITIAL_COLOR,
  alice: INITIAL_COLOR,
  bob: INITIAL_COLOR,
  charlie: INITIAL_COLOR,
};
const ZERO: Record<NodeKey, number> = {
  global: 0,
  edge1: 0,
  edge2: 0,
  alice: 0,
  bob: 0,
  charlie: 0,
};

type WriteEvent = { id: number; writer: NodeKey; value: string; waves: Hop[][] };

function NodeCard({
  nodeKey,
  color,
  pulseKey,
  onWrite,
}: {
  nodeKey: NodeKey;
  color: string | null;
  pulseKey: number;
  onWrite?: () => void;
}) {
  const meta = NODES[nodeKey];
  const Icon = TIER_ICONS[meta.tier];
  return (
    <NodeShell
      style={{
        width: "10rem",
        padding: "0.5rem 0.75rem",
        alignItems: "center",
        textAlign: "center",
      }}
    >
      {pulseKey > 0 && (
        <span
          key={pulseKey}
          className="diagram-pulse"
          style={{
            position: "absolute",
            inset: "-2px",
            borderRadius: "inherit",
            pointerEvents: "none",
          }}
        />
      )}
      <NodeIcon>
        <Icon />
      </NodeIcon>
      <NodeTitle>{meta.label}</NodeTitle>
      <NodeSubtitle>{meta.subtitle}</NodeSubtitle>
      <NodeFooter
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "0.375rem",
          borderRadius: "0.25rem",
          padding: "0.25rem 0.5rem",
          backgroundColor: color ? `${color}26` : undefined,
          transition: "background-color 0.2s",
        }}
      >
        <span
          style={{
            fontSize: "10px",
            fontFamily: "var(--font-mono, ui-monospace, monospace)",
            color: "var(--diagram-muted)",
          }}
        >
          color
        </span>
        <span
          aria-live="polite"
          style={{
            display: "inline-block",
            overflow: "hidden",
            height: "1em",
            fontSize: "0.75rem",
            fontFamily: "var(--font-mono, ui-monospace, monospace)",
            color: "var(--diagram-fg)",
            lineHeight: 1,
          }}
        >
          <span
            key={`hex-${color ?? "none"}`}
            className="diagram-tally"
            style={{ display: "block" }}
          >
            {color ?? "—"}
          </span>
        </span>
        <span
          style={{
            display: "inline-block",
            overflow: "hidden",
            borderRadius: "2px",
            border: "1px solid var(--diagram-border, #e4e4e7)",
            height: "0.9rem",
            width: "0.9rem",
            flexShrink: 0,
          }}
        >
          <span
            key={`sw-${color ?? "none"}`}
            className="diagram-tally"
            style={{
              display: "block",
              height: "100%",
              width: "100%",
              backgroundColor: color ?? "transparent",
            }}
          />
        </span>
      </NodeFooter>
      {onWrite && (
        <NodeAction onClick={onWrite} aria-label={`New colour from ${meta.label}`}>
          <Plus size={12} />
          <span>New colour</span>
        </NodeAction>
      )}
    </NodeShell>
  );
}

// One drawn line per link: the downward (parent→child) direction only.
// Upward hops animate along this same line, reversed — so a trace always
// follows the visible edge.
const EDGES: GraphEdge[] = (Object.entries(TOPOLOGY) as [NodeKey, NodeKey[]][]).flatMap(
  ([from, tos]) =>
    tos.flatMap((to) =>
      TIER_DEPTH[NODES[from].tier] < TIER_DEPTH[NODES[to].tier] ? [{ from, to }] : [],
    ),
);

// Geometry for a hop: the drawn edge forward, or its reverse if the hop runs
// against the drawn (downward) direction.
function geomForHop(
  byId: (id: string) => RoutedEdge | undefined,
  h: Hop,
): { d: string; length: number } | null {
  const fwd = byId(hopEdgeId(h));
  if (fwd) return { d: fwd.d, length: fwd.length };
  const rev = byId(`${h.to}->${h.from}`);
  if (rev) return { d: rev.reverse, length: rev.length };
  return null;
}

export function TierSyncDiagram() {
  const [colors, setColors] = useState<ColorState>(INITIAL_COLORS);
  const [pulses, setPulses] = useState<Record<NodeKey, number>>(ZERO);
  const [events, setEvents] = useState<WriteEvent[]>([]);

  const traces = useDiagramTraces();
  const geomRef = useRef<GraphOverlayCtx | null>(null);
  const [geomReady, setGeomReady] = useState(false);

  const colorsRef = useRef<ColorState>(INITIAL_COLORS);
  const lastSeenRef = useRef<Record<NodeKey, number>>({ ...ZERO });
  const eventIdRef = useRef(0);
  const scheduledRef = useRef<Set<number>>(new Set());
  const lastInteractionRef = useRef(0);
  const fireRef = useRef<(w: NodeKey, user?: boolean) => void>(() => {});

  function fireWrite(writer: NodeKey, isUser = true) {
    if (isUser) lastInteractionRef.current = performance.now();
    const id = ++eventIdRef.current;
    const value = pickNextColor(colorsRef.current[writer]);
    colorsRef.current = { ...colorsRef.current, [writer]: value };
    lastSeenRef.current = { ...lastSeenRef.current, [writer]: id };
    setColors((c) => ({ ...c, [writer]: value }));
    setPulses((p) => ({ ...p, [writer]: p[writer] + 1 }));
    setEvents((evs) => [...evs, { id, writer, value, waves: bfsWaves(TOPOLOGY, writer) }]);
  }
  fireRef.current = fireWrite;

  // Schedule newly-added events once geometry is available; LWW on arrival.
  useEffect(() => {
    const byId = geomRef.current?.byId;
    if (!byId) return;
    for (const ev of events) {
      if (scheduledRef.current.has(ev.id)) continue;
      scheduledRef.current.add(ev.id);
      const traceId = (h: Hop) => `${ev.id}:${hopEdgeId(h)}`;
      const specs = ev.waves.map((wave) =>
        wave.flatMap((h) => {
          const g = geomForHop(byId, h);
          if (!g) return [];
          return [
            {
              id: traceId(h),
              d: g.d,
              length: g.length,
              timing: { min: 700, max: STAGE_DURATION - 60, perPx: 1.3 },
              follow: true,
              fadeAfter: true,
              onArrive: () => {
                const target = h.to as NodeKey;
                if (ev.id <= (lastSeenRef.current[target] ?? 0)) return;
                lastSeenRef.current = { ...lastSeenRef.current, [target]: ev.id };
                colorsRef.current = { ...colorsRef.current, [target]: ev.value };
                setColors((c) => ({ ...c, [target]: ev.value }));
                setPulses((p) => ({ ...p, [target]: p[target] + 1 }));
              },
            },
          ];
        }),
      );
      traces.stage(specs, STAGE_DURATION);
      const total = ev.waves.length * STAGE_DURATION + PULSE_MS + 400;
      setTimeout(() => {
        scheduledRef.current.delete(ev.id);
        setEvents((evs) => evs.filter((e) => e.id !== ev.id));
      }, total);
    }
  }, [events, geomReady, traces]);

  // Demo on mount, then ambient writes while the reader is idle.
  useEffect(() => {
    const demo = setTimeout(() => fireRef.current("alice", false), 900);
    let loop: ReturnType<typeof setTimeout>;
    const tick = () => {
      const idle = performance.now() - lastInteractionRef.current;
      if (idle >= 5000) {
        fireRef.current(CLIENTS[Math.floor(Math.random() * CLIENTS.length)], false);
        loop = setTimeout(tick, 7000 + Math.random() * 6000);
      } else {
        loop = setTimeout(tick, 5000 - idle + 200);
      }
    };
    loop = setTimeout(tick, 5000);
    return () => {
      clearTimeout(demo);
      clearTimeout(loop);
    };
  }, []);

  const onGeometry = useCallback((ctx: GraphOverlayCtx) => {
    geomRef.current = ctx;
    setGeomReady(true);
  }, []);

  const overlay = useCallback(
    (ctx: GraphOverlayCtx) => (
      <>
        <g>
          {events.flatMap((ev) =>
            ev.waves.flatMap((wave) =>
              wave.map((h) => {
                const id = `${ev.id}:${hopEdgeId(h)}`;
                const g = geomForHop(ctx.byId, h);
                return (
                  <path
                    key={id}
                    ref={traces.pathRef(id)}
                    className="diagram-path"
                    d={g?.d ?? ""}
                    fill="none"
                    stroke={ev.value}
                    strokeWidth={2}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                );
              }),
            ),
          )}
        </g>
        <g>
          {events.flatMap((ev) =>
            ev.waves.flatMap((wave) =>
              wave.map((h) => {
                const id = `${ev.id}:${hopEdgeId(h)}`;
                return (
                  <circle
                    key={id}
                    ref={traces.dotRef(id)}
                    className="diagram-dot"
                    r={5}
                    cx={0}
                    cy={0}
                    fill={ev.value}
                    style={{ opacity: 0 }}
                  />
                );
              }),
            ),
          )}
        </g>
      </>
    ),
    [events, traces],
  );

  const nodes: GraphNode[] = (Object.keys(NODES) as NodeKey[]).map((key) => ({
    id: key,
    slot: SLOTS[key],
    content: (
      <NodeCard
        nodeKey={key}
        color={colors[key]}
        pulseKey={pulses[key]}
        onWrite={CLIENTS.includes(key) ? () => fireWrite(key) : undefined}
      />
    ),
  }));

  return (
    <Graph
      eyebrow="Live sync"
      description={
        <>
          Click{" "}
          <span style={{ fontFamily: "var(--font-mono, ui-monospace, monospace)" }}>
            New colour
          </span>{" "}
          on any client to set a new colour for the row. It propagates up to that client's edge,
          across to siblings and the global core, then down through the other edge. If two clients
          write at once, the most recent write wins, and every node ends up showing the same colour.
        </>
      }
      direction="TD"
      nodes={nodes}
      edges={EDGES}
      grid={{ columns: "repeat(3, minmax(0, 1fr))", gap: "4.5rem 1.5rem" }}
      arrows={false}
      traces={traces}
      overlay={overlay}
      onGeometry={onGeometry}
    />
  );
}
