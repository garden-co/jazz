"use client";

import { type ReactNode, useCallback, useEffect, useRef, useState } from "react";

import { cn } from "@/lib/cn";
import {
  Graph,
  type GraphEdge,
  type GraphNode,
  type GraphOverlayCtx,
  type Hop,
  hopEdgeId,
  NodeShell,
  NodeSubtitle,
  NodeTitle,
  playWaves,
  useDiagramTraces,
} from "./diagram";

type NodeId = "v1" | "a2" | "b2" | "m3";

type VersionVisual = {
  id: NodeId;
  rank: number;
  order?: number;
  author?: "a" | "b";
  title: string;
  done: boolean;
};

// Alice renames "Buy milk" → "oat milk"; Bob marks the same baseline complete.
// Different fields, so both writes survive on m4. Every node carries the full
// row state — the reader can read each version as a complete snapshot.
const NODES: readonly VersionVisual[] = [
  { id: "v1", rank: 0, title: "Buy milk", done: false },
  { id: "a2", rank: 1, order: 0, author: "a", title: "Buy oat milk", done: false },
  { id: "b2", rank: 1, order: 1, author: "b", title: "Buy milk", done: true },
  { id: "m3", rank: 2, title: "Buy oat milk", done: true },
] as const;

const EDGES = [
  { from: "v1", to: "a2" },
  { from: "v1", to: "b2" },
  { from: "a2", to: "m3" },
  { from: "b2", to: "m3" },
] as const;

// Cumulative reveal: which nodes/edges are visible at each beat. Each beat is
// triggered by a setTimeout chain; the last beat lights m3.
type Beat = {
  delayMs: number;
  newNodes: NodeId[];
  newEdges: string[];
};

const BEATS: readonly Beat[] = [
  { delayMs: 0, newNodes: ["v1"], newEdges: [] },
  { delayMs: 700, newNodes: ["a2"], newEdges: ["v1->a2"] },
  { delayMs: 900, newNodes: ["b2"], newEdges: ["v1->b2"] },
  { delayMs: 1800, newNodes: ["m3"], newEdges: ["a2->m3", "b2->m3"] },
] as const;

// Waves for the periodic pulse that re-flows root → leaves. Defined explicitly
// rather than via bfsWaves: BFS visits each node once, which would drop the
// second edge into the converging m3 (b2->m3). We want BOTH branches to pulse.
const PULSE_WAVES: Hop[][] = [
  [
    { from: "v1", to: "a2" },
    { from: "v1", to: "b2" },
  ],
  [
    { from: "a2", to: "m3" },
    { from: "b2", to: "m3" },
  ],
];

const PULSE_TIMING = { min: 350, max: 700, perPx: 1.8 } as const;
const BUILD_MS = BEATS[BEATS.length - 1].delayMs + 1200;
const PULSE_INTERVAL_MS = 5000;

function VersionCard({ v, revealed }: { v: VersionVisual; revealed: boolean }) {
  const tint = v.author
    ? {
        borderColor: `var(--diagram-author-${v.author})`,
        background: `rgb(var(--diagram-author-${v.author}-rgb) / 0.08)`,
      }
    : undefined;
  return (
    <NodeShell className={cn("cwd-card", !revealed && "cwd-card--hidden")} style={tint}>
      <NodeTitle className="cwd-header">
        <span className="cwd-version">{v.id}</span>
        {v.author && (
          <span className="cwd-by" style={{ color: `var(--diagram-author-${v.author})` }}>
            {v.author === "a" ? "Alice" : "Bob"}
          </span>
        )}
      </NodeTitle>
      <div className="cwd-field">
        <NodeSubtitle>title</NodeSubtitle>
        <span className="cwd-field-value">&ldquo;{v.title}&rdquo;</span>
      </div>
      <div className="cwd-field">
        <NodeSubtitle>done</NodeSubtitle>
        <span className="cwd-field-value">{v.done ? "true" : "false"}</span>
      </div>
    </NodeShell>
  );
}

export function ConcurrentWritesDiagram() {
  const [revealedNodes, setRevealedNodes] = useState<Set<NodeId>>(new Set());
  const [revealedEdges, setRevealedEdges] = useState<Set<string>>(new Set());
  const [geomReady, setGeomReady] = useState(false);

  const traces = useDiagramTraces();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const overlayCtxRef = useRef<GraphOverlayCtx | null>(null);
  const pathEls = useRef<Map<string, SVGPathElement | null>>(new Map());
  const playedRef = useRef(false);
  const pulseIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Kick the sequence the first time geometry settles. A ref (not state)
  // tracks "played" so flipping it doesn't tear down pending beat timers via
  // the effect's cleanup. inView/IntersectionObserver isn't worth the
  // complexity — the diagram is small enough that running on mount is fine.
  useEffect(() => {
    if (!geomReady || playedRef.current) return;
    playedRef.current = true;
    const timers: ReturnType<typeof setTimeout>[] = [];
    for (const beat of BEATS) {
      const t = setTimeout(() => {
        if (beat.newNodes.length) {
          setRevealedNodes((prev) => {
            const next = new Set(prev);
            for (const id of beat.newNodes) next.add(id);
            return next;
          });
        }
        // A node with no incoming edge this beat (the v1 origin) pulses on
        // reveal; nodes reached by an edge pulse when the trace arrives.
        const edgeTargets = new Set(beat.newEdges.map((id) => id.split("->")[1]));
        for (const id of beat.newNodes) {
          if (!edgeTargets.has(id)) traces.pulse(id);
        }
        for (const edgeId of beat.newEdges) {
          const ctx = overlayCtxRef.current;
          const r = ctx?.byId(edgeId);
          const to = edgeId.split("->")[1];
          if (r) {
            // Pulse fades after drawing; the engine's grey structural edge
            // (revealed below) is what persists as the resting line.
            traces.play({
              id: edgeId,
              d: r.d,
              length: r.length,
              follow: true,
              fadeAfter: true,
              timing: PULSE_TIMING,
              onArrive: () => traces.pulse(to),
            });
          }
          setRevealedEdges((prev) => {
            const next = new Set(prev);
            next.add(edgeId);
            return next;
          });
        }
      }, beat.delayMs);
      timers.push(t);
    }
    return () => timers.forEach((t) => clearTimeout(t));
  }, [geomReady, traces]);

  // After the build settles, re-flow a pulse root → leaves on an interval so
  // the diagram stays alive without controls.
  useEffect(() => {
    if (!geomReady) return;
    const start = setTimeout(() => {
      const fire = () => {
        const ctx = overlayCtxRef.current;
        if (!ctx) return;
        traces.pulse("v1");
        playWaves(traces, PULSE_WAVES, {
          byId: ctx.byId,
          traceId: hopEdgeId,
          gapMs: 450,
          timing: PULSE_TIMING,
          onArrive: (node) => traces.pulse(node),
        });
      };
      fire();
      const interval = setInterval(fire, PULSE_INTERVAL_MS);
      pulseIntervalRef.current = interval;
    }, BUILD_MS);
    return () => {
      clearTimeout(start);
      if (pulseIntervalRef.current) clearInterval(pulseIntervalRef.current);
    };
  }, [geomReady, traces]);

  const onGeometry = useCallback((ctx: GraphOverlayCtx) => {
    overlayCtxRef.current = ctx;
    setGeomReady(true);
  }, []);

  const overlay = useCallback(
    (ctx: GraphOverlayCtx) => (
      <g>
        {EDGES.map((e) => {
          const id = `${e.from}->${e.to}`;
          const r = ctx.byId(id);
          if (!r) return null;
          return (
            <g key={id}>
              <path
                ref={(el) => {
                  traces.pathRef(id)(el);
                  pathEls.current.set(id, el);
                }}
                className="diagram-path"
                d={r.d}
                fill="none"
                stroke="var(--diagram-accent)"
                strokeWidth={2}
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <circle
                ref={traces.dotRef(id)}
                r={4}
                cx={0}
                cy={0}
                fill="var(--diagram-accent)"
                style={{ opacity: 0 }}
                className="diagram-dot"
              />
            </g>
          );
        })}
      </g>
    ),
    [traces],
  );

  const nodes: GraphNode[] = NODES.map((v) => {
    const revealed = revealedNodes.has(v.id);
    return {
      id: v.id,
      ...(v.order != null ? { rank: v.rank, order: v.order } : { rank: v.rank }),
      content: <VersionCard v={v} revealed={revealed} />,
    };
  });

  const edges: GraphEdge[] = EDGES.map(({ from, to }) => ({
    from,
    to,
    variant: revealedEdges.has(`${from}->${to}`) ? "solid" : "hidden",
  }));

  return (
    <div ref={rootRef} className="cwd-root">
      <CwdStyles />
      <Graph
        eyebrow="Row version history"
        description={
          <>
            Starting from a baseline row, Alice and Bob edit it concurrently — Alice renames it, Bob
            marks it complete. They touch different fields, so both writes survive in the reconciled
            visible state. Every peer ends up at the same <code>m3</code>.
          </>
        }
        direction="LR"
        converge
        grid={{ gap: "1.5rem 3.5rem" }}
        nodes={nodes}
        edges={edges}
        traces={traces}
        overlay={overlay}
        onGeometry={onGeometry}
      />
    </div>
  );
}

const CWD_STYLES_CSS = `
.cwd-root {
  position: relative;
}
.cwd-card {
  /* NodeShell provides border/radius/card-bg via tokens. Card flow + reveal
     transition is the consumer's. A fixed width keeps all four columns equal
     so the converge routing forks/merges symmetrically (the author label
     would otherwise widen a2/b2). */
  box-sizing: border-box;
  width: 10rem;
  padding: 0.45rem 0.65rem 0.55rem;
  gap: 0.3rem;
  display: flex;
  flex-direction: column;
  transition: opacity 0.45s ease;
}
.cwd-card--hidden {
  opacity: 0;
}
/* The engine pulses by appending a .diagram-pulse span with an inline
   border-radius:inherit; the (class-less) node wrapper has no radius, so it
   inherits 0 and ripples as a square. Override the inline value (hence
   !important) to roughly match the card's 8px corners plus the -2px inset. */
.cwd-root .diagram-pulse {
  border-radius: 10px !important;
}
.cwd-header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.6rem;
  border-bottom: 1px solid var(--diagram-border);
  padding-bottom: 0.25rem;
}
.cwd-by {
  font-size: 0.62rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.cwd-field {
  display: flex;
  justify-content: space-between;
  gap: 0.5rem;
  align-items: baseline;
}
.cwd-field-value {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
  font-size: 0.7rem;
  font-weight: 500;
  color: var(--diagram-fg);
}
`;

function CwdStyles(): ReactNode {
  return (
    <style href="cwd-styles" precedence="default">
      {CWD_STYLES_CSS}
    </style>
  );
}
