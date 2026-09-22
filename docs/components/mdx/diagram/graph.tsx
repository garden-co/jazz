"use client";

import {
  type CSSProperties,
  type ReactNode,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  type Anchors,
  gridPlacement,
  PATH_INSET,
  type RouteDirection,
  type RoutedEdge,
  routeEdges,
} from "./geometry";
import { DiagramFrame } from "./frame";
import { NodeShell, NodeTitle } from "./kit";
import { DiagramStyles } from "./styles";
import type { DiagramTraces } from "./traces";

export type GraphSlot = { row: number | string; col: number | string };

// Provide `content` for a bespoke node, or just `label` for the engine's
// default node (a tidy titled chip) — the latter keeps simple graphs pure
// data, so they inline straight into MDX with no component import.
export type GraphNode = { id: string; content?: ReactNode; label?: string } & (
  | { slot: GraphSlot; rank?: never; order?: never }
  | { rank: number; order?: number; slot?: never }
);

export type GraphEdge = {
  from: string;
  to: string;
  label?: string;
  // "hidden" ⇒ routed (available to the overlay via byId) but not drawn —
  // e.g. the reverse direction of a bidirectional link.
  variant?: "solid" | "dashed" | "hidden";
};

// The engine owns layout/measurement/routing and hands the live geometry to
// the definition's overlay, which draws its own animated layer (traces, dots,
// per-event colour) in engine coordinate space. Behaviour/policy stays in the
// definition; the engine stays diagram-agnostic.
export type GraphOverlayCtx = {
  routed: RoutedEdge[];
  byId: (edgeId: string) => RoutedEdge | undefined;
  /** measured node geometry in natural coordinates — for definitions that
   *  draw bespoke connectors (e.g. a chain through a variable card subset) */
  anchors: Record<string, Anchors>;
  size: { w: number; h: number };
  isStatic: boolean;
};
export type GraphOverlay = (ctx: GraphOverlayCtx) => ReactNode;
export type GraphGeometryListener = (ctx: GraphOverlayCtx) => void;

export type GraphProps = {
  eyebrow: string;
  description: ReactNode;
  direction: RouteDirection;
  nodes: GraphNode[];
  edges: GraphEdge[];
  grid?: { columns?: string; rows?: string; gap?: string };
  /** trace controller from the definition (for node pulses / overlay) */
  traces?: DiagramTraces;
  /** definition-drawn animated layer in engine coordinate space */
  overlay?: GraphOverlay;
  /** draw the overlay behind the nodes (default false ⇒ above). Lens's
   *  connector runs behind its cards. */
  overlayBehindNodes?: boolean;
  /** a second overlay layer always above the nodes (e.g. a tip dot that should
   *  ride over cards while the path sits behind them) */
  overlayFront?: GraphOverlay;
  /** read side of the same mechanism: live geometry pushed to the definition */
  onGeometry?: GraphGeometryListener;
  /** Opt-in: below this *container* width, render the resting static state
   *  instead of the interactive overlay. Default 0 (never) — a docs column is
   *  far narrower than the viewport, so a non-zero default would silently
   *  disable interactivity on normal screens. Diagrams whose interaction is
   *  unusable on a phone can set e.g. 420. */
  staticBelow?: number;
  /** arrowheads on structural edges (default true; off for undirected graphs) */
  arrows?: boolean;
  /** "center" (default) shrink-wraps each node so measurement == the visible
   *  node even in spanning cells; "stretch" fills the grid cell */
  nodeAlign?: "center" | "stretch";
  /** Fixed design width (px) for fluid/wrapping layouts (e.g. Lens), so
   *  columns and text wrap deterministically and scale-to-fit shrinks that
   *  fixed design. Omit ⇒ size to content (max-content). */
  naturalWidth?: number;
  /** Collapse same-side endpoints to a single anchor (git-graph fork/merge)
   *  instead of laned spreading. Default false (keeps laned routing). */
  converge?: boolean;
};

const DEFAULT_STATIC_BELOW = 0;
const DEFAULT_GAP = "2.5rem";
// Below this scale we stop shrinking and let the diagram scroll horizontally
// instead of becoming illegible.
const MIN_SCALE = 0.6;

export function Graph({
  eyebrow,
  description,
  direction,
  nodes,
  edges,
  grid,
  traces,
  overlay,
  overlayBehindNodes = false,
  overlayFront,
  onGeometry,
  staticBelow = DEFAULT_STATIC_BELOW,
  arrows = true,
  nodeAlign = "center",
  naturalWidth,
  converge = false,
}: GraphProps) {
  const arrowId = `dg-arrow-${useId().replace(/:/g, "")}`;
  const fitRef = useRef<HTMLDivElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const nodeEls = useRef(new Map<string, HTMLElement | null>());

  const [svg, setSvg] = useState({ w: 0, h: 0 });
  const [fit, setFit] = useState({ scale: 1, scroll: false });
  const [layoutW, setLayoutW] = useState<number | null>(null);
  const [routed, setRouted] = useState<RoutedEdge[]>([]);
  const [anchors, setAnchors] = useState<Record<string, Anchors>>({});

  const usesSlots = nodes.some((n) => "slot" in n && n.slot);
  const ranks = nodes.flatMap((n) => ("rank" in n && n.rank != null ? [n.rank] : []));
  const orders = nodes.flatMap((n) => ("order" in n && n.order != null ? [n.order] : []));
  const rankCount = ranks.length ? Math.max(...ranks) + 1 : 1;
  const crossCount = orders.length ? Math.max(...orders) + 1 : 1;

  // Re-measure only when the structural shape actually changes (consumers may
  // pass fresh array literals every render).
  const shapeKey =
    direction +
    "|" +
    nodes.map((n) => n.id).join(",") +
    "|" +
    edges.map((e) => `${e.from}>${e.to}`).join(",") +
    "|" +
    (converge ? "c" : "");

  useLayoutEffect(() => {
    function measure() {
      const el = containerRef.current;
      const fitEl = fitRef.current;
      if (!el || !fitEl) return;
      const avail = fitEl.clientWidth;
      // Width the diagram lays out at. With `naturalWidth` set it is a
      // *minimum*: at/above it the diagram fills the column at full size
      // (scale 1, text wraps as designed); below it we fix at that width and
      // scale down. Without it, size to content (max-content) and scale.
      let appliedW: number;
      let scale: number;
      let scroll: boolean;
      if (naturalWidth != null) {
        if (avail >= naturalWidth) {
          appliedW = avail;
          scale = 1;
          scroll = false;
        } else {
          appliedW = naturalWidth;
          const raw = avail > 0 ? avail / naturalWidth : 1;
          scale = Math.max(MIN_SCALE, raw);
          scroll = raw < MIN_SCALE;
        }
        setLayoutW((p) => (p === appliedW ? p : appliedW));
      } else {
        appliedW = el.offsetWidth;
        const raw = appliedW > 0 && avail > 0 ? avail / appliedW : 1;
        scale = Math.min(1, Math.max(MIN_SCALE, raw));
        scroll = raw < MIN_SCALE;
      }
      // `offset*` is unaffected by the CSS scale transform, so measurement
      // never feeds back on itself.
      const naturalH = el.offsetHeight;
      if (appliedW === 0) return;
      const measured: Record<string, Anchors> = {};
      for (const n of nodes) {
        const ne = nodeEls.current.get(n.id);
        if (!ne) continue;
        const l = ne.offsetLeft;
        const t = ne.offsetTop;
        const w = ne.offsetWidth;
        const h = ne.offsetHeight;
        measured[n.id] = {
          left: l + PATH_INSET,
          right: l + w - PATH_INSET,
          top: t + PATH_INSET,
          bottom: t + h - PATH_INSET,
          midX: l + w / 2,
          midY: t + h / 2,
        };
      }
      if (Object.keys(measured).length !== nodes.length) return;
      setSvg((p) => (p.w === appliedW && p.h === naturalH ? p : { w: appliedW, h: naturalH }));
      setFit((p) => (p.scale === scale && p.scroll === scroll ? p : { scale, scroll }));
      setAnchors(measured);
      setRouted(
        routeEdges(
          measured,
          edges.map((e) => ({ from: e.from, to: e.to })),
          direction,
          { converge },
        ),
      );
    }
    measure();
    const ro = new ResizeObserver(measure);
    if (fitRef.current) ro.observe(fitRef.current);
    if (containerRef.current) ro.observe(containerRef.current);
    for (const n of nodes) {
      const ne = nodeEls.current.get(n.id);
      if (ne) ro.observe(ne);
    }
    return () => ro.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shapeKey]);

  const isStatic = svg.w > 0 && svg.w < staticBelow;

  const byId = useMemo(() => {
    const m = new Map(routed.map((r) => [r.id, r] as const));
    return (id: string) => m.get(id);
  }, [routed]);

  useLayoutEffect(() => {
    // Gate on measurement, not edges — edge-less diagrams (Lens) still need
    // their anchors pushed.
    if (onGeometry && svg.w > 0) {
      onGeometry({ routed, byId, anchors, size: svg, isStatic });
    }
  }, [onGeometry, routed, byId, anchors, svg, isStatic]);

  const gridStyle: CSSProperties = usesSlots
    ? {
        gridTemplateColumns: grid?.columns,
        gridTemplateRows: grid?.rows,
      }
    : direction === "TD"
      ? {
          gridTemplateRows: grid?.rows ?? `repeat(${rankCount}, auto)`,
          gridTemplateColumns: grid?.columns ?? `repeat(${crossCount}, minmax(0, 1fr))`,
        }
      : {
          gridTemplateColumns: grid?.columns ?? `repeat(${rankCount}, auto)`,
          gridTemplateRows: grid?.rows ?? `repeat(${crossCount}, minmax(0, 1fr))`,
        };

  const align = nodeAlign === "stretch" ? "stretch" : "center";
  function placement(n: GraphNode): CSSProperties {
    if ("slot" in n && n.slot) {
      return { ...gridPlacement(n, direction), justifySelf: align, alignSelf: align };
    }
    const base = gridPlacement(n, direction);
    // A lone node in its rank spans the cross axis so it sits centred against
    // multi-node ranks.
    if (n.order == null) {
      return direction === "TD"
        ? { ...base, gridColumn: "1 / -1", justifySelf: "center", alignSelf: align }
        : { ...base, gridRow: "1 / -1", alignSelf: "center", justifySelf: align };
    }
    return { ...base, justifySelf: align, alignSelf: align };
  }

  return (
    <DiagramFrame eyebrow={eyebrow} description={description} responsive>
      <DiagramStyles />
      <div ref={fitRef} style={{ width: "100%", overflowX: fit.scroll ? "auto" : "visible" }}>
        <div
          style={{
            position: "relative",
            width: svg.w ? svg.w * fit.scale : undefined,
            height: svg.h ? svg.h * fit.scale : undefined,
            margin: "0 auto",
          }}
        >
          <div
            ref={containerRef}
            style={{
              position: "absolute",
              top: 0,
              left: 0,
              width: naturalWidth != null ? `${layoutW ?? naturalWidth}px` : "max-content",
              transform: `scale(${fit.scale})`,
              transformOrigin: "top left",
              display: "grid",
              gap: grid?.gap ?? DEFAULT_GAP,
              ...gridStyle,
            }}
          >
            {svg.w > 0 && (
              <svg
                aria-hidden
                style={{ position: "absolute", inset: 0, zIndex: 0, pointerEvents: "none" }}
                width={svg.w}
                height={svg.h}
                viewBox={`0 0 ${svg.w} ${svg.h}`}
              >
                {arrows && (
                  <defs>
                    <marker
                      id={arrowId}
                      viewBox="0 0 10 10"
                      refX={8.5}
                      refY={5}
                      markerWidth={7}
                      markerHeight={7}
                      markerUnits="userSpaceOnUse"
                      orient="auto"
                    >
                      <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--diagram-edge, #9ca3af)" />
                    </marker>
                  </defs>
                )}
                {routed.map((r) => {
                  const e = edges.find((x) => `${x.from}->${x.to}` === r.id);
                  if (e?.variant === "hidden") return null;
                  return (
                    <path
                      key={r.id}
                      d={r.d}
                      fill="none"
                      stroke="var(--diagram-edge, #9ca3af)"
                      strokeWidth={1.5}
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeDasharray={e?.variant === "dashed" ? "4 4" : undefined}
                      markerEnd={arrows ? `url(#${arrowId})` : undefined}
                    />
                  );
                })}
              </svg>
            )}

            {/* Definition-drawn animated layer, in engine coordinate space. */}
            {svg.w > 0 && !isStatic && overlay && (
              <svg
                style={{
                  position: "absolute",
                  inset: 0,
                  zIndex: overlayBehindNodes ? 5 : 30,
                  pointerEvents: "none",
                }}
                width={svg.w}
                height={svg.h}
                viewBox={`0 0 ${svg.w} ${svg.h}`}
              >
                {overlay({ routed, byId, anchors, size: svg, isStatic })}
              </svg>
            )}

            {nodes.map((n) => (
              <div
                key={n.id}
                ref={(el) => {
                  nodeEls.current.set(n.id, el);
                  traces?.nodeRef(n.id)(el);
                }}
                style={{ position: "relative", zIndex: 10, ...placement(n) }}
              >
                {n.content ?? (
                  <NodeShell className="dg-node--label">
                    <NodeTitle>{n.label ?? n.id}</NodeTitle>
                  </NodeShell>
                )}
              </div>
            ))}

            {svg.w > 0 && !isStatic && overlayFront && (
              <svg
                style={{ position: "absolute", inset: 0, zIndex: 40, pointerEvents: "none" }}
                width={svg.w}
                height={svg.h}
                viewBox={`0 0 ${svg.w} ${svg.h}`}
              >
                {overlayFront({ routed, byId, anchors, size: svg, isStatic })}
              </svg>
            )}
          </div>
        </div>
      </div>
    </DiagramFrame>
  );
}
