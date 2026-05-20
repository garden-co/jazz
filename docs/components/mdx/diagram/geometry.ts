// Shared geometry primitives for diagram path builders. Pure functions over
// DOMRects — used by both the lens diagram and the tier-sync diagram.

// 2px CSS border + 2px SVG stroke ⇒ centreline inset by 1px from the outer
// edge of the box, so the path centre coincides with the border centre.
export const PATH_INSET = 1;

// rounded-lg (8px outer) minus the inset ⇒ centreline corner radius.
export const DEFAULT_CORNER_RADIUS = 8 - PATH_INSET;

export type Anchors = {
  left: number;
  right: number;
  top: number;
  bottom: number;
  midX: number;
  midY: number;
};

export function insetRect(rect: DOMRect, container: DOMRect): Anchors {
  return {
    left: rect.left - container.left + PATH_INSET,
    right: rect.right - container.left - PATH_INSET,
    top: rect.top - container.top + PATH_INSET,
    bottom: rect.bottom - container.top - PATH_INSET,
    midX: (rect.left + rect.right) / 2 - container.left,
    midY: (rect.top + rect.bottom) / 2 - container.top,
  };
}

export function anchorsFor(el: HTMLElement | null | undefined, container: DOMRect): Anchors | null {
  if (!el) return null;
  return insetRect(el.getBoundingClientRect(), container);
}

export type LoopSide = "top" | "bottom" | "left" | "right";

// CCW (screen-space) perimeter loop around a rounded box, starting and ending
// at the same point on one side. The pen is assumed to already be at the entry
// point before this runs.
//
// For "top"/"bottom" sides, `entryAt` is the X coordinate of the entry; for
// "left"/"right" sides it is the Y coordinate. Pass `a.midX` / `a.midY` to
// enter at the centre of the side.
export function loopRoundedBox(
  a: Anchors,
  entryAt: number,
  side: LoopSide,
  cornerRadius: number = DEFAULT_CORNER_RADIUS,
): string {
  const r = cornerRadius;
  switch (side) {
    case "top":
      return [
        `L ${a.left + r} ${a.top}`,
        `A ${r} ${r} 0 0 0 ${a.left} ${a.top + r}`,
        `L ${a.left} ${a.bottom - r}`,
        `A ${r} ${r} 0 0 0 ${a.left + r} ${a.bottom}`,
        `L ${a.right - r} ${a.bottom}`,
        `A ${r} ${r} 0 0 0 ${a.right} ${a.bottom - r}`,
        `L ${a.right} ${a.top + r}`,
        `A ${r} ${r} 0 0 0 ${a.right - r} ${a.top}`,
        `L ${entryAt} ${a.top}`,
      ].join(" ");
    case "bottom":
      return [
        `L ${a.right - r} ${a.bottom}`,
        `A ${r} ${r} 0 0 0 ${a.right} ${a.bottom - r}`,
        `L ${a.right} ${a.top + r}`,
        `A ${r} ${r} 0 0 0 ${a.right - r} ${a.top}`,
        `L ${a.left + r} ${a.top}`,
        `A ${r} ${r} 0 0 0 ${a.left} ${a.top + r}`,
        `L ${a.left} ${a.bottom - r}`,
        `A ${r} ${r} 0 0 0 ${a.left + r} ${a.bottom}`,
        `L ${entryAt} ${a.bottom}`,
      ].join(" ");
    case "left":
      return [
        `L ${a.left} ${a.bottom - r}`,
        `A ${r} ${r} 0 0 0 ${a.left + r} ${a.bottom}`,
        `L ${a.right - r} ${a.bottom}`,
        `A ${r} ${r} 0 0 0 ${a.right} ${a.bottom - r}`,
        `L ${a.right} ${a.top + r}`,
        `A ${r} ${r} 0 0 0 ${a.right - r} ${a.top}`,
        `L ${a.left + r} ${a.top}`,
        `A ${r} ${r} 0 0 0 ${a.left} ${a.top + r}`,
        `L ${a.left} ${entryAt}`,
      ].join(" ");
    case "right":
      return [
        `L ${a.right} ${a.top + r}`,
        `A ${r} ${r} 0 0 0 ${a.right - r} ${a.top}`,
        `L ${a.left + r} ${a.top}`,
        `A ${r} ${r} 0 0 0 ${a.left} ${a.top + r}`,
        `L ${a.left} ${a.bottom - r}`,
        `A ${r} ${r} 0 0 0 ${a.left + r} ${a.bottom}`,
        `L ${a.right - r} ${a.bottom}`,
        `A ${r} ${r} 0 0 0 ${a.right} ${a.bottom - r}`,
        `L ${a.right} ${entryAt}`,
      ].join(" ");
  }
}

// ───────────────────────────────────────────────────────────────────────────
// Generic laned router
//
// A direction-agnostic orthogonal router that generalises the tier-shaped
// builders: any node to any node, both directions, with multiple edges sharing
// a node side spread across that side so forks/merges never overlap. Pure —
// no DOM — so it is unit-testable and the segment length is exact (axis-aligned
// runs), removing the need for `getTotalLength`.

export type Point = { x: number; y: number };
export type RouteDirection = "TD" | "LR";
export type RoutedEdge = {
  id: string;
  from: string;
  to: string;
  d: string;
  /** the same visible line traversed target→source (for reverse-direction
   *  traces, so they animate along the identical drawn edge) */
  reverse: string;
  length: number;
  source: Point;
  target: Point;
};

type Side = "top" | "bottom" | "left" | "right";

function fmt(n: number): string {
  const r = Math.round(n * 1000) / 1000;
  return Object.is(r, -0) ? "0" : String(r);
}

const DEFAULT_EDGE_RADIUS = 8;

function dedupe(points: Point[]): Point[] {
  const pts: Point[] = [];
  for (const p of points) {
    const last = pts[pts.length - 1];
    if (!last || last.x !== p.x || last.y !== p.y) pts.push(p);
  }
  return pts;
}

function straightLength(pts: Point[]): number {
  let length = 0;
  for (let i = 1; i < pts.length; i++) {
    length += Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y);
  }
  return length;
}

// Polyline through `pts`, with genuine bends rounded by a quadratic arc
// (corner radius clamped to half the shorter adjacent segment). Collinear
// vertices stay as straight `L`s so straight runs are byte-identical.
function roundedPath(pts: Point[], r: number): string {
  if (pts.length === 0) return "";
  const seg = (p: Point, cmd: "M" | "L") => `${cmd} ${fmt(p.x)} ${fmt(p.y)}`;
  if (pts.length < 3 || r <= 0) {
    return pts.map((p, i) => seg(p, i === 0 ? "M" : "L")).join(" ");
  }
  let d = seg(pts[0], "M");
  for (let i = 1; i < pts.length - 1; i++) {
    const prev = pts[i - 1];
    const v = pts[i];
    const next = pts[i + 1];
    const inDx = v.x - prev.x;
    const inDy = v.y - prev.y;
    const outDx = next.x - v.x;
    const outDy = next.y - v.y;
    const inLen = Math.hypot(inDx, inDy);
    const outLen = Math.hypot(outDx, outDy);
    const isCorner = inLen > 0 && outLen > 0 && Math.abs(inDx * outDy - inDy * outDx) > 1e-6;
    if (!isCorner) {
      d += ` ${seg(v, "L")}`;
      continue;
    }
    const rr = Math.min(r, inLen / 2, outLen / 2);
    const a = { x: v.x - (inDx / inLen) * rr, y: v.y - (inDy / inLen) * rr };
    const b = { x: v.x + (outDx / outLen) * rr, y: v.y + (outDy / outLen) * rr };
    d += ` ${seg(a, "L")} Q ${fmt(v.x)} ${fmt(v.y)} ${fmt(b.x)} ${fmt(b.y)}`;
  }
  return `${d} ${seg(pts[pts.length - 1], "L")}`;
}

function mainOfSide(a: Anchors, side: Side): number {
  return side === "top" ? a.top : side === "bottom" ? a.bottom : side === "left" ? a.left : a.right;
}

export function routeEdges(
  anchors: Record<string, Anchors>,
  edges: Array<{ from: string; to: string }>,
  direction: RouteDirection,
  opts?: { cornerRadius?: number; converge?: boolean },
): RoutedEdge[] {
  const radius = opts?.cornerRadius ?? DEFAULT_EDGE_RADIUS;
  // converge: collapse same-side endpoints to one anchor (git-graph fork/merge)
  // instead of spreading them into lanes. Opt-in; default keeps laned routing.
  const converge = opts?.converge ?? false;
  const vertical = direction === "TD";
  const crossOf = (a: Anchors) => (vertical ? a.midX : a.midY);
  const crossSpan = (a: Anchors): [number, number] =>
    vertical ? [a.left, a.right] : [a.top, a.bottom];

  type Pre = {
    from: string;
    to: string;
    s: Anchors;
    t: Anchors;
    exitSide: Side;
    enterSide: Side;
    armMain: number;
  };

  const pre: Pre[] = edges.map(({ from, to }) => {
    const s = anchors[from];
    const t = anchors[to];
    if (!s || !t) throw new Error(`routeEdges: missing anchor for ${from} or ${to}`);
    if (vertical) {
      const forward = s.midY <= t.midY; // downward
      const upper = forward ? s : t;
      const lower = forward ? t : s;
      return {
        from,
        to,
        s,
        t,
        exitSide: forward ? "bottom" : "top",
        enterSide: forward ? "top" : "bottom",
        armMain: (upper.bottom + lower.top) / 2,
      };
    }
    const forward = s.midX <= t.midX; // rightward
    const leftN = forward ? s : t;
    const rightN = forward ? t : s;
    return {
      from,
      to,
      s,
      t,
      exitSide: forward ? "right" : "left",
      enterSide: forward ? "left" : "right",
      armMain: (leftN.right + rightN.left) / 2,
    };
  });

  // Lane assignment: group endpoints by (node, side); within a group order by
  // the OTHER endpoint's cross coordinate, then spread across the node's cross
  // span so the ordering minimises crossings.
  type Endpoint = { idx: number; role: "exit" | "enter" };
  const groups = new Map<string, Endpoint[]>();
  const add = (node: string, side: Side, ep: Endpoint) => {
    const k = `${node}|${side}`;
    let arr = groups.get(k);
    if (!arr) {
      arr = [];
      groups.set(k, arr);
    }
    arr.push(ep);
  };
  pre.forEach((p, idx) => {
    add(p.from, p.exitSide, { idx, role: "exit" });
    add(p.to, p.enterSide, { idx, role: "enter" });
  });

  const otherCross = (ep: Endpoint): number => {
    const p = pre[ep.idx];
    return crossOf(ep.role === "exit" ? p.t : p.s);
  };

  const exitCross = new Array<number>(pre.length);
  const enterCross = new Array<number>(pre.length);
  for (const [k, eps] of groups) {
    const nodeId = k.slice(0, k.lastIndexOf("|"));
    const [lo, hi] = crossSpan(anchors[nodeId]);
    const ordered = eps.slice().sort((a, b) => otherCross(a) - otherCross(b) || a.idx - b.idx);
    ordered.forEach((ep, i) => {
      const c = converge ? (lo + hi) / 2 : lo + (hi - lo) * ((i + 1) / (ordered.length + 1));
      if (ep.role === "exit") exitCross[ep.idx] = c;
      else enterCross[ep.idx] = c;
    });
  }

  return pre.map((p, idx) => {
    const exitMain = mainOfSide(p.s, p.exitSide);
    const enterMain = mainOfSide(p.t, p.enterSide);
    const sc = exitCross[idx];
    const tc = enterCross[idx];
    const source = vertical ? { x: sc, y: exitMain } : { x: exitMain, y: sc };
    const target = vertical ? { x: tc, y: enterMain } : { x: enterMain, y: tc };
    const a1 = vertical ? { x: sc, y: p.armMain } : { x: p.armMain, y: sc };
    const a2 = vertical ? { x: tc, y: p.armMain } : { x: p.armMain, y: tc };
    const pts = dedupe([source, a1, a2, target]);
    return {
      id: `${p.from}->${p.to}`,
      from: p.from,
      to: p.to,
      d: roundedPath(pts, radius),
      reverse: roundedPath([...pts].reverse(), radius),
      length: straightLength(pts),
      source,
      target,
    };
  });
}

export type GraphPlacement =
  | {
      id: string;
      slot: { row: number | string; col: number | string };
      rank?: never;
      order?: never;
    }
  | { id: string; rank: number; order?: number; slot?: never };

// Maps a node's declared placement to CSS grid line values. Slot mode passes
// through; rank mode maps rank to the main axis (rows for TD, columns for LR)
// and optional `order` to the cross axis. Grid lines are 1-based.
export function gridPlacement(
  node: GraphPlacement,
  direction: RouteDirection,
): { gridRow?: string; gridColumn?: string } {
  if ("slot" in node && node.slot) {
    return { gridRow: String(node.slot.row), gridColumn: String(node.slot.col) };
  }
  const rankLine = String(node.rank + 1);
  const orderLine = node.order == null ? undefined : String(node.order + 1);
  if (direction === "TD") {
    return orderLine == null ? { gridRow: rankLine } : { gridRow: rankLine, gridColumn: orderLine };
  }
  return orderLine == null
    ? { gridColumn: rankLine }
    : { gridColumn: rankLine, gridRow: orderLine };
}

// One continuous rounded path threading an ordered subset of nodes by centre;
// `direction` picks the elbow axis for diagonal hops. Pure.
export function connectChain(
  anchors: Record<string, Anchors>,
  ids: string[],
  direction: RouteDirection,
  opts?: { cornerRadius?: number },
): { d: string; length: number } {
  const radius = opts?.cornerRadius ?? DEFAULT_EDGE_RADIUS;
  const centres = ids
    .map((id) => anchors[id])
    .filter((a): a is Anchors => !!a)
    .map((a) => ({ x: a.midX, y: a.midY }));
  if (centres.length < 2) return { d: "", length: 0 };
  const pts: Point[] = [centres[0]];
  for (let i = 1; i < centres.length; i++) {
    const prev = centres[i - 1];
    const c = centres[i];
    if (Math.abs(c.x - prev.x) > 0.5 && Math.abs(c.y - prev.y) > 0.5) {
      pts.push(direction === "LR" ? { x: c.x, y: prev.y } : { x: prev.x, y: c.y });
    }
    pts.push(c);
  }
  const deduped = dedupe(pts);
  return { d: roundedPath(deduped, radius), length: straightLength(deduped) };
}
