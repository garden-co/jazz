// Tier-sync geometry — arms in a parent's "trunk + horizontal arm + drops"
// shape, plus optional loops around source/target receivers. Shared geometry
// primitives live in ./diagram/geometry.

import { type Anchors, anchorsFor, loopRoundedBox } from "./diagram/geometry";

export { anchorsFor, type Anchors };

export type ConnectionDirection = "up" | "down";

// A single propagation hop:
//   source edge → (optional source loop) → drop to arm → horizontal arm →
//   drop to target edge → loop target.
// `destReachDistance` is the path length up to the target edge, i.e. where the
// leading dot should fade and the receiver should pulse.
export function buildConnectionPath(args: {
  source: Anchors;
  target: Anchors;
  direction: ConnectionDirection;
  armY: number;
  includeSourceLoop: boolean;
}): { d: string; destReachDistance: number } {
  const { source, target, direction, armY, includeSourceLoop } = args;
  const sourceEdgeY = direction === "up" ? source.top : source.bottom;
  const targetEdgeY = direction === "up" ? target.bottom : target.top;
  const sourceSide = direction === "up" ? "top" : "bottom";
  const targetSide = direction === "up" ? "bottom" : "top";

  let prefix = `M ${source.midX} ${sourceEdgeY}`;
  if (includeSourceLoop) {
    prefix += " " + loopRoundedBox(source, source.midX, sourceSide);
  }
  prefix += ` L ${source.midX} ${armY}`;
  prefix += ` L ${target.midX} ${armY}`;
  prefix += ` L ${target.midX} ${targetEdgeY}`;

  const full = prefix + " " + loopRoundedBox(target, target.midX, targetSide);

  let destReachDistance = 0;
  if (typeof document !== "undefined") {
    const tmp = document.createElementNS("http://www.w3.org/2000/svg", "path");
    tmp.setAttribute("d", prefix);
    destReachDistance = tmp.getTotalLength();
  }

  return { d: full, destReachDistance };
}

// Tree-shaped static topology: a trunk drops from each parent, joins a
// horizontal arm spanning its children, and a vertical drop reaches each child.
export function buildStaticTopology(
  groups: Array<{ parent: Anchors; children: Anchors[]; armY: number }>,
): string {
  const segs: string[] = [];
  for (const { parent, children, armY } of groups) {
    segs.push(`M ${parent.midX} ${parent.bottom} L ${parent.midX} ${armY}`);
    const xs = [parent.midX, ...children.map((c) => c.midX)];
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);
    if (maxX - minX > 0.5) {
      segs.push(`M ${minX} ${armY} L ${maxX} ${armY}`);
    }
    for (const c of children) {
      segs.push(`M ${c.midX} ${armY} L ${c.midX} ${c.top}`);
    }
  }
  return segs.join(" ");
}
