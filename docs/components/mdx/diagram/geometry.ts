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
