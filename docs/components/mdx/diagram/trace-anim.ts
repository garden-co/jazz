// Imperative helpers for the stroke-dasharray + leading-dot trace animation
// pattern shared by every animated diagram. They operate on raw DOM elements
// so callers stay in control of scheduling.

// Trigger the "draw" animation on a path: stroke-dasharray = length, animate
// dashoffset from length to 0 over `durationMs`. Returns the path length so
// the caller can compute timing offsets without measuring again.
export function drawPath(
  pathEl: SVGPathElement,
  durationMs: number,
  options?: { easing?: string; opacity?: string },
): number {
  const len = pathEl.getTotalLength();
  pathEl.style.transition = "none";
  pathEl.style.strokeDasharray = `${len}`;
  pathEl.style.strokeDashoffset = `${len}`;
  pathEl.style.opacity = options?.opacity ?? "1";
  // Force reflow so the next transition starts from the dashoffset we just set.
  void pathEl.getBoundingClientRect();
  pathEl.style.transition = `stroke-dashoffset ${durationMs}ms ${options?.easing ?? "ease-out"}`;
  pathEl.style.strokeDashoffset = "0";
  return len;
}

// Snap a path straight to its fully-drawn state without animating. Useful when
// a re-render (resize, hot reload) shouldn't replay the trace.
export function snapPathDrawn(pathEl: SVGPathElement, dotEl?: SVGCircleElement | null): number {
  const len = pathEl.getTotalLength();
  pathEl.style.transition = "none";
  pathEl.style.strokeDasharray = `${len}`;
  pathEl.style.strokeDashoffset = "0";
  pathEl.style.opacity = "1";
  if (dotEl) {
    const end = pathEl.getPointAtLength(len);
    dotEl.setAttribute("cx", String(end.x));
    dotEl.setAttribute("cy", String(end.y));
  }
  return len;
}

// Follow a path's currently-animating stroke-dashoffset with a leading dot
// element. The caller supplies an optional `opacityForDistance` to fade the
// dot during a particular stretch of the path (e.g. while it loops a node).
//
// Returns a cleanup function that cancels the RAF loop.
export function trackDotAlongPath(
  pathEl: SVGPathElement,
  dotEl: SVGCircleElement,
  options?: {
    opacityForDistance?: (distance: number, length: number) => number;
    onComplete?: () => void;
  },
): () => void {
  const length = pathEl.getTotalLength();
  let rafId = 0;
  const tick = () => {
    const offset = parseFloat(getComputedStyle(pathEl).strokeDashoffset);
    if (Number.isNaN(offset)) {
      rafId = 0;
      return;
    }
    const distance = Math.max(0, length - offset);
    const point = pathEl.getPointAtLength(distance);
    dotEl.setAttribute("cx", String(point.x));
    dotEl.setAttribute("cy", String(point.y));
    const opacity = options?.opacityForDistance ? options.opacityForDistance(distance, length) : 1;
    dotEl.style.opacity = String(opacity);
    if (offset > 0.5) {
      rafId = requestAnimationFrame(tick);
    } else {
      dotEl.style.opacity = "0";
      options?.onComplete?.();
    }
  };
  rafId = requestAnimationFrame(tick);
  return () => {
    if (rafId) cancelAnimationFrame(rafId);
  };
}
