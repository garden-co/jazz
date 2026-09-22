"use client";

import { useEffect, useMemo, useRef } from "react";

import { drawPath, snapPathDrawn, trackDotAlongPath } from "./trace-anim";

// Default trace duration from a routed path length, clamped. Every TraceSpec
// can override the bounds (a definition tunes its own feel).
export function traceDuration(
  length: number,
  opts?: { min?: number; max?: number; perPx?: number },
): number {
  const min = opts?.min ?? 500;
  const max = opts?.max ?? 2600;
  const perPx = opts?.perPx ?? 1.35;
  return Math.max(min, Math.min(length * perPx, max));
}

const FADE_MS = 700;

export type TraceSpec = {
  id: string;
  /** routed path data — also set as the live <path>'s d by the consumer */
  d: string;
  /** exact routed length (from the pure router) — avoids getTotalLength */
  length?: number;
  durationMs?: number;
  /** clamp overrides when durationMs is derived from length */
  timing?: { min?: number; max?: number; perPx?: number };
  /** leading dot follows the draw */
  follow?: boolean;
  /** fade the path out once drawn */
  fadeAfter?: boolean;
  /** fires when the trace reaches its target (dot arrival / draw end) */
  onArrive?: () => void;
};

export type DiagramTraces = {
  /** ref callbacks the consumer attaches to the rendered elements */
  pathRef: (id: string) => (el: SVGPathElement | null) => void;
  dotRef: (id: string) => (el: SVGCircleElement | null) => void;
  nodeRef: (id: string) => (el: HTMLElement | null) => void;
  play: (spec: TraceSpec) => void;
  /** draw straight to the finished state with no animation (resize / no-replay) */
  snap: (id: string) => void;
  pulse: (nodeId: string) => void;
  /** sequenced waves (BFS); each wave plays together, `gapMs` between waves */
  stage: (waves: TraceSpec[][], gapMs: number) => void;
  /** cancel every timer/RAF and remove transient pulse overlays */
  reset: () => void;
};

export function useDiagramTraces(): DiagramTraces {
  const paths = useRef(new Map<string, SVGPathElement | null>());
  const dots = useRef(new Map<string, SVGCircleElement | null>());
  const nodes = useRef(new Map<string, HTMLElement | null>());

  const timers = useRef(new Set<ReturnType<typeof setTimeout>>());
  const stoppers = useRef(new Set<() => void>());
  const pulseSpans = useRef(new Set<HTMLSpanElement>());

  // Stable ref-callback factories, memoised per id so React doesn't detach on
  // every render.
  const refFactory = useMemo(() => {
    const cache = new Map<string, (el: unknown) => void>();
    return <T>(store: () => Map<string, T | null>, kind: string) =>
      (id: string) => {
        const key = `${kind}:${id}`;
        let cb = cache.get(key);
        if (!cb) {
          cb = (el: unknown) => store().set(id, (el as T) ?? null);
          cache.set(key, cb);
        }
        return cb as (el: T | null) => void;
      };
  }, []);

  const api = useMemo<DiagramTraces>(() => {
    const after = (ms: number, fn: () => void) => {
      const t = setTimeout(() => {
        timers.current.delete(t);
        fn();
      }, ms);
      timers.current.add(t);
      return t;
    };

    const play = (spec: TraceSpec) => {
      const pathEl = paths.current.get(spec.id);
      if (!pathEl) return;
      const len = spec.length ?? pathEl.getTotalLength();
      const duration = spec.durationMs ?? traceDuration(len, spec.timing);

      drawPath(pathEl, duration);

      if (spec.fadeAfter) {
        after(duration, () => {
          if (!pathEl.isConnected) return;
          pathEl.style.transition = `opacity ${FADE_MS}ms ease-out`;
          pathEl.style.opacity = "0";
        });
      }

      const dotEl = spec.follow ? dots.current.get(spec.id) : null;
      if (dotEl) {
        const stop = trackDotAlongPath(pathEl, dotEl, {
          onComplete: spec.onArrive,
        });
        stoppers.current.add(stop);
      } else if (spec.onArrive) {
        after(duration, spec.onArrive);
      }
    };

    const pulse = (nodeId: string) => {
      const node = nodes.current.get(nodeId);
      if (!node) return;
      const span = document.createElement("span");
      span.className = "diagram-pulse";
      span.style.position = "absolute";
      span.style.inset = "-2px";
      span.style.borderRadius = "inherit";
      span.style.pointerEvents = "none";
      span.addEventListener("animationend", () => {
        span.remove();
        pulseSpans.current.delete(span);
      });
      pulseSpans.current.add(span);
      node.appendChild(span);
    };

    const reset = () => {
      timers.current.forEach((t) => clearTimeout(t));
      timers.current.clear();
      stoppers.current.forEach((s) => s());
      stoppers.current.clear();
      pulseSpans.current.forEach((s) => s.remove());
      pulseSpans.current.clear();
    };

    return {
      pathRef: refFactory(() => paths.current, "path"),
      dotRef: refFactory(() => dots.current, "dot"),
      nodeRef: refFactory(() => nodes.current, "node"),
      play,
      snap: (id) => {
        const p = paths.current.get(id);
        if (p) snapPathDrawn(p, dots.current.get(id));
      },
      pulse,
      stage: (waves, gapMs) => {
        waves.forEach((wave, w) => {
          for (const spec of wave) {
            if (w === 0) play(spec);
            else after(w * gapMs, () => play(spec));
          }
        });
      },
      reset,
    };
  }, [refFactory]);

  // Tear everything down on unmount.
  useEffect(() => () => api.reset(), [api]);

  return api;
}
