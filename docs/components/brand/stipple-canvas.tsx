"use client";

import { type CSSProperties, useEffect, useRef } from "react";
import { stipple, type StipplePattern } from "./stipple";
import type { StippleRequest, StippleResponse } from "./stipple.worker";

type Dots = { a: Float32Array; b: Float32Array };
type Bounds = StippleRequest["bounds"];

let worker: Worker | undefined;
let busy = false;
type Job = { pattern: StipplePattern; bounds: Bounds; done: (dots: Dots | undefined) => void };
let current: Job | undefined;
let waiting: Job | undefined;

/**
 * Computes dots in a shared worker, one job at a time. A job that is still
 * waiting when a newer one arrives is dropped (resolves to undefined), so
 * dragging a slider never queues up stale work.
 */
function computeDots(pattern: StipplePattern, bounds: Bounds): Promise<Dots | undefined> {
  if (typeof Worker === "undefined") return Promise.resolve(collectDots(pattern, bounds));
  return new Promise((done) => {
    waiting?.done(undefined);
    waiting = { pattern, bounds, done };
    runNext();
  });
}

function runNext() {
  if (busy || !waiting) return;
  if (!worker) {
    worker = new Worker(new URL("./stipple.worker.ts", import.meta.url), { type: "module" });
    worker.onmessage = ({ data }: MessageEvent<StippleResponse>) => {
      busy = false;
      current?.done(data);
      runNext();
    };
  }
  current = waiting;
  waiting = undefined;
  busy = true;
  const request: StippleRequest = { pattern: current.pattern, bounds: current.bounds };
  worker.postMessage(request);
}

function collectDots(pattern: StipplePattern, bounds: Bounds): Dots {
  const dots = { a: [] as number[], b: [] as number[] };
  stipple(pattern, bounds, (dot) => dots[dot.ink].push(dot.x, dot.y));
  return { a: Float32Array.from(dots.a), b: Float32Array.from(dots.b) };
}

/** The pattern area a canvas shows: one unit is half its height, centre in the middle. */
function boundsOf(width: number, height: number): Bounds {
  const halfWidth = width / height;
  return { left: -halfWidth, right: halfWidth, top: -1, bottom: 1 };
}

/**
 * Draws a stipple pattern into a canvas that fills its box. The pattern's
 * centre sits at the box centre and one pattern unit is half the box height,
 * so the composition scales with the box. Dots are worked out in a worker and
 * kept while only the box's size changes; the canvas gets `data-drawn` once it
 * shows the pattern.
 *
 * Three optional CSS custom properties (read from the canvas, so they can be
 * set on any ancestor) pin details to screen pixels instead, so a bigger box
 * shows more of them rather than bigger ones: `--pattern-dot-radius` and
 * `--pattern-dot-spacing` (px) replace the pattern's `radius` and `spacing`,
 * and `--pattern-rib-unit` (px) is the size of one pattern unit for fluting
 * widths, so each rib is `period × rib unit` pixels wide.
 */
export function StippleCanvas({
  pattern,
  className,
  style,
}: {
  pattern: StipplePattern;
  className?: string;
  style?: CSSProperties;
}) {
  const ref = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    let cancelled = false;
    let frame = 0;
    let cached: { key: string; dots: Dots } | undefined;
    const draw = async () => {
      const dpr = window.devicePixelRatio || 1;
      const width = Math.round(canvas.clientWidth * dpr);
      const height = Math.round(canvas.clientHeight * dpr);
      if (!width || !height) return;
      const bounds = boundsOf(width, height);
      const sized = sizePattern(pattern, canvas);
      // Dots depend on the aspect ratio and pixel-pinned details, not the size.
      const key = JSON.stringify([
        (width / height).toFixed(3),
        sized.spacing,
        sized.radius,
        sized.layers,
      ]);
      if (cached?.key !== key) {
        const dots = await computeDots(sized, bounds);
        if (cancelled || !dots) return;
        cached = { key, dots };
      }
      canvas.width = width;
      canvas.height = height;
      paint(canvas, sized, cached.dots);
      canvas.dataset.drawn = "";
    };
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => void draw());
    });
    observer.observe(canvas);
    return () => {
      cancelled = true;
      cancelAnimationFrame(frame);
      observer.disconnect();
    };
  }, [pattern]);

  return <canvas ref={ref} aria-hidden className={className} style={style} />;
}

/** Applies the pixel-pinned details a canvas's CSS asks for, if any. */
function sizePattern(pattern: StipplePattern, canvas: HTMLCanvasElement): StipplePattern {
  const style = getComputedStyle(canvas);
  const px = (name: string) => {
    const value = Number.parseFloat(style.getPropertyValue(name));
    return Number.isFinite(value) && value > 0 ? value : undefined;
  };
  // Pixels per pattern unit, rounded so sub-pixel resizes keep the dots.
  const unit = Math.round(canvas.clientHeight / 2);
  const round = (v: number) => Number(v.toPrecision(4));
  const radius = px("--pattern-dot-radius");
  const spacing = px("--pattern-dot-spacing");
  const ribUnit = px("--pattern-rib-unit");
  return {
    ...pattern,
    radius: radius ? round(radius / unit) : pattern.radius,
    spacing: spacing ? round(spacing / unit) : pattern.spacing,
    layers: ribUnit
      ? pattern.layers.map((layer) => ({
          ...layer,
          warps: layer.warps?.map((warp) =>
            warp.type === "flute"
              ? { ...warp, period: round((warp.period * ribUnit) / unit) }
              : warp,
          ),
        }))
      : pattern.layers,
  };
}

function paint(canvas: HTMLCanvasElement, pattern: StipplePattern, dots: Dots) {
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const { width, height } = canvas;
  const scale = height / 2;
  const radius = Math.max(pattern.radius * scale, 0.5);
  ctx.clearRect(0, 0, width, height);
  for (const ink of ["a", "b"] as const) {
    const path = new Path2D();
    const xy = dots[ink];
    for (let i = 0; i < xy.length; i += 2) {
      const x = width / 2 + xy[i] * scale;
      const y = height / 2 + xy[i + 1] * scale;
      path.moveTo(x + radius, y);
      path.arc(x, y, radius, 0, Math.PI * 2);
    }
    ctx.fillStyle = pattern.inks[ink];
    ctx.fill(path);
  }
}

/** Renders `pattern` over the whole canvas at its current pixel size, in place. */
export function drawStipple(canvas: HTMLCanvasElement, pattern: StipplePattern) {
  paint(canvas, pattern, collectDots(pattern, boundsOf(canvas.width, canvas.height)));
}
