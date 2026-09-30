"use client";

import { type CSSProperties, useEffect, useRef } from "react";
import { stipple, type StipplePattern } from "./stipple";

/**
 * Draws a stipple pattern into a canvas that fills its box. The pattern's
 * centre sits at the box centre and one pattern unit is half the box height,
 * so the dots scale with the box and stay put as it resizes.
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
    let frame = 0;
    const draw = () => {
      const dpr = window.devicePixelRatio || 1;
      const width = Math.round(canvas.clientWidth * dpr);
      const height = Math.round(canvas.clientHeight * dpr);
      if (!width || !height) return;
      canvas.width = width;
      canvas.height = height;
      drawStipple(canvas, pattern);
    };
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(draw);
    });
    observer.observe(canvas);
    return () => {
      cancelAnimationFrame(frame);
      observer.disconnect();
    };
  }, [pattern]);

  return <canvas ref={ref} aria-hidden className={className} style={style} />;
}

/** Renders `pattern` over the whole canvas at its current pixel size. */
export function drawStipple(canvas: HTMLCanvasElement, pattern: StipplePattern) {
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const { width, height } = canvas;
  const scale = height / 2;
  const halfWidth = width / 2 / scale;
  const radius = Math.max(pattern.radius * scale, 0.5);
  const paths = { a: new Path2D(), b: new Path2D() };
  stipple(pattern, { left: -halfWidth, right: halfWidth, top: -1, bottom: 1 }, (dot) => {
    const x = width / 2 + dot.x * scale;
    const y = height / 2 + dot.y * scale;
    const path = paths[dot.ink];
    path.moveTo(x + radius, y);
    path.arc(x, y, radius, 0, Math.PI * 2);
  });
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = pattern.inks.a;
  ctx.fill(paths.a);
  ctx.fillStyle = pattern.inks.b;
  ctx.fill(paths.b);
}
