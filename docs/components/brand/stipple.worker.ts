// Computes stipple dots off the main thread for StippleCanvas.
import { stipple, type StipplePattern } from "./stipple";

export type StippleRequest = {
  pattern: StipplePattern;
  bounds: { left: number; top: number; right: number; bottom: number };
};

/** Dot centres per ink, as x, y pairs in pattern units. */
export type StippleResponse = { a: Float32Array; b: Float32Array };

self.onmessage = ({ data }: MessageEvent<StippleRequest>) => {
  const dots = { a: [] as number[], b: [] as number[] };
  stipple(data.pattern, data.bounds, (dot) => dots[dot.ink].push(dot.x, dot.y));
  const a = Float32Array.from(dots.a);
  const b = Float32Array.from(dots.b);
  const response: StippleResponse = { a, b };
  (self as unknown as Worker).postMessage(response, [a.buffer, b.buffer]);
};
