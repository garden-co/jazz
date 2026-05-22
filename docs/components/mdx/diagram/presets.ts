// Parameterised interaction presets. The engine owns the *mechanism* (breadth-
// first wave ordering, scheduling traces along routed edges); the diagram
// definition keeps the *policy* (payload, conflict resolution, when to fire).

import type { RoutedEdge } from "./geometry";
import type { DiagramTraces, TraceSpec } from "./traces";

export type Hop = { from: string; to: string };

// Breadth-first propagation waves from `start` over a directed adjacency map.
// Each node is reached once; cycles terminate. Pure — unit tested.
export function bfsWaves(adjacency: Record<string, string[]>, start: string): Hop[][] {
  const visited = new Set<string>([start]);
  const waves: Hop[][] = [];
  let frontier: string[] = [start];
  while (frontier.length > 0) {
    const next: string[] = [];
    const hops: Hop[] = [];
    for (const node of frontier) {
      for (const neighbour of adjacency[node] ?? []) {
        if (visited.has(neighbour)) continue;
        visited.add(neighbour);
        hops.push({ from: node, to: neighbour });
        next.push(neighbour);
      }
    }
    if (hops.length > 0) waves.push(hops);
    frontier = next;
  }
  return waves;
}

export const hopEdgeId = (h: Hop): string => `${h.from}->${h.to}`;

// Drives a set of BFS waves through the trace controller: each hop animates
// along its routed edge, the wave cadence is `gapMs`, and `onArrive` fires as
// the trace reaches each target (where the definition applies its policy —
// LWW, colour, pulse). `traceId` lets callers namespace concurrent events.
export function playWaves(
  traces: DiagramTraces,
  waves: Hop[][],
  opts: {
    byId: (edgeId: string) => RoutedEdge | undefined;
    traceId: (h: Hop) => string;
    gapMs: number;
    durationMs?: number;
    timing?: TraceSpec["timing"];
    onArrive?: (node: string, h: Hop) => void;
  },
): void {
  const specs: TraceSpec[][] = waves.map((wave) =>
    wave.flatMap((h) => {
      const routed = opts.byId(hopEdgeId(h));
      if (!routed) return [];
      return [
        {
          id: opts.traceId(h),
          d: routed.d,
          length: routed.length,
          durationMs: opts.durationMs,
          timing: opts.timing,
          follow: true,
          fadeAfter: true,
          onArrive: () => opts.onArrive?.(h.to, h),
        } satisfies TraceSpec,
      ];
    }),
  );
  traces.stage(specs, opts.gapMs);
}
