# Diagram engine

A small, declarative, framework-agnostic engine for the docs diagrams. You
describe a diagram as data (and, where it helps, supply interactive behaviour);
the engine handles layout, measurement, orthogonal edge routing, responsive
scaling, theming and animation.

Two primitives cover every diagram today: `<Graph>` (data-flow graphs and
DAGs, static or interactive — the version-history DAG, tier-sync, lens and
write-tier are all `<Graph>`) and `<Sequence>` (sequence diagrams).

## Design posture: extract-ready island

Everything in `components/mdx/diagram/` is a self-contained island. It imports
**nothing** framework-specific so it could later be lifted into a standalone
package with no rewrite:

- No `fumadocs-*`, no `next-themes`, no `@/lib/cn`, no `fd-*` classes, no
  Tailwind utilities, no `lucide-react`. Icons are consumer-supplied content,
  not engine.
- Styling is portable CSS delivered via React 19 `<style precedence>` plus
  `--diagram-*` custom properties with built-in defaults.
- **`index.ts` is the only public surface.** Consumers import from
  `./diagram` (the barrel), never deep into the folder. Extraction later is
  roughly: move the folder, add a `package.json`.

This boundary is upheld by discipline and review, not a lint rule. The barrel
is the structural mechanism that keeps the surface honest.

## Quick start: a diagram is just data

The two generic primitives, `Sequence` and `Graph`, are registered as MDX
components, so the simplest diagrams are written inline in the `.mdx` file with
no wrapper component at all.

A sequence diagram (pure data, no import needed):

```mdx
<Sequence
  eyebrow="Provider sign-in"
  description="An external JWT provider connecting to a Jazz server."
  participants={[
    { id: "browser", label: "Browser" },
    { id: "auth", label: "Auth provider" },
    { id: "jazz", label: "Jazz server", createAtStep: 2 },
  ]}
  steps={[
    { kind: "message", from: "browser", to: "auth", text: "Sign in" },
    { kind: "message", from: "auth", to: "browser", text: "JWT", line: "dashed" },
    { kind: "message", from: "browser", to: "jazz", text: "Connect with JWT" },
  ]}
/>
```

A graph is pure data too: give each node a `label` and the engine renders its
default node (a tidy titled pill — `.dg-node--label`, with a `min-width` so
short labels do not collapse), so it inlines with no import. `converge` gives
the git-graph fork/merge look:

```mdx
<Graph
  eyebrow="Row version history"
  description="One device edits linearly; concurrent edits branch then reconcile."
  direction="LR"
  converge
  grid={{ gap: "1.25rem 3rem" }}
  nodes={[
    { id: "v1", rank: 0, label: "v1" },
    { id: "v2", rank: 1, label: "v2" },
    { id: "a3", rank: 2, order: 0, label: "a3" },
    { id: "b3", rank: 2, order: 1, label: "b3" },
    { id: "m4", rank: 3, label: "m4" },
  ]}
  edges={[
    { from: "v1", to: "v2" },
    { from: "v2", to: "a3" },
    { from: "v2", to: "b3" },
    { from: "a3", to: "m4" },
    { from: "b3", to: "m4" },
  ]}
/>
```

For a **bespoke** node, pass `content` instead of `label`. JSX inside a prop
expression is not resolved via the MDX component map, so a custom node
component must be a real in-scope identifier — import it via the `@/` alias:
`import { Thing } from "@/components/mdx/thing";` then
`content: <Thing .../>`. Prefer `label` unless the node genuinely needs a
custom visual.

Rule of thumb:

- **Static / data-only diagram** (a sequence, a fixed DAG): write it inline in
  the MDX as pure data — `label` nodes need no import at all.
- **Interactive or policy-bearing diagram** (tier-sync, lens): keep it as a
  component file under `components/mdx/`. It owns the domain policy and wires
  the engine's interactivity hooks. The engine deliberately does not know about
  LWW, hue-gap colours, demo loops or lens projection.

## `<Sequence>`

Pure analytic layout, no DOM measurement. Inherently static and mobile-safe:
the SVG renders at 1:1 on desktop and downscales only when the container is
narrower than the diagram, so text size is consistent regardless of how many
participants a diagram has.

```ts
type SequenceProps = {
  eyebrow: string;
  description: ReactNode;
  participants: Participant[];
  steps: SequenceStep[];
};

type Participant = { id: string; label: string; createAtStep?: number };

type SequenceStep =
  | { kind: "message"; from: string; to: string; text: string; line?: "solid" | "dashed" }
  | { kind: "note"; over: string | string[]; text: string };
```

Behaviour notes:

- `createAtStep` drops a participant in at that step's row (its lifeline starts
  there) instead of in the header.
- `from === to` renders a self-message loop.
- `line: "dashed"` is the convention for returns/responses.
- A `note` spans one or more participants (`over` is an id or list of ids).
- Long labels **wrap**: past an internal width budget a label flows onto more
  lines and grows that row's height; it never widens the canvas. An unbreakable
  token (e.g. a code call) is capped so it can't rescale the diagram.
- Message and self-message labels paint a background-coloured mask behind the
  text so a label crossing a lifeline stays readable. The mask is the page
  background colour with no padding; its only job is to break the dashed line.
- The step kind is a discriminated union so new kinds are additive.

## `<Graph>`

CSS-grid layout with measured nodes, laned orthogonal edge routing, responsive
scale-to-fit, and an optional definition-drawn animated overlay.

```ts
type GraphProps = {
  eyebrow: string;
  description: ReactNode;
  direction: "TD" | "LR";
  nodes: GraphNode[];
  edges: GraphEdge[];
  grid?: { columns?: string; rows?: string; gap?: string };

  // interactivity (see "Interactivity" below)
  traces?: DiagramTraces;
  overlay?: GraphOverlay;
  overlayBehindNodes?: boolean;
  overlayFront?: GraphOverlay;
  onGeometry?: GraphGeometryListener;

  // layout knobs
  staticBelow?: number; // container px; below ⇒ resting static render. Default 0 (never).
  arrows?: boolean; // arrowheads on structural edges. Default true.
  nodeAlign?: "center" | "stretch"; // default "center" (shrink-wrap each node)
  naturalWidth?: number; // fixed design width for fluid/wrapping layouts (e.g. Lens)
  converge?: boolean; // git-graph fork/merge: collapse same-side endpoints
  // to one anchor instead of laned spreading. Default false.
};

type GraphNode = { id: string; content?: ReactNode; label?: string } & (
  | { slot: { row: number | string; col: number | string } } // explicit placement
  | { rank: number; order?: number } // auto-placement
);
// `label` ⇒ engine default node (pure data, inline-able). `content` ⇒ bespoke
// node (any ReactNode). Provide one; `content` wins if both are set.

type GraphEdge = {
  from: string;
  to: string;
  label?: string;
  variant?: "solid" | "dashed" | "hidden"; // "hidden" ⇒ routed but not drawn
};
```

Placement:

- **Rank mode**: `rank` is the row for `TD` / column for `LR`; `order` sequences
  nodes within a rank. Auto-placed; good for trees and flows.
- **Slot mode**: explicit `{ row, col }` grid placement (used by the lens
  diagram's 3-column / 6-row layout). Requires `grid`.

Routing: by default, multiple edges on the same side spread across laned
anchors, lane order chosen by the cross-axis position of the other endpoint,
which minimises crossings deterministically (right for hub-like nodes, e.g.
TierSync/Lens). Set `converge` for the git-graph look instead: same-side
endpoints collapse to one anchor, so a fork emanates from a single point and a
merge lands on a single point (used by the version DAG). `variant: "hidden"`
keeps an edge in the routed set (so an overlay can read it via `byId`) without
drawing it, e.g. the reverse leg of a bidirectional link.

Responsiveness: with `naturalWidth` set, the diagram lays out at that fixed
design width and scales to fit (down to a floor), with horizontal scroll as the
last resort, so fluid/wrapping content stays deterministic. Without it, the
graph sizes to content.

Static vs interactive: `staticBelow` defaults to `0` (always interactive). A
docs column is far narrower than the viewport, so a non-zero default would
silently disable interactivity on normal screens. A diagram whose interaction
is unusable on a phone can opt in (e.g. `staticBelow={420}`); below that width
it renders the resting structural state with no overlay.

## Node kit

Composable, portable-CSS, token-driven node parts. Reproduce the shared card
look without Tailwind or `fd-*`.

| Component      | Element / class         | Notes                                                                                                           |
| -------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------- |
| `NodeShell`    | `div.dg-node`           | Border / radius / card bg via tokens. Forwards `ref` (the measured + pulse target). Accepts arbitrary children. |
| `NodeIcon`     | `span.dg-node-icon`     | 1rem square, muted.                                                                                             |
| `NodeTitle`    | `div.dg-node-title`     | 0.75rem, weight 600, `--diagram-fg`. The canonical node header (sequence actor boxes match this).               |
| `NodeSubtitle` | `div.dg-node-subtitle`  | 0.625rem, muted.                                                                                                |
| `NodeFooter`   | `div.dg-node-footer`    | Layout slot for actions/extras.                                                                                 |
| `NodeAction`   | `button.dg-node-action` | Clickable affordance; `onClick`, `disabled`, `aria-label`. `disabled` greys it out (not-allowed, no hover).     |

Genuine outliers (the lens diagram's phone frame, tier-sync's hex tally
swatch) stay diagram-side and are dropped into `NodeShell` as raw children.
`NodeShell` remains measure/anchor/pulse-capable regardless of content. The
short-label commit pill is no longer a diagram-side outlier — it is the
engine's default `label` node (`.dg-node--label`).

## Interactivity

Three layers cooperate: the engine owns layout/measurement/routing; a trace
controller owns animation scheduling; the definition owns policy.

`useDiagramTraces()` returns a controller with ref callbacks you attach to the
rendered SVG elements plus imperative drivers:

```ts
type DiagramTraces = {
  pathRef: (id: string) => (el: SVGPathElement | null) => void;
  dotRef: (id: string) => (el: SVGCircleElement | null) => void;
  nodeRef: (id: string) => (el: HTMLElement | null) => void;
  play: (spec: TraceSpec) => void; // animate one routed path (+ optional dot)
  snap: (id: string) => void; // jump to finished state, no animation (resize)
  pulse: (nodeId: string) => void; // transient highlight on a node
  stage: (waves: TraceSpec[][], gapMs: number) => void; // sequenced BFS waves
  reset: () => void; // cancel all timers/RAF, clear transient overlays
};

type TraceSpec = {
  id: string;
  d: string; // routed path data (also set as the live <path> d)
  length?: number; // exact routed length (skips getTotalLength)
  durationMs?: number;
  timing?: { min?: number; max?: number; perPx?: number };
  follow?: boolean; // leading dot follows the draw
  fadeAfter?: boolean; // fade the path out once drawn
  onArrive?: () => void; // fires when the trace reaches its target
};
```

The definition draws its own animated layer via `overlay` (and `overlayFront`
for a layer that must sit above the nodes, e.g. a tip dot riding over cards
while the path runs behind them). Both receive live geometry:

```ts
type GraphOverlayCtx = {
  routed: RoutedEdge[];
  byId: (edgeId: string) => RoutedEdge | undefined;
  anchors: Record<string, Anchors>; // measured node geometry, natural coords
  size: { w: number; h: number };
  isStatic: boolean;
};
```

`onGeometry` is the read side of the same context (for definitions that capture
anchors to draw bespoke connectors).

Presets parameterise the _mechanism_; policy stays in the definition:

```ts
bfsWaves(adjacency: Record<string, string[]>, start: string): Hop[][]
hopEdgeId(h: Hop): string                       // "from->to"
playWaves(traces, waves, {
  byId, traceId, gapMs, durationMs?, timing?, onArrive?
}): void
traceDuration(length, { min?, max?, perPx? }): number
```

`bfsWaves` is pure and unit-tested. `playWaves` schedules each hop along its
routed edge with a wave cadence, firing `onArrive(node, hop)` as the trace
reaches each target, which is where the definition applies its policy (LWW,
colour, pulse). `traceId` namespaces concurrent events so they animate
independently.

Worked pattern (a propagation diagram):

```tsx
const traces = useDiagramTraces();
// in an effect, on click or on an idle loop:
playWaves(traces, bfsWaves(adjacency, source), {
  byId,
  traceId: (h) => `${eventId}:${hopEdgeId(h)}`,
  gapMs: 450,
  timing: { min: 700, max: 4000, perPx: 2.9 },
  onArrive: (node) => applyDomainPolicy(node),
});
```

## Geometry helpers

For overlays that draw bespoke connectors rather than relying on `edges`:

- `connectChain` — orthogonal rounded path through an ordered point list.
- `roundedPath(points, r)` — the low-level builder behind `connectChain`:
  a polyline through `Point[]` with genuine bends rounded by quadratic arcs
  (collinear vertices stay straight). Use it for fully bespoke elbow
  connectors — the write-tier trunk/branch is built from it.
- `loopRoundedBox` — a rounded self-loop around a box (`LoopSide`).
- `RoutedEdge` — `{ id, from, to, d, reverse, length, source, target }`;
  `reverse` is the same path reversed (use it to animate an upward hop along a
  downward-drawn link).
- `Point` — `{ x, y }`; the input to `roundedPath`.
- `Anchors` — per-node edge anchor points in natural coordinates.
- `PATH_INSET`, `DEFAULT_CORNER_RADIUS` — shared constants.

## Theming

The engine reads `--diagram-*` only and ships sensible defaults wrapped in
`@layer diagram-defaults` (so a host adapter, which is unlayered, always wins).

| Token                   | Default           | Purpose                                                  |
| ----------------------- | ----------------- | -------------------------------------------------------- |
| `--diagram-accent`      | `#146aff`         | Brand-blue trace colour. **Not** theme-mapped by design. |
| `--diagram-accent-rgb`  | `20 106 255`      | RGB triple for `rgb(... / α)` glows.                     |
| `--diagram-accent-fg`   | `#ffffff`         | Foreground on accent.                                    |
| `--diagram-accent-soft` | `rgb(... / 0.15)` | Soft accent fill (hover).                                |
| `--diagram-bg`          | `#ffffff`         | Page/surface colour. Used by the sequence label mask.    |
| `--diagram-border`      | `#e4e4e7`         | Node/box borders.                                        |
| `--diagram-edge`        | `#9ca3af`         | Edges, lifelines, arrowheads.                            |
| `--diagram-card`        | `#ffffff`         | Node/card background.                                    |
| `--diagram-card-muted`  | `#f4f4f5`         | Note boxes.                                              |
| `--diagram-fg`          | `#18181b`         | Primary text.                                            |
| `--diagram-muted`       | `#71717a`         | Secondary/muted text.                                    |

The **only** coupling point to fumadocs is one unlayered block in
`docs/app/global.css` mapping these to `--color-fd-*` (so diagrams follow the
site theme, including dark mode, for free). `--diagram-accent` is intentionally
left unmapped. To extract the engine, drop that block; the defaults take over.

## Testing

- **Pure functions: red-green TDD.** Geometry/layout (`*.test.ts` here, run via
  the docs `vitest`): lane assignment, routed path `d`, sequence layout
  coordinates, text wrapping, BFS waves, trace duration. Write the failing test
  first.
- **Interactive / visual parity: manual.** Animated-SVG end-to-end tests are
  high-cost / low-value; verify behaviour and look by eye at desktop and mobile
  widths, light and dark.

## Files

| File                                  | Role                                                              |
| ------------------------------------- | ----------------------------------------------------------------- |
| `index.ts`                            | Public barrel — the only import surface.                          |
| `graph.tsx`                           | `<Graph>` renderer (layout, measure, routing, overlay scaffold).  |
| `sequence.tsx` / `sequence-layout.ts` | `<Sequence>` renderer + pure analytic layout.                     |
| `traces.ts`                           | `useDiagramTraces` controller (wraps `trace-anim.ts`).            |
| `presets.ts`                          | `bfsWaves` / `playWaves` / `hopEdgeId` — parameterised behaviour. |
| `kit.tsx`                             | Composable node parts.                                            |
| `geometry.ts`                         | Pure routing/anchor maths.                                        |
| `styles.tsx`                          | `--diagram-*` defaults + portable CSS via `<style precedence>`.   |
| `frame.tsx`                           | Shared diagram chrome (eyebrow, description, card).               |
| `trace-anim.ts`                       | Low-level path draw / dot tracking primitives.                    |
| `*.test.ts`                           | Pure-function specs.                                              |
