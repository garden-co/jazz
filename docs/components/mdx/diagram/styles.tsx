// Shared CSS for animated MDX diagrams. Rendered once via React 19's
// <style href="..."> dedup — any diagram can drop in <DiagramStyles /> and
// reuse the classes below.

const DIAGRAM_STYLES_CSS = `
/* Theme contract. The engine only ever reads these --diagram-* custom
   properties; these are the standalone defaults. A host app (the docs) maps
   them to its own design tokens via one adapter — that adapter is the ONLY
   coupling point, and it lives outside this folder so the engine stays
   framework-agnostic and extractable. */
/* Defaults live in a layer so an (unlayered) host adapter ALWAYS wins,
   independent of stylesheet injection order. Standalone (no adapter) still
   gets these sensible values. */
@layer diagram-defaults {
  .diagram-host {
    --diagram-accent: #146aff;
    --diagram-accent-rgb: 20 106 255;
    --diagram-accent-fg: #ffffff;
    --diagram-accent-soft: rgb(var(--diagram-accent-rgb) / 0.15);
    --diagram-border: #e4e4e7;
    --diagram-edge: #9ca3af;
    --diagram-bg: #ffffff;
    --diagram-card: #ffffff;
    --diagram-card-muted: #f4f4f5;
    --diagram-fg: #18181b;
    --diagram-muted: #71717a;
  }
}
.diagram-path {
  stroke-dasharray: 99999;
  stroke-dashoffset: 99999;
  opacity: 0;
}
@keyframes diagram-pulse-frames {
  0%   { box-shadow: 0 0 0 0 rgb(var(--diagram-accent-rgb) / 0.55); opacity: 1; }
  20%  { box-shadow: 0 0 0 4px rgb(var(--diagram-accent-rgb) / 0.45); opacity: 1; }
  100% { box-shadow: 0 0 0 14px rgb(var(--diagram-accent-rgb) / 0); opacity: 0; }
}
.diagram-pulse {
  animation: diagram-pulse-frames 1400ms ease-out forwards;
}
.diagram-dot {
  filter: drop-shadow(0 0 5px rgb(var(--diagram-accent-rgb) / 1))
    drop-shadow(0 0 12px rgb(var(--diagram-accent-rgb) / 0.55));
}
@keyframes diagram-tally-frames {
  0%   { transform: translateY(60%); }
  100% { transform: translateY(0); }
}
.diagram-tally {
  animation: diagram-tally-frames 180ms cubic-bezier(0.2, 0.9, 0.4, 1);
}

/* Composable node kit. Token-driven, framework-agnostic (no Tailwind). The
   shared shell carries border/radius/background via --diagram-* vars; size and
   spacing are the consumer's, so a definition can shape its own cards. */
.dg-node {
  position: relative;
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  border: 2px solid var(--diagram-border);
  border-radius: 8px;
  background: var(--diagram-card);
  color: var(--diagram-fg);
}
/* Engine default node (GraphNode label shorthand): a tidy centred pill.
   min-width keeps short labels (e.g. "v1") from collapsing to a cramped
   chip, which also gives the router enough node span to converge cleanly. */
.dg-node--label {
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 3rem;
  border-width: 1px;
  border-radius: 999px;
  padding: 0.4rem 0.9rem;
  text-align: center;
}
.dg-node-icon {
  display: inline-flex;
  width: 1rem;
  height: 1rem;
  margin-bottom: 0.25rem;
  color: var(--diagram-muted);
}
.dg-node-icon > svg {
  width: 100%;
  height: 100%;
}
.dg-node-title {
  font-size: 0.75rem;
  font-weight: 600;
  line-height: 1.15;
  color: var(--diagram-fg);
}
.dg-node-subtitle {
  font-size: 0.625rem;
  line-height: 1.15;
  color: var(--diagram-muted);
}
.dg-node-footer {
  margin-top: 0.5rem;
  width: 100%;
}
.dg-node-action {
  appearance: none;
  margin-top: 0.5rem;
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.25rem;
  border: 1px solid var(--diagram-border);
  border-radius: 4px;
  background: var(--diagram-card);
  color: var(--diagram-fg);
  font: inherit;
  font-size: 0.75rem;
  font-weight: 500;
  padding: 0.25rem 0.5rem;
  cursor: pointer;
  transition: background-color 0.15s;
}
.dg-node-action:hover {
  background: var(--diagram-accent-soft);
}
.dg-node-action:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}
.dg-node-action:disabled:hover {
  background: var(--diagram-card);
}

/* Shared diagram frame chrome. */
.dg-frame {
  margin: 1.5rem 0;
  border: 1px solid var(--diagram-border);
  border-radius: 0.75rem;
  background: color-mix(in srgb, var(--diagram-card) 30%, transparent);
  padding: 1.25rem;
}
.dg-frame-eyebrow {
  margin: 0;
  font-size: 0.75rem;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--diagram-accent);
}
.dg-frame-desc {
  margin: 0 0 1.5rem;
  font-size: 0.875rem;
  color: var(--diagram-muted);
}
.dg-frame--desktop-only {
  display: none;
}
@media (min-width: 1024px) {
  .dg-frame--desktop-only {
    display: block;
  }
}
`;

export function DiagramStyles() {
  return (
    <style href="diagram-shared-styles" precedence="default">
      {DIAGRAM_STYLES_CSS}
    </style>
  );
}
