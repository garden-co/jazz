// Shared CSS for animated MDX diagrams. Rendered once via React 19's
// <style href="..."> dedup — any diagram can drop in <DiagramStyles /> and
// reuse the classes below.

const DIAGRAM_STYLES_CSS = `
.diagram-path {
  stroke-dasharray: 99999;
  stroke-dashoffset: 99999;
  opacity: 0;
}
@keyframes diagram-pulse-frames {
  0%   { box-shadow: 0 0 0 0 rgba(20, 106, 255, 0.55); opacity: 1; }
  20%  { box-shadow: 0 0 0 4px rgba(20, 106, 255, 0.45); opacity: 1; }
  100% { box-shadow: 0 0 0 14px rgba(20, 106, 255, 0); opacity: 0; }
}
.diagram-pulse {
  animation: diagram-pulse-frames 1400ms ease-out forwards;
}
.diagram-dot {
  filter: drop-shadow(0 0 5px rgba(20, 106, 255, 1)) drop-shadow(0 0 12px rgba(20, 106, 255, 0.55));
}
@keyframes diagram-tally-frames {
  0%   { transform: translateY(60%); }
  100% { transform: translateY(0); }
}
.diagram-tally {
  animation: diagram-tally-frames 180ms cubic-bezier(0.2, 0.9, 0.4, 1);
}
`;

export function DiagramStyles() {
  return (
    <style href="diagram-shared-styles" precedence="default">
      {DIAGRAM_STYLES_CSS}
    </style>
  );
}
