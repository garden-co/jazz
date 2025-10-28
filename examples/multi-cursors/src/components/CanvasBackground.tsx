interface CanvasBackgroundProps {
  bgPosition: { x: number; y: number };
  dottedGridSize: number;
}

export function CanvasBackground({
  bgPosition,
  dottedGridSize,
}: CanvasBackgroundProps) {
  const bgProps = { x: "-10000", y: "-10000", width: "20000", height: "20000" };

  return (
    <>
      <defs>
        <pattern
          id="dottedGrid"
          width={dottedGridSize}
          height={dottedGridSize}
          patternUnits="userSpaceOnUse"
          patternContentUnits="userSpaceOnUse"
        >
          <circle cx="20" cy="20" r="2" fill="oklch(0.923 0.003 48.717)" />
        </pattern>
      </defs>

      {/* backgrounds using translate to appear infinite by moving it to visible area */}
      <g transform={`translate(${bgPosition.x}, ${bgPosition.y})`}>
        <rect {...bgProps} fill="oklch(0.97 0.001 106.424)" />
        <rect {...bgProps} fill="url(#dottedGrid)" />
      </g>
    </>
  );
}
