export function CanvasDemoContent() {
  return (
    <>
      <circle cx={0} cy={0} r="200" fill="oklch(0.985 0.001 106.423)" />

      <text
        x={0}
        y={0}
        textAnchor="middle"
        dominantBaseline="middle"
        fontSize="32"
        fontFamily="ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace"
        fill="#3318F7"
      >
        Hello, World!
      </text>
    </>
  );
}
