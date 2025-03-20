import { useState, useEffect } from "react";

function Canvas() {
  const [isDragging, setIsDragging] = useState(false);
  const [viewBox, setViewBox] = useState({
    x: 0,
    y: 0,
    width: window.innerWidth,
    height: window.innerHeight,
  });
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });

  const center = { x: 0, y: 0 };

  useEffect(() => {
    const handleResize = () => {
      setViewBox((prev) => ({
        ...prev,
        width: window.innerWidth,
        height: window.innerHeight,
      }));
    };

    setViewBox((prev) => ({
      ...prev,
      x: center.x - window.innerWidth / 2,
      y: center.y - window.innerHeight / 2,
    }));

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const handleMouseDown = (e: React.MouseEvent<SVGSVGElement>) => {
    setIsDragging(true);
    setDragStart({
      x: e.clientX,
      y: e.clientY,
    });
  };

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!isDragging) return;

    const dx = e.clientX - dragStart.x;
    const dy = e.clientY - dragStart.y;

    setViewBox((prev) => ({
      ...prev,
      x: prev.x - dx,
      y: prev.y - dy,
    }));

    setDragStart({
      x: e.clientX,
      y: e.clientY,
    });
  };

  const handleMouseUp = () => {
    setIsDragging(false);
  };

  const bgPosition = {
    x: Math.floor(viewBox.x / 40) * 40,
    y: Math.floor(viewBox.y / 40) * 40,
  };

  return (
    <svg
      width="100%"
      height="100%"
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
      onMouseLeave={handleMouseUp}
      viewBox={`${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`}
      style={{ cursor: isDragging ? "grabbing" : "grab" }}
      className="select-none"
    >
      <defs>
        <pattern
          id="dottedGrid"
          width="40"
          height="40"
          patternUnits="userSpaceOnUse"
          patternContentUnits="userSpaceOnUse"
        >
          <circle cx="20" cy="20" r="2" fill="rgba(0,0,0,0.1)" />
        </pattern>
      </defs>

      {/* backgrounds using translate to appear infinite by moving it to visible area */}
      <g transform={`translate(${bgPosition.x}, ${bgPosition.y})`}>
        <rect x="-10000" y="-10000" width="20000" height="20000" fill="white" />
        <rect
          x="-10000"
          y="-10000"
          width="20000"
          height="20000"
          fill="url(#dottedGrid)"
        />
      </g>

      <circle cx={center.x} cy={center.y} r="200" fill="black" />
      <text
        x={center.x}
        y={center.y}
        textAnchor="middle"
        dominantBaseline="middle"
        fontSize="32"
        fontFamily="ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace"
        fill="white"
      >
        Hello, World!
      </text>
    </svg>
  );
}

export default Canvas;
