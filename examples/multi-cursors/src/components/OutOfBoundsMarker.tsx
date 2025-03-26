import { Vec2, ViewBox } from "../types";

interface OutOfBoundsMarkerProps {
  position: Vec2;
  color: string;
  name: string;
  bounds?: ViewBox;
}

function middleOfBounds(bounds?: ViewBox): Vec2 {
  if (!bounds) {
    return { x: 0, y: 0 };
  }

  return {
    x: bounds.x + bounds.width / 2,
    y: bounds.y + bounds.height / 2,
  };
}

function calculateBoundaryIntersection(
  center: Vec2,
  point: Vec2,
  bounds: ViewBox,
): Vec2 {
  // Calculate direction vector
  const dx = point.x - center.x;
  const dy = point.y - center.y;

  // Calculate all possible intersections
  let horizontalIntersection: Vec2 | null = null;
  let verticalIntersection: Vec2 | null = null;

  // Check horizontal bounds
  if (point.x < bounds.x) {
    const y = center.y + (dy * (bounds.x - center.x)) / dx;
    if (y >= bounds.y && y <= bounds.y + bounds.height) {
      horizontalIntersection = { x: bounds.x, y };
    }
  } else if (point.x > bounds.x + bounds.width) {
    const y = center.y + (dy * (bounds.x + bounds.width - center.x)) / dx;
    if (y >= bounds.y && y <= bounds.y + bounds.height) {
      horizontalIntersection = { x: bounds.x + bounds.width, y };
    }
  }

  // Check vertical bounds
  if (point.y < bounds.y) {
    const x = center.x + (dx * (bounds.y - center.y)) / dy;
    if (x >= bounds.x && x <= bounds.x + bounds.width) {
      verticalIntersection = { x, y: bounds.y };
    }
  } else if (point.y > bounds.y + bounds.height) {
    const x = center.x + (dx * (bounds.y + bounds.height - center.y)) / dy;
    if (x >= bounds.x && x <= bounds.x + bounds.width) {
      verticalIntersection = { x, y: bounds.y + bounds.height };
    }
  }

  // Choose the intersection point that's closest to the actual point
  if (horizontalIntersection && verticalIntersection) {
    const horizontalDist = Math.hypot(
      point.x - horizontalIntersection.x,
      point.y - horizontalIntersection.y,
    );
    const verticalDist = Math.hypot(
      point.x - verticalIntersection.x,
      point.y - verticalIntersection.y,
    );
    return horizontalDist < verticalDist
      ? horizontalIntersection
      : verticalIntersection;
  }

  return (
    horizontalIntersection ||
    verticalIntersection || {
      x: Math.max(bounds.x, Math.min(bounds.x + bounds.width, point.x)),
      y: Math.max(bounds.y, Math.min(bounds.y + bounds.height, point.y)),
    }
  );
}

function OutOfBoundsMarker({
  position,
  bounds,
  name,
  color,
}: OutOfBoundsMarkerProps) {
  const center = middleOfBounds(bounds);
  if (!bounds) {
    console.log("no bounds");
    return null;
  }

  const intersectionPoint = calculateBoundaryIntersection(
    center,
    position,
    bounds,
  );

  return (
    <>
      <circle
        cx={intersectionPoint.x}
        cy={intersectionPoint.y}
        r={4}
        fill={color}
      />
      <text
        x={intersectionPoint.x + 5}
        y={intersectionPoint.y + 5}
        fill={color}
        stroke="white"
        strokeWidth="3"
        strokeLinejoin="round"
        paintOrder="stroke"
        fontSize="14"
        dominantBaseline="hanging"
        style={{
          fontFamily: "Inter, Manrope, system-ui, sans-serif",
          fontWeight: 500,
        }}
      >
        {name}
      </text>
      {/* <rect x={position.x} y={position.y} width={4} height={4} fill="red" /> */}
      {/* <line
        x1={center.x}
        y1={center.y}
        x2={intersectionPoint.x}
        y2={intersectionPoint.y}
        stroke={color}
        strokeWidth="1"
        strokeLinejoin="round"
      /> */}
    </>
  );
}

export { OutOfBoundsMarker };
