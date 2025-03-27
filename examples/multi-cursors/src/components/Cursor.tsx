import { animated, to, useSpring } from "@react-spring/web";
import { Vec2, ViewBox } from "../types";
import { calculateBoundaryIntersection } from "../utils/boundaryIntersection";
import { isOutOfBounds } from "../utils/isOutOfBounds";

interface CursorProps {
  position: { x: number; y: number };
  color: string;
  isDragging: boolean;
  isRemote: boolean;
  name: string;
  age?: number;
  centerOfBounds: Vec2;
  bounds?: ViewBox;
}

export function Cursor({
  position,
  color,
  isDragging,
  isRemote,
  name,
  age = 0,
  centerOfBounds,
  bounds,
}: CursorProps) {
  if (!bounds) {
    console.log("Boundless!");
    return null;
  }

  const intersectionPoint = calculateBoundaryIntersection(
    centerOfBounds,
    position,
    bounds,
  );

  const isCursorOutOfBounds = isOutOfBounds(position, bounds);

  const springs = useSpring({
    x: position.x,
    y: position.y,
    opacity: age > 60000 ? 0 : 1,
    immediate: !isRemote,
    config: {
      tension: 170,
      friction: 26,
    },
  });

  return (
    <>
      <animated.g
        transform={to(
          [springs.x, springs.y],
          (x: number, y: number) => `translate(${x}, ${y})`,
        )}
      >
        {isCursorOutOfBounds && <circle cx={0} cy={0} r={4} fill={color} />}
        {!isOutOfBounds(position, bounds, 20) && (
          <polygon
            points="0,0 0,20 14.3,14.3"
            fill={
              isDragging
                ? color
                : `color-mix(in oklch, ${color}, transparent 56%)`
            }
            stroke={color}
            strokeWidth="3"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        )}

        {!isCursorOutOfBounds && (
          <text
            x="10"
            y="25"
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
        )}
      </animated.g>
      {isCursorOutOfBounds && (
        <>
          <circle
            cx={intersectionPoint.x}
            cy={intersectionPoint.y}
            r={4}
            fill={color}
          />
        </>
      )}

      {isCursorOutOfBounds && (
        <text
          x={intersectionPoint.x + 10}
          y={intersectionPoint.y + 25}
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
      )}
    </>
  );
}
