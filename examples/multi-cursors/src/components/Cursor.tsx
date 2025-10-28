import { animated, to, useSpring } from "@react-spring/web";
import { Vec2, ViewBox } from "../types";
import { calculateBoundaryIntersection } from "../utils/boundaryIntersection";
import { isOutOfBounds } from "../utils/isOutOfBounds";
import { CursorLabel } from "./CursorLabel";

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

const LABEL_BOUNDS_PADDING = 32;
const CURSOR_VISIBILITY_OFFSET = 20;

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
  if (!bounds) return null;

  const intersectionPoint = calculateBoundaryIntersection(
    centerOfBounds,
    position,
    bounds,
  );

  const labelBounds = {
    x: bounds.x + LABEL_BOUNDS_PADDING / 2,
    y: bounds.y + LABEL_BOUNDS_PADDING / 2,
    width: bounds.width - LABEL_BOUNDS_PADDING,
    height: bounds.height - LABEL_BOUNDS_PADDING,
  };

  const cursorIntersectionPoint = calculateBoundaryIntersection(
    centerOfBounds,
    position,
    labelBounds,
  );

  const isStrictlyOutOfBounds = isOutOfBounds(position, bounds);
  const shouldHideCursor = isOutOfBounds(
    position,
    bounds,
    CURSOR_VISIBILITY_OFFSET,
  );

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

  const intersectionSprings = useSpring({
    x: intersectionPoint.x,
    y: intersectionPoint.y,
    config: {
      tension: 170,
      friction: 26,
    },
  });

  return (
    <>
      {/* @ts-expect-error - TODO: invalid after the React 19 upgrade */}
      <animated.g
        transform={to(
          [springs.x, springs.y],
          (x: number, y: number) => `translate(${x}, ${y})`,
        )}
      >
        {isStrictlyOutOfBounds ? (
          <circle cx={0} cy={0} r={4} fill={color} />
        ) : null}
        {!shouldHideCursor ? (
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
        ) : null}
      </animated.g>

      {isRemote ? (
        <>
          <CursorLabel
            name={name}
            color={color}
            position={cursorIntersectionPoint}
            bounds={bounds}
            isOutOfBounds={isStrictlyOutOfBounds}
          />
          {isStrictlyOutOfBounds ? (
            // @ts-expect-error - TODO: invalid after the React 19 upgrade
            <animated.g
              transform={to(
                [intersectionSprings.x, intersectionSprings.y],
                (x: number, y: number) => {
                  const angle =
                    Math.atan2(centerOfBounds.y - y, centerOfBounds.x - x) *
                    (180 / Math.PI);
                  return `translate(${x}, ${y}) rotate(${angle})`;
                },
              )}
            >
              <path
                d="M 8,-4 L 2,0 L 8,4"
                fill="none"
                stroke={color}
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </animated.g>
          ) : null}
        </>
      ) : null}
    </>
  );
}
