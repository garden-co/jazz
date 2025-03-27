import { animated, to, useSpring } from "@react-spring/web";
import { useEffect, useRef, useState } from "react";
import { Vec2, ViewBox } from "../types";

interface CursorLabelProps {
  name: string;
  color: string;
  position: Vec2;
  bounds?: ViewBox;
  isOutOfBounds?: boolean;
  style?: React.CSSProperties;
}

const defaultStyle = {
  fontFamily: "Inter, Manrope, system-ui, sans-serif",
  fontWeight: 500,
} as const;

interface LabelPosition {
  x: number;
  y: number;
}

interface TextDimensions {
  width: number;
  height: number;
}

const OFFSET = 25;

export function CursorLabel({
  name,
  color,
  position,
  bounds,
  isOutOfBounds,
  style = defaultStyle,
}: CursorLabelProps) {
  const textRef = useRef<SVGTextElement>(null);
  const [dimensions, setDimensions] = useState<TextDimensions>({
    width: 0,
    height: 0,
  });

  useEffect(() => {
    if (textRef.current) {
      const bbox = textRef.current.getBBox();
      setDimensions({ width: bbox.width, height: bbox.height });
    }
  }, [name]);

  const getLabelPosition = (): LabelPosition => {
    if (!isOutOfBounds || !bounds) {
      return { x: OFFSET, y: OFFSET };
    }

    // Bounds: bounds.x, bounds.y, bounds.width, bounds.height
    // Text dimensions: dimensions.width, dimensions.height
    // Intersection point: position.x, position.y

    return {
      x: position.x,
      y: position.y,
    };
  };

  const labelSprings = useSpring<LabelPosition>({
    ...getLabelPosition(),
    config: {
      tension: 170,
      friction: 26,
    },
  });

  return (
    <>
      <animated.text
        ref={textRef}
        x={to([labelSprings.x], (x) => x)}
        y={to([labelSprings.y], (y) => y)}
        fill={color}
        stroke="white"
        strokeWidth="3"
        strokeLinejoin="round"
        paintOrder="stroke"
        fontSize="14"
        dominantBaseline="hanging"
        textAnchor="start"
        style={style}
      >
        {name}
      </animated.text>
      <animated.rect
        x={to([labelSprings.x], (x) => x)}
        y={to([labelSprings.y], (y) => y)}
        width={dimensions.width}
        height={dimensions.height}
        fill="none"
        stroke="red"
        strokeWidth="1"
      />
    </>
  );
}
