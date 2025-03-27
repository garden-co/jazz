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
    const bbox = textRef.current?.getBBox();
    setDimensions({ width: bbox?.width ?? 0, height: bbox?.height ?? 0 });
  }, [name]);

  const getLabelPosition = (): LabelPosition => {
    if (!isOutOfBounds || !bounds) {
      return { x: position.x + 15, y: position.y + 25 };
    }

    // Calculate the percentage of the bounds that the intersection point is from the left
    const percentageH = position.x / bounds.width + 0.5;
    const percentageV = position.y / bounds.height + 0.5;

    return {
      x: position.x - percentageH * dimensions.width,
      y: position.y - percentageV * dimensions.height,
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
  );
}
