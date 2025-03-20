interface UserCursorProps {
  position: { x: number; y: number };
  color: string;
  isDragging: boolean;
}

export function UserCursor({ position, color, isDragging }: UserCursorProps) {
  return (
    <polygon
      points="0,0 0,20 14.3,14.3"
      fill={isDragging ? color : `${color}44`}
      stroke={color}
      strokeWidth="3"
      strokeLinecap="round"
      strokeLinejoin="round"
      transform={`translate(${position.x}, ${position.y})`}
    />
  );
}
