import { cn } from "@/lib/utils";

interface LabelBadgeProps {
  name: string;
  color: string;
  className?: string;
}

export function LabelBadge({ name, color, className }: LabelBadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium",
        className
      )}
      style={{
        backgroundColor: `${color}20`,
        color: color,
      }}
    >
      {name}
    </span>
  );
}
