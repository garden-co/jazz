import { cn } from "@/lib/utils";
import { PRIORITY_COLORS, type Priority } from "@/utils/constants";
import {
  AlertTriangle,
  SignalHigh,
  SignalLow,
  SignalMedium,
} from "lucide-react";

interface PriorityIconProps {
  priority: string;
  className?: string;
  showLabel?: boolean;
}

const PRIORITY_ICONS: Record<Priority, React.ReactNode> = {
  low: <SignalLow className="h-4 w-4" />,
  medium: <SignalMedium className="h-4 w-4" />,
  high: <SignalHigh className="h-4 w-4" />,
  urgent: <AlertTriangle className="h-4 w-4" />,
};

export function PriorityIcon({
  priority,
  className,
  showLabel,
}: PriorityIconProps) {
  const p = priority as Priority;
  const color = PRIORITY_COLORS[p] || "#6B7280";
  const icon = PRIORITY_ICONS[p] || <SignalLow className="h-4 w-4" />;

  return (
    <div
      className={cn("flex items-center gap-1", className)}
      style={{ color }}
      title={priority}
    >
      {icon}
      {showLabel && <span className="text-xs capitalize">{priority}</span>}
    </div>
  );
}
