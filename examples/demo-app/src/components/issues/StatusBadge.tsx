import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { STATUS_COLORS, STATUS_LABELS, type Status } from "@/utils/constants";
import { CheckCircle2, Circle, Timer, XCircle } from "lucide-react";

interface StatusBadgeProps {
  status: string;
  className?: string;
}

const STATUS_ICONS: Record<Status, React.ReactNode> = {
  todo: <Circle className="h-3 w-3" />,
  in_progress: <Timer className="h-3 w-3" />,
  done: <CheckCircle2 className="h-3 w-3" />,
  cancelled: <XCircle className="h-3 w-3" />,
};

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const s = status as Status;
  const label = STATUS_LABELS[s] || status;
  const color = STATUS_COLORS[s] || "#6B7280";
  const icon = STATUS_ICONS[s] || <Circle className="h-3 w-3" />;

  return (
    <Badge
      variant="outline"
      className={cn("gap-1 font-normal", className)}
      style={{ borderColor: color, color }}
    >
      {icon}
      {label}
    </Badge>
  );
}
