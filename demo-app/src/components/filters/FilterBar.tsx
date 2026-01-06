import { useAll } from "@jazz/react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import { STATUSES, STATUS_LABELS, PRIORITIES, PRIORITY_LABELS } from "@/utils/constants";
import { app } from "@/generated/client";

interface FilterBarProps {
  statusFilter: string | undefined;
  onStatusFilterChange: (status: string | undefined) => void;
  priorityFilter: string | undefined;
  onPriorityFilterChange: (priority: string | undefined) => void;
  assigneeFilter: string | undefined;
  onAssigneeFilterChange: (userId: string | undefined) => void;
  labelFilter: string | undefined;
  onLabelFilterChange: (labelId: string | undefined) => void;
}

export function FilterBar({
  statusFilter,
  onStatusFilterChange,
  priorityFilter,
  onPriorityFilterChange,
  assigneeFilter,
  onAssigneeFilterChange,
  labelFilter,
  onLabelFilterChange,
}: FilterBarProps) {
  // Fetch users and labels internally
  const [users] = useAll(app.users);
  const [labels] = useAll(app.labels);
  const hasFilters = statusFilter || priorityFilter || assigneeFilter || labelFilter;

  const clearFilters = () => {
    onStatusFilterChange(undefined);
    onPriorityFilterChange(undefined);
    onAssigneeFilterChange(undefined);
    onLabelFilterChange(undefined);
  };

  return (
    <div className="flex items-center gap-2 border-b px-4 py-2">
      <span className="text-sm font-medium text-muted-foreground">Filters:</span>

      <Select
        value={statusFilter || "all"}
        onValueChange={(v) => onStatusFilterChange(v === "all" ? undefined : v)}
      >
        <SelectTrigger className="w-[140px] h-8">
          <SelectValue placeholder="Status" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Statuses</SelectItem>
          {STATUSES.map((s) => (
            <SelectItem key={s} value={s}>
              {STATUS_LABELS[s]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={priorityFilter || "all"}
        onValueChange={(v) => onPriorityFilterChange(v === "all" ? undefined : v)}
      >
        <SelectTrigger className="w-[140px] h-8">
          <SelectValue placeholder="Priority" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Priorities</SelectItem>
          {PRIORITIES.map((p) => (
            <SelectItem key={p} value={p}>
              {PRIORITY_LABELS[p]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={assigneeFilter || "all"}
        onValueChange={(v) => onAssigneeFilterChange(v === "all" ? undefined : v)}
      >
        <SelectTrigger className="w-[160px] h-8">
          <SelectValue placeholder="Assignee" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Assignees</SelectItem>
          {users.map((u) => (
            <SelectItem key={u.id} value={u.id}>
              {u.name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={labelFilter || "all"}
        onValueChange={(v) => onLabelFilterChange(v === "all" ? undefined : v)}
      >
        <SelectTrigger className="w-[140px] h-8">
          <SelectValue placeholder="Label" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Labels</SelectItem>
          {labels.map((l) => (
            <SelectItem key={l.id} value={l.id}>
              <div className="flex items-center gap-2">
                <div
                  className="h-2 w-2 rounded-full"
                  style={{ backgroundColor: l.color }}
                />
                {l.name}
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {hasFilters && (
        <Button variant="ghost" size="sm" onClick={clearFilters} className="h-8">
          <X className="h-4 w-4 mr-1" />
          Clear
        </Button>
      )}
    </div>
  );
}
