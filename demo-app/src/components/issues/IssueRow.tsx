import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { StatusBadge } from "./StatusBadge";
import { PriorityIcon } from "./PriorityIcon";
import { LabelBadge } from "./LabelBadge";
import type { LoadedIssue } from "@/App";

interface IssueRowProps {
  issue: LoadedIssue;
  onClick: () => void;
}

export function IssueRow({ issue, onClick }: IssueRowProps) {
  // Extract labels from the included IssueLabels
  const labels = issue.IssueLabels.map((il) => il.label);

  // Extract assignees from the included IssueAssignees
  const assignees = issue.IssueAssignees.map((ia) => ia.user);

  return (
    <div
      className="flex items-center gap-4 border-b px-4 py-3 hover:bg-muted/50 cursor-pointer transition-colors"
      onClick={onClick}
    >
      <PriorityIcon priority={issue.priority} />

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-medium truncate">{issue.title}</span>
        </div>
        <div className="flex items-center gap-2 mt-1">
          <div className="flex items-center gap-1 text-xs text-muted-foreground">
            <div
              className="h-2 w-2 rounded-full"
              style={{ backgroundColor: issue.project.color }}
            />
            {issue.project.name}
          </div>
          {labels.length > 0 && (
            <div className="flex items-center gap-1">
              {labels.slice(0, 3).map((label) => (
                <LabelBadge key={label.id} name={label.name} color={label.color} />
              ))}
              {labels.length > 3 && (
                <span className="text-xs text-muted-foreground">
                  +{labels.length - 3}
                </span>
              )}
            </div>
          )}
        </div>
      </div>

      <StatusBadge status={issue.status} />

      <div className="flex -space-x-2">
        {assignees.slice(0, 3).map((user) => {
          const initials = user.name
            .split(" ")
            .map((n) => n[0])
            .join("")
            .toUpperCase();
          return (
            <Avatar key={user.id} className="h-6 w-6 border-2 border-background">
              <AvatarFallback
                style={{ backgroundColor: user.avatarColor }}
                className="text-white text-[10px]"
              >
                {initials}
              </AvatarFallback>
            </Avatar>
          );
        })}
        {assignees.length > 3 && (
          <Avatar className="h-6 w-6 border-2 border-background">
            <AvatarFallback className="text-[10px] bg-muted">
              +{assignees.length - 3}
            </AvatarFallback>
          </Avatar>
        )}
      </div>
    </div>
  );
}
