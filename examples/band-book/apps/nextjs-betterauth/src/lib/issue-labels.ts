import type { IssuePriority, IssueStatus } from "@/schema";

export const ISSUE_STATUSES: IssueStatus[] = ["backlog", "todo", "in_progress", "done"];
export const ISSUE_PRIORITIES: IssuePriority[] = ["none", "low", "medium", "high", "urgent"];

export const STATUS_LABELS: Record<IssueStatus, string> = {
  backlog: "Backlog",
  todo: "To do",
  in_progress: "In progress",
  done: "Done",
};

export const STATUS_BADGES: Record<IssueStatus, "neutral" | "info" | "warning" | "success"> = {
  backlog: "neutral",
  todo: "info",
  in_progress: "warning",
  done: "success",
};

export const PRIORITY_LABELS: Record<IssuePriority, string> = {
  none: "No priority",
  low: "Low",
  medium: "Medium",
  high: "High",
  urgent: "Urgent",
};
