export const STATUSES = ["todo", "in_progress", "done", "cancelled"] as const;
export type Status = (typeof STATUSES)[number];

export const STATUS_LABELS: Record<Status, string> = {
  todo: "Todo",
  in_progress: "In Progress",
  done: "Done",
  cancelled: "Cancelled",
};

export const STATUS_COLORS: Record<Status, string> = {
  todo: "#6B7280",
  in_progress: "#3B82F6",
  done: "#10B981",
  cancelled: "#EF4444",
};

export const PRIORITIES = ["low", "medium", "high", "urgent"] as const;
export type Priority = (typeof PRIORITIES)[number];

export const PRIORITY_LABELS: Record<Priority, string> = {
  low: "Low",
  medium: "Medium",
  high: "High",
  urgent: "Urgent",
};

export const PRIORITY_COLORS: Record<Priority, string> = {
  low: "#6B7280",
  medium: "#F59E0B",
  high: "#F97316",
  urgent: "#EF4444",
};

export const USER_COLORS = [
  "#EF4444",
  "#F59E0B",
  "#10B981",
  "#3B82F6",
  "#8B5CF6",
];

export const PROJECT_DATA = [
  { name: "Frontend", color: "#3B82F6", description: "Web application development" },
  { name: "Backend", color: "#10B981", description: "API and services" },
  { name: "Infrastructure", color: "#F59E0B", description: "DevOps and cloud" },
];

export const LABEL_DATA = [
  { name: "bug", color: "#EF4444" },
  { name: "feature", color: "#3B82F6" },
  { name: "enhancement", color: "#8B5CF6" },
  { name: "documentation", color: "#6B7280" },
  { name: "design", color: "#EC4899" },
  { name: "testing", color: "#10B981" },
  { name: "performance", color: "#F59E0B" },
  { name: "security", color: "#DC2626" },
  { name: "refactor", color: "#06B6D4" },
  { name: "blocked", color: "#991B1B" },
];

export const USER_NAMES = [
  "Alice Chen",
  "Bob Smith",
  "Carol Williams",
  "David Jones",
  "Eve Brown",
];

export const ISSUE_TITLES = [
  "Fix login button not responding on mobile",
  "Add dark mode support",
  "Optimize database queries for dashboard",
  "Implement user avatar upload",
  "Update API documentation",
  "Fix memory leak in real-time updates",
  "Add export to CSV feature",
  "Improve error messages",
  "Set up CI/CD pipeline",
  "Add keyboard shortcuts",
  "Fix timezone handling",
  "Implement search functionality",
  "Add email notifications",
  "Refactor authentication module",
  "Add unit tests for core functions",
  "Fix responsive layout issues",
  "Implement rate limiting",
  "Add activity logging",
  "Improve page load performance",
  "Add multi-language support",
];
