import { co, z } from "jazz-tools";

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  endDate: z.optional(z.date()),
});
type Project = co.loaded<typeof Project>;

export function isProjectActive(project: Project) {
  const now = new Date();
  return (
    now >= project.startDate && (!project.endDate || now <= project.endDate)
  );
}

export function formatProjectDuration(
  project: Project,
  format: "short" | "full",
) {
  const start = project.startDate.toLocaleDateString();
  if (!project.endDate) {
    return format === "full" ? `Started on ${start}, ongoing` : `From ${start}`;
  }

  const end = project.endDate.toLocaleDateString();
  return format === "full"
    ? `From ${start} to ${end}`
    : `${(project.endDate.getTime() - project.startDate.getTime()) / 86400000} days`;
}

const project = Project.create({
  name: "My project",
  startDate: new Date("2025-04-01"),
  endDate: new Date("2025-04-04"),
});

console.log(isProjectActive(project)); // false
console.log(formatProjectDuration(project, "short")); // "3 days"
