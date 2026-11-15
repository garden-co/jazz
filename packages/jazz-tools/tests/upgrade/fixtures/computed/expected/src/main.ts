import { app, type Task } from "../jazz.generated";
import { type Priority } from "../jazz.computed";

export function describe(task: Task): string {
  const priority: Priority = task.priority;
  return `${task.label} ${priority} ${task.tags.join(" ")}`;
}

export const query = app.tasks.where({ score: { gt: 3 } });
