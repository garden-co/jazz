import type { Db } from "../runtime/db.js";
import type { RowOf } from "../typed-app.js";
import { app } from "./tasks/jazz.generated.js";

declare const db: Db;
db.insert(app.tasks, {
  title: "t",
  projectId: "p",
  tags: [],
  assigneeIds: [],
  dueAt: "2026-01-01",
});
app.tasks.where({ status: { in: ["todo", "done"] } });
// @ts-expect-error enum columns have no `match` operator
app.tasks.where({ status: { match: { type: "todo" } } });
app.tasks.where({ dueAt: { gt: new Date() }, score: { gte: 3 } });
// @ts-expect-error not an enum variant
app.tasks.where({ status: "nope" });
const row: RowOf<typeof app.tasks> = null as never;
const priority: "low" | "high" = row.priority;
String(priority);
const withProject = app.tasks.include({ project: true, assignees: true }).requireIncludes();
const r2: RowOf<typeof withProject> = null as never;
const pname: string = r2.project.name;
const firstAssignee: string | undefined = r2.assignees[0]?.email;
String(pname + firstAssignee);
