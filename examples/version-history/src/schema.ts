/**
 * Learn about schemas here:
 * https://jazz.tools/docs/react/schemas/covalues
 */

import { co, z } from "jazz-tools";

export const Issue = co.map({
  title: z.string(),
  description: co.plainText(),
  estimate: z.number(),
  status: z.literal(["backlog", "in progress", "done"]),
});

export const Project = co.map({
  name: z.string(),
  issues: co.list(Issue),
});
