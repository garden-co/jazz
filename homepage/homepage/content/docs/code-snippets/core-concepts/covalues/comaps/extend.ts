// [!code hide]
import { co, z } from "jazz-tools";

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
});

const ProjectWithStatus = Project.extend({
  status: z.literal(["planning", "active", "completed"]),
});

const project = ProjectWithStatus.create({
  name: "My project",
  startDate: new Date("2025-04-01"),
  status: "planning",
});
