// [!code hide]
import { co, z } from "jazz-tools";

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
});

const ProjectWithFixedName = Project.safeExtend({
  name: z.literal("My project"),
});

const project = ProjectWithFixedName.create({
  name: "My project",
  startDate: new Date("2025-04-01"),
});
