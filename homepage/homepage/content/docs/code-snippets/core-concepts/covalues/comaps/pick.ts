// [!code hide]
import { co, z } from "jazz-tools";
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
});

const ProjectStep1 = Project.pick({
  name: true,
  startDate: true,
});

// We don't provide the status field
const project = ProjectStep1.create({
  name: "My project",
  startDate: new Date("2025-04-01"),
});
