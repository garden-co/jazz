// [!code hide]
import { co, z } from "jazz-tools";

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
});

const ProjectWithoutStatus = Project.omit({
  status: true,
});

const project = ProjectWithoutStatus.create({
  name: "My project",
  startDate: new Date("2025-04-01"),
});
