// [!code hide]
import { co, z } from "jazz-tools";
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
});

const ProjectDraft = Project.partial();

// The fields are all optional now
const project = ProjectDraft.create({});
