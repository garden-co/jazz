const Member = co.map({
  name: z.string(),
});
// #region WithGetter
import { co, z } from "jazz-tools";

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: co.optional(Member),
  get subProject() {
    return Project.optional();
  },
});

// @ts-expect-error duplicated
export type Project = co.loaded<typeof Project>;
// #endregion

// #region WithTypedGetter
const ProjectWithTypedGetter = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: co.optional(Member),
  // [!code ++:3]
  get subProjects(): co.Optional<co.List<typeof Project>> {
    return co.optional(co.list(Project));
  },
});

// @ts-expect-error duplicated
export type Project = co.loaded<typeof Project>;
// #endregion
