import { co, z } from "jazz-tools";

export const JazzProfile = co.profile({
  file: z.optional(co.fileStream()),
});

export const JazzAccount = co.account({
  profile: JazzProfile,
  root: co.map({}),
});
