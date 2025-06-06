import { co, z } from "jazz-tools";

export const JazzProfile = co.profile({
  image: z.optional(co.image()),
});

export const JazzAccount = co.account({
  profile: JazzProfile,
  root: co.map({}),
});
