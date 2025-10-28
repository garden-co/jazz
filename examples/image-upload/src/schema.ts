import { co } from "jazz-tools";

export const JazzProfile = co.profile({
  image: co.optional(co.image()),
});

export const JazzAccount = co.account({
  profile: JazzProfile,
  root: co.map({}),
});
