import { co } from "jazz-tools";

export const JazzProfile = co.profile({
  file: co.optional(co.fileStream()),
});

export const JazzAccount = co.account({
  profile: JazzProfile,
  root: co.map({}),
});
