import { co } from "jazz-tools";

export const Profile = co.profile({
  image: co.optional(co.image()),
});

export const TestAccount = co.account({
  profile: Profile,
  root: co.map({}),
});
