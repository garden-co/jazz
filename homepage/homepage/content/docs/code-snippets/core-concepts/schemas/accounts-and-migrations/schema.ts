import { Group } from "jazz-tools";
const Chat = co.map({});
// #region Basic
import { co, z } from "jazz-tools";

const MyAppRoot = co.map({
  myChats: co.list(Chat),
});

export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: co.profile(),
});
// #endregion

// #region Account
// ...somewhere in jazz-tools itself...
const Account = co.account({
  root: co.map({}),
  profile: co.profile(),
});
// #endregion

// #region Profile
export const MyAppProfile = co.profile({
  name: z.string(), // compatible with default Profile schema
  avatar: co.optional(co.image()),
});

export const MyAppAccountWithProfile = co.account({
  root: MyAppRoot,
  profile: MyAppProfile,
});
// #endregion

// #region WithMigration
export const MyAppAccountWithMigration = co
  .account({
    root: MyAppRoot,
    profile: MyAppProfile,
  })
  .withMigration((account, creationProps?: { name: string }) => {
    // we use has to check if the root has ever been set
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        myChats: [],
      });
    }

    if (!account.$jazz.has("profile")) {
      const profileGroup = Group.create();
      // Unlike the root, we want the profile to be publicly readable.
      profileGroup.makePublic();

      account.$jazz.set(
        "profile",
        MyAppProfile.create(
          {
            name: creationProps?.name ?? "New user",
          },
          profileGroup,
        ),
      );
    }
  });
// #endregion
