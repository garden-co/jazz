import { co, z, Group } from "jazz-tools";
const Chat = co.map({});
export const MyAppProfile = co.profile({
  name: z.string(), // compatible with default Profile schema
  avatar: co.optional(co.image()),
});
const Bookmark = co.map({});

// #region ExtendedMigration
const MyAppRoot = co.map({
  myChats: co.list(Chat),
  myBookmarks: co.optional(co.list(Bookmark)), // [!code ++:1]
});

export const MyAppAccount = co
  .account({
    root: MyAppRoot,
    profile: MyAppProfile,
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        myChats: [],
      });
    }

    // We need to load the root field to check for the myBookmarks field
    const { root } = await account.$jazz.ensureLoaded({
      resolve: { root: true },
    });

    if (!root.$jazz.has("myBookmarks")) {
      // [!code ++:3]
      root.$jazz.set(
        "myBookmarks",
        co.list(Bookmark).create([], Group.create()),
      );
    }
  });
// #endregion
