import { co, Group, z } from "jazz-tools";

export const TodoProfile = co
  .profile({
    name: z.string(),
    todos: co.list(co.plainText()),
  })
  .resolved({
    todos: { $each: true },
  });

export const JazzAccount = co
  .account({
    profile: TodoProfile,
    root: co.map({}),
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("profile")) {
      account.$jazz.set(
        "profile",
        TodoProfile.create(
          {
            name: "Anonymous",
            todos: [],
          },
          Group.create().makePublic(),
        ),
      );
    }

    const { profile } = await account.$jazz.ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    if (!profile.$jazz.has("todos")) {
      profile.$jazz.set("todos", []);
    }
  })
  .resolved({
    profile: TodoProfile.resolveQuery,
  });
