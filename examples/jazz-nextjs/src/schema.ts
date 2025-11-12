import { co, z } from "jazz-tools";

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
    console.log("migration");
    if (!account.$jazz.has("profile")) {
      account.$jazz.set(
        "profile",
        TodoProfile.create({
          name: "Anonymous",
          todos: [],
        }),
      );
    }

    const { profile } = await account.$jazz.ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    if (!profile.$jazz.has("todos")) {
      console.log("setting todos");
      profile.$jazz.set("todos", []);
    } else {
      console.log("todos already set");
    }
  })
  .resolved({
    profile: TodoProfile.resolveQuery,
  });
