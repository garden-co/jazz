import { co, Group, Loaded, z, setDefaultValidationMode } from "jazz-tools";

setDefaultValidationMode("strict");

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
  });

export const JazzAccountWithTodos = JazzAccount.resolved({
  profile: TodoProfile.resolveQuery,
});
export type JazzAccountWithTodos = co.loaded<typeof JazzAccountWithTodos>;
