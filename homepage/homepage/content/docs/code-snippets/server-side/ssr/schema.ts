import { co, z } from "jazz-tools";

export const Band = co
  .map({
    name: z.string(), // Zod primitive type
  })
  // [!code ++:3]
  .withMigration((band) => {
    band.$jazz.owner.makePublic();
  });

export const Festival = co.list(Band);

export const JazzFestAccountRoot = co.map({
  myFestival: Festival,
});

export const JazzFestAccount = co
  .account({
    root: JazzFestAccountRoot,
    profile: co.profile(),
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        myFestival: [],
      });

      // [!code ++:8]
      if (account.root.$isLoaded) {
        const { myFestival } = await account.root.$jazz.ensureLoaded({
          resolve: {
            myFestival: true,
          },
        });
        myFestival.$jazz.owner.makePublic();
      }
    }
  });
