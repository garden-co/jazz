import { co, z } from "jazz-tools";

export const Band = co.map({
  name: z.string(), // Zod primitive type
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
  .withMigration((account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        myFestival: [],
      });
    }
  });
