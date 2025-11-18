import { co, z } from "jazz-tools";

export const Band = co.map({
  name: z.string(),
});

export const BandList = co.list(Band);

export const JazzFestWorkerAccount = co
  .account({
    root: co.map({
      bandList: BandList,
    }),
    profile: co.profile(),
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        bandList: [],
      });
      if (account.root.$isLoaded) {
        account.root.$jazz.owner.makePublic();
      }
    }
  });
