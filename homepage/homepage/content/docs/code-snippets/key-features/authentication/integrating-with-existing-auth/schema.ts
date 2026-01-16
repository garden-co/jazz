import { co, Group, z } from "jazz-tools";

const MyAppAccountRoot = co.map({
  someMap: co.map({}),
});
export const MyAppAccount = co
  .account({
    root: MyAppAccountRoot,
    profile: co.profile(),
  })
  .withMigration(async (account, creationProps?: { name: string }) => {
    if (!account.$jazz.has("root")) {
      if (!process.env.JAZZ_WORKER_ACCOUNT)
        throw new Error("JAZZ_WORKER_ACCOUNT is not set");
      // Load the worker
      const worker = await co.account().load(process.env.JAZZ_WORKER_ACCOUNT);
      const group = Group.create();
      // Add it as a member of a group
      // Note that by doing this, we grant the server worker full access to the user's account root. Consider whether this is appropriate for your use case.
      worker.$isLoaded && group.addMember(worker, "admin");
      // Create the root using the group to grant your server worker admin access on the user's account root.
      const myRoot = MyAppAccountRoot.create(
        {
          someMap: {},
        },
        group,
      );

      account.$jazz.set("root", myRoot);
    }
  });
