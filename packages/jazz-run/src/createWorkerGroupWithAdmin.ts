import { createWorkerAccount } from "./createWorkerAccount.js";
import { createWorkerGroup } from "./greateWorkerGroup.js";

export async function createWorkerGroupWithAdmin({
  name,
  peer,
}: { name: string; peer: string }) {
  const { accountID: adminAccountID, agentSecret: adminAgentSecret } =
    await createWorkerAccount({ name: name + "_admin", peer });

  const { groupID, groupAsOwner } = await createWorkerGroup({
    owner: adminAccountID,
    ownerSecret: adminAgentSecret,
    peer,
  });

  const { account, accountID, agentSecret } = await createWorkerAccount({
    name: name + "_0",
    peer,
  });

  groupAsOwner.addMember(account, "writer");

  await account.waitForAllCoValuesSync({ timeout: 4_000 });

  return {
    adminAccountID,
    adminAgentSecret,
    groupID,
    accountID,
    agentSecret,
  };
}
