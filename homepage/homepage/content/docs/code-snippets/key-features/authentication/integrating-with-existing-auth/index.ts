import { MyAppAccount } from "./schema";
import { startWorker } from "jazz-tools/worker";

const { worker } = await startWorker({
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
});

// Get all users from your existing database
// @ts-expect-error This is a virtual implementation
const allUsers = await db.query.users.findMany();

for (const user of allUsers) {
  const { credentials } = await MyAppAccount.createAs(worker, {
    creationProps: { name: user.name },
  });
  const { accountID, accountSecret } = credentials;
  // @ts-expect-error This is a virtual implementation
  const encryptedSecret = await encrypt(accountSecret); // use your implementation here
  // Persist the Jazz account ID and secret in your existing database
  // @ts-expect-error This is a virtual implementation
  await db.query.users.update({
    where: { id: user.id },
    data: {
      jazzAccountID: accountID,
      jazzAccountSecret: encryptedSecret,
    },
  });
}
