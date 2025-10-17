import { startWorker } from "jazz-tools/worker";
import { co, z } from "jazz-tools";
import { registerWebhook } from "jazz-webhook";

startWorker({
  syncServer: "wss://cloud.jazz.tools/?key=webhook-example@garden.co",
}).then(async ({ worker, shutdownWorker }) => {
  const MyCoMap = co.map({
    name: z.string(),
  });

  const myCoMap = MyCoMap.create({
    name: "My CoMap",
  });

  console.log(myCoMap);

  const webhookId = await registerWebhook(
    process.env.JAZZ_WEBHOOK_REGISTRY_ID!,
    "http://localhost:4444",
    myCoMap.$jazz.id,
  );

  console.log(webhookId);

  console.log("Making a change to the CoMap");
  myCoMap.$jazz.set("name", "My CoMap Changed");
  console.log("Change made, waiting 2 seconds");
  await new Promise((resolve) => setTimeout(resolve, 2000));
  console.log("2 seconds passed, making another change");
  myCoMap.$jazz.set("name", "My CoMap Changed Again");
  console.log("Change made, exiting");
  await shutdownWorker();
});
