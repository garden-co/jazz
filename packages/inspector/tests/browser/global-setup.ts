import { fetchSchemaHashes } from "jazz-tools";
import runServer from "../../scripts/dev-sync-server.js";

export default async function globalSetup(): Promise<() => Promise<void>> {
  const { serverHandle } = await runServer();

  const { hashes } = await fetchSchemaHashes(serverHandle.url, {
    adminSecret: serverHandle.adminSecret,
  });

  const publishedSchemaHash = hashes.at(-1);
  if (!publishedSchemaHash) {
    throw new Error("No schema hashes were published during inspector browser global setup.");
  }

  process.env.PUBLISHED_SCHEMA_HASH = publishedSchemaHash;

  return async () => {
    await serverHandle.stop();
  };
}
