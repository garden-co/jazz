import { startAuthServer } from "./auth-server.ts";

const server = await startAuthServer({ port: 3001, dataDir: ".jazz" });

console.log(`Auth server and Jazz ready at ${server.url}`);

let stopping = false;

async function shutdown() {
  if (stopping) {
    return;
  }

  stopping = true;
  await server.stop();
}

process.on("SIGINT", () => {
  void shutdown().finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
  void shutdown().finally(() => process.exit(0));
});
