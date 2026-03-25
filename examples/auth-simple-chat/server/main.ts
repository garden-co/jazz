import { AUTH_JWT_KID } from "../constants.js";
import { startAuthServer } from "./auth-server.js";

const server = await startAuthServer({
  port: 3001,
  jwtKid: AUTH_JWT_KID,
});

console.log(`Auth server ready at ${server.url}`);

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
