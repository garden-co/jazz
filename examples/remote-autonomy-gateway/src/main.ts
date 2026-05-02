import { createRemoteAutonomyGateway } from "./app.js";

const gateway = createRemoteAutonomyGateway();
const port = process.env.REMOTE_AUTONOMY_PORT
  ? Number(process.env.REMOTE_AUTONOMY_PORT)
  : 7474;
const hostname = process.env.REMOTE_AUTONOMY_HOST ?? "0.0.0.0";

gateway.app.listen({ hostname, port });

console.log(
  JSON.stringify(
    {
      ok: true,
      service: "remote-autonomy-gateway",
      url: `http://${hostname}:${port}`,
    },
    null,
    2,
  ),
);

const shutdown = async () => {
  await gateway.close();
  process.exit(0);
};

process.once("SIGINT", () => {
  void shutdown();
});
process.once("SIGTERM", () => {
  void shutdown();
});
