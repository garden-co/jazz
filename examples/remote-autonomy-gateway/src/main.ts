import { createRemoteAutonomyGateway } from "./app.js";
import { startRemoteAutonomyWorker } from "./worker.js";

const gateway = createRemoteAutonomyGateway();
const port = process.env.REMOTE_AUTONOMY_PORT ? Number(process.env.REMOTE_AUTONOMY_PORT) : 7474;
const hostname = process.env.REMOTE_AUTONOMY_HOST ?? "0.0.0.0";

gateway.app.listen({ hostname, port });
const worker = truthy(process.env.REMOTE_AUTONOMY_WORKER)
  ? startRemoteAutonomyWorker({
      gatewayUrl: `http://127.0.0.1:${port}`,
    })
  : null;

console.log(
  JSON.stringify(
    {
      ok: true,
      service: "remote-autonomy-gateway",
      url: `http://${hostname}:${port}`,
      worker: worker ? "enabled" : "disabled",
    },
    null,
    2,
  ),
);

const shutdown = async () => {
  worker?.stop();
  await gateway.close();
  process.exit(0);
};

process.once("SIGINT", () => {
  void shutdown();
});
process.once("SIGTERM", () => {
  void shutdown();
});

function truthy(value: string | undefined): boolean {
  return value === "1" || value === "true" || value === "yes" || value === "on";
}
