// Seed (unless --no-seed) and serve big-label/jazz for a variant; stays up until killed.
//   node serve.mjs --variant thin|default|now [--no-seed]
import { parseArgs } from "node:util";
const { values } = parseArgs({ options: { variant: { type: "string", default: "default" }, "no-seed": { type: "boolean", default: false } } });
const app = await import("../../../../examples/big-label/comparison/harness.mjs");
const ctx = values["no-seed"] ? app.lastSeed() : await app.seed({ variant: values.variant });
console.error("seeded", JSON.stringify(ctx));
const server = await app.start({ variant: values.variant });
console.error("SERVING", server.url, values.variant);
const stop = async () => { await server.stop(); process.exit(0); };
process.on("SIGTERM", stop); process.on("SIGINT", stop);
setInterval(() => {}, 1 << 30);
