// Run before `next build`. A build is a deployment artifact, so it is checked
// as production whatever NODE_ENV the calling shell has.
import { assertConfiguration, readConfig } from "./config.mjs";

const config = assertConfiguration(readConfig({ ...process.env, NODE_ENV: "production" }));
console.log(`BandChat build config: ${config.origin}`);
