// Run before `next build`. A build is a deployment artifact, so it is checked
// as production whatever NODE_ENV the calling shell has.
import { assertBuildConfiguration, paymentProvider, readBuildConfig } from "./build-config.mjs";

const config = assertBuildConfiguration(
  readBuildConfig({ ...process.env, NODE_ENV: "production" }),
);
console.log(`Jamazon build config: ${config.origin}, ${paymentProvider(config)} payments`);
