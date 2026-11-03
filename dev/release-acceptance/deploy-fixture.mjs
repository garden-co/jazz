#!/usr/bin/env node
// Publishes a fixture's schema.ts + permissions.ts straight to a sync server's
// /deploy route, for release acceptance runs that talk to a bare core.
//
// `jazz-tools deploy` is gone: apps publish through createJazz when their
// backend starts. These harnesses still drive a core directly, so they call
// the package's catalogue deploy themselves until they run through createJazz.
//
// Usage: deploy-fixture.mjs <jazz-tools package dir> <appId> <schemaDir> <serverUrl>
// The admin secret comes from JAZZ_ADMIN_SECRET.
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [packageDir, appId, schemaDir, serverUrl] = process.argv.slice(2);
const adminSecret = process.env.JAZZ_ADMIN_SECRET;
if (!packageDir || !appId || !schemaDir || !serverUrl || !adminSecret) {
  console.error(
    "Usage: JAZZ_ADMIN_SECRET=... deploy-fixture.mjs <jazz-tools package dir> <appId> <schemaDir> <serverUrl>",
  );
  process.exit(1);
}

const { deploy } = await import(
  pathToFileURL(join(packageDir, "dist", "dev", "catalogue-project.js")).href
);
const result = await deploy({ appId, serverUrl, adminSecret, schemaDir });
console.log(
  result.changed ? `Deployed schema ${result.schema.hash}.` : "Deployment is already up to date.",
);
