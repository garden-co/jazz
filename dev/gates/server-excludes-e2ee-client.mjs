#!/usr/bin/env node
/**
 * Refuse a jazz-server build that enables jazz-db's `e2ee-client` feature.
 * Only clients seal and open encrypted cells; a server built with the
 * client's encryption hooks is a build mistake (E2EE_CORE_HOOKS.md, "Feature
 * flag and build split"). The
 * check reads `cargo tree -p jazz-server -e features -i jazz-db`, the
 * features of jazz-db the server enables when built alone, and fails if any
 * line names the feature.
 *
 * `--self-test` first checks a build that does enable it (jazz-core with
 * `e2ee-client`), so the check cannot pass by never seeing the feature.
 *
 * Usage: node dev/gates/server-excludes-e2ee-client.mjs [--self-test]
 */
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const FEATURE = "e2ee-client";

/** The lines of a `cargo tree -e features` listing that name the feature. */
export function e2eeClientLines(tree) {
  return tree.split("\n").filter((line) => line.includes(`"${FEATURE}"`));
}

/** The features of jazz-db the build of `args` enables, as `cargo tree` lists them. */
function featureTree(args) {
  return execFileSync("cargo", ["tree", "-e", "features", "-i", "jazz-db", ...args], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  });
}

function main() {
  if (process.argv.includes("--self-test")) {
    const client = e2eeClientLines(featureTree(["-p", "jazz-core", "--features", FEATURE]));
    if (client.length === 0) {
      console.error(`self-test: jazz-core with ${FEATURE} lists no ${FEATURE} feature`);
      process.exit(1);
    }
  }
  const server = e2eeClientLines(featureTree(["-p", "jazz-server"]));
  if (server.length > 0) {
    console.error(`jazz-server enables ${FEATURE}:\n${[...new Set(server)].join("\n")}`);
    process.exit(1);
  }
  console.log(`jazz-server builds without ${FEATURE}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) main();
