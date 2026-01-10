import { seedDb } from "./seed.ts";
import { queryCoValues } from "./queryCoValues.ts";
import { runLoad } from "./runLoad.ts";
import { parseArgs } from "./utils/args.ts";

function usage(): string {
  return [
    "Usage:",
    "  pnpm run seed [-- --db ./seed.db --items 100 --pdf ./assets/sample.pdf]",
    "  pnpm run query [-- --db ./seed.db]",
    "  pnpm run run [-- --db ./seed.db --workers 8 --durationMs 60000 --inflight 4 --mix 1f:1m] [--mixMode round_robin|randomized] [--host 127.0.0.1] [--port 0]",
    "  pnpm run all [-- --db ./seed.db --items 100 --pdf ./assets/sample.pdf --workers 8 --durationMs 60000 --inflight 4 --mix 1f:1m]",
    "",
    "Notes:",
    "  - This is a runnable harness (not vitest).",
    "  - The sync server uses the seeded SQLite DB as its only persistence layer.",
    "  - Defaults match the example values shown above.",
  ].join("\n");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cmd = args._[0];

  if (!cmd || cmd === "help" || cmd === "--help" || cmd === "-h") {
    console.log(usage());
    process.exit(0);
  }

  if (cmd === "seed") {
    await seedDb(args);
    return;
  }

  if (cmd === "query") {
    await queryCoValues(args);
    return;
  }

  if (cmd === "run") {
    await runLoad(args);
    return;
  }

  if (cmd === "all") {
    await seedDb(args);
    await queryCoValues(args);
    await runLoad(args);
    return;
  }

  console.error(`Unknown command: ${cmd}\n\n${usage()}`);
  process.exit(1);
}

await main();
