import { duration, batch } from "./scenarios/index.ts";
import { queryCoValues } from "./queryCoValues.ts";
import { parseArgs } from "./utils/args.ts";

function usage(): string {
  return [
    "Usage:",
    "",
    "  # Prepare databases (seed both scenarios with 10k items)",
    "  pnpm run prepare",
    "",
    "  # Seed individual scenarios",
    "  pnpm run seed:duration [-- --db ./duration.db --items 100 --pdf ./assets/sample.pdf]",
    "  pnpm run seed:batch [-- --db ./batch.db --maps 100 --minSize 100 --maxSize 1024]",
    "",
    "  # Push batch data to remote sync server",
    "  pnpm run push:batch [-- --db ./batch.db --peer wss://remote-server.com]",
    "",
    "  # Query seeded data",
    "  pnpm run query [-- --db ./seed.db]",
    "",
    "  # Run scenarios (local mode)",
    "  pnpm run duration [-- --db ./duration.db --workers 8 --durationMs 60000 --inflight 4 --mix 1f:1m]",
    "  pnpm run batch [-- --db ./batch.db --workers 8 --runs 5 --maps 1000]",
    "",
    "  # Run batch scenario (remote mode)",
    "  pnpm run batch [-- --peer wss://remote-server.com --config-id co_abc123 --workers 8 --runs 5]",
    "",
    "Scenarios:",
    "  duration  - Load files and comaps for a specified duration",
    "  batch     - Load X maps on N workers, run N times, calculate percentiles",
    "",
    "Seed options (duration scenario):",
    "  --items <n>         - Number of items to create (files + maps, default: 100)",
    "  --pdf <path>        - Path to PDF file for file streams (default: ./assets/sample.pdf)",
    "",
    "Seed options (batch scenario):",
    "  --maps <n>          - Number of maps to create (default: 100)",
    "  --minSize <bytes>   - Minimum payload size (default: 100)",
    "  --maxSize <bytes>   - Maximum payload size (default: 1024)",
    "",
    "Push options (batch scenario):",
    "  --db <path>         - Path to local SQLite database",
    "  --peer <url>        - Remote sync server URL (e.g., wss://remote-server.com)",
    "",
    "Run options (duration scenario):",
    "  --durationMs <ms>   - Duration to run the test (default: 60000)",
    "  --inflight <n>      - Max concurrent operations per worker (default: 4)",
    "  --mix <spec>        - Mix of files:maps e.g. 1f:1m (default: 1f:1m)",
    "  --mixMode <mode>    - round_robin or randomized (default: round_robin)",
    "",
    "Run options (batch scenario):",
    "  --runs <n>          - Number of benchmark runs (default: 5)",
    "  --maps <n>          - Limit number of maps to load per run (default: all)",
    "  --peer <url>        - Remote sync server URL (uses remote mode)",
    "  --config-id <id>    - Config ID (required for remote mode)",
    "",
    "Common options:",
    "  --db <path>         - Path to SQLite database (default: ./seed.db)",
    "  --workers <n>       - Number of worker threads (default: 8)",
    "  --host <host>       - Sync server host (default: 127.0.0.1)",
    "  --port <port>       - Sync server port (default: 4200)",
    "  --random-port       - Use a random port instead of 4200",
    "",
    "Notes:",
    "  - The seed command stores a SeedConfig CoValue containing all IDs",
    "  - The run command loads this CoValue to get the IDs before starting workers",
    "  - The sync server uses the seeded SQLite DB as its persistence layer",
    "  - Remote mode: use --peer to connect to a remote sync server (no cache clearing)",
  ].join("\n");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cmd = args._[0];

  if (!cmd || cmd === "help" || cmd === "--help" || cmd === "-h") {
    console.log(usage());
    process.exit(0);
  }

  // Seed commands
  if (cmd === "seed:duration") {
    await duration.seed(args);
    return;
  }

  if (cmd === "seed:batch") {
    await batch.seed(args);
    return;
  }

  // Push batch data to remote sync server
  if (cmd === "push:batch") {
    await batch.push(args);
    return;
  }

  // Combined prepare command - seeds both scenarios with 10k items
  if (cmd === "prepare") {
    console.log("=== Preparing duration scenario (10k items) ===");
    const durationArgs = parseArgs([
      "--db",
      "./duration.db",
      "--items",
      "10000",
      "--pdf",
      "./assets/sample.pdf",
    ]);
    await duration.seed(durationArgs);

    console.log("\n=== Preparing batch scenario (10k maps) ===");
    const batchArgs = parseArgs([
      "--db",
      "./batch.db",
      "--maps",
      "15000",
      "--minSize",
      "100",
      "--maxSize",
      "1024",
    ]);
    await batch.seed(batchArgs);

    console.log("\n=== Preparation complete ===");
    return;
  }

  // Query command
  if (cmd === "query") {
    await queryCoValues(args);
    return;
  }

  // Run commands - use scenario name directly
  if (cmd === "duration") {
    await duration.run(args);
    return;
  }

  if (cmd === "batch") {
    await batch.run(args);
    return;
  }

  console.error(`Unknown command: ${cmd}\n\n${usage()}`);
  process.exit(1);
}

await main();
