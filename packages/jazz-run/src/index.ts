#!/usr/bin/env node
/* istanbul ignore file -- @preserve */
import { Command, Options } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect } from "effect";
import { createWorkerAccount } from "./createWorkerAccount.js";
import { startSyncServer } from "./startSyncServer.js";
import { serverDefaults } from "./config.js";

const jazzTools = Command.make("jazz-tools");

const nameOption = Options.text("name").pipe(Options.withAlias("n"));
const jsonOption = Options.boolean("json");
const peerOption = Options.text("peer")
  .pipe(Options.withAlias("p"))
  .pipe(Options.withDefault("wss://cloud.jazz.tools"));

const createAccountCommand = Command.make(
  "create",
  { name: nameOption, peer: peerOption, json: jsonOption },
  ({ name, peer, json }) => {
    return Effect.gen(function* () {
      const { accountID, agentSecret } = yield* Effect.promise(() =>
        createWorkerAccount({ name, peer }),
      );

      if (json) {
        yield* Console.log(JSON.stringify({ accountID, agentSecret }));
      } else {
        yield* Console.log(`
# Credentials for Jazz account "${name}":
JAZZ_WORKER_ACCOUNT=${accountID}
JAZZ_WORKER_SECRET=${agentSecret}
`);
      }
    });
  },
);

const accountCommand = Command.make("account").pipe(
  Command.withSubcommands([createAccountCommand]),
);

const hostOption = Options.text("host")
  .pipe(Options.withAlias("h"))
  .pipe(
    Options.withDescription(
      `The host to listen on. Default is ${serverDefaults.host}`,
    ),
  )
  .pipe(Options.withDefault(serverDefaults.host));

const portOption = Options.text("port")
  .pipe(Options.withAlias("p"))
  .pipe(
    Options.withDescription(
      `Select a different port for the WebSocket server. Default is ${serverDefaults.port}`,
    ),
  )
  .pipe(Options.withDefault(serverDefaults.port.toString()));

const inMemoryOption = Options.boolean("in-memory").pipe(
  Options.withDescription("Use an in-memory storage instead of file-based."),
);

const dbOption = Options.file("db")
  .pipe(
    Options.withDescription(
      `The path to the file where to store the data. Default is '${serverDefaults.db}'`,
    ),
  )
  .pipe(Options.withDefault(serverDefaults.db));

const startSyncServerCommand = Command.make(
  "sync",
  {
    host: hostOption,
    port: portOption,
    inMemory: inMemoryOption,
    db: dbOption,
  },
  ({ host, port, inMemory, db }) => {
    return Effect.gen(function* () {
      const server = yield* Effect.promise(() =>
        startSyncServer({ host, port, inMemory, db }),
      );

      const serverAddress = server.address();

      if (!serverAddress) {
        return yield* Effect.fail(new Error("Failed to start sync server."));
      }

      const socketAddress =
        typeof serverAddress === "object"
          ? `${serverAddress.address}:${serverAddress.port}`
          : serverAddress;

      yield* Console.log(
        `COJSON sync server listening on ws://${socketAddress}`,
      );

      // Keep the server up
      yield* Effect.never;
    });
  },
);

const command = jazzTools.pipe(
  Command.withSubcommands([accountCommand, startSyncServerCommand]),
);

const cli = Command.run(command, {
  name: "Jazz CLI Tools",
  version: "v0.8.11",
});

Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(NodeContext.layer),
  NodeRuntime.runMain,
);
