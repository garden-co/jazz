import { bench, group, run } from "mitata";
import * as localTools from "jazz-tools";
import * as latestPublishedTools from "jazz-tools-latest";
import { WasmCrypto as LocalWasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as LatestPublishedWasmCrypto } from "cojson-latest/crypto/WasmCrypto";

const sampleReactions = ["👍", "❤️", "😄", "🎉"];
const sampleHiddenIn = ["user1", "user2", "user3"];
const MESSAGE_COUNT = 1000;

type SchemaRuntime = Awaited<ReturnType<typeof createSchema>>;

async function createSchema(
  tools: typeof localTools,
  wasmCrypto: typeof LocalWasmCrypto,
) {
  const Embed = tools.co.map({
    url: tools.z.string(),
    title: tools.z.string().optional(),
    description: tools.z.string().optional(),
    image: tools.z.string().optional(),
  });

  const Message = tools.co.map({
    content: tools.z.string(),
    createdAt: tools.z.date(),
    updatedAt: tools.z.date(),
    hiddenIn: tools.co.list(tools.z.string()),
    replyTo: tools.z.string().optional(),
    reactions: tools.co.list(tools.z.string()),
    softDeleted: tools.z.boolean().optional(),
    embeds: tools.co.optional(tools.co.list(Embed)),
    author: tools.z.string().optional(),
    threadId: tools.z.string().optional(),
  });

  const Messages = tools.co.list(Message);

  const ctx = await tools.createJazzContextForNewAccount({
    creationProps: {
      name: "Test Account",
    },
    peers: [],
    crypto: await wasmCrypto.create(),
  });

  return {
    Message,
    Messages,
    sampleReactions,
    sampleHiddenIn,
    Group: tools.Group,
    account: ctx.account,
    localNode: ctx.node,
  };
}

async function runMessageCreation(schemaDef: SchemaRuntime) {
  const group = schemaDef.Group.create(schemaDef.account);
  const messages = schemaDef.Messages.create([], { owner: group });
  const messagesToAdd = Array.from({ length: MESSAGE_COUNT }, (_, i) => i).map(
    () =>
      schemaDef.Message.create(
        schemaDef.Message.create(
          {
            content: "A".repeat(1024),
            createdAt: new Date(),
            updatedAt: new Date(),
            hiddenIn: sampleHiddenIn,
            reactions: sampleReactions,
            author: "user123",
          },
          group,
        ),
      ),
  );
  messages.$jazz.push(...messagesToAdd);
  await messages.$jazz.waitForSync();
  await schemaDef.localNode.gracefulShutdown();
}

function registerBenchmark(label: string, tools: any, wasmCrypto: any) {
  bench(function* () {
    yield {
      [0]() {
        return createSchema(tools, wasmCrypto);
      },
      async bench(schemaDef: SchemaRuntime) {
        await runMessageCreation(schemaDef);
      },
    };
  })
    .name(label)
    .gc("inner");
}

group("Message.create × 1000 entries", () => {
  // Note: Benchmark runs affect subsequent runs
  // This is minimized by:
  // - waiting for CoValues to be synced before completing a benchmark run
  // - using a fresh CoValue schema on each benchmark run
  // - running the garbage collector between runs
  // Still, some impact remains. Expect the first benchmark to be ~2% faster than the second one.
  registerBenchmark(
    "Jazz 0.19.2",
    latestPublishedTools,
    LatestPublishedWasmCrypto,
  );
  registerBenchmark("current version", localTools, LocalWasmCrypto);
});

if (!globalThis.gc) {
  console.warn(
    "Run this benchmark with NODE_OPTIONS=--expose-gc so Mitata can force GC between runs.",
  );
}

await run();
