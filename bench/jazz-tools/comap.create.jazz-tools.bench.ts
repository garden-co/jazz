import cronometro from "cronometro";
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

  const Dog = tools.co.map({
    type: tools.z.literal("dog"),
    breed: tools.z.string(),
  });
  const Cat = tools.co.map({
    type: tools.z.literal("cat"),
    color: tools.z.string(),
  });
  const Pet = tools.co.discriminatedUnion("type", [Dog, Cat]);

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
    pet: Pet,
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
  for (let i = 0; i < 500; i++) {
    schemaDef.Message.create({
      content: "A".repeat(1024),
      createdAt: new Date(),
      updatedAt: new Date(),
      hiddenIn: sampleHiddenIn,
      reactions: sampleReactions,
      author: "user123",
      pet: {
        type: "dog",
        breed: "Labrador",
      },
    });
  }
}

let schemaDef: SchemaRuntime;
await cronometro(
  {
    "CoMap.create - jazz-tools@latest": {
      async before() {
        schemaDef = await createSchema(
          // @ts-expect-error
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
      },
      async test() {
        await runMessageCreation(schemaDef);
      },
      async after() {
        await schemaDef.localNode.gracefulShutdown();
      },
    },
    "CoMap.create - jazz-tools@workspace": {
      async before() {
        schemaDef = await createSchema(localTools, LocalWasmCrypto);
      },
      async test() {
        await runMessageCreation(schemaDef);
      },
      async after() {
        await schemaDef.localNode.gracefulShutdown();
      },
    },
  },
  {
    iterations: 20,
    warmup: true,
    print: {
      colors: true,
      compare: true,
    },
    onTestError: (testName, error) => {
      console.error(`\n❌ Error in test "${testName}":`);
      console.error(error);
    },
  },
);
