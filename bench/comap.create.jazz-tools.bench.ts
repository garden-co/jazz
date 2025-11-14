import { beforeEach, describe, bench } from "vitest";
import * as localTools from "jazz-tools";
import * as publishedTools from "jazz-tools-latest";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as WasmCryptoLatest } from "cojson-latest/crypto/WasmCrypto";

const sampleReactions = ["👍", "❤️", "😄", "🎉"];
const sampleHiddenIn = ["user1", "user2", "user3"];

// Define the schemas based on the provided Message schema
async function createSchema(
  tools: typeof localTools,
  wasmCrypto: typeof WasmCrypto,
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
  };
}

const schema = await createSchema(localTools, WasmCrypto);

// @ts-expect-error
const schemaLatest = await createSchema(publishedTools, WasmCryptoLatest);

describe("Message.create", () => {
  beforeEach(async () => {
    // @ts-expect-error
    globalThis.gc();
  });

  bench(
    "Jazz 0.19.2",
    () => {
      const group = schemaLatest.Group.create(schemaLatest.account);
      const messages = schemaLatest.Messages.create([], { owner: group });
      for (let i = 0; i < 1000; i++) {
        messages.$jazz.push(
          schemaLatest.Message.create(
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
        );
      }
    },
    { iterations: 5, warmupIterations: 2 },
  );

  bench(
    "current version",
    () => {
      const group = schema.Group.create(schema.account);
      const messages = schema.Messages.create([], { owner: group });
      for (let i = 0; i < 1000; i++) {
        messages.$jazz.push(
          schema.Message.create(
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
        );
      }
    },
    { iterations: 5, warmupIterations: 2 },
  );
});
