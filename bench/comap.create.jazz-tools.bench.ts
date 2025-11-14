import { describe, bench } from "vitest";
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

  const ctx = await tools.createJazzContextForNewAccount({
    creationProps: {
      name: "Test Account",
    },
    peers: [],
    crypto: await wasmCrypto.create(),
  });

  return {
    Message,
    sampleReactions,
    sampleHiddenIn,
    Group: tools.Group,
    account: ctx.account,
  };
}

const schema = await createSchema(localTools, WasmCrypto);

// @ts-expect-error
const schemaLatest = await createSchema(publishedTools, WasmCryptoLatest);

const message = schema.Message.create(
  {
    content: "A".repeat(1024),
    createdAt: new Date(),
    updatedAt: new Date(),
    hiddenIn: sampleHiddenIn,
    reactions: sampleReactions,
    author: "user123",
  },
  schema.Group.create(schema.account).makePublic(),
);

schema.account.$jazz.localNode.internalDeleteCoValue(message.$jazz.raw.id);
schemaLatest.account.$jazz.localNode.internalDeleteCoValue(
  message.$jazz.raw.id,
);

describe("Message.create", () => {
  bench(
    "current version",
    () => {
      schema.Message.create(
        {
          content: "A".repeat(1024),
          createdAt: new Date(),
          updatedAt: new Date(),
          hiddenIn: sampleHiddenIn,
          reactions: sampleReactions,
          author: "user123",
        },
        schema.Group.create(schema.account),
      );
    },
    { iterations: 2000, warmupIterations: 500 },
  );

  bench(
    "Jazz 0.19.2",
    () => {
      schemaLatest.Message.create(
        {
          content: "A".repeat(1024),
          createdAt: new Date(),
          updatedAt: new Date(),
          hiddenIn: sampleHiddenIn,
          reactions: sampleReactions,
          author: "user123",
        },
        schemaLatest.Group.create(schemaLatest.account),
      );
    },
    { iterations: 2000, warmupIterations: 500 },
  );
});
