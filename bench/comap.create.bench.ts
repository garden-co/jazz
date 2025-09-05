import { describe, bench } from "vitest";
import * as tools from "jazz-tools";
import * as toolsLatest from "jazz-tools-latest";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as WasmCryptoLatest } from "cojson-latest/crypto/WasmCrypto";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { PureJSCrypto as PureJSCryptoLatest } from "cojson-latest/crypto/PureJSCrypto";

const sampleReactions = ["👍", "❤️", "😄", "🎉"];
const sampleHiddenIn = ["user1", "user2", "user3"];

// Define the schemas based on the provided Message schema
async function createSchema(
  tools: typeof toolsLatest,
  WasmCrypto: typeof WasmCryptoLatest,
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
    peersToLoadFrom: [],
    crypto: await WasmCrypto.create(),
  });

  return {
    Message,
    sampleReactions,
    sampleHiddenIn,
    Group: tools.Group,
    account: ctx.account,
  };
}

const PUREJS = false;

// @ts-expect-error
const schema = await createSchema(tools, PUREJS ? PureJSCrypto : WasmCrypto);
const schemaLatest = await createSchema(
  toolsLatest,
  // @ts-expect-error
  PUREJS ? PureJSCryptoLatest : WasmCryptoLatest,
);

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

const content = await tools.exportCoValue(
  schema.Message,
  message.$jazz.raw.id,
  {
    // @ts-expect-error
    loadAs: schema.account,
  },
);
tools.importContentPieces(content ?? [], schema.account as any);
toolsLatest.importContentPieces(content ?? [], schemaLatest.account);
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
    { iterations: 1000 },
  );

  bench(
    "Jazz 0.18.5",
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
    { iterations: 1000 },
  );
});

describe("Message import", () => {
  bench(
    "current version",
    () => {
      tools.importContentPieces(content ?? [], schema.account as any);
      schema.account.$jazz.localNode.internalDeleteCoValue(
        message.$jazz.raw.id,
      );
    },
    { iterations: 5000 },
  );

  bench(
    "Jazz 0.18.5",
    () => {
      toolsLatest.importContentPieces(content ?? [], schemaLatest.account);
      schemaLatest.account.$jazz.localNode.internalDeleteCoValue(
        message.$jazz.raw.id,
      );
    },
    { iterations: 5000 },
  );
});

describe("import+ decrypt", () => {
  bench(
    "current version",
    () => {
      tools.importContentPieces(content ?? [], schema.account as any);

      const node = schema.account.$jazz.localNode;

      node.expectCoValueLoaded(message.$jazz.raw.id).getCurrentContent();
      node.internalDeleteCoValue(message.$jazz.raw.id);
    },
    { iterations: 5000 },
  );

  bench(
    "Jazz 0.18.5",
    () => {
      toolsLatest.importContentPieces(content ?? [], schemaLatest.account);

      const node = schemaLatest.account.$jazz.localNode;

      node.expectCoValueLoaded(message.$jazz.raw.id).getCurrentContent();
      node.internalDeleteCoValue(message.$jazz.raw.id);
    },
    { iterations: 5000 },
  );
});
