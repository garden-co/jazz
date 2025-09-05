import { describe, bench } from "vitest";
import * as tools from "jazz-tools";
import * as toolsLatest from "jazz-tools-latest";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as WasmCryptoLatest } from "cojson-latest/crypto/WasmCrypto";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { PureJSCrypto as PureJSCryptoLatest } from "cojson-latest/crypto/PureJSCrypto";

// Define the schemas based on the provided Message schema
async function createSchema(
  { co, Group, z, createJazzContextForNewAccount }: typeof tools,
  WasmCrypto: typeof WasmCryptoLatest,
) {
  const List = co.list(z.string());

  const ctx = await createJazzContextForNewAccount({
    creationProps: {
      name: "Test Account",
    },
    // @ts-expect-error
    crypto: await WasmCrypto.create(),
  });

  return {
    List,
    Group,
    account: ctx.account,
  };
}

const PUREJS = false;

// @ts-expect-error
const schema = await createSchema(tools, PUREJS ? PureJSCrypto : WasmCrypto);
const schemaLatest = await createSchema(
  toolsLatest as any,
  // @ts-expect-error
  PUREJS ? PureJSCryptoLatest : WasmCryptoLatest,
);

const list = schema.List.create(
  [],
  schema.Group.create(schema.account).makePublic(),
);

for (let i = 0; i < 1000; i++) {
  list.$jazz.push("A");
}

for (let i = 0; i < 100; i++) {
  const index = Math.floor(Math.random() * list.length);
  list.$jazz.set(index, "B");
}

list.$jazz.remove(() => Math.random() < 0.3);

const content = await tools.exportCoValue(schema.List, list.$jazz.id, {
  loadAs: schema.account,
});
tools.importContentPieces(content ?? [], schema.account as any);
toolsLatest.importContentPieces(content ?? [], schemaLatest.account as any);

describe("list loading", () => {
  bench(
    "current version",
    () => {
      const node = schema.account.$jazz.raw.core.node;

      const coValue = node.expectCoValueLoaded(list.$jazz.id as any);

      coValue.getCurrentContent();
      // @ts-expect-error
      coValue._cachedContent = undefined;
    },
    { iterations: 5000 },
  );

  bench(
    "Jazz 0.17.9",
    () => {
      // @ts-expect-error
      const node = schemaLatest.account._raw.core.node;

      const coValue = node.expectCoValueLoaded(list.$jazz.id as any);

      coValue.getCurrentContent();
      coValue._cachedContent = undefined;
    },
    { iterations: 5000 },
  );
});
