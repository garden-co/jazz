import { describe, bench } from "vitest";

import * as cojson from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";

const PUREJS = false;

const crypto = PUREJS ? await PureJSCrypto.create() : await WasmCrypto.create();

const NUM_ITEMS = 1000;

function generateFixtures(module: typeof cojson, crypto: any) {
  const account = module.LocalNode.internalCreateAccount({
    crypto,
  });

  const group = account.core.node.createGroup();
  const list = group.createList();

  for (let i = 0; i <= NUM_ITEMS; i++) {
    list.append("A");
  }

  for (let i = NUM_ITEMS; i > 0; i--) {
    if (i % 3 === 0) {
      list.delete(i);
    } else if (i % 3 === 1) {
      list.replace(i, "B");
    }
  }

  return list;
}

const list = generateFixtures(cojson, crypto);

const content = list.core.verified?.newContentSince(undefined) ?? [];

describe("list import", () => {
  function importList(list: any, content: any) {
    list.core.node.getCoValue(list.id).unmount();
    for (const msg of content) {
      list.core.node.syncManager.handleNewContent(msg, "storage");
    }
  }

  bench(
    "current version",
    () => {
      importList(list, content);
    },
    { iterations: 500 },
  );
});

describe("list import + content load", () => {
  function loadList(list: any, content: any) {
    list.core.node.getCoValue(list.id).unmount();
    for (const msg of content) {
      list.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = list.core.node.getCoValue(list.id);
    coValue.getCurrentContent();
  }

  bench(
    "current version",
    () => {
      loadList(list, content);
    },
    { iterations: 500 },
  );
});

describe("list updating", () => {
  const list = generateFixtures(cojson, crypto);

  bench(
    "current version",
    () => {
      list.append("A");
    },
    { iterations: 5000 },
  );
});
