import { describe, bench } from "vitest";

import * as cojson from "cojson";
import * as cojsonFromNpm from "cojson-latest";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as WasmCryptoLatest } from "cojson-latest/crypto/WasmCrypto";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { PureJSCrypto as PureJSCryptoLatest } from "cojson-latest/crypto/PureJSCrypto";

const PUREJS = false;

const crypto = PUREJS ? await PureJSCrypto.create() : await WasmCrypto.create();
const napiCrypto = await NapiCrypto.create();
const cryptoFromNpm = PUREJS
  ? await PureJSCryptoLatest.create()
  : await WasmCryptoLatest.create();

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
const listNAPI = generateFixtures(cojson, napiCrypto);
// @ts-expect-error
const listFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

const content = list.core.verified?.newContentSince(undefined) ?? [];
const contentNAPI = listNAPI.core.verified?.newContentSince(undefined) ?? [];
const contentFromNpm =
  listFromNpm.core.verified?.newContentSince(undefined) ?? [];

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

  bench(
    "current version (NAPI)",
    () => {
      importList(listNAPI, contentNAPI);
    },
    { iterations: 500 },
  );

  bench(
    "Jazz 0.18.18",
    () => {
      importList(listFromNpm, contentFromNpm);
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

  bench(
    "current version (NAPI)",
    () => {
      loadList(listNAPI, contentNAPI);
    },
    { iterations: 500 },
  );

  bench(
    "Jazz 0.18.18",
    () => {
      loadList(listFromNpm, contentFromNpm);
    },
    { iterations: 500 },
  );
});

describe("list updating", () => {
  const list = generateFixtures(cojson, crypto);
  const listNAPI = generateFixtures(cojson, napiCrypto);
  // @ts-expect-error
  const listFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

  bench(
    "current version",
    () => {
      list.append("A");
    },
    { iterations: 5000 },
  );

  bench(
    "current version (NAPI)",
    () => {
      listNAPI.append("A");
    },
    { iterations: 5000 },
  );

  bench(
    "Jazz 0.18.18",
    () => {
      listFromNpm.append("A");
    },
    { iterations: 5000 },
  );
});
