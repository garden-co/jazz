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

const NUM_KEYS = 10;
const NUM_UPDATES = 100;

function generateFixtures(module: typeof cojson, crypto: any) {
  const account = module.LocalNode.internalCreateAccount({
    crypto,
  });

  const group = account.core.node.createGroup();
  const map = group.createMap();

  for (let i = 0; i <= NUM_KEYS; i++) {
    for (let j = 0; j <= NUM_UPDATES; j++) {
      map.set(i.toString(), j.toString(), "private");
    }
  }

  return map;
}

const map = generateFixtures(cojson, crypto);
const napiMap = generateFixtures(cojson, napiCrypto);
// @ts-expect-error
const mapFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

const content = map.core.verified?.newContentSince(undefined) ?? [];
const contentNAPI = napiMap.core.verified?.newContentSince(undefined) ?? [];
const contentFromNpm =
  mapFromNpm.core.verified?.newContentSince(undefined) ?? [];

describe("map import", () => {
  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
  }

  bench(
    "current version",
    () => {
      importMap(map, content);
    },
    { iterations: 500 },
  );

  bench(
    "current version (NAPI)",
    () => {
      importMap(napiMap, contentNAPI);
    },
    { iterations: 500 },
  );

  bench(
    "Jazz 0.18.18",
    () => {
      importMap(mapFromNpm, contentFromNpm);
    },
    { iterations: 500 },
  );
});

describe("list import + content load", () => {
  function loadMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "current version",
    () => {
      loadMap(map, content);
    },
    { iterations: 500 },
  );

  bench(
    "current version (NAPI)",
    () => {
      loadMap(napiMap, contentNAPI);
    },
    { iterations: 500 },
  );

  bench(
    "Jazz 0.18.18",
    () => {
      loadMap(mapFromNpm, contentFromNpm);
    },
    { iterations: 500 },
  );
});

describe("map updating", () => {
  const map = generateFixtures(cojson, crypto);
  const mapNAPI = generateFixtures(cojson, napiCrypto);
  // @ts-expect-error
  const mapFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);
  bench(
    "current version",
    () => {
      map.set("A", Math.random().toString(), "private");
    },
    { iterations: 5000 },
  );

  bench(
    "current version (NAPI)",
    () => {
      mapNAPI.set("A", Math.random().toString(), "private");
    },
    { iterations: 5000 },
  );

  bench(
    "Jazz 0.18.18",
    () => {
      mapFromNpm.set("A", Math.random().toString(), "private");
    },
    { iterations: 5000 },
  );
});
