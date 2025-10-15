import { bench, group, run, summary } from "mitata";

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

group("map import", () => {
  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
  }
  summary(() => {
    bench("current version", () => {
      importMap(map, content);
    });

    bench("current version (NAPI)", () => {
      importMap(napiMap, contentNAPI);
    });

    bench("Jazz 0.18.18", () => {
      importMap(mapFromNpm, contentFromNpm);
    });
  });
});

group("list import + content load", () => {
  function loadMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }
  summary(() => {
    bench("current version", () => {
      loadMap(map, content);
    });

    bench("current version (NAPI)", () => {
      loadMap(napiMap, contentNAPI);
    });

    bench("Jazz 0.18.18", () => {
      loadMap(mapFromNpm, contentFromNpm);
    });
  });
});

group("map updating", () => {
  summary(() => {
    bench("current version", () => {
      map.set("A", Math.random().toString(), "private");
    });

    bench("current version (NAPI)", () => {
      napiMap.set("A", Math.random().toString(), "private");
    });

    bench("Jazz 0.18.18", () => {
      mapFromNpm.set("A", Math.random().toString(), "private");
    });
  });
});

await run();
