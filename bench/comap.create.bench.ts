import { Bench } from "tinybench";
import { displayBenchmarkResults } from "./utils.js";

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
const mapFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

const content = map.core.verified?.newContentSince(undefined) ?? [];
const contentNAPI = napiMap.core.verified?.newContentSince(undefined) ?? [];
const contentFromNpm =
  mapFromNpm.core.verified?.newContentSince(undefined) ?? [];

async function runMapImportBench() {
  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
  }

  console.log("\n📦 Map Import Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 500 });

  bench
    .add("current version", () => {
      importMap(map, content);
    })
    .add("current version (NAPI)", () => {
      importMap(napiMap, contentNAPI);
    })
    .add("Jazz 0.18.18", () => {
      importMap(mapFromNpm, contentFromNpm);
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

async function runMapImportAndLoadBench() {
  function loadMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  console.log("\n📦 Map Import + Content Load Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 500 });

  bench
    .add("current version", () => {
      loadMap(map, content);
    })
    .add("current version (NAPI)", () => {
      loadMap(napiMap, contentNAPI);
    })
    .add("Jazz 0.18.18", () => {
      loadMap(mapFromNpm, contentFromNpm);
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

async function runMapUpdatingBench() {
  const map = generateFixtures(cojson, crypto);
  const mapNAPI = generateFixtures(cojson, napiCrypto);
  const mapFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

  console.log("\n📝 Map Updating Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 5000 });

  bench
    .add("current version", () => {
      map.set("A", Math.random().toString(), "private");
    })
    .add("current version (NAPI)", () => {
      mapNAPI.set("A", Math.random().toString(), "private");
    })
    .add("Jazz 0.18.18", () => {
      mapFromNpm.set("A", Math.random().toString(), "private");
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

// Run all benchmarks
async function main() {
  console.log("\n╔════════════════════════════════════════════════════════╗");
  console.log("║            CoMap Benchmarks (tinybench)                ║");
  console.log("╚════════════════════════════════════════════════════════╝");

  await runMapImportBench();
  await runMapImportAndLoadBench();
  await runMapUpdatingBench();

  console.log("\n✅ All benchmarks completed!\n");
}

main().catch(console.error);
