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
const listFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

const content = list.core.verified?.newContentSince(undefined) ?? [];
const contentNAPI = listNAPI.core.verified?.newContentSince(undefined) ?? [];
const contentFromNpm =
  listFromNpm.core.verified?.newContentSince(undefined) ?? [];

async function runListImportBench() {
  function importList(list: any, content: any) {
    list.core.node.getCoValue(list.id).unmount();
    for (const msg of content) {
      list.core.node.syncManager.handleNewContent(msg, "storage");
    }
  }

  console.log("\n📦 List Import Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 500 });

  bench
    .add("current version", () => {
      importList(list, content);
    })
    .add("current version (NAPI)", () => {
      importList(listNAPI, contentNAPI);
    })
    .add("Jazz 0.18.18", () => {
      importList(listFromNpm, contentFromNpm);
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

async function runListImportAndLoadBench() {
  function loadList(list: any, content: any) {
    list.core.node.getCoValue(list.id).unmount();
    for (const msg of content) {
      list.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = list.core.node.getCoValue(list.id);
    coValue.getCurrentContent();
  }

  console.log("\n📦 List Import + Content Load Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 500 });

  bench
    .add("current version", () => {
      loadList(list, content);
    })
    .add("current version (NAPI)", () => {
      loadList(listNAPI, contentNAPI);
    })
    .add("Jazz 0.18.18", () => {
      loadList(listFromNpm, contentFromNpm);
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

async function runListUpdatingBench() {
  const list = generateFixtures(cojson, crypto);
  const listNAPI = generateFixtures(cojson, napiCrypto);
  const listFromNpm = generateFixtures(cojsonFromNpm, cryptoFromNpm);

  console.log("\n📝 List Updating Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 5000 });

  bench
    .add("current version", () => {
      list.append("A");
    })
    .add("current version (NAPI)", () => {
      listNAPI.append("A");
    })
    .add("Jazz 0.18.18", () => {
      listFromNpm.append("A");
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

// Run all benchmarks
async function main() {
  console.log("\n╔════════════════════════════════════════════════════════╗");
  console.log("║            CoList Benchmarks (tinybench)               ║");
  console.log("╚════════════════════════════════════════════════════════╝");

  await runListImportBench();
  await runListImportAndLoadBench();
  await runListUpdatingBench();

  console.log("\n✅ All benchmarks completed!\n");
}

main().catch(console.error);
